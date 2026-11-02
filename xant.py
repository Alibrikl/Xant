import os
import re
import json
import time
import asyncio
import logging
import mimetypes
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import (
    FileReferenceExpiredError,
    FloodWaitError,
    MessageNotModifiedError,
)
from telethon.tl import types
from telethon.tl.types import (
    DocumentAttributeFilename,
    DocumentAttributeAudio,
    DocumentAttributeVideo,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ENV_FILE = os.path.join(BASE_DIR, ".env")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
QUEUE_DIR = os.path.join(DOWNLOAD_DIR, "queues")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(QUEUE_DIR, exist_ok=True)

LIMIT_FREE = 2 * 1024 ** 3
LIMIT_PREMIUM = 4 * 1024 ** 3

CONCURRENT_DOWNLOADS = 3
CLEANUP_DELAY = 10

REORDER_AFTER_EACH_FILE = True
REORDER_DEBOUNCE = 3.0

VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v", ".3gp"}
AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".flac", ".wav", ".ogg", ".opus"}

if not os.path.exists(ENV_FILE):
    print("==================================================")
    print("🚀 Добро пожаловать в Xant")
    print("Получите свои учетные данные API https://my.telegram.org")
    print("==================================================\n")
    api_id_input = input("Введите свой TELEGRAM_API_ID: ").strip()
    api_hash_input = input("Введите свой TELEGRAM_API_HASH: ").strip()
    with open(ENV_FILE, "w", encoding="utf-8") as f:
        f.write(f"TELEGRAM_API_ID={api_id_input}\n")
        f.write(f"TELEGRAM_API_HASH={api_hash_input}\n")
    print("\n✅ Конфигурация сохранена в файле .env!\n")

load_dotenv()

try:
    API_ID = int(os.getenv("TELEGRAM_API_ID"))
    API_HASH = os.getenv("TELEGRAM_API_HASH")
except (TypeError, ValueError):
    print("❌ Ошибка: Неверный TELEGRAM_API_ID в файле .env.")
    exit(1)

client = TelegramClient(
    os.path.join(BASE_DIR, "user_session"), API_ID, API_HASH,
    connection_retries=10, retry_delay=3, timeout=60,
)

LINK_PATTERN = r"https://t\.me/(?:c/(\d+)|([\w_]+))/(\d+)"

link_queue: asyncio.Queue = asyncio.Queue()
processed: set[tuple] = set()
cancel_flag = {"cancel": False}
in_progress = {"count": 0}

batches: dict[int, dict] = {}
batch_counter = {"id": 0}

upload_lock = asyncio.Lock()
account_state = {"is_premium": False}

queues: dict[int, dict] = {}

shutdown_event = asyncio.Event()

_pending_deletions: set[asyncio.Task] = set()

# ---------- Отложенное удаление ----------
async def _delete_later(*messages, delay: float = CLEANUP_DELAY):
    if delay > 0:
        await asyncio.sleep(delay)
    for m in messages:
        if m is None:
            continue
        while True:
            try:
                await m.delete()
                break
            except FloodWaitError as fwe:
                print(f"\n[FloodWait] Пауза {fwe.seconds} сек. перед удалением...")
                await asyncio.sleep(fwe.seconds)
            except Exception:
                break


def _schedule_delete(*messages, delay: float = CLEANUP_DELAY):
    task = asyncio.create_task(_delete_later(*messages, delay=delay))
    _pending_deletions.add(task)
    task.add_done_callback(_pending_deletions.discard)
    return task

_LOG_PREFIX = "[reconcile]"


def _log(msg: str):
    print(f"{_LOG_PREFIX} {msg}")


def _queue_snapshot(q: dict) -> str:
    """Краткая сводка по статусам элементов очереди."""
    counts = {}
    for it in q.get("items", []):
        s = it.get("status", "?")
        counts[s] = counts.get(s, 0) + 1
    if not counts:
        return "пусто"
    order = ("pending", "downloading", "downloaded", "uploading",
             "done", "fail", "skip")
    parts = [f"{k}={counts[k]}" for k in order if k in counts]
    for k in counts:
        if k not in order:
            parts.append(f"{k}={counts[k]}")
    return ", ".join(parts)


_INVALID_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_MULTI_UNDERSCORE = re.compile(r'_+')
_MAX_NAME_BYTES = 230


def _sanitize(name: str) -> str:
    name = _INVALID_CHARS.sub("_", name)
    name = _MULTI_UNDERSCORE.sub("_", name)
    return name.strip(" ._")


def _truncate_to_bytes(s: str, max_bytes: int) -> str:
    encoded = s.encode("utf-8")
    if len(encoded) <= max_bytes:
        return s
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _detect_extension(message) -> str:
    mime = getattr(message.file, "mime_type", None) if message.file else None
    if mime:
        ext = mimetypes.guess_extension(mime.split(";")[0].strip())
        if ext:
            return ".jpg" if ext == ".jpe" else ext
    if isinstance(message.media, types.MessageMediaPhoto):
        return ".jpg"
    if isinstance(message.media, types.MessageMediaDocument):
        doc = message.media.document
        if isinstance(doc, types.Document):
            # 1. Явное имя файла — самый надёжный источник расширения
            for attr in doc.attributes:
                if isinstance(attr, types.DocumentAttributeFilename):
                    _, ext = os.path.splitext(attr.file_name or "")
                    if ext:
                        return ext.lower()

            # 2. Фолбэк по типу вложения
            for attr in doc.attributes:
                if isinstance(attr, types.DocumentAttributeVideo):
                    return ".mp4"
                if isinstance(attr, types.DocumentAttributeAudio):
                    return ".ogg" if getattr(attr, "voice", False) else ".mp3"
                if isinstance(attr, types.DocumentAttributeSticker):
                    return ".webp"
                if isinstance(attr, types.DocumentAttributeAnimated):
                    return ".mp4"
    return ".bin"


def _detect_kind(message) -> str:
    if isinstance(message.media, types.MessageMediaPhoto):
        return "photo"
    if isinstance(message.media, types.MessageMediaDocument):
        doc = message.media.document
        if isinstance(doc, types.Document):
            for attr in doc.attributes:
                if isinstance(attr, types.DocumentAttributeVideo):
                    return "video"
                if isinstance(attr, types.DocumentAttributeAudio):
                    return "voice" if getattr(attr, "voice", False) else "audio"
                if isinstance(attr, types.DocumentAttributeSticker):
                    return "sticker"
                if isinstance(attr, types.DocumentAttributeAnimated):
                    return "animation"
    return "file"


def build_filename(message) -> str:
    original = message.file.name if message.file and message.file.name else None
    if original:
        base, ext = os.path.splitext(original)
        base = _sanitize(base) or "file"
        ext = _sanitize(ext.lstrip("."))
        ext = f".{ext}" if ext else ""
    else:
        base = f"{_detect_kind(message)}_{message.id}"
        ext = _detect_extension(message)
    if not ext:
        ext = _detect_extension(message) or ".bin"
    filename = f"{base}_{message.id}{ext}"
    filename = _truncate_to_bytes(filename, _MAX_NAME_BYTES)
    return filename


def unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    reserve = 6
    trimmed = _truncate_to_bytes(base, max(1, _MAX_NAME_BYTES - reserve))
    i = 1
    while os.path.exists(f"{trimmed} ({i}){ext}"):
        i += 1
    return f"{trimmed} ({i}){ext}"


def extract_all_links(text: str):
    results = []
    for match in re.finditer(LINK_PATTERN, text):
        channel_part, username, message_id = match.groups()
        chat_id = int(f"-100{channel_part}") if channel_part else username
        results.append((chat_id, int(message_id), match.group(0)))
    return results


def human_size(num: float) -> str:
    if num < 1024 ** 3:
        return f"{num / (1024 ** 2):.2f} MB"
    for unit in ("GB", "TB", "PB"):
        num /= 1024 ** 3
        if num < 1024:
            return f"{num:.2f} {unit}"
    return f"{num:.2f} PB"


def _make_fake_event():
    class _FakeEvent:
        async def respond(self, text):
            return await client.send_message("me", text)
    return _FakeEvent()

# ---------- JSON-очередь ----------
def _queue_path(batch_id: int) -> str:
    return os.path.join(QUEUE_DIR, f"queue_{batch_id}.json")


def _save_queue(batch_id: int):
    q = queues.get(batch_id)
    if q is None:
        return
    tmp = q["path"] + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(q, f, ensure_ascii=False, indent=2)
    os.replace(tmp, q["path"])


def _load_queue(batch_id: int) -> dict | None:
    path = _queue_path(batch_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _create_queue(batch_id: int, items: list[dict]):
    q = {"batch_id": batch_id, "path": _queue_path(batch_id), "items": items}
    queues[batch_id] = q
    _save_queue(batch_id)


def _find_item(batch_id: int, chat_id, msg_id) -> dict | None:
    q = queues.get(batch_id)
    if q is None:
        return None
    for it in q["items"]:
        if it["chat_id"] == chat_id and it["msg_id"] == msg_id:
            return it
    return None


def _mark_item(batch_id: int, chat_id, msg_id, **fields):
    it = _find_item(batch_id, chat_id, msg_id)
    if it is None:
        return
    it.update(fields)
    _save_queue(batch_id)


def _list_queue_files() -> list[int]:
    ids = []
    if not os.path.isdir(QUEUE_DIR):
        return ids
    for name in os.listdir(QUEUE_DIR):
        m = re.fullmatch(r"queue_(\d+)\.json", name)
        if m:
            ids.append(int(m.group(1)))
    return sorted(ids)


# ---------- Скачивание ----------
async def download_with_retry(
    chat_id, msg_id, progress_callback,
    size_holder: dict | None = None, max_retries: int = 3,
):
    target_msg = await client.get_messages(chat_id, ids=msg_id)
    if not target_msg or not target_msg.media:
        return None

    if size_holder is not None:
        size_holder["size"] = target_msg.file.size if target_msg.file else 0

    suggested_filename = build_filename(target_msg)
    local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))

    attempt = 0
    while True:
        try:
            return await client.download_media(
                target_msg, file=local_path, progress_callback=progress_callback,
            )
        except FileReferenceExpiredError:
            attempt += 1
            print(f"\n[Внимание] Ссылка устарела (попытка {attempt}). Перезапрашиваем...")
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            if attempt >= max_retries:
                raise
            target_msg = await client.get_messages(chat_id, ids=msg_id, use_cache=False)
            if not target_msg or not target_msg.media:
                return None
            suggested_filename = build_filename(target_msg)
            local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))
        except FloodWaitError as fwe:
            print(f"\n[FloodWait] Ждём {fwe.seconds} сек...")
            await asyncio.sleep(fwe.seconds)
        except Exception as e:
            attempt += 1
            print(
                f"[download_with_retry] {type(e).__name__} "
                f"(попытка {attempt}/{max_retries}): {e}"
            )
            if attempt >= max_retries:
                raise e
            await asyncio.sleep(2 * attempt)


# ---------- Метаданные ----------
def _to_seconds(value) -> int:
    if value is None:
        return 0
    if hasattr(value, "total_seconds"):
        return int(value.total_seconds())
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _video_meta_sync(path: str):
    try:
        from hachoir.metadata import extractMetadata
        from hachoir.parser import createParser
        parser = createParser(path)
        if not parser:
            return 0, 0, 0
        meta = extractMetadata(parser)
        if not meta:
            return 0, 0, 0
        d = _to_seconds(meta.get("duration"))
        w = int(meta.get("width") or 0)
        h = int(meta.get("height") or 0)
        return d, w, h
    except Exception:
        return 0, 0, 0


def _audio_meta_sync(path: str) -> int:
    try:
        from hachoir.metadata import extractMetadata
        from hachoir.parser import createParser
        parser = createParser(path)
        if not parser:
            return 0
        meta = extractMetadata(parser)
        if not meta:
            return 0
        return _to_seconds(meta.get("duration"))
    except Exception:
        return 0

# ---------- Отправка медиа ----------
def _get_send_kind(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext in VIDEO_EXTS:
        return "video"
    if ext in AUDIO_EXTS:
        return "audio"
    return "document"


async def _send_media(chat, path: str, display_name: str, progress_cb=None):
    kind = _get_send_kind(path)
    name_attr = DocumentAttributeFilename(display_name)

    if kind == "video":
        d, w, h = await asyncio.to_thread(_video_meta_sync, path)
        if w > 0 and h > 0:
            return await client.send_file(
                chat, path,
                attributes=[
                    DocumentAttributeVideo(
                        duration=d, w=w, h=h, supports_streaming=True,
                    ),
                    name_attr,
                ],
                progress_callback=progress_cb,
            )
        return await client.send_file(
            chat, path,
            force_document=True,
            attributes=[name_attr],
            progress_callback=progress_cb,
        )
    elif kind == "audio":
        d = await asyncio.to_thread(_audio_meta_sync, path)
        return await client.send_file(
            chat, path,
            attributes=[DocumentAttributeAudio(duration=d), name_attr],
            progress_callback=progress_cb,
        )
    else:
        return await client.send_file(
            chat, path,
            force_document=True,
            attributes=[name_attr],
            progress_callback=progress_cb,
        )


# ---------- Статусы ----------
def _render_status(st: dict, full_link: str) -> str:
    if st["status"] == "done":
        return f"✅ {full_link}"
    if st["status"] == "fail":
        return f"❌ {full_link}"
    if st["status"] == "skip":
        return f"⚠️ {full_link} (пропущено)"
    if st["status"] == "downloaded":
        return f"⏳ ожидание... {full_link}"
    if st["status"] == "uploading":
        cur = st.get("up_current", 0)
        tot = st.get("up_total", 0)
        pct = st.get("up_percent", 0.0)
        return (
            f"📤 {pct:.1f}% "
            f"({human_size(cur)} / {human_size(tot)}) "
            f"{full_link}"
        )
    if st["status"] == "downloading":
        cur = st.get("current", 0)
        tot = st.get("total", 0)
        return (
            f"⬇️ {st['percent']:.1f}% "
            f"({human_size(cur)} / {human_size(tot)}) "
            f"{full_link}"
        )
    return f"📋 {full_link}"


async def _update_status_msg(batch: dict, key: tuple, full_link: str, force: bool = False):
    st = batch["link_states"].get(key)
    status_msg = batch["status_msgs"].get(key)
    if st is None or status_msg is None:
        return
    async with batch["lock"]:
        now = time.time()
        if not force and now - st.get("last_edit", 0) < 3:
            return
        st["last_edit"] = now
        try:
            await status_msg.edit(_render_status(st, full_link))
        except MessageNotModifiedError:
            pass
        except FloodWaitError as fwe:
            await asyncio.sleep(fwe.seconds)
        except Exception:
            pass

# ---------- Реконсиляция ----------
async def reconcile_batch(batch_id: int, force: bool = False):
    q = queues.get(batch_id)
    batch = batches.get(batch_id)
    if q is None or batch is None:
        return

    if not force and not REORDER_AFTER_EACH_FILE:
        if not all(it["status"] in ("done", "fail", "skip") for it in q["items"]):
            return

    async with batch["reorder_lock"]:
        # 1. Собираем id всех текущих слотов (без дублей)
        to_delete_ids: list[int] = []
        for it in q["items"]:
            for k in ("status_msg_id", "slot_msg_id"):
                mid = it.get(k)
                if mid:
                    to_delete_ids.append(mid)
        to_delete_ids = list(dict.fromkeys(to_delete_ids))

        _log(
            f"batch #{batch_id}: старт reconcile | "
            f"items={len(q['items'])}, к удалению={len(to_delete_ids)}"
        )

        # 2. Удаляем пачками, запоминаем успешно удалённые
        deleted_ok: set[int] = set()
        if to_delete_ids:
            for i in range(0, len(to_delete_ids), 100):
                chunk = to_delete_ids[i:i + 100]
                try:
                    await client.delete_messages("me", chunk)
                    deleted_ok.update(chunk)
                    _log(
                        f"  🗑  удалено пачкой: {len(chunk)} шт. "
                        f"(ids: {chunk[:10]}{'…' if len(chunk) > 10 else ''})"
                    )
                except FloodWaitError as fwe:
                    _log(
                        f"  ⏳ FloodWait {fwe.seconds}s при удалении пачки "
                        f"({len(chunk)} шт.)"
                    )
                    await asyncio.sleep(fwe.seconds)
                    try:
                        await client.delete_messages("me", chunk)
                        deleted_ok.update(chunk)
                        _log(f"  🗑  удалено после FloodWait: {len(chunk)} шт.")
                    except Exception as e:
                        _log(
                            f"  ❌ повторное удаление не удалось "
                            f"({len(chunk)} шт.): {e}"
                        )
                except Exception as e:
                    _log(f"  ❌ ошибка удаления пачки ({len(chunk)} шт.): {e}")

        failed_delete = set(to_delete_ids) - deleted_ok
        if failed_delete:
            _log(f"  ⚠️  не удалось удалить: {sorted(failed_delete)}")

        # 3. Обнуляем id только у успешно удалённых
        for it in q["items"]:
            for k in ("status_msg_id", "slot_msg_id"):
                mid = it.get(k)
                if mid and mid in deleted_ok:
                    it[k] = None
        _save_queue(batch_id)

        # 4. Публикуем заново по порядку
        new_status_msgs: dict[tuple, any] = {}
        for idx, it in enumerate(q["items"]):
            key = (it["chat_id"], it["msg_id"])
            full_link = it["link"]
            status = it["status"]
            tag = f"item #{idx} {full_link} ({status})"

            if status == "done" and it.get("file_message_id"):
                # Слот не удалён — НЕ пересылаем, иначе будет дубликат.
                if it.get("slot_msg_id"):
                    _log(
                        f"  ⏭  ПРОПУСК пересылки {tag}: "
                        f"старый слот {it['slot_msg_id']} не удалён "
                        f"(иначе будет дубликат)"
                    )
                    continue
                try:
                    fwd = await client.forward_messages(
                        "me", it["file_message_id"], from_peer="me",
                    )
                    slot_id = fwd[0].id if isinstance(fwd, list) else fwd.id
                    _log(f"  ➕ переслан файл {tag}: новый слот {slot_id}")
                except FloodWaitError as fwe:
                    _log(f"  ⏳ FloodWait {fwe.seconds}s при пересылке {tag}")
                    await asyncio.sleep(fwe.seconds)
                    try:
                        fwd = await client.forward_messages(
                            "me", it["file_message_id"], from_peer="me",
                        )
                        slot_id = fwd[0].id if isinstance(fwd, list) else fwd.id
                        _log(
                            f"  ➕ переслан файл после FloodWait {tag}: "
                            f"слот {slot_id}"
                        )
                    except Exception as e:
                        _log(f"  ❌ пересылка не удалась {tag}: {e}")
                        msg = await client.send_message("me", f"✅ {full_link}")
                        slot_id = msg.id
                        _log(
                            f"  ➕ создан текстовый плейсхолдер {tag}: "
                            f"слот {slot_id}"
                        )
                except Exception as e:
                    _log(f"  ❌ пересылка не удалась {tag}: {e}")
                    msg = await client.send_message("me", f"✅ {full_link}")
                    slot_id = msg.id
                    _log(
                        f"  ➕ создан текстовый плейсхолдер {tag}: "
                        f"слот {slot_id}"
                    )

                it["slot_msg_id"] = slot_id
                it["status_msg_id"] = None
                _save_queue(batch_id)
            else:
                if status == "fail":
                    text = f"❌ {full_link}"
                elif status == "skip":
                    text = f"⚠️ {full_link} (пропущено)"
                elif status == "done":
                    text = f"✅ {full_link}"
                elif status == "downloaded":
                    text = f"⏳ {full_link} (ожидание...)"
                elif status == "uploading":
                    text = f"📤 {full_link} (отправляю...)"
                else:
                    text = f"📋 {full_link}"
                try:
                    msg = await client.send_message("me", text)
                except FloodWaitError as fwe:
                    _log(
                        f"  ⏳ FloodWait {fwe.seconds}s при создании "
                        f"статуса {tag}"
                    )
                    await asyncio.sleep(fwe.seconds)
                    msg = await client.send_message("me", text)
                it["slot_msg_id"] = msg.id
                if status in ("pending", "downloading", "downloaded", "uploading"):
                    it["status_msg_id"] = msg.id
                    new_status_msgs[key] = msg
                    _log(f"  ➕ создан статус {tag}: слот/статус {msg.id}")
                else:
                    it["status_msg_id"] = None
                    _log(f"  ➕ создан слот {tag}: {msg.id}")
                _save_queue(batch_id)

        # 5. Обновляем in-memory
        batch["status_msgs"] = new_status_msgs
        for it in q["items"]:
            key = (it["chat_id"], it["msg_id"])
            st = batch["link_states"].get(key)
            if st is None:
                continue
            if it["status"] in ("done", "fail", "skip"):
                st["status"] = it["status"]
            elif it["status"] == "downloaded":
                st["status"] = "downloaded"
                st["up_current"] = 0
                st["up_total"] = 0
                st["up_percent"] = 0.0
                st["last_up_cb"] = 0.0
            else:
                st["last_edit"] = 0.0
                st["last_cb"] = 0.0

        # Снимок in-memory состояний (link_states)
        inmem_counts: dict[str, int] = {}
        for st in batch["link_states"].values():
            s = st.get("status", "?")
            inmem_counts[s] = inmem_counts.get(s, 0) + 1
        inmem_str = ", ".join(
            f"{k}={inmem_counts[k]}"
            for k in ("pending", "downloading", "downloaded",
                      "uploading", "done", "fail", "skip")
            if k in inmem_counts
        ) or "пусто"

        _log(
            f"batch #{batch_id}: конец reconcile | "
            f"удалено={len(deleted_ok)}, "
            f"создано={sum(1 for it in q['items'] if it.get('slot_msg_id'))}, "
            f"активных статусов={len(new_status_msgs)}"
        )
        _log(f"  📋 очередь (json):  {_queue_snapshot(q)}")
        _log(f"  🧠 состояния (in-memory): {inmem_str}")

async def request_reconcile(batch_id: int, force: bool = False):
    batch = batches.get(batch_id)
    if batch is None:
        return

    state = batch.setdefault("reorder_state", {
        "running": False, "pending": False, "last": 0.0,
    })
    state["pending"] = True

    if state["running"]:
        return

    while True:
        now = time.time()
        wait = max(0.0, REORDER_DEBOUNCE - (now - state["last"]))
        if wait > 0:
            await asyncio.sleep(wait)

        state["pending"] = False
        state["running"] = True
        state["last"] = time.time()
        try:
            await reconcile_batch(batch_id, force=force)
        except Exception as e:
            print(f"[request_reconcile] Ошибка реконсиляции пачки {batch_id}: {e}")
        finally:
            state["running"] = False

        if not state["pending"]:
            break


# ---------- Обработка одной ссылки ----------
async def process_link(chat_id, msg_id, full_link, batch_id):
    batch = batches.get(batch_id)
    if batch is None:
        return "fail"

    key = (chat_id, msg_id)
    if key in processed:
        return "skip"
    processed.add(key)

    st = batch["link_states"].get(key)
    if st is None:
        processed.discard(key)
        return "fail"

    item = _find_item(batch_id, chat_id, msg_id)
    if item is None:
        processed.discard(key)
        return "fail"

    filename = None
    size_holder = {"size": 0}

    _log(
        f"▶ начинаю {full_link} "
        f"(chat_id={chat_id}, msg_id={msg_id}, batch=#{batch_id})"
    )

    async def download_cb(current, total):
        now = time.time()
        if now - st.get("last_cb", 0) > 3 or current == total:
            st["last_cb"] = now
            st["current"] = current
            st["total"] = total
            st["percent"] = (current / total) * 100 if total else 0
            st["status"] = "downloading"
            if key in batch["status_msgs"]:
                await _update_status_msg(batch, key, full_link)

    try:
        already = item.get("local_file")
        if already and os.path.exists(os.path.join(DOWNLOAD_DIR, already)):
            filename = os.path.join(DOWNLOAD_DIR, already)
            _log(f"📂 файл уже скачан ранее: {filename}")
            st["status"] = "downloaded"
            _mark_item(batch_id, chat_id, msg_id, status="downloaded")
            await _update_status_msg(batch, key, full_link, force=True)
        else:
            filename = await download_with_retry(
                chat_id, msg_id, download_cb, size_holder=size_holder,
            )
            if not filename:
                st["status"] = "fail"
                _mark_item(batch_id, chat_id, msg_id, status="fail")
                await _update_status_msg(batch, key, full_link, force=True)
                processed.discard(key)
                _log(f"❌ не удалось скачать {full_link}")
                if REORDER_AFTER_EACH_FILE:
                    await request_reconcile(batch_id, force=False)
                return "fail"

            limit = LIMIT_PREMIUM if account_state["is_premium"] else LIMIT_FREE
            if size_holder["size"] > limit:
                _log(
                    f"⚠️  файл {full_link} превышает лимит "
                    f"({human_size(limit)}) — пропуск."
                )
                st["status"] = "fail"
                _mark_item(batch_id, chat_id, msg_id, status="fail")
                await _update_status_msg(batch, key, full_link, force=True)
                processed.discard(key)
                try:
                    os.remove(filename)
                except Exception:
                    pass
                if REORDER_AFTER_EACH_FILE:
                    await request_reconcile(batch_id, force=False)
                return "fail"

            local_name = os.path.basename(filename)
            _mark_item(
                batch_id, chat_id, msg_id,
                status="downloaded",
                local_file=local_name,
                downloaded_at=time.time(),
            )
            st["status"] = "downloaded"
            _log(f"⬇️  скачан {full_link} → {local_name}")
            await _update_status_msg(batch, key, full_link, force=True)

        # ---------- Отправка в me ----------
        display_name = os.path.basename(filename)
        file_size = size_holder.get("size") or (
            os.path.getsize(filename) if os.path.exists(filename) else 0
        )
        st["up_current"] = 0
        st["up_total"] = file_size
        st["up_percent"] = 0.0
        st["last_up_cb"] = 0.0

        async def upload_cb(current, total):
            now = time.time()
            if now - st.get("last_up_cb", 0) > 3 or current == total:
                st["last_up_cb"] = now
                st["up_current"] = current
                st["up_total"] = total
                st["up_percent"] = (current / total) * 100 if total else 0
                st["status"] = "uploading"
                _mark_item(batch_id, chat_id, msg_id, status="uploading")
                if key in batch["status_msgs"]:
                    await _update_status_msg(batch, key, full_link)

        async with upload_lock:
            sent = await _send_media("me", filename, display_name, upload_cb)

        sent_msg = sent[0] if isinstance(sent, list) else sent
        sent_id = getattr(sent_msg, "id", None)

        _log(
            f"✅ файл загружен в «Избранное»: {full_link} "
            f"→ сообщение id={sent_id}, "
            f"размер={human_size(file_size)}"
        )

        _mark_item(
            batch_id, chat_id, msg_id,
            status="done",
            file_message_id=sent_id,
            local_file=None,
            downloaded_at=None,
        )
        st["status"] = "done"

        status_msg = batch["status_msgs"].pop(key, None)
        if status_msg is not None:
            old_id = status_msg.id
            try:
                await status_msg.delete()
                _log(
                    f"🗑  удалён статус загрузки {full_link}: "
                    f"сообщение id={old_id}"
                )
            except Exception as e:
                _log(
                    f"⚠️  не удалось удалить статус {full_link} "
                    f"(id={old_id}): {e}"
                )

        if os.path.exists(filename):
            os.remove(filename)
            _log(f"🧹 удалён локальный файл: {os.path.basename(filename)}")

        if REORDER_AFTER_EACH_FILE:
            await request_reconcile(batch_id, force=False)
        return "ok"

    except Exception as e:
        _log(f"❌ {full_link}: {type(e).__name__}: {e}")
        if filename and os.path.exists(filename):
            local_name = os.path.basename(filename)
            _mark_item(
                batch_id, chat_id, msg_id,
                status="downloaded",
                local_file=local_name,
                downloaded_at=time.time(),
            )
        else:
            _mark_item(batch_id, chat_id, msg_id, status="fail")
        st["status"] = "fail"
        await _update_status_msg(batch, key, full_link, force=True)
        processed.discard(key)
        if REORDER_AFTER_EACH_FILE:
            await request_reconcile(batch_id, force=False)
        return "fail"

# ---------- Воркер ----------
async def link_worker(worker_id: int):
    while True:
        try:
            chat_id, msg_id, full_link, batch_id = await link_queue.get()
        except asyncio.CancelledError:
            return

        in_progress["count"] += 1

        try:
            if cancel_flag["cancel"]:
                result = "skip"
                _mark_item(batch_id, chat_id, msg_id, status="skip")
                processed.add((chat_id, msg_id))
                batch = batches.get(batch_id)
                if batch is not None:
                    st = batch["link_states"].get((chat_id, msg_id))
                    if st is not None:
                        st["status"] = "skip"
                        await _update_status_msg(
                            batch, (chat_id, msg_id), full_link, force=True
                        )
                _log(f"⏭  worker #{worker_id}: пропуск {full_link} (cancel)")
            else:
                result = await process_link(chat_id, msg_id, full_link, batch_id)

            batch = batches.get(batch_id)
            if batch is not None:
                batch[result] += 1
                _log(
                    f"worker #{worker_id}: {result} | "
                    f"batch #{batch_id}: ok={batch['ok']}, "
                    f"skip={batch['skip']}, fail={batch['fail']}, "
                    f"total={batch['total']}"
                )
                await _maybe_finish_batch(batch_id)

        except Exception as e:
            _log(f"worker #{worker_id}: необработанная ошибка: {e}")
            _mark_item(batch_id, chat_id, msg_id, status="fail")
            batch = batches.get(batch_id)
            if batch is not None:
                batch["fail"] += 1
                await _maybe_finish_batch(batch_id)

        finally:
            in_progress["count"] -= 1

async def _maybe_finish_batch(batch_id: int):
    b = batches.get(batch_id)
    if b is None:
        return
    done = b["ok"] + b["skip"] + b["fail"]
    if done < b["total"]:
        return
    if b.get("finished"):
        return
    b["finished"] = True

    _log(
        f"batch #{batch_id}: завершение | "
        f"ok={b['ok']}, skip={b['skip']}, fail={b['fail']}, "
        f"total={b['total']}"
    )

    try:
        await request_reconcile(batch_id, force=True)
    except Exception as e:
        _log(f"batch #{batch_id}: ошибка перетусовки: {e}")

    if b["fail"] > 0 or b["skip"] > 0:
        summary_msg = await b["event"].respond(
            f"✅ **Пачка обработана!**\n"
            f"├ Успешно: {b['ok']}\n"
            f"├ Пропущено (дубли): {b['skip']}\n"
            f"└ Ошибок: {b['fail']}"
        )
        b["to_delete"].append(summary_msg)

    if b["to_delete"]:
        _log(f"batch #{batch_id}: к отложенному удалению: {len(b['to_delete'])} сообщ.")

    _schedule_delete(*b["to_delete"])
    batches.pop(batch_id, None)

    try:
        queues.pop(batch_id, None)
        os.remove(_queue_path(batch_id))
    except Exception:
        pass

    _log(f"batch #{batch_id}: пачка закрыта и удалена из памяти")

# ---------- Возобновление после перезапуска ----------
async def cleanup_orphan_files():
    if not os.path.isdir(DOWNLOAD_DIR):
        return

    protected: set[str] = set()
    for qid in _list_queue_files():
        q = _load_queue(qid)
        if not q:
            continue
        for it in q.get("items", []):
            if it.get("status") in ("downloaded", "uploading") and it.get("local_file"):
                protected.add(it["local_file"])

    now = time.time()
    orphans = []
    for name in os.listdir(DOWNLOAD_DIR):
        full = os.path.join(DOWNLOAD_DIR, name)
        if not os.path.isfile(full):
            continue
        if name in protected:
            continue
        try:
            if now - os.path.getmtime(full) < 600:
                continue
        except Exception:
            pass
        orphans.append(full)

    if not orphans:
        return

    print(f"\n🧹 Найдено недокачанных/осиротевших файлов: {len(orphans)}")
    for f in orphans:
        try:
            size = human_size(os.path.getsize(f))
        except Exception:
            size = "?"
        try:
            os.remove(f)
            print(f"  🗑  удалён {os.path.basename(f)} ({size})")
        except Exception as e:
            print(f"  ⚠️  не удалось удалить {os.path.basename(f)}: {e}")


async def resume_pending_queues():
    queue_ids = _list_queue_files()
    if not queue_ids:
        return

    _log(f"♻️  найдено очередей на диске: {len(queue_ids)}")
    for qid in queue_ids:
        q = _load_queue(qid)
        if not q:
            _log(f"  • Очередь #{qid}: не удалось прочитать JSON — пропуск")
            continue
        items = q.get("items", [])
        if not items:
            _log(f"  • Очередь #{qid}: пустая — удаляю JSON")
            try:
                os.remove(_queue_path(qid))
            except Exception:
                pass
            queues.pop(qid, None)
            continue

        pending = [
            it for it in items
            if it["status"] in ("pending", "downloading", "fail")
        ]
        to_send = [
            it for it in items
            if it["status"] in ("downloaded", "uploading")
        ]
        done = [it for it in items if it["status"] == "done"]

        if not pending and not to_send:
            _log(
                f"  • Очередь #{qid}: всё готово ({len(done)}), удаляю JSON."
            )
            try:
                os.remove(_queue_path(qid))
            except Exception:
                pass
            queues.pop(qid, None)
            continue

        _log(
            f"  • Очередь #{qid}: done={len(done)}, "
            f"to_send={len(to_send)}, к возобновлению={len(pending)}"
        )

        queues[qid] = q

        link_states = {}
        status_msgs = {}
        for it in items:
            key = (it["chat_id"], it["msg_id"])
            st = {
                "status": it["status"] if it["status"] in (
                    "done", "fail", "skip", "downloaded", "uploading"
                ) else "pending",
                "percent": 0.0,
                "current": 0,
                "total": 0,
                "last_cb": 0.0,
                "last_edit": 0.0,
                "up_current": 0,
                "up_total": 0,
                "up_percent": 0.0,
                "last_up_cb": 0.0,
            }
            link_states[key] = st

            if it.get("status_msg_id") and it["status"] in (
                "pending", "downloading", "downloaded", "uploading", "fail"
            ):
                old_id = it["status_msg_id"]
                try:
                    m = await client.get_messages("me", ids=old_id)
                except Exception as e:
                    _log(
                        f"    ⚠️  #{qid} {it['link']}: не удалось "
                        f"прочитать статус id={old_id}: {type(e).__name__}: {e}"
                    )
                    m = None
                if not m:
                    # сообщение-статус потеряно — пересоздаём
                    try:
                        m = await client.send_message(
                            "me", f"📋 {it['link']}"
                        )
                        _mark_item(qid, it["chat_id"], it["msg_id"],
                                   status_msg_id=m.id)
                        _log(
                            f"    ➕ #{qid} {it['link']}: "
                            f"статус id={old_id} потерян → "
                            f"пересоздан id={m.id}"
                        )
                    except Exception as e:
                        _log(
                            f"    ❌ #{qid} {it['link']}: "
                            f"не удалось пересоздать статус: "
                            f"{type(e).__name__}: {e}"
                        )
                        m = None
                else:
                    _log(
                        f"    ✓  #{qid} {it['link']}: "
                        f"статус id={old_id} восстановлен из чата"
                    )
                if m:
                    status_msgs[key] = m

        batches[qid] = {
            "event": _make_fake_event(),
            "links": [(it["chat_id"], it["msg_id"], it["link"]) for it in items],
            "status_msgs": status_msgs,
            "link_states": link_states,
            "lock": asyncio.Lock(),
            "reorder_lock": asyncio.Lock(),
            "reorder_state": {"running": False, "pending": False, "last": 0.0},
            "total": len(items),
            "ok": len(done),
            "skip": 0, "fail": 0,
            "finished": False,
            "to_delete": [],
        }

        for it in pending:
            await link_queue.put((it["chat_id"], it["msg_id"], it["link"], qid))
        if pending:
            _log(f"    ↻  #{qid}: поставлено в очередь pending: {len(pending)}")
        # ВАЖНО: to_send не ставим в очередь — их подхватит offer_send_downloaded()

        if not pending and not to_send:
            await _maybe_finish_batch(qid)

def _find_downloaded_items() -> list[tuple[int, dict]]:
    result = []
    for qid in _list_queue_files():
        q = _load_queue(qid)
        if not q:
            continue
        for it in q.get("items", []):
            if it.get("status") not in ("downloaded", "uploading"):
                continue
            local = it.get("local_file")
            if not local:
                continue
            if os.path.exists(os.path.join(DOWNLOAD_DIR, local)):
                result.append((qid, it))
    return result


async def offer_send_downloaded():
    items = _find_downloaded_items()
    if not items:
        return

    by_batch: dict[int, list[dict]] = {}
    for qid, it in items:
        by_batch.setdefault(qid, []).append(it)

    total = len(items)
    print(f"\n📥 Найдено уже скачанных, но не отправленных файлов: {total}")
    shown = 0
    for qid, its in by_batch.items():
        print(f"  • Очередь #{qid}: {len(its)} файл(ов)")
        for it in its:
            if shown >= 15:
                break
            path = os.path.join(DOWNLOAD_DIR, it["local_file"])
            try:
                sz = human_size(os.path.getsize(path))
            except Exception:
                sz = "?"
            print(f"      – {it['local_file']} ({sz}) ← {it['link']}")
            shown += 1
        if shown >= 15 and total > 15:
            print(f"      … и ещё {total - shown}")
            break

    answer = input(
        "\nОтправить эти файлы в Избранное?\n"
        "  y — отправить\n"
        "  n — удалить\n"
        "  s — отложить до следующего запуска\n"
        "Выбор [y/n/s]: "
    ).strip().lower()

    if answer in ("s", ""):
        _log(
            f"  ⏭  пользователь отложил отправку — "
            f"файлы остаются downloaded в JSON, пачки закрываются "
            f"без отправки, при следующем запуске спросим снова"
        )
        # Файлы остаются на диске и в статусе downloaded в JSON.
        # Пачки, которые сейчас в памяти, закрываем — чтобы не висели.
        # При следующем запуске resume_pending_queues / offer_send_downloaded
        # снова их найдут и предложат отправить.
        for qid in {qid for qid, _ in items}:
            b = batches.get(qid)
            if b is None:
                continue
            b["finished"] = True
            # Гасим статус-сообщения, чтобы не оставались висеть в чате
            for m in list(b["status_msgs"].values()):
                _schedule_delete(m)
            b["status_msgs"].clear()
            batches.pop(qid, None)
            # JSON НЕ удаляем — при следующем запуске снова найдём
            _log(
                f"    ⏸  #{qid}: пачка закрыта без отправки, "
                f"JSON сохранён для следующего запуска"
            )
        return

    if answer in ("n", "no", "нет", "н"):
        for qid, it in items:
            # 1. Удаляем локальный файл
            path = os.path.join(DOWNLOAD_DIR, it["local_file"])
            try:
                os.remove(path)
                _log(f"    🗑  #{qid} {it['link']}: удалён файл {it['local_file']}")
            except Exception as e:
                _log(
                    f"    ⚠️  #{qid} {it['link']}: "
                    f"не удалось удалить файл {it['local_file']}: {e}"
                )

            # 2. Помечаем элемент как skip в JSON (больше не качаем)
            _mark_item(
                qid, it["chat_id"], it["msg_id"],
                status="skip",
                local_file=None,
                downloaded_at=None,
            )

            # 3. Удаляем статус-сообщение из чата и учитываем в счётчиках
            if qid in batches:
                b = batches[qid]
                key = (it["chat_id"], it["msg_id"])
                st = b["link_states"].get(key)
                if st is not None:
                    st["status"] = "skip"
                    st["last_edit"] = 0.0
                    st["last_cb"] = 0.0
                old_msg = b["status_msgs"].pop(key, None)
                if old_msg is not None:
                    try:
                        await old_msg.delete()
                        _log(
                            f"    🗑  #{qid} {it['link']}: "
                            f"удалён статус id={old_msg.id} (n → отмена)"
                        )
                    except Exception as e:
                        _log(
                            f"    ⚠️  #{qid} {it['link']}: "
                            f"не удалось удалить статус id={old_msg.id}: {e}"
                        )
                _mark_item(qid, it["chat_id"], it["msg_id"], status_msg_id=None)

                # Учитываем как skip, чтобы пачка закрылась
                b["skip"] = b.get("skip", 0) + 1
                asyncio.create_task(_maybe_finish_batch(qid))
        print(f"  🗑  Удалено {total} файл(ов) и их статусов.")
        return

    for qid, it in items:
        if qid not in batches:
            batches[qid] = {
                "event": _make_fake_event(),
                "links": [(it["chat_id"], it["msg_id"], it["link"])],
                "status_msgs": {},
                "link_states": {},
                "lock": asyncio.Lock(),
                "reorder_lock": asyncio.Lock(),
                "reorder_state": {"running": False, "pending": False, "last": 0.0},
                "total": 0, "ok": 0, "skip": 0, "fail": 0,
                "finished": False,
                "to_delete": [],
            }
        b = batches[qid]
        key = (it["chat_id"], it["msg_id"])
        b["link_states"].setdefault(key, {
            "status": "downloaded", "percent": 0.0, "current": 0,
            "total": 0, "last_cb": 0.0, "last_edit": 0.0,
            "up_current": 0, "up_total": 0, "up_percent": 0.0, "last_up_cb": 0.0,
        })
        b["total"] = max(b["total"], len(b["link_states"]))
        await link_queue.put((it["chat_id"], it["msg_id"], it["link"], qid))
    print(f"  📤 Поставлено в очередь на отправку: {total}")


# ---------- Отправка старых файлов из downloads/ ----------
async def upload_existing_downloads():
    files = [
        os.path.join(DOWNLOAD_DIR, f)
        for f in os.listdir(DOWNLOAD_DIR)
        if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))
    ]
    if not files:
        return

    total = len(files)
    print(f"\n📤 Отправка {total} файл(ов) из {DOWNLOAD_DIR}/ в Избранное...")

    status = await client.send_message(
        "me", f"📤 Отправка {total} файлов из `{DOWNLOAD_DIR}/`..."
    )

    ok = fail = 0
    for i, path in enumerate(files, start=1):
        name = os.path.basename(path)
        size_str = human_size(os.path.getsize(path))
        pd = {"last_update": 0}

        async def cb(current, total_bytes, *, _i=i, _name=name,
                     _total=total, _pd=pd, _status=status):
            now = time.time()
            if now - _pd["last_update"] > 3 or current == total_bytes:
                _pd["last_update"] = now
                percent = (current / total_bytes) * 100 if total_bytes else 0
                try:
                    await _status.edit(
                        f"📤 **Отправка файлов [{_i}/{_total}]**\n"
                        f"📁 `{_name}`\n"
                        f"📊 **{percent:.1f}%** "
                        f"({human_size(current)} / {human_size(total_bytes)})"
                    )
                except MessageNotModifiedError:
                    pass
                except FloodWaitError as fwe:
                    await asyncio.sleep(fwe.seconds)
                except Exception:
                    pass

        try:
            async with upload_lock:
                await _send_media("me", path, name, cb)
            os.remove(path)
            ok += 1
            print(f"  ✅ [{i}/{total}] {name} ({size_str})")
        except Exception as e:
            fail += 1
            print(f"  ❌ [{i}/{total}] {name}: {e}")

    try:
        await status.edit(
            f"✅ **Скачанные файлы обработаны**\n"
            f"├ Успешно: {ok}\n"
            f"└ Ошибок: {fail}"
        )
    except Exception:
        pass
    _schedule_delete(status)

# ---------- Обработчик сообщений ----------
@client.on(events.NewMessage(chats="me"))
async def handle_saved_messages(event):
    text = event.raw_text
    cmd = text.strip().lower()

    if cmd == "/stop":
        cancel_flag["cancel"] = True
        await event.respond(
            "⛔ Новые ссылки будут пропущены. Уже скачиваемые загрузки продолжатся."
        )
        return

    if cmd == "/resume":
        cancel_flag["cancel"] = False
        await event.respond("▶️ Обработка новых ссылок возобновлена.")
        return

    if cmd in ("/quit", "/exit", "/shutdown"):
        cancel_flag["cancel"] = True
        await event.respond(
            "🛑 Останавливаюсь. Текущие загрузки завершатся, затем выход."
        )
        shutdown_event.set()
        return

    if cmd == "/clear":
        cancel_flag["cancel"] = False
        cleared = 0
        while not link_queue.empty():
            try:
                link_queue.get_nowait()
                cleared += 1
            except asyncio.QueueEmpty:
                break

        cleared_batches = 0
        for batch_id, b in list(batches.items()):
            if b.get("finished"):
                continue
            b["finished"] = True
            for m in list(b["status_msgs"].values()):
                _schedule_delete(m)
            b["status_msgs"].clear()
            batches.pop(batch_id, None)
            queues.pop(batch_id, None)
            try:
                os.remove(_queue_path(batch_id))
            except Exception:
                pass
            cleared_batches += 1

        await event.respond(
            f"🧹 Очищено ссылок в очереди: {cleared}\n"
            f"🧹 Отменено пачек: {cleared_batches}"
        )
        return

    if cmd == "/stats":
        await event.respond(
            f"📊 **Статистика**\n"
            f"├ Ожидает в очереди: {link_queue.qsize()}\n"
            f"├ В работе: {in_progress['count']} / {CONCURRENT_DOWNLOADS}\n"
            f"├ Активных пачек: {len(batches)}\n"
            f"├ Пауза (/stop): {'да' if cancel_flag['cancel'] else 'нет'}\n"
            f"└ Обработано уникальных: {len(processed)}"
        )
        return

    if "t.me/" not in text:
        return

    links = extract_all_links(text)
    if not links:
        return

    batch_counter["id"] += 1
    batch_id = batch_counter["id"]
    link_states = {}
    status_msgs = {}
    items = []

    for chat_id, msg_id, full_link in links:
        status_msg = await event.respond(f"📋 {full_link}")
        key = (chat_id, msg_id)
        status_msgs[key] = status_msg
        link_states[key] = {
            "status": "pending",
            "percent": 0.0,
            "current": 0,
            "total": 0,
            "last_cb": 0.0,
            "last_edit": 0.0,
            "up_current": 0,
            "up_total": 0,
            "up_percent": 0.0,
            "last_up_cb": 0.0,
        }
        items.append({
            "chat_id": chat_id,
            "msg_id": msg_id,
            "link": full_link,
            "status": "pending",
            "status_msg_id": status_msg.id,
            "slot_msg_id": None,
            "file_message_id": None,
            "local_file": None,
            "downloaded_at": None,
        })

    _create_queue(batch_id, items)

    batches[batch_id] = {
        "event": event,
        "links": links,
        "status_msgs": status_msgs,
        "link_states": link_states,
        "lock": asyncio.Lock(),
        "reorder_lock": asyncio.Lock(),
        "reorder_state": {"running": False, "pending": False, "last": 0.0},
        "total": len(link_states),
        "ok": 0, "skip": 0, "fail": 0,
        "finished": False,
        "to_delete": [],          # ← исходное сообщение больше не откладываем
    }

    for chat_id, msg_id, full_link in links:
        await link_queue.put((chat_id, msg_id, full_link, batch_id))

    # Исходное сообщение со ссылками больше не нужно — удаляем сразу
    try:
        await event.message.delete()
        _log(f"🗑  удалено исходное сообщение со ссылками (batch #{batch_id})")
    except FloodWaitError as fwe:
        _log(
            f"  ⏳ FloodWait {fwe.seconds}s при удалении исходного "
            f"сообщения (batch #{batch_id})"
        )
        await asyncio.sleep(fwe.seconds)
        try:
            await event.message.delete()
            _log(f"🗑  удалено исходное сообщение (после FloodWait, batch #{batch_id})")
        except Exception:
            pass
    except Exception:
        pass

# ---------- Запуск ----------
async def main():
    files_in_downloads = [
        f for f in os.listdir(DOWNLOAD_DIR)
        if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))
    ]
    send_old = False
    if files_in_downloads:
        print(f"\n📂 В папке `{DOWNLOAD_DIR}/` найдено {len(files_in_downloads)} файл(ов):")
        for f in files_in_downloads[:20]:
            print(f"   • {f} ({human_size(os.path.getsize(os.path.join(DOWNLOAD_DIR, f)))})")
        if len(files_in_downloads) > 20:
            print(f"   … и ещё {len(files_in_downloads) - 20}")
        answer = input("\nОтправить их в Избранное? [y/n]: ").strip().lower()
        send_old = answer in ("y", "yes", "да", "д")

    session_file = os.path.join(BASE_DIR, "user_session.session")
    had_session_file = os.path.exists(session_file)

    print("⏳ Подключение к Telegram...")
    await client.start()

    authorized = await client.is_user_authorized()
    if not had_session_file or not authorized:
        print("  📇 Первый запуск или сессия недействительна — загружаем диалоги...")
        await client.get_dialogs()
    else:
        print("  📇 Сессия в порядке — диалоги не перезагружаем.")

    me = await client.get_me()
    account_state["is_premium"] = bool(getattr(me, "premium", False))
    limit = LIMIT_PREMIUM if account_state["is_premium"] else LIMIT_FREE
    print(
        f"  👤 Аккаунт: @{me.username or me.id} "
        f"({'Premium' if account_state['is_premium'] else 'Free'})"
    )
    print(f"  📦 Лимит на отправку: {human_size(limit)}")

    workers = [
        asyncio.create_task(link_worker(i + 1))
        for i in range(CONCURRENT_DOWNLOADS)
    ]
    print(f"  ▶ Запущено воркеров: {CONCURRENT_DOWNLOADS}")

    await cleanup_orphan_files()
    await resume_pending_queues()
    await offer_send_downloaded()

    if send_old:
        await upload_existing_downloads()

    print(
        f"\n🚀 Готово!\n"
        f"├ Каждая ссылка — отдельное сообщение\n"
        f"├ Одновременно качается: {CONCURRENT_DOWNLOADS}\n"
        f"├ После загрузки: файл заменит сообщение (только при успехе)\n"
        f"├ Перетусовка: {'после каждого файла' if REORDER_AFTER_EACH_FILE else 'по завершении пачки'}\n"
        f"└ Команды: /stop, /resume, /quit, /clear, /stats"
    )

    try:
        disconnect_task = asyncio.ensure_future(client.disconnected)
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        await asyncio.wait(
            [disconnect_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in (disconnect_task, shutdown_task):
            if not t.done():
                t.cancel()
        await asyncio.gather(disconnect_task, shutdown_task, return_exceptions=True)
        print("🛑 Завершение работы...")
    finally:
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

        if _pending_deletions:
            await asyncio.gather(*_pending_deletions, return_exceptions=True)

        await asyncio.sleep(0.5)
        try:
            await client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.WARNING,
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    try:
        client.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлено пользователем (Ctrl+C).")
    except Exception as crash_error:
        print(f"\n❌ КРИТИЧЕСКИЙ СБОЙ: {crash_error}")