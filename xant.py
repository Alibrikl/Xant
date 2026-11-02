import os
import re
import json
import time
import asyncio
import logging
import mimetypes
import shutil
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

batch_create_lock = asyncio.Lock()

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
    try:
        os.chmod(ENV_FILE, 0o600)
    except Exception:
        pass  # Windows / FAT32 — не критично
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
# Регистронезависимый регекс — чтобы ловить HTTPS://T.ME/...,
# Https://T.me/... и т.п. Проверка "t.me/" in text.lower() в
# handle_saved_messages пропускает такие строки, но без IGNORECASE
# extract_all_links вернул бы [] → ссылка проигнорирована молча.
LINK_REGEX = re.compile(LINK_PATTERN, re.IGNORECASE)

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
    for match in LINK_REGEX.finditer(text):
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

# Кэш: (display_name, file_size) -> message_id
# Кэшируем ТОЛЬКО успешные находки (int).
# None (файл не найден) НЕ кэшируем — иначе при повторной проверке
# после отправки watchdog'ом получим ложный fail и потеряем файл.
# Живёт в течение одного запуска скрипта.
_find_cache: dict[tuple[str, int | None], int] = {}


async def _find_in_me(display_name: str, file_size: int | None,
                      scan_limit: int = 400, max_retries: int = 2) -> int | None:
    if not display_name:
        return None

    cache_key = (display_name, file_size if file_size and file_size > 0 else None)

    # 1) Проверяем кэш — только успешные находки
    cached = _find_cache.get(cache_key)
    if cached is not None:
        _log(f"  💾 _find_in_me({display_name}): из кэша (id={cached})")
        return cached

    check_size = bool(file_size and file_size > 0)
    attempt = 0
    while True:
        try:
            # 2) Серверный поиск по имени — в разы дешевле полного скана
            found: int | None = None
            async for msg in client.iter_messages(
                "me", search=display_name, limit=20
            ):
                if not msg.file:
                    continue
                if msg.file.name != display_name:
                    continue
                if check_size:
                    size = getattr(msg.file, "size", 0) or 0
                    if size != file_size:
                        continue
                found = msg.id
                break

            # 3) Фолбэк: если серверный поиск ничего не дал — полный скан
            #    (редкий случай: спецсимволы в имени, поиск не сработал)
            if found is None:
                async for msg in client.iter_messages("me", limit=scan_limit):
                    if not msg.file:
                        continue
                    if msg.file.name != display_name:
                        continue
                    if check_size:
                        size = getattr(msg.file, "size", 0) or 0
                        if size != file_size:
                            continue
                    found = msg.id
                    break

            # 4) Кэшируем ТОЛЬКО успешную находку.
            #    None — не кэшируем: файл мог ещё не долететь до me,
            #    или появиться позже (watchdog, повторная отправка).
            if found is not None:
                _find_cache[cache_key] = found
                _log(
                    f"  💾 _find_in_me({display_name}): найдено id={found} "
                    f"— кэширую"
                )
            else:
                _log(
                    f"  🔍 _find_in_me({display_name}): не найдено "
                    f"— НЕ кэширую (повторная проверка возможна)"
                )
            return found

        except FloodWaitError as fwe:
            attempt += 1
            if attempt > max_retries:
                _log(
                    f"  ⚠️  _find_in_me({display_name}): FloodWait "
                    f"— сдаюсь после {max_retries} попыток"
                )
                raise
            _log(f"  ⏳ _find_in_me: FloodWait {fwe.seconds}s — жду")
            await asyncio.sleep(fwe.seconds)
        except Exception as e:
            _log(f"  ⚠️  _find_in_me({display_name}): {type(e).__name__}: {e}")
            # Не кэшируем ошибки — пусть попробует в следующий раз
            return None

def _mark_item(batch_id: int, chat_id, msg_id, **fields):
    it = _find_item(batch_id, chat_id, msg_id)
    if it is None:
        _log(
            f"⚠️  _mark_item: очередь #{batch_id} не в памяти "
            f"или элемент ({chat_id}, {msg_id}) не найден — пропуск. "
            f"Поля: {list(fields.keys())}"
        )
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

def _scan_downloads() -> dict:
    """
    Классифицирует файлы в DOWNLOAD_DIR:
      - manual:      файла нет в JSON-очередях (пользователь положил руками)
      - own_partial: есть в JSON, статус pending/downloading (недокачан)
      - own_ready:   есть в JSON, статус downloaded/uploading (готов к отправке)
    """
    own_by_name: dict[str, tuple[int, dict]] = {}
    for qid in _list_queue_files():
        q = _load_queue(qid)
        if not q:
            continue
        for it in q.get("items", []):
            lf = it.get("local_file")
            if lf:
                own_by_name[lf] = (qid, it)

    manual: list[str] = []
    own_partial: list[tuple[int, dict, str]] = []
    own_ready: list[tuple[int, dict, str]] = []

    if not os.path.isdir(DOWNLOAD_DIR):
        return {"manual": manual, "own_partial": own_partial, "own_ready": own_ready}

    for name in os.listdir(DOWNLOAD_DIR):
        full = os.path.join(DOWNLOAD_DIR, name)
        if not os.path.isfile(full):
            continue
        entry = own_by_name.get(name)
        if entry is None:
            manual.append(full)
        else:
            qid, it = entry
            if it.get("status") in ("pending", "downloading"):
                own_partial.append((qid, it, full))
            else:  # downloaded / uploading
                own_ready.append((qid, it, full))

    return {"manual": manual, "own_partial": own_partial, "own_ready": own_ready}

# ---------- Скачивание ----------
async def download_with_retry(
    chat_id, msg_id, progress_callback,
    size_holder: dict | None = None, max_retries: int = 3,
    on_path_known=None,
):
    target_msg = await client.get_messages(chat_id, ids=msg_id)
    if not target_msg or not target_msg.media:
        return None

    if size_holder is not None:
        size_holder["size"] = target_msg.file.size if target_msg.file else 0

        # ✅ Проверка свободного места ДО начала скачивания:
        # иначе download_media упадёт посередине, оставит частичный файл
        # в downloads/, и download_with_retry попробует ещё 2 раза —
        # каждый раз заново, накапливая мусор.
        #
        # Возвращаем None (а не raise), чтобы process_link обработал это
        # как обычный "не удалось скачать" → fail → корректная пометка
        # в JSON и статус-сообщении.
        if size_holder["size"] > 0:
            try:
                free = shutil.disk_usage(DOWNLOAD_DIR).free
                need = size_holder["size"] * 1.1  # +10% запас
                if free < need:
                    print(
                        f"\n[disk check] недостаточно места для "
                        f"{chat_id}/{msg_id}: нужно {human_size(need)}, "
                        f"свободно {human_size(free)} — пропуск"
                    )
                    return None
            except Exception as e:
                # Не смогли проверить — не блокируем скачивание,
                # пусть попробует (лучше попытаться, чем упасть на проверке)
                print(f"[disk check] не удалось проверить место: {e}")

    suggested_filename = build_filename(target_msg)
    local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))
    if size_holder is not None:
        size_holder["path"] = local_path
    if on_path_known is not None:
        on_path_known(local_path)

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
            if size_holder is not None:
                size_holder["path"] = local_path
            if on_path_known is not None:
                on_path_known(local_path)
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
            # Запоминаем local_file в JSON сразу, чтобы при аварии
            # файл в downloads/ считался «нашим недокачанным», а не «ручным»
            path = size_holder.get("path")
            if path and not item.get("local_file"):
                _mark_item(
                    batch_id, chat_id, msg_id,
                    status="downloading",
                    local_file=os.path.basename(path),
                )
            if key in batch["status_msgs"]:
                await _update_status_msg(batch, key, full_link)

    try:
        already = item.get("local_file")
        if already and os.path.exists(os.path.join(DOWNLOAD_DIR, already)):
            filename = os.path.join(DOWNLOAD_DIR, already)
            _log(f"📂 файл уже скачан ранее: {filename}")

            # ✅ Если статус uploading — возможно, уже отправлен.
            # Проверим в me перед тем, как скачать/отправить снова.
            if item.get("status") == "uploading":
                local_size = os.path.getsize(filename)
                _log(
                    f"🔍 {full_link}: статус uploading — проверяю me "
                    f"(имя={os.path.basename(filename)}, "
                    f"размер={human_size(local_size)})"
                )
                try:
                    found_id = await _find_in_me(
                        display_name=os.path.basename(filename),
                        file_size=local_size,
                    )
                except FloodWaitError:
                    _log(
                        f"⚠️  {full_link}: FloodWait при проверке me — "
                        f"оставляю uploading (безопаснее)"
                    )
                    _mark_item(
                        batch_id, chat_id, msg_id,
                        status="uploading",
                    )
                    st["status"] = "uploading"
                    await _update_status_msg(batch, key, full_link, force=True)
                    processed.discard(key)
                    # ✅ "deferred" — link_worker НЕ инкрементит счётчик,
                    # batch НЕ закроется, элемент будет обработан при
                    # следующем запуске через resume_pending_queues.
                    return "deferred"

                if found_id is not None:
                    _log(
                        f"✅ {full_link}: найден в me (id={found_id}), "
                        f"пропускаю повторную отправку"
                    )
                    _mark_item(
                        batch_id, chat_id, msg_id,
                        status="done",
                        file_message_id=found_id,
                        local_file=None,
                        downloaded_at=None,
                    )
                    st["status"] = "done"
                    try:
                        os.remove(filename)
                        _log(f"🧹 удалён локальный файл: "
                             f"{os.path.basename(filename)}")
                    except Exception:
                        pass
                    status_msg = batch["status_msgs"].pop(key, None)
                    if status_msg is not None:
                        try:
                            await status_msg.delete()
                        except Exception:
                            pass
                    if REORDER_AFTER_EACH_FILE:
                        await request_reconcile(batch_id, force=False)
                    return "ok"
                else:
                    _log(
                        f"📂 {full_link}: в me не найден — "
                        f"переотправлю"
                    )

            st["status"] = "downloaded"
            _mark_item(batch_id, chat_id, msg_id, status="downloaded")
            await _update_status_msg(batch, key, full_link, force=True)
        else:
            def _on_path(path):
                if not item.get("local_file"):
                    _mark_item(
                        batch_id, chat_id, msg_id,
                        status="downloading",
                        local_file=os.path.basename(path),
                    )

            filename = await download_with_retry(
                chat_id, msg_id, download_cb,
                size_holder=size_holder,
                on_path_known=_on_path,
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
                if result == "deferred":
                    # ✅ Отложено (FloodWait/временная ошибка) — НЕ инкрементим,
                    # batch остаётся в памяти до следующего запуска.
                    _log(
                        f"worker #{worker_id}: {full_link} — отложено "
                        f"(не инкременчу счётчики)"
                    )
                else:
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

    # ✅ ЗАЩИТА: счётчики сошлись, но в JSON могут оставаться элементы
    # со статусами, предполагающими работу:
    #   - pending/downloading  — ещё в очереди или в работе у воркера
    #   - downloaded/uploading — ждут постановки в link_queue
    #                            (offer_send_downloaded) или идут upload
    # Если такие есть — НЕ закрываем batch. Иначе получим:
    #   • JSON удалён, а файл ещё не отправлен → потерян при рестарте
    #   • статус-сообщение висит без обновления
    #   • to_send никогда не попадёт в очередь
    q = queues.get(batch_id)
    if q is not None:
        active_statuses = {"pending", "downloading", "downloaded", "uploading"}
        stuck = [
            it for it in q.get("items", [])
            if it.get("status") in active_statuses
        ]
        if stuck:
            # Краткая сводка для лога: статус → сколько
            counts: dict[str, int] = {}
            for it in stuck:
                s = it.get("status", "?")
                counts[s] = counts.get(s, 0) + 1
            summary = ", ".join(f"{k}={v}" for k, v in counts.items())
            _log(
                f"batch #{batch_id}: счётчики сошлись "
                f"(done={done}/{b['total']}), но в JSON остались "
                f"активные элементы: {summary} — откладываю завершение"
            )
            return

    # ✅ Все счётчики сошлись и активных элементов в JSON нет — закрываем.
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

    if not b.get("is_manual") and (b["fail"] > 0 or b["skip"] > 0):
        try:
            summary_msg = await b["event"].respond(
                f"✅ **Пачка обработана!**\n"
                f"├ Успешно: {b['ok']}\n"
                f"├ Пропущено (дубли): {b['skip']}\n"
                f"└ Ошибок: {b['fail']}"
            )
            b["to_delete"].append(summary_msg)
        except Exception as e:
            _log(f"batch #{batch_id}: не удалось отправить summary: {e}")

    if b["to_delete"]:
        _log(f"batch #{batch_id}: к отложенному удалению: {len(b['to_delete'])} сообщ.")

    try:
        _schedule_delete(*b["to_delete"])
    finally:
        batches.pop(batch_id, None)
        queues.pop(batch_id, None)
        try:
            os.remove(_queue_path(batch_id))
        except Exception:
            pass

    _log(f"batch #{batch_id}: пачка закрыта и удалена из памяти")

# ---------- Watchdog зависших batch ----------
async def _watchdog_stuck_batches(interval: float = 60.0):
    """Периодически ставит в очередь элементы downloaded/uploading,
    которые не были обработаны (например, из-за FloodWait в deferred).
    Без этого batch висит вечно."""
    while True:
        try:
            await asyncio.sleep(interval)
            now = time.time()
            for batch_id, b in list(batches.items()):
                if b.get("finished"):
                    continue
                q = queues.get(batch_id)
                if q is None:
                    continue

                # downloaded — безопасно переотправить
                # uploading — сначала проверим в me, вдруг уже улетело
                candidates = [
                    it for it in q.get("items", [])
                    if it.get("status") in ("downloaded", "uploading")
                ]
                if not candidates:
                    continue

                key = tuple((id(it), it.get("status")) for it in candidates)
                wd = b.setdefault("_wd", {})
                if wd.get("key") != key:
                    wd["key"] = key
                    wd["ts"] = now
                    _log(
                        f"🐕 watchdog: batch #{batch_id} — "
                        f"{len(candidates)} элементов ждут отправки. "
                        f"Дам {interval:.0f}s..."
                    )
                    continue
                elapsed = now - wd.get("ts", now)
                if elapsed < interval:
                    continue

                _log(
                    f"🐕 watchdog: batch #{batch_id} висит {elapsed:.0f}s "
                    f"с {len(candidates)} элементами — обрабатываю"
                )
                any_fail = False
                for it in candidates:
                    local = it.get("local_file")
                    full_path = os.path.join(DOWNLOAD_DIR, local) if local else None
                    file_exists = bool(full_path and os.path.exists(full_path))

                    # ✅ uploading без файла на диске — проверяем в me
                    if it.get("status") == "uploading" and not file_exists:
                        if not local:
                            _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                       status="fail", local_file=None)
                            b["fail"] = b.get("fail", 0) + 1
                            any_fail = True
                            _log(f"  🐕 #{batch_id} {it['link']}: uploading без local_file → fail")
                            continue
                        _log(f"  🐕 #{batch_id} {it['link']}: uploading без файла — проверяю me")
                        try:
                            found_id = await _find_in_me(display_name=local, file_size=None)
                        except FloodWaitError:
                            _log(f"  🐕 #{batch_id} {it['link']}: FloodWait — откладываю")
                            continue
                        if found_id is not None:
                            _mark_item(
                                batch_id, it["chat_id"], it["msg_id"],
                                status="done", file_message_id=found_id,
                                local_file=None,
                            )
                            b["ok"] = b.get("ok", 0) + 1
                            _log(
                                f"  ✅ #{batch_id} {it['link']}: uploading → done "
                                f"(в me: id={found_id})"
                            )
                        else:
                            _mark_item(
                                batch_id, it["chat_id"], it["msg_id"],
                                status="fail", local_file=None,
                            )
                            b["fail"] = b.get("fail", 0) + 1
                            any_fail = True
                            _log(f"  ❌ #{batch_id} {it['link']}: uploading → fail")
                        continue

                    # downloaded (или uploading с файлом — считаем как downloaded)
                    if not file_exists:
                        _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                   status="fail", local_file=None)
                        b["fail"] = b.get("fail", 0) + 1
                        any_fail = True
                        _log(f"  🐕 #{batch_id} {it['link']}: файл пропал → fail")
                        continue

                    retries = it.get("upload_retries", 0)
                    if retries >= 3:
                        _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                   status="fail", local_file=None)
                        b["fail"] = b.get("fail", 0) + 1
                        any_fail = True
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"превышено попыток ({retries}) → fail"
                        )
                        continue
                    _mark_item(batch_id, it["chat_id"], it["msg_id"],
                               status="downloaded",
                               upload_retries=retries + 1)

                    try:
                        await link_queue.put(
                            (it["chat_id"], it["msg_id"], it["link"], batch_id)
                        )
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"повторно в очереди (попытка {retries + 1}/3)"
                        )
                    except Exception as e:
                        _log(f"  ❌ #{batch_id} {it['link']}: {e}")
                if any_fail:
                    await _maybe_finish_batch(batch_id)
                wd["key"] = None
        except asyncio.CancelledError:
            return
        except Exception as e:
            _log(f"🐕 watchdog: ошибка: {type(e).__name__}: {e}")

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

        # ✅ Устанавливаем очередь ДО любых _mark_item
        queues[qid] = q

        # ✅ uploading: проверяем me, чтобы не отправить дубликат
        uploading_done = 0
        uploading_downloaded = 0
        uploading_fail = 0
        for it in items:
            if it.get("status") != "uploading":
                continue

            local = it.get("local_file")
            full_path = os.path.join(DOWNLOAD_DIR, local) if local else None
            file_exists = bool(full_path and os.path.exists(full_path))

            if not local:
                # Нет записи о файле — не знаем, что отправлять.
                it["status"] = "fail"
                uploading_fail += 1
                _log(
                    f"  ⚠️  #{qid} {it['link']}: uploading без local_file → fail"
                )
                continue

            if file_exists:
                try:
                    local_size = os.path.getsize(full_path)
                except Exception:
                    local_size = None
                _log(
                    f"  🔍 #{qid} {it['link']}: uploading, проверяю me "
                    f"(имя={local}, размер={human_size(local_size) if local_size else '?'})"
                )
                try:
                    found_id = await _find_in_me(
                        display_name=local, file_size=local_size,
                    )
                except FloodWaitError:
                    # Не смогли проверить — оставляем uploading (безопаснее)
                    uploading_fail += 1
                    _log(
                        f"  ⚠️  #{qid} {it['link']}: FloodWait при проверке me — "
                        f"оставляю uploading"
                    )
                    continue
                if found_id is not None:
                    it["status"] = "done"
                    it["file_message_id"] = found_id
                    it["local_file"] = None
                    try:
                        os.remove(full_path)
                    except Exception:
                        pass
                    uploading_done += 1
                    _log(
                        f"  ♻️  #{qid} {it['link']}: uploading → done "
                        f"(в me: id={found_id}, локальный удалён)"
                    )
                else:
                    it["status"] = "downloaded"
                    uploading_downloaded += 1
                    _log(
                        f"  ♻️  #{qid} {it['link']}: uploading → downloaded "
                        f"(в me не найден — переотправлю)"
                    )
            else:
                # Файла нет на диске — ищем в me только по имени
                _log(
                    f"  🔍 #{qid} {it['link']}: uploading, файла нет "
                    f"на диске — ищу в me по имени ({local})"
                )
                try:
                    found_id = await _find_in_me(
                        display_name=local, file_size=None,
                    )
                except FloodWaitError:
                    uploading_fail += 1
                    _log(
                        f"  ⚠️  #{qid} {it['link']}: FloodWait при проверке me — "
                        f"оставляю uploading"
                    )
                    continue
                if found_id is not None:
                    it["status"] = "done"
                    it["file_message_id"] = found_id
                    it["local_file"] = None
                    uploading_done += 1
                    _log(
                        f"  ♻️  #{qid} {it['link']}: uploading → done "
                        f"(в me: id={found_id})"
                    )
                else:
                    it["status"] = "fail"
                    uploading_fail += 1
                    _log(
                        f"  ⚠️  #{qid} {it['link']}: uploading, файла нет "
                        f"и в me не найден → fail"
                    )

        if uploading_done or uploading_downloaded or uploading_fail:
            _log(
                f"  📊 #{qid}: uploading → done={uploading_done}, "
                f"→ downloaded={uploading_downloaded}, → fail={uploading_fail}"
            )
        _save_queue(qid)

        # ✅ НОВЫЙ БЛОК: закрываем очередь, если после обработки
        # uploading все элементы оказались в терминальных статусах.
        # Без этого JSON с одними fail/done/skip будет висеть на диске
        # вечно: batches[qid] ещё не создан, а _maybe_finish_batch
        # вызывается только из воркеров/watchdog, которые подхватят
        # только downloaded/uploading.
        _close_queue_if_finished(qid)


def _close_queue_if_finished(qid: int) -> bool:
    """
    Закрывает JSON-очередь, если все её элементы в терминальных статусах
    (done/fail/skip). Удаляет JSON с диска и чистит память.

    Возвращает True, если очередь была закрыта.

    НЕ трогает статус-сообщения в me — они либо уже удалены
    (для done), либо остаются как «❌/⚠️» без обновления.
    Это приемлемо при старте: пользователь видит результат
    восстановления и может удалить вручную.
    """
    q = queues.get(qid) or _load_queue(qid)
    if q is None:
        return False

    items = q.get("items", [])
    if not items:
        # Пустая очередь — тоже мусор
        queues.pop(qid, None)
        try:
            os.remove(_queue_path(qid))
        except Exception:
            pass
        return True

    active_statuses = {"pending", "downloading", "downloaded", "uploading"}
    has_active = any(
        it.get("status") in active_statuses for it in items
    )
    if has_active:
        return False

    # Все в done/fail/skip — закрываем
    counts: dict[str, int] = {}
    for it in items:
        s = it.get("status", "?")
        counts[s] = counts.get(s, 0) + 1
    summary = ", ".join(f"{k}={v}" for k, v in counts.items())

    _log(
        f"  🧹 Очередь #{qid}: все элементы завершены ({summary}) — "
        f"удаляю JSON"
    )
    queues.pop(qid, None)
    try:
        os.remove(_queue_path(qid))
    except Exception:
        pass
    return True

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

    print(f"\n📥 Найдено уже скачанных, но не отправленных файлов: {len(items)} — отправляю")

    placed = 0
    failed: list[tuple[int, dict, str]] = []   # (qid, it, причина)

    for qid, it in items:
        try:
            b = batches.get(qid)
            if b is None:
                _log(f"  ⚠️  #{qid} {it['link']}: batch не восстановлен — пропуск")
                failed.append((qid, it, "batch не восстановлен"))
                continue

            key = (it["chat_id"], it["msg_id"])

            # Гарантируем статус-сообщение
            if key not in b["status_msgs"]:
                old_id = it.get("status_msg_id")
                m = None
                if old_id:
                    try:
                        m = await client.get_messages("me", ids=old_id)
                    except Exception:
                        m = None
                if not m:
                    try:
                        m = await client.send_message(
                            "me", f"⏳ {it['link']} (ожидание...)"
                        )
                        _mark_item(qid, it["chat_id"], it["msg_id"],
                                   status_msg_id=m.id)
                    except Exception as e:
                        _log(f"  ⚠️  #{qid} {it['link']}: статус не создан: {e}")
                        m = None
                if m:
                    b["status_msgs"][key] = m

            b["link_states"].setdefault(key, {
                "status": "downloaded", "percent": 0.0, "current": 0,
                "total": 0, "last_cb": 0.0, "last_edit": 0.0,
                "up_current": 0, "up_total": 0, "up_percent": 0.0, "last_up_cb": 0.0,
            })

            await link_queue.put((it["chat_id"], it["msg_id"], it["link"], qid))
            placed += 1
            _log(f"  📤 #{qid} {it['link']}: поставлен в очередь на отправку")

        except Exception as e:
            # ✅ Ошибка одного элемента не ломает остальные
            _log(f"  ❌ #{qid} {it['link']}: постановка не удалась: "
                 f"{type(e).__name__}: {e}")
            failed.append((qid, it, f"{type(e).__name__}: {e}"))

    print(f"  📤 Поставлено в очередь на отправку: {placed}")
    if failed:
        print(f"  ⚠️  Не удалось поставить: {len(failed)}")

    # Обновить статусы после постановки
    for qid in {qid for qid, _ in items}:
        if qid in batches:
            try:
                await request_reconcile(qid, force=True)
            except Exception as e:
                _log(f"  ⚠️  reconcile #{qid}: {e}")

    # ✅ Возвращаем список «не поставленных» — пригодится для
    # эскалации в main() (Уровень 3)
    return failed

# ---------- Отправка «ручных» файлов (не из JSON) ----------

async def upload_manual_file(batch_id: int, item: dict, path: str):
    """Загружает локальный файл в «Избранное», обновляя статус-сообщение."""
    chat_id, msg_id = item["chat_id"], item["msg_id"]
    batch = batches.get(batch_id)
    if batch is None:
        return
    key = (chat_id, msg_id)
    st = batch["link_states"].get(key)
    if st is None:
        return

    full_link = item["link"]  # для ручных — это имя файла
    display_name = os.path.basename(path)
    file_size = os.path.getsize(path)

    st["status"] = "uploading"
    st["up_current"] = 0
    st["up_total"] = file_size
    st["up_percent"] = 0.0
    st["last_up_cb"] = 0.0
    _mark_item(batch_id, chat_id, msg_id, status="uploading")
    await _update_status_msg(batch, key, full_link, force=True)

    async def upload_cb(current, total):
        now = time.time()
        if now - st.get("last_up_cb", 0) > 3 or current == total:
            st["last_up_cb"] = now
            st["up_current"] = current
            st["up_total"] = total
            st["up_percent"] = (current / total) * 100 if total else 0
            if key in batch["status_msgs"]:
                await _update_status_msg(batch, key, full_link)

    async with upload_lock:
        sent = await _send_media("me", path, display_name, upload_cb)

    sent_msg = sent[0] if isinstance(sent, list) else sent
    sent_id = getattr(sent_msg, "id", None)

    _log(
        f"✅ Файл загружен в «Избранное»: {display_name} "
        f"→ сообщение id={sent_id}, размер={human_size(file_size)}"
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
        try:
            await status_msg.delete()
        except Exception as e:
            _log(f"⚠️  не удалось удалить статус файла: {e}")

    try:
        if os.path.exists(path):
            os.remove(path)
            _log(f"🧹 удалён локальный файл: {display_name}")
    except Exception as e:
        _log(f"⚠️  не удалось удалить {display_name}: {e}")


async def _run_manual_upload(batch_id: int, item: dict, path: str):
    b = batches.get(batch_id)
    try:
        await upload_manual_file(batch_id, item, path)
        if b is not None:
            b["ok"] += 1
    except Exception as e:
        _log(f"❌  Файл {os.path.basename(path)}: {type(e).__name__}: {e}")
        _mark_item(batch_id, item["chat_id"], item["msg_id"], status="fail")
        if b is not None:
            b["fail"] += 1
    finally:
        await _maybe_finish_batch(batch_id)

async def enqueue_manual_files(paths: list[str]):
    if not paths:
        return

    async with batch_create_lock:
        batch_counter["id"] += 1
        batch_id = batch_counter["id"]

        items, link_states, status_msgs = [], {}, {}

        for idx, path in enumerate(paths, start=1):
            name = os.path.basename(path)
            status_msg = await client.send_message(
                "me", f"📋 {name} (локальный файл)"
            )
            key = ("me", -idx)
            status_msgs[key] = status_msg
            link_states[key] = {
                "status": "downloaded",
                "percent": 0.0, "current": 0, "total": 0,
                "last_cb": 0.0, "last_edit": 0.0,
                "up_current": 0, "up_total": 0,
                "up_percent": 0.0, "last_up_cb": 0.0,
            }
            items.append({
                "chat_id": "me",
                "msg_id": -idx,
                "link": name,
                "status": "downloaded",
                "status_msg_id": status_msg.id,
                "slot_msg_id": None,
                "file_message_id": None,
                "local_file": name,
                "downloaded_at": time.time(),
            })

        _create_queue(batch_id, items)
        batches[batch_id] = {
            "event": _make_fake_event(),
            "links": [(it["chat_id"], it["msg_id"], it["link"]) for it in items],
            "status_msgs": status_msgs,
            "link_states": link_states,
            "lock": asyncio.Lock(),
            "reorder_lock": asyncio.Lock(),
            "reorder_state": {"running": False, "pending": False, "last": 0.0},
            "total": len(items),
            "ok": 0, "skip": 0, "fail": 0,
            "finished": False,
            "to_delete": [],
            "is_manual": True,
        }

        for it in items:
            p = os.path.join(DOWNLOAD_DIR, it["local_file"])
            asyncio.create_task(_run_manual_upload(batch_id, it, p))

# ---------- Обработчик сообщений ----------
@client.on(events.NewMessage(chats="me"))
async def handle_saved_messages(event):
    text = event.raw_text
    cmd = text.strip().lower()

    # ---------- /stop ----------
    if cmd == "/stop":
        cancel_flag["cancel"] = True
        await event.respond(
            "⛔ Новые ссылки будут пропущены. "
            "Уже скачиваемые загрузки продолжатся."
        )
        return

    # ---------- /resume ----------
    if cmd == "/resume":
        cancel_flag["cancel"] = False
        await event.respond("▶️ Обработка новых ссылок возобновлена.")
        return

    # ---------- /quit ----------
    if cmd in ("/quit", "/exit", "/shutdown"):
        cancel_flag["cancel"] = True
        await event.respond(
            "🛑 Останавливаюсь. Текущие загрузки завершатся, затем выход."
        )
        shutdown_event.set()
        return

    # ---------- /clear ----------
    if cmd == "/clear":
        cancel_flag["cancel"] = False

        # ✅ Предупреждаем, если воркеры уже что-то качают:
        # их элементы уже в process_link, /clear их не отменит.
        # Файлы скачаются и удалятся, но прогресс не отобразится.
        # Сообщение отправляем ДО лока — пользователь увидит его сразу,
        # не дожидаясь, пока освободится batch_create_lock.
        if in_progress["count"] > 0:
            await event.respond(
                f"⏳ В работе: {in_progress['count']} загрузок — "
                f"они завершатся, новые отменены."
            )

        # Лок нужен, чтобы /clear не столкнулся с созданием batch
        # в другом обработчике (между _create_queue и batches[...]).
        async with batch_create_lock:
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

    # ---------- /stats ----------
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

    # ---------- Ссылки ----------
    # Регистронезависимая проверка — на случай HTTPS://T.ME/...
    if "t.me/" not in text.lower():
        return

    links = extract_all_links(text)
    if not links:
        return

    # Всё создание batch — под локом, чтобы параллельные сообщения
    # не получили одинаковый batch_id и не перемешали порядок сообщений.
    async with batch_create_lock:
        batch_counter["id"] += 1
        batch_id = batch_counter["id"]

        link_states: dict[tuple, dict] = {}
        status_msgs: dict[tuple, any] = {}
        items: list[dict] = []

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
            "ok": 0,
            "skip": 0,
            "fail": 0,
            "finished": False,
            "to_delete": [],
        }

        for chat_id, msg_id, full_link in links:
            await link_queue.put((chat_id, msg_id, full_link, batch_id))

    # Удаление исходного сообщения — вне лока: не блокирует создание
    # других batch'ей и не может «съесть» ответ другого обработчика.
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
            _log(
                f"🗑  удалено исходное сообщение "
                f"(после FloodWait, batch #{batch_id})"
            )
        except Exception:
            pass
    except Exception:
        pass

# ---------- Запуск ----------
async def main():
    # Синхронизация batch_counter с существующими JSON
    existing_ids = _list_queue_files()
    if existing_ids:
        batch_counter["id"] = max(existing_ids)
        _log(f"🔢 batch_counter инициализирован: {batch_counter['id']}")

    # --- 1. Классификация файлов в downloads/ (до любых удалений) ---
    scan = _scan_downloads()

    manual_send = False
    if scan["manual"]:
        print(f"\n📂 Найдено файлов без записи в очереди: {len(scan['manual'])}")
        for f in scan["manual"][:20]:
            try:
                sz = human_size(os.path.getsize(f))
            except Exception:
                sz = "?"
            print(f"   • {os.path.basename(f)} ({sz})")
        if len(scan["manual"]) > 20:
            print(f"   … и ещё {len(scan['manual']) - 20}")
        ans = (await asyncio.to_thread(
            input, "\nОтправить их в Избранное? [y/n]: "
        )).strip().lower()
        manual_send = ans in ("y", "yes", "да", "д")

    if scan["own_partial"]:
        print(
            f"\n📂 Найдено недокачанных файлов "
            f"(есть запись в очереди): {len(scan['own_partial'])} — перекачиваю"
        )
        for _, it, f in scan["own_partial"][:20]:
            print(f"   • {os.path.basename(f)} ← {it['link']}")
        if len(scan["own_partial"]) > 20:
            print(f"   … и ещё {len(scan['own_partial']) - 20}")

    # ✅ НОВЫЙ БЛОК: информируем о готовых к отправке файлах.
    # _scan_downloads уже нашёл их (own_ready), но раньше мы не выводили.
    # Теперь пользователь видит, что скрипт подхватил — до того, как
    # offer_send_downloaded начнёт ставить их в link_queue.
    if scan["own_ready"]:
        print(
            f"\n📂 Найдено готовых к отправке файлов "
            f"(есть запись в очереди): {len(scan['own_ready'])} — "
            f"поставлю в очередь на отправку"
        )
        for _, it, f in scan["own_ready"][:20]:
            print(f"   • {os.path.basename(f)} ← {it['link']}")
        if len(scan["own_ready"]) > 20:
            print(f"   … и ещё {len(scan['own_ready']) - 20}")

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

    # ✅ Watchdog для зависших batch (элементы downloaded/uploading,
    # которые не поставил offer_send_downloaded)
    watchdog_task = asyncio.create_task(_watchdog_stuck_batches(interval=60.0))
    print("  🐕 Запущен watchdog зависших пачек")

    # --- 2. Разбираемся со «своими недокачанными» ---
    for qid, it, full in scan["own_partial"]:
        try:
            os.remove(full)
            _log(f"🧹 удалён недокачанный файл: {os.path.basename(full)}")
        except Exception as e:
            _log(f"⚠️  не удалось удалить {full}: {e}")

        # ✅ Правим JSON напрямую — не зависим от состояния `queues` в памяти.
        q = queues.get(qid) or _load_queue(qid)
        if q is None:
            _log(f"⚠️  очередь #{qid} не найдена — пропуск {it['link']}")
            continue

        target = None
        for entry in q.get("items", []):
            if (entry.get("chat_id") == it["chat_id"]
                    and entry.get("msg_id") == it["msg_id"]):
                target = entry
                break

        if target is None:
            _log(
                f"⚠️  элемент {it['chat_id']}/{it['msg_id']} "
                f"не найден в очереди #{qid}"
            )
            continue

        target["status"] = "pending"
        target["local_file"] = None
        target["downloaded_at"] = None

        # Сохраняем на диск (и синхронизируем с памятью)
        queues[qid] = q
        _save_queue(qid)

        _log(f"♻️  {it['link']}: сброшен в pending — будет перекачан")

    # --- 3. Штатное восстановление очередей ---
    await resume_pending_queues()
    await offer_send_downloaded()

     # ✅ Дополнительная страховка: если offer_send_downloaded не поставил
    # ничего в очередь (все downloaded-элементы потеряли файлы и стали fail),
    # закрываем «мёртвые» JSON.
    for qid in _list_queue_files():
        _close_queue_if_finished(qid)

    # --- 4. «Ручные» файлы (y → создаём JSON + показываем прогресс) ---
    if manual_send:
        await enqueue_manual_files(scan["manual"])

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
        # ✅ Останавливаем watchdog раньше воркеров — чтобы он не начал
        # новую постановку в очередь во время завершения
        watchdog_task.cancel()

        for w in workers:
            w.cancel()
        await asyncio.gather(
            watchdog_task, *workers, return_exceptions=True
        )

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