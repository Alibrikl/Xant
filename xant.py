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

processed: set[tuple] = set()
cancel_flag = {"cancel": False}
in_progress = {"count": 0}

batches: dict[int, dict] = {}
batch_counter = {"id": 0}

account_state = {"is_premium": False}

queues: dict[int, dict] = {}

link_queue = None
batch_create_lock = None
upload_lock = None
shutdown_event = None

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
        try:
            m = await client.get_messages("me", ids=cached)
            if m is None:
                _find_cache.pop(cache_key, None)
            else:
                _log(f"  💾 _find_in_me({display_name}): из кэша (id={cached})")
                return cached
        except Exception:
            return cached

    check_size = bool(file_size and file_size > 0)
    attempt = 0
    while True:
        try:
            # ✅ Серверный search=display_name не работает для имён файлов
            # (Telegram ищет по тексту сообщений, не по DocumentAttributeFilename).
            # Сразу полный скан — экономит один бесполезный запрос.
            found: int | None = None
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
    # ✅ Manual-батчи не сохраняем на диск — при рестарте
    # пользователь снова ответит y/n (лучше, чем дубликат)
    if not batches.get(batch_id, {}).get("is_manual"):
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

    Файлы со статусами done/fail/skip в JSON — аномалия (файл должен был
    быть удалён или не создаваться). Считаем их manual, чтобы спросить
    пользователя при старте, а не терять.
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
            # Файла нет в JSON — ручной
            manual.append(full)
            continue

        qid, it = entry
        status = it.get("status")

        if status in ("pending", "downloading"):
            # Недокачан — перекачать
            own_partial.append((qid, it, full))
        elif status in ("downloaded", "uploading"):
            # Готов к отправке — поставить в очередь
            own_ready.append((qid, it, full))
        else:
            # done / fail / skip с файлом на диске — аномалия.
            # Файл должен был быть удалён (done) или не создаваться (fail/skip).
            # Считаем manual, чтобы не потерять: спросим пользователя при старте.
            print(
                f"  ⚠️  файл {name}: статус «{status}» в JSON, "
                f"но файл на диске — считаю manual"
            )
            manual.append(full)

    return {"manual": manual, "own_partial": own_partial, "own_ready": own_ready}

# ---------- Скачивание ----------

def _cleanup_partial(local_path: str, expected_size: int = 0):
    """
    Удаляет частично скачанный файл и его .part-версию.
    Если expected_size > 0 и файл уже полного размера — НЕ трогает
    (значит, скачивание фактически завершилось, просто упало на
    пост-обработке).
    Возвращает True, если что-то удалили.
    """
    if not local_path:
        return False

    removed = False
    for path in (local_path, local_path + ".part"):
        if not os.path.exists(path):
            continue
        try:
            if expected_size > 0:
                actual = os.path.getsize(path)
                if actual >= expected_size:
                    # файл полного размера — не наш случай, не трогаем
                    continue
            os.remove(path)
            removed = True
            _log(f"[cleanup] удалён частичный файл: {os.path.basename(path)}")
        except Exception as e:
            _log(f"[cleanup] не удалось удалить {path}: {e}")
    return removed


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

        if size_holder["size"] > 0:
            try:
                free = shutil.disk_usage(DOWNLOAD_DIR).free
                need = size_holder["size"] * 1.1
                if free < need:
                    print(
                        f"\n[disk check] недостаточно места для "
                        f"{chat_id}/{msg_id}: нужно {human_size(need)}, "
                        f"свободно {human_size(free)} — пропуск"
                    )
                    return None
            except Exception as e:
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
            _cleanup_partial(local_path, size_holder.get("size", 0) if size_holder else 0)
            if attempt >= max_retries:
                raise
            target_msg = await client.get_messages(chat_id, ids=msg_id, use_cache=False)
            if not target_msg or not target_msg.media:
                return None
            # ✅ обновляем размер — файл в канале мог измениться
            if size_holder is not None:
                size_holder["size"] = target_msg.file.size if target_msg.file else 0

            # ✅ запоминаем старый путь, чтобы после unique_path()
            # обнаружить смену имени (файл заменили в канале)
            old_path = local_path

            suggested_filename = build_filename(target_msg)
            local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))

            # ✅ Если путь сменился, а старый файл остался (был полным
            # при первой попытке — _cleanup_partial его не тронул),
            # удаляем его: в JSON уже записан новый local_file,
            # старый файл стал бы сиротой.
            if old_path != local_path and os.path.exists(old_path):
                try:
                    os.remove(old_path)
                    _log(
                        f"  🧹 FileReferenceExpired: путь сменился "
                        f"({os.path.basename(old_path)} → "
                        f"{os.path.basename(local_path)}) — "
                        f"старый файл удалён"
                    )
                except Exception as e:
                    _log(
                        f"  ⚠️  FileReferenceExpired: путь сменился, "
                        f"но старый файл не удалён ({old_path}): {e}"
                    )
            elif old_path != local_path:
                _log(
                    f"  ℹ️  FileReferenceExpired: путь сменился "
                    f"({os.path.basename(old_path)} → "
                    f"{os.path.basename(local_path)}), "
                    f"старого файла на диске нет"
                )

            if size_holder is not None:
                size_holder["path"] = local_path
            if on_path_known is not None:
                on_path_known(local_path)

        except FloodWaitError as fwe:
            # ✅ чистим частичный файл ПЕРЕД ожиданием:
            # иначе после паузы download_media может дописать в него
            _cleanup_partial(local_path, size_holder.get("size", 0) if size_holder else 0)
            print(f"\n[FloodWait] Ждём {fwe.seconds} сек...")
            await asyncio.sleep(fwe.seconds)

        except Exception as e:
            attempt += 1
            print(
                f"[download_with_retry] {type(e).__name__} "
                f"(попытка {attempt}/{max_retries}): {e}"
            )
            # ✅ чистим частичный файл ПЕРЕД следующей попыткой
            _cleanup_partial(local_path, size_holder.get("size", 0) if size_holder else 0)
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

def _initial_status_text(it: dict) -> str:
    """Начальный текст статус-сообщения для восстановленного элемента."""
    link = it.get("link", "")
    status = it.get("status", "pending")
    if status == "downloading":
        return f"⬇️ 0.0% {link}"
    if status == "downloaded":
        return f"⏳ ожидание... {link}"
    if status == "uploading":
        return f"📤 0.0% {link}"
    # pending и всё остальное
    return f"📋 {link}"

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
                    # ✅ done-слот НЕ удаляем: иначе reconcile форвардит
                    # file_message_id заново, а оригинал остаётся в чате
                    # → дубликаты файла.
                    if it.get("status") == "done" and k == "slot_msg_id":
                        continue
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
                # ✅ Слот уже есть в чате — НЕ пересоздаём.
                # Иначе forward_messages создаст копию file_message_id,
                # а оригинал останется → дубликаты.
                if it.get("slot_msg_id"):
                    _log(
                        f"  ⏭  ПРОПУСК пересылки {tag}: "
                        f"слот {it['slot_msg_id']} уже в чате"
                    )
                    continue

                # Слот отсутствует (был удалён вручную или при краше).
                # Оригинал file_message_id НЕ трогаем — он уже в чате.
                # Создаём текстовый плейсхолдер как слот (без forward),
                # чтобы slot_msg_id указывал на реально существующее
                # сообщение и следующая перетусовка не сломалась.
                try:
                    msg = await client.send_message("me", f"✅ {full_link}")
                    it["slot_msg_id"] = msg.id
                    it["status_msg_id"] = None
                    _save_queue(batch_id)
                    _log(
                        f"  ✅ {tag}: создан плейсхолдер {msg.id} "
                        f"(оригинал {it['file_message_id']} не тронут)"
                    )
                except FloodWaitError as fwe:
                    _log(
                        f"  ⏳ FloodWait {fwe.seconds}s при создании "
                        f"плейсхолдера {tag}"
                    )
                    await asyncio.sleep(fwe.seconds)
                    try:
                        msg = await client.send_message(
                            "me", f"✅ {full_link}"
                        )
                        it["slot_msg_id"] = msg.id
                        it["status_msg_id"] = None
                        _save_queue(batch_id)
                        _log(
                            f"  ✅ {tag}: создан плейсхолдер {msg.id} "
                            f"(после FloodWait)"
                        )
                    except Exception as e:
                        _log(
                            f"  ⚠️  {tag}: не удалось создать "
                            f"плейсхолдер: {e}"
                        )
                except Exception as e:
                    _log(
                        f"  ⚠️  {tag}: не удалось создать "
                        f"плейсхолдер: {e}"
                    )
                # slot_msg_id останется None — попробуем при след. reconcile
                continue

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
# ---------- Хелпер ----------
def _release_processed(key: tuple, result: str) -> str:
    """
    Снимает блокировку processed, если элемент НЕ отложен.
    'deferred' оставляем в processed — watchdog подхватит.
    Возвращает result без изменений (удобно для `return _release_processed(...)`).
    """
    if result != "deferred":
        processed.discard(key)
    return result

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
        _release_processed(key, "fail")
        return "fail"

    item = _find_item(batch_id, chat_id, msg_id)
    if item is None:
        _release_processed(key, "fail")
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
            # ✅ Не перезаписываем терминальные статусы (гонка с watchdog)
            cur_item = _find_item(batch_id, chat_id, msg_id)
            if cur_item and cur_item.get("status") in ("fail", "done", "skip"):
                return
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
                    _release_processed(key, "deferred")
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
                        upload_retries=0,       # ✅ сброс счётчика
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
                    # Обнуляем status_msg_id в JSON,
                    # т.к. сообщение уже удалено (или попытались удалить).
                    # Делаем это ПОСЛЕ delete(), чтобы не затирать id,
                    # если delete() упадёт — попробуем при следующем reconcile.
                    _mark_item(batch_id, chat_id, msg_id, status_msg_id=None)
                    if REORDER_AFTER_EACH_FILE:
                        await request_reconcile(batch_id, force=False)
                    return _release_processed(key, "ok")
                else:
                    _log(
                        f"📂 {full_link}: в me не найден — "
                        f"переотправлю"
                    )

            st["status"] = "downloaded"
            _mark_item(
                batch_id, chat_id, msg_id,
                status="downloaded",
                file_size=os.path.getsize(filename),
            )
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
                _release_processed(key, "fail")
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
                _release_processed(key, "fail")
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
                file_size=size_holder["size"],
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
                # ✅ Не перезаписываем терминальные статусы (гонка с watchdog)
                cur_item = _find_item(batch_id, chat_id, msg_id)
                if cur_item and cur_item.get("status") in ("fail", "done", "skip"):
                    return
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

        # ✅ Правка 1: сброс upload_retries при успехе
        _mark_item(
            batch_id, chat_id, msg_id,
            status="done",
            file_message_id=sent_id,
            local_file=None,
            downloaded_at=None,
            upload_retries=0,
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
        # Обнуляем status_msg_id в JSON после удаления
        _mark_item(batch_id, chat_id, msg_id, status_msg_id=None)

        if os.path.exists(filename):
            os.remove(filename)
            _log(f"🧹 удалён локальный файл: {os.path.basename(filename)}")

        if REORDER_AFTER_EACH_FILE:
            await request_reconcile(batch_id, force=False)
        return _release_processed(key, "ok")

    except Exception as e:
        _log(f"❌ {full_link}: {type(e).__name__}: {e}")

        # ✅ filename может быть None, если download_with_retry бросил
        # исключение на последней попытке. Но size_holder["path"]
        # устанавливается ДО цикла попыток и всегда актуален.
        fallback_path = size_holder.get("path")
        expected = size_holder.get("size", 0)

        if fallback_path and os.path.exists(fallback_path):
            try:
                actual = os.path.getsize(fallback_path)
            except Exception:
                actual = 0

            if expected > 0 and actual >= expected:
                # файл целый — не теряем, помечаем downloaded,
                # чтобы watchdog / рестарт переотправил
                local_name = os.path.basename(fallback_path)
                _mark_item(
                    batch_id, chat_id, msg_id,
                    status="downloaded",
                    local_file=local_name,
                    downloaded_at=time.time(),
                    file_size=expected,
                )
                st["status"] = "downloaded"
                _log(
                    f"♻️  {full_link}: файл целый "
                    f"({human_size(actual)}) — помечен downloaded "
                    f"для повторной отправки"
                )
                await _update_status_msg(batch, key, full_link, force=True)
                _release_processed(key, "deferred")
                if REORDER_AFTER_EACH_FILE:
                    await request_reconcile(batch_id, force=False)
                # ✅ "deferred" — НЕ инкрементим fail, batch остаётся
                # открытым, watchdog/рестарт подхватит downloaded
                return "deferred"
            else:
                # частичный файл — чистим и помечаем fail
                _cleanup_partial(fallback_path, expected)
                _mark_item(batch_id, chat_id, msg_id,
                           status="fail", local_file=None)
                st["status"] = "fail"
                _log(
                    f"🗑  {full_link}: частичный файл "
                    f"({human_size(actual)} из {human_size(expected)}) "
                    f"удалён — fail"
                )
        else:
            # файла на диске нет — обычный fail
            _mark_item(batch_id, chat_id, msg_id, status="fail")
            st["status"] = "fail"

        await _update_status_msg(batch, key, full_link, force=True)
        _release_processed(key, "fail")
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
                # ✅ Определяем q ДО всех проверок — используется и в manual,
                # и в обычной ветке.
                q = queues.get(batch_id)
                if q is None:
                    continue

                if b.get("is_manual"):
                    # ✅ Исключаем элементы, которые уже в работе
                    uploading_keys = b.get("_uploading", set())
                    stuck_manual = [
                        it for it in q.get("items", [])
                        if it.get("status") in ("downloaded", "uploading")
                        and (it["chat_id"], it["msg_id"]) not in uploading_keys
                    ]
                    if not stuck_manual:
                        continue
                    # Аналогичная логика таймаута (60+60 сек)
                    mkey = tuple(id(it) for it in stuck_manual)
                    mwd = b.setdefault("_wd_manual", {})
                    if mwd.get("key") != mkey:
                        mwd["key"] = mkey
                        mwd["ts"] = now
                        _log(
                            f"🐕 watchdog (manual): batch #{batch_id} — "
                            f"{len(stuck_manual)} зависших. Дам {interval:.0f}s..."
                        )
                        continue
                    if now - mwd.get("ts", now) < interval:
                        continue

                    for it in stuck_manual:
                        local = it.get("local_file")
                        if not local or not os.path.exists(
                                os.path.join(DOWNLOAD_DIR, local)):
                            _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                       status="fail", local_file=None)
                            b["fail"] = b.get("fail", 0) + 1
                            _log(f"  🐕 #{batch_id} {it['link']}: файл пропал → fail")
                            continue
                        retries = it.get("upload_retries", 0)
                        if retries >= 3:
                            _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                       status="fail", local_file=None)
                            b["fail"] = b.get("fail", 0) + 1
                            _log(
                                f"  🐕 #{batch_id} {it['link']}: "
                                f"превышено попыток ({retries}) → fail"
                            )
                            continue
                        _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                   upload_retries=retries + 1,
                                   status="downloaded")
                        asyncio.create_task(
                            _run_manual_upload(
                                batch_id, it,
                                os.path.join(DOWNLOAD_DIR, local)
                            )
                        )
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"повторно в manual upload (попытка {retries + 1}/3)"
                        )
                    await _maybe_finish_batch(batch_id)
                    mwd["key"] = None
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
                    # ✅ Пропускаем, если элемент уже терминальный
                    # (гонка: process_link мог успеть поставить done/fail,
                    #  пока мы собирали candidates)
                    cur = _find_item(batch_id, it["chat_id"], it["msg_id"])
                    if cur is None:
                        continue
                    if cur.get("status") in ("done", "fail", "skip"):
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"уже {cur.get('status')} — пропуск"
                        )
                        continue

                    # ✅ Пропуск, если process_link уже работает над элементом.
                    # send_file для 1 GB идёт минуты, watchdog за это время
                    # успевает 3 раза инкрементить upload_retries → fail,
                    # хотя файл успешно отправляется.
                    if (it["chat_id"], it["msg_id"]) in processed:
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"уже в работе (process_link) — пропуск"
                        )
                        continue

                    local = cur.get("local_file")
                    full_path = os.path.join(DOWNLOAD_DIR, local) if local else None
                    file_exists = bool(full_path and os.path.exists(full_path))

                    # ✅ uploading без файла на диске — проверяем в me
                    if cur.get("status") == "uploading" and not file_exists:
                        if not local:
                            # ✅ Защита от бесконечного цикла:
                            # если upload_retries >= 3 — уже пробовали, пропускаем
                            if cur.get("upload_retries", 0) >= 3:
                                _log(
                                    f"  🐕 #{batch_id} {it['link']}: "
                                    f"uploading без local_file "
                                    f"(уже {cur.get('upload_retries')} попыток) — пропуск"
                                )
                                continue
                            _mark_item(
                                batch_id, it["chat_id"], it["msg_id"],
                                status="fail", local_file=None,
                                upload_retries=3,   # ✅ фиксируем, что пробовали
                            )
                            b["fail"] = b.get("fail", 0) + 1
                            any_fail = True
                            _log(
                                f"  🐕 #{batch_id} {it['link']}: "
                                f"uploading без local_file → fail"
                            )
                            continue

                        _log(f"  🐕 #{batch_id} {it['link']}: uploading без файла — проверяю me")
                        try:
                            found_id = await _find_in_me(
                                display_name=local,
                                file_size=cur.get("file_size"),
                            )
                        except FloodWaitError:
                            _log(f"  🐕 #{batch_id} {it['link']}: FloodWait — откладываю")
                            continue
                        if found_id is not None:
                            _mark_item(
                                batch_id, it["chat_id"], it["msg_id"],
                                status="done", file_message_id=found_id,
                                local_file=None,
                                upload_retries=0,
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
                                upload_retries=3,   # ✅ фиксируем
                            )
                            b["fail"] = b.get("fail", 0) + 1
                            any_fail = True
                            _log(f"  ❌ #{batch_id} {it['link']}: uploading → fail")
                        continue

                    # downloaded (или uploading с файлом — считаем как downloaded)
                    if not file_exists:
                        # ✅ Защита от бесконечного цикла
                        if cur.get("upload_retries", 0) >= 3:
                            _log(
                                f"  🐕 #{batch_id} {it['link']}: "
                                f"файл пропал (уже {cur.get('upload_retries')} попыток) — пропуск"
                            )
                            continue
                        _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                   status="fail", local_file=None,
                                   upload_retries=3)
                        b["fail"] = b.get("fail", 0) + 1
                        any_fail = True
                        _log(f"  🐕 #{batch_id} {it['link']}: файл пропал → fail")
                        continue

                    retries = cur.get("upload_retries", 0)
                    if retries >= 3:
                        # ✅ Защита от бесконечного цикла
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

        # ============================================================
        # ЭТАП 1. Обработка uploading-элементов
        # ============================================================
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
                        display_name=local, file_size=it.get("file_size"),
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
            _save_queue(qid)      # ← СОХРАНИТЬ прямые правки

        # ============================================================
        # ЭТАП 2. Обработка pending-элементов (перекачка)
        # ============================================================
        # pending = элемент был в очереди, но не начал качаться
        # (например, own_partial сброшен в pending в main()).
        # Нужно поставить его в link_queue, предварительно создав
        # batch в памяти с link_states и статус-сообщениями.
        pending_items = [
            it for it in items
            if it.get("status") == "pending"
        ]

        if pending_items:
            _log(
                f"  🔄 #{qid}: найдено pending-элементов для перекачки: "
                f"{len(pending_items)}"
            )

            # Проверяем, есть ли уже batch (не должно быть при старте,
            # но на всякий случай — вдруг resume вызван повторно)
            b = batches.get(qid)
            if b is None:
                # Создаём batch для восстановления
                status_msgs: dict[tuple, any] = {}
                link_states: dict[tuple, dict] = {}

                for it in items:
                    key = (it["chat_id"], it["msg_id"])

                    # link_states для ВСЕХ элементов (не только pending),
                    # чтобы reconcile и _update_status_msg работали корректно
                    st = {
                        "status": it.get("status", "pending"),
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

                    # Статус-сообщение: пробуем переиспользовать
                    # status_msg_id / slot_msg_id из JSON
                    existing_msg = None
                    for field in ("status_msg_id", "slot_msg_id"):
                        mid = it.get(field)
                        if not mid:
                            continue
                        try:
                            m = await client.get_messages("me", ids=mid)
                            if m:
                                existing_msg = m
                                break
                        except FloodWaitError as fwe:
                            _log(
                                f"  ⏳ #{qid} {it['link']}: FloodWait "
                                f"{fwe.seconds}s при чтении статус-сообщения"
                            )
                            await asyncio.sleep(fwe.seconds)
                        except Exception:
                            pass

                    if existing_msg is not None:
                        status_msgs[key] = existing_msg
                    elif it.get("status") in (
                        "pending", "downloading", "downloaded", "uploading"
                    ):
                        # Для активных статусов — создаём новое сообщение
                        try:
                            text = _initial_status_text(it)
                            m = await client.send_message("me", text)
                            status_msgs[key] = m
                            it["status_msg_id"] = m.id
                            it["slot_msg_id"] = m.id
                            _log(
                                f"  ➕ #{qid} {it['link']}: создан статус "
                                f"(id={m.id})"
                            )
                        except FloodWaitError as fwe:
                            _log(
                                f"  ⏳ #{qid} {it['link']}: FloodWait "
                                f"{fwe.seconds}s при создании статуса"
                            )
                            await asyncio.sleep(fwe.seconds)
                            try:
                                m = await client.send_message(
                                    "me", _initial_status_text(it)
                                )
                                status_msgs[key] = m
                                it["status_msg_id"] = m.id
                                it["slot_msg_id"] = m.id
                            except Exception as e:
                                _log(
                                    f"  ⚠️  #{qid} {it['link']}: не удалось "
                                    f"создать статус: {e}"
                                )
                        except Exception as e:
                            _log(
                                f"  ⚠️  #{qid} {it['link']}: не удалось "
                                f"создать статус: {e}"
                            )

                # Создаём batch
                batches[qid] = {
                    "event": _make_fake_event(),
                    "links": [
                        (it["chat_id"], it["msg_id"], it["link"])
                        for it in items
                    ],
                    "status_msgs": status_msgs,
                    "link_states": link_states,
                    "lock": asyncio.Lock(),
                    "reorder_lock": asyncio.Lock(),
                    "reorder_state": {
                        "running": False, "pending": False, "last": 0.0,
                    },
                    "total": len(items),
                    "ok": sum(
                        1 for it in items if it.get("status") == "done"
                    ),
                    "skip": sum(
                        1 for it in items if it.get("status") == "skip"
                    ),
                    "fail": sum(
                        1 for it in items if it.get("status") == "fail"
                    ),
                    "finished": False,
                    "to_delete": [],
                }
                _save_queue(qid)
                _log(
                    f"  🆕 #{qid}: создан batch для восстановления "
                    f"(total={len(items)}, "
                    f"pending={len(pending_items)}, "
                    f"status_msgs={len(status_msgs)})"
                )
            else:
                # batch уже есть — обновим только pending-статусы
                for it in pending_items:
                    key = (it["chat_id"], it["msg_id"])
                    if key not in b["link_states"]:
                        b["link_states"][key] = {
                            "status": "pending",
                            "percent": 0.0, "current": 0, "total": 0,
                            "last_cb": 0.0, "last_edit": 0.0,
                            "up_current": 0, "up_total": 0,
                            "up_percent": 0.0, "last_up_cb": 0.0,
                        }
                    else:
                        b["link_states"][key]["status"] = "pending"

            # Ставим pending в link_queue
            placed = 0
            for it in pending_items:
                try:
                    await link_queue.put(
                        (it["chat_id"], it["msg_id"], it["link"], qid)
                    )
                    placed += 1
                    _log(
                        f"  📥 #{qid} {it['link']}: pending → "
                        f"в очередь на скачивание"
                    )
                except Exception as e:
                    _log(
                        f"  ❌ #{qid} {it['link']}: не удалось поставить "
                        f"pending в очередь: {type(e).__name__}: {e}"
                    )
            if placed:
                _log(
                    f"  📤 #{qid}: поставлено pending в очередь: {placed}"
                )

        # ✅ Закрываем очередь, если после обработки uploading
        # все элементы оказались в терминальных статусах.
        _close_queue_if_finished(qid)


def _close_queue_if_finished(qid: int) -> bool:
    # ✅ Не трогаем JSON, если batch активен — пусть _maybe_finish_batch
    #    сделает финальную перетусовку и закроет сам.
    b = batches.get(qid)
    if b is not None and not b.get("finished"):
        return False
    
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

    # ============================================================
    # ✅ ЭТАП 0. Восстановление batch для qid, у которых его нет в памяти.
    #
    # Типичный случай: в JSON-очереди остались только downloaded/uploading
    # элементы, но НЕТ pending. Тогда resume_pending_queues() не создаёт
    # batch (там создание только при наличии pending_items), и
    # offer_send_downloaded падал в failed с причиной
    # "batch не восстановлен" — файл никогда не отправлялся.
    #
    # Теперь: для каждого qid без batch — создаём его из JSON.
    # ============================================================
    missing_qids = {qid for qid, _ in items if qid not in batches}
    for qid in missing_qids:
        q = _load_queue(qid)
        if not q:
            _log(f"  ⚠️  #{qid}: JSON не читается — batch не восстановлен")
            continue

        q_items = q.get("items", [])
        if not q_items:
            _log(f"  ⚠️  #{qid}: очередь пустая — batch не восстановлен")
            continue

        try:
            status_msgs: dict[tuple, any] = {}
            link_states: dict[tuple, dict] = {}

            for it in q_items:
                key = (it["chat_id"], it["msg_id"])

                # link_states для всех элементов, чтобы reconcile
                # и _update_status_msg работали корректно
                link_states[key] = {
                    "status": it.get("status", "downloaded"),
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

                # Пробуем переиспользовать существующее статус-сообщение
                mid = it.get("status_msg_id") or it.get("slot_msg_id")
                if mid:
                    try:
                        m = await client.get_messages("me", ids=mid)
                        if m:
                            status_msgs[key] = m
                    except FloodWaitError as fwe:
                        _log(
                            f"  ⏳ #{qid} {it['link']}: FloodWait "
                            f"{fwe.seconds}s при чтении статус-сообщения"
                        )
                        await asyncio.sleep(fwe.seconds)
                        try:
                            m = await client.get_messages("me", ids=mid)
                            if m:
                                status_msgs[key] = m
                        except Exception:
                            pass
                    except Exception:
                        pass

                # Для активных статусов без сообщения — создаём новое,
                # иначе статус не будет обновляться при отправке
                if key not in status_msgs and it.get("status") in (
                    "pending", "downloading", "downloaded", "uploading"
                ):
                    try:
                        m = await client.send_message(
                            "me", _initial_status_text(it)
                        )
                        status_msgs[key] = m
                        it["status_msg_id"] = m.id
                        it["slot_msg_id"] = m.id
                        _log(
                            f"  ➕ #{qid} {it['link']}: создан статус "
                            f"(id={m.id})"
                        )
                    except FloodWaitError as fwe:
                        _log(
                            f"  ⏳ #{qid} {it['link']}: FloodWait "
                            f"{fwe.seconds}s при создании статуса"
                        )
                        await asyncio.sleep(fwe.seconds)
                        try:
                            m = await client.send_message(
                                "me", _initial_status_text(it)
                            )
                            status_msgs[key] = m
                            it["status_msg_id"] = m.id
                            it["slot_msg_id"] = m.id
                        except Exception as e:
                            _log(
                                f"  ⚠️  #{qid} {it['link']}: "
                                f"статус не создан: {e}"
                            )
                    except Exception as e:
                        _log(
                            f"  ⚠️  #{qid} {it['link']}: "
                            f"статус не создан: {e}"
                        )

            batches[qid] = {
                "event": _make_fake_event(),
                "links": [
                    (it["chat_id"], it["msg_id"], it["link"])
                    for it in q_items
                ],
                "status_msgs": status_msgs,
                "link_states": link_states,
                "lock": asyncio.Lock(),
                "reorder_lock": asyncio.Lock(),
                "reorder_state": {
                    "running": False, "pending": False, "last": 0.0,
                },
                "total": len(q_items),
                "ok": sum(1 for it in q_items if it.get("status") == "done"),
                "skip": sum(1 for it in q_items if it.get("status") == "skip"),
                "fail": sum(1 for it in q_items if it.get("status") == "fail"),
                "finished": False,
                "to_delete": [],
            }
            queues[qid] = q
            _save_queue(qid)
            _log(
                f"  🆕 #{qid}: batch восстановлен в offer_send_downloaded "
                f"(total={len(q_items)}, "
                f"status_msgs={len(status_msgs)}, "
                f"ok={batches[qid]['ok']}, "
                f"fail={batches[qid]['fail']})"
            )
        except Exception as e:
            _log(
                f"  ⚠️  #{qid}: не удалось восстановить batch: "
                f"{type(e).__name__}: {e}"
            )

    # ============================================================
    # ✅ ЭТАП 1. Постановка downloaded/uploading в link_queue
    # ============================================================
    for qid, it in items:
        try:
            b = batches.get(qid)
            if b is None:
                # Не должны сюда попасть — этап 0 должен был создать batch.
                # Но если _load_queue вернул None (битый JSON) — здесь.
                _log(
                    f"  ⚠️  #{qid} {it['link']}: batch не восстановлен — "
                    f"пропуск"
                )
                failed.append((qid, it, "batch не восстановлен"))
                continue

            key = (it["chat_id"], it["msg_id"])

            # Гарантируем статус-сообщение (страховка на случай,
            # если этап 0 не смог создать из-за FloodWait)
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
                "status": "downloaded",
                "percent": 0.0, "current": 0, "total": 0,
                "last_cb": 0.0, "last_edit": 0.0,
                "up_current": 0, "up_total": 0,
                "up_percent": 0.0, "last_up_cb": 0.0,
            })

            await link_queue.put((it["chat_id"], it["msg_id"], it["link"], qid))
            placed += 1
            _log(f"  📤 #{qid} {it['link']}: поставлен в очередь на отправку")

        except Exception as e:
            # ✅ Ошибка одного элемента не ломает остальные
            _log(
                f"  ❌ #{qid} {it['link']}: постановка не удалась: "
                f"{type(e).__name__}: {e}"
            )
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

    # ✅ Возвращаем список «не поставленных» — main() уведомит
    # пользователя, если что-то не удалось.
    return failed

# ---------- Отправка «ручных» файлов (не из JSON) ----------
async def upload_manual_file(batch_id: int, item: dict, path: str) -> bool:
    """
    Загружает локальный файл в «Избранное», обновляя статус-сообщение.
    Возвращает True при успехе. Бросает исключение при любой ошибке —
    _run_manual_upload инкрементит b["fail"] только по исключению.
    """
    chat_id, msg_id = item["chat_id"], item["msg_id"]
    batch = batches.get(batch_id)
    if batch is None:
        # ✅ Бросаем, а не return None — иначе _run_manual_upload посчитает ok
        raise RuntimeError(
            f"upload_manual_file: batch #{batch_id} не в памяти"
        )
    key = (chat_id, msg_id)
    st = batch["link_states"].get(key)
    if st is None:
        # ✅ Бросаем, а не return None — иначе _run_manual_upload посчитает ok
        raise RuntimeError(
            f"upload_manual_file: link_states[{key}] отсутствует "
            f"в batch #{batch_id}"
        )

    # ✅ Защита от гонки: файл могли удалить между сканом и загрузкой.
    # chat_id/msg_id/st уже определены выше — используем их.
    if not os.path.exists(path):
        _log(f"⚠️  manual-файл пропал: {path} → fail")
        _mark_item(batch_id, chat_id, msg_id, status="fail")
        st["status"] = "fail"
        # ✅ Бросаем — _run_manual_upload инкрементит b["fail"]
        raise FileNotFoundError(path)

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
        upload_retries=0,
    )
    st["status"] = "done"

    status_msg = batch["status_msgs"].pop(key, None)
    if status_msg is not None:
        try:
            await status_msg.delete()
        except Exception as e:
            _log(f"⚠️  не удалось удалить статус файла: {e}")
    # Обнуляем status_msg_id в JSON после удаления
    _mark_item(batch_id, chat_id, msg_id, status_msg_id=None)

    try:
        if os.path.exists(path):
            os.remove(path)
            _log(f"🧹 удалён локальный файл: {display_name}")
    except Exception as e:
        _log(f"⚠️  не удалось удалить {display_name}: {e}")

    # ✅ Явный успех — _run_manual_upload инкрементит b["ok"]
    return True

async def _run_manual_upload(batch_id: int, item: dict, path: str):
    key = (item["chat_id"], item["msg_id"])

    # ✅ Помечаем как «в работе» для watchdog
    b_init = batches.get(batch_id)
    if b_init is not None:
        b_init.setdefault("_uploading", set()).add(key)

    try:
        # ✅ upload_manual_file возвращает True при успехе,
        # бросает исключение при любой ошибке.
        ok = await upload_manual_file(batch_id, item, path)
        b = batches.get(batch_id)
        if b is not None:
            if ok:
                b["ok"] += 1
            else:
                # Защита: если функция вернула None/False без исключения —
                # считаем это fail, чтобы счётчики сошлись корректно.
                _log(
                    f"⚠️  _run_manual_upload: upload_manual_file вернул "
                    f"{ok!r} вместо True для {os.path.basename(path)} — fail"
                )
                b["fail"] += 1
    except Exception as e:
        _log(f"❌  Файл {os.path.basename(path)}: {type(e).__name__}: {e}")
        _mark_item(batch_id, item["chat_id"], item["msg_id"], status="fail")
        b = batches.get(batch_id)
        if b is not None:
            b["fail"] += 1
    finally:
        # ✅ Снимаем флаг
        b_fin = batches.get(batch_id)
        if b_fin is not None:
            b_fin.get("_uploading", set()).discard(key)
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
            try:
                _fsize = os.path.getsize(path)
            except Exception:
                _fsize = 0

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
                "file_size": _fsize,
            })

        # ✅ НЕ создаём JSON для manual-батчей — они не должны
        # переживать рестарт. queues[batch_id] создаём только в памяти.
        q = {"batch_id": batch_id, "path": _queue_path(batch_id), "items": items}
        queues[batch_id] = q

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

    # ✅ Дедупликация: одна и та же (chat_id, msg_id) в сообщении
    # не должна попасть в batch дважды — иначе в reconcile появятся
    # два элемента с одинаковым key и один status_msg на двоих.
    seen_keys: set[tuple] = set()
    unique_links: list[tuple] = []
    for link in links:
        lkey = (link[0], link[1])
        if lkey in seen_keys:
            _log(f"♻️  дубликат ссылки в сообщении: {link[2]} — пропуск")
            continue
        seen_keys.add(lkey)
        unique_links.append(link)
    links = unique_links

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


def _print_scan_summary(scan: dict):
    """Информационный вывод о результатах скана downloads/."""
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

    if scan["own_partial"]:
        print(
            f"\n📂 Найдено недокачанных файлов "
            f"(есть запись в очереди): {len(scan['own_partial'])} — перекачиваю"
        )
        for _, it, f in scan["own_partial"][:20]:
            print(f"   • {os.path.basename(f)} ← {it['link']}")
        if len(scan["own_partial"]) > 20:
            print(f"   … и ещё {len(scan['own_partial']) - 20}")

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

# ---------- Запуск ----------
async def main():
    global link_queue, batch_create_lock, shutdown_event, upload_lock
    link_queue = asyncio.Queue()
    batch_create_lock = asyncio.Lock()
    shutdown_event = asyncio.Event()
    upload_lock = asyncio.Lock()
    # Синхронизация batch_counter с существующими JSON
    existing_ids = _list_queue_files()
    if existing_ids:
        batch_counter["id"] = max(existing_ids)
        _log(f"🔢 batch_counter инициализирован: {batch_counter['id']}")

    # --- 1. Классификация файлов в downloads/ ---
    scan = _scan_downloads()
    _print_scan_summary(scan)

    # --- 2. «Свои недокачанные» → удаляем файл + JSON в pending ---
    # ВАЖНО: делаем это ДО подключения — не требует client.
    # После этого JSON-очереди содержат pending-элементы, которые
    # подхватит resume_pending_queues().
    for qid, it, full in scan["own_partial"]:
        try:
            os.remove(full)
            _log(f"🧹 удалён недокачанный файл: {os.path.basename(full)}")
        except Exception as e:
            _log(f"⚠️  не удалось удалить {full}: {e}")

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

        queues[qid] = q
        _save_queue(qid)

        _log(f"♻️  {it['link']}: сброшен в pending — будет перекачан")

    # --- 3. Вопрос про manual-файлы (ДО подключения — быстрый ответ) ---
    manual_send = False
    if scan["manual"]:
        ans = (await asyncio.to_thread(
            input, "\nОтправить их в Избранное? [y/n]: "
        )).strip().lower()
        manual_send = ans in ("y", "yes", "да", "д")
        if not manual_send:
            print("   ⏭  Пропущено — спрошу при следующем запуске.")

    # --- 4. Подключение к Telegram ---
    session_file = os.path.join(BASE_DIR, "user_session.session")
    had_session_file = os.path.exists(session_file)

    print("\n⏳ Подключение к Telegram...")
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

    # --- 5. Запуск воркеров и watchdog ---
    workers = [
        asyncio.create_task(link_worker(i + 1))
        for i in range(CONCURRENT_DOWNLOADS)
    ]
    print(f"  ▶ Запущено воркеров: {CONCURRENT_DOWNLOADS}")

    watchdog_task = asyncio.create_task(_watchdog_stuck_batches(interval=60.0))
    print("  🐕 Запущен watchdog зависших пачек")

    # --- 6. Восстановление очередей: pending → link_queue ---
    await resume_pending_queues()

    # --- 7. Постановка downloaded/uploading в очередь на отправку ---
    # ✅ Сохраняем список «не поставленных» и логируем его.
    # Это индикатор проблем: если failed не пуст — часть файлов
    # не отправится автоматически, нужно вмешательство.
    failed_send = await offer_send_downloaded()

    if failed_send:
        print(
            f"\n⚠️  ВНИМАНИЕ: {len(failed_send)} файлов не удалось "
            f"поставить в очередь на отправку:"
        )
        for qid, it, reason in failed_send:
            print(f"   • #{qid} {it.get('link', '?')}: {reason}")

        # ✅ Пытаемся уведомить пользователя в me — одна сводка,
        # чтобы не спамить по каждому элементу.
        try:
            lines = [
                f"⚠️ **Не удалось поставить в очередь: {len(failed_send)}**",
                "",
            ]
            for qid, it, reason in failed_send[:10]:
                lines.append(
                    f"• `#{qid}` {it.get('link', '?')} — {reason}"
                )
            if len(failed_send) > 10:
                lines.append(
                    f"… и ещё {len(failed_send) - 10} "
                    f"(см. логи в консоли)"
                )
            lines.append("")
            lines.append(
                "Файлы остались в `downloads/`, статусы в JSON — "
                "`downloaded`. Будут повторены при следующем запуске."
            )
            await client.send_message("me", "\n".join(lines))
        except Exception as e:
            print(f"   ⚠️  не удалось уведомить в me: {e}")
    else:
        print("  ✅ Все найденные файлы поставлены в очередь на отправку")

    # --- 8. Страховка: закрытие «мёртвых» JSON ---
    for qid in _list_queue_files():
        _close_queue_if_finished(qid)

    # --- 9. Ручные файлы (после восстановления — приоритет у восстановления) ---
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
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлено пользователем (Ctrl+C).")
    except Exception as crash_error:
        print(f"\n❌ КРИТИЧЕСКИЙ СБОЙ: {crash_error}")