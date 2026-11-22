import os
import re
import json
import time
import asyncio
import mimetypes
import sys
import signal as _signal
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
import logging

def _log(msg: str):
    print(msg, flush=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(BASE_DIR, ".env")

# ---------- Выбор папки скачивания ----------
def _can_write(dir_path: str) -> bool:
    test_file = os.path.join(dir_path, ".write_test")
    try:
        with open(test_file, "w") as f:
            f.write("x")
        os.remove(test_file)
        return True
    except Exception:
        try:
            if os.path.exists(test_file):
                os.remove(test_file)
        except Exception:
            pass
        return False

def _pick_download_dir() -> str:
    storage_root = os.path.expanduser("~/storage/downloads")
    preferred = os.path.join(storage_root, "Xant")
    if os.path.isdir(storage_root):
        try:
            os.makedirs(preferred, exist_ok=True)
            if _can_write(preferred):
                return preferred
            _log(
                f"[download-dir] {preferred} существует, но запись не удалась "
                f"— fallback рядом со скриптом"
            )
        except Exception as e:
            _log(f"[download-dir] не удалось создать {preferred}: {e} — fallback")
    else:
        _log(f"[download-dir] {storage_root} не найден — fallback рядом со скриптом")

    fallback = os.path.join(BASE_DIR, "downloads")
    os.makedirs(fallback, exist_ok=True)
    if not _can_write(fallback):
        raise RuntimeError(
            f"Не удалось подготовить папку для скачивания: {fallback} "
            f"(нет прав на запись). Проверьте права и свободное место."
        )
    return fallback


DOWNLOAD_DIR = _pick_download_dir()
QUEUE_DIR = os.path.join(DOWNLOAD_DIR, "queues")
os.makedirs(QUEUE_DIR, exist_ok=True)


LIMIT_FREE = 2 * 1024 ** 3     # 2 ГиБ (2048 МиБ)
LIMIT_PREMIUM = 4 * 1024 ** 3  # 4 ГиБ (4096 МиБ)
CONCURRENT_DOWNLOADS = 3
CONCURRENT_UPLOADS = 3
CLEANUP_DELAY = 10
VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v", ".3gp"}
AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".flac", ".wav", ".ogg", ".opus", ".oga"}
PHOTO_EXTS = {
    ".jpg", ".jpeg", ".jpe", ".png", ".webp",
    ".bmp", ".tif", ".tiff", ".heic", ".heif",
}
_USELESS_BASENAMES = {
    "record", "audio", "voice", "file", "track", "sound",
    "audio_file", "voice_file", "record_file",
}

if not os.path.exists(ENV_FILE):
    print("==================================================")
    print("🚀 Добро пожаловать в Xant")
    print("Получите свои учётные данные API https://my.telegram.org")
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
    # Глушим бесконечный спам переподключений Telethon при обрыве сети
    logging.getLogger("telethon").setLevel(logging.CRITICAL)
    client = TelegramClient(
        os.path.join(BASE_DIR, "user_session"),
        API_ID,
        API_HASH,
        connection_retries=None,
        retry_delay=5,
        timeout=30,
    )

except (TypeError, ValueError):
    print("❌ Ошибка: Неверный TELEGRAM_API_ID в файле .env.")
    exit(1)

LINK_PATTERN = r"https://t\.me/(?:c/(\d+)|([\w_]+))/(\d+)"
LINK_REGEX = re.compile(LINK_PATTERN, re.IGNORECASE)

processed: set[tuple] = set()
in_progress = {"count": 0}
batches: dict[int, dict] = {}
batch_counter = {"id": 0}
active_dl_tasks: set[asyncio.Task] = set()
active_ul_tasks: set[asyncio.Task] = set()
account_state = {"is_premium": False, "download_only": False}
queues: dict[int, dict] = {}
link_queue = None
batch_create_lock = None
shutdown_event = None
_pending_deletions: set[asyncio.Task] = set()

async def _delete_later(*messages, delay: float = CLEANUP_DELAY):
    if delay > 0:
        await asyncio.sleep(delay)
    MAX_ATTEMPTS = 5
    for m in messages:
        if m is None:
            continue
        attempts = 0
        while attempts < MAX_ATTEMPTS:
            attempts += 1
            try:
                if isinstance(m, int):
                    await client.delete_messages("me", [m])
                else:
                    await m.delete()
                break
            except FloodWaitError as fwe:
                _log(f"[FloodWait] пауза {fwe.seconds}s перед удалением")
                await asyncio.sleep(fwe.seconds)
            except Exception as e:
                _log(f"⚠️  [delete_later] не удалось удалить сообщение: {type(e).__name__}: {e}")
                break
        else:
            _log(f"⚠️  [delete_later] превышено попыток удаления ({MAX_ATTEMPTS})")

def _schedule_delete(*messages, delay: float = CLEANUP_DELAY):
    task = asyncio.create_task(_delete_later(*messages, delay=delay))
    _pending_deletions.add(task)
    task.add_done_callback(_pending_deletions.discard)
    return task

_INVALID_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f\x7f-\x9f]')
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
            for attr in doc.attributes:
                if isinstance(attr, types.DocumentAttributeFilename):
                    _, ext = os.path.splitext(attr.file_name or "")
                    if ext:
                        return ext.lower()
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

def _is_meaningful_name(filename: str) -> bool:
    stem = os.path.splitext(filename)[0].strip().lower()
    stem = re.sub(r"[_\s]*\(\d+\)$", "", stem)
    stem = re.sub(r"_\d+$", "", stem)
    return bool(stem) and stem not in _USELESS_BASENAMES


def build_filename(message) -> str:
    ext = _detect_extension(message)
    raw_name = getattr(message.file, "name", None) if message.file else None

    if raw_name:
        base_raw, orig_ext = os.path.splitext(raw_name)
        if orig_ext:
            ext = orig_ext.lower()
        clean_base = _sanitize(base_raw)
        if clean_base and _is_meaningful_name(clean_base + ext):
            max_base_bytes = max(1, _MAX_NAME_BYTES - len(ext.encode("utf-8")))
            clean_base = _truncate_to_bytes(clean_base, max_base_bytes).rstrip(" ._")
            if clean_base:
                return f"{clean_base}{ext}"

    chat_id = getattr(message, "chat_id", None)
    msg_id = getattr(message, "id", 0)
    prefix = f"tg_{abs(chat_id)}_{msg_id}" if chat_id else f"tg_{msg_id}"
    return f"{prefix}{ext}"

def unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    reserve = 8
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
    if num < 1024:
        return f"{int(num)} B"
    if num < 1024 ** 2:
        return f"{num / 1024:.2f} KB"
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

_find_cache: dict[tuple[str, int | None], int] = {}
_find_miss_cache: dict[tuple[str, int | None], float] = {}
_find_in_me_lock = asyncio.Lock()
_FIND_CACHE_MAX = 1000
_FIND_MISS_TTL = 15.0


def _find_cache_put(key, value):
    if len(_find_cache) >= _FIND_CACHE_MAX:
        oldest = next(iter(_find_cache))
        _find_cache.pop(oldest, None)
    _find_cache[key] = value


async def _find_in_me(
    display_name: str,
    file_size: int | None,
    scan_limit: int = 400,
    max_retries: int = 2,
) -> int | None:
    if not display_name:
        return None

    _ext = os.path.splitext(display_name)[1].lower()
    if _ext in PHOTO_EXTS or _ext in {".webp", ".gif", ".tgs"}:
        return None

    cache_key = (display_name, file_size if file_size and file_size > 0 else None)
    check_size = bool(file_size and file_size > 0)

    if time.monotonic() - _find_miss_cache.get(cache_key, 0.0) < _FIND_MISS_TTL:
        return None

    async with _find_in_me_lock:
        if time.monotonic() - _find_miss_cache.get(cache_key, 0.0) < _FIND_MISS_TTL:
            return None

        cached = _find_cache.get(cache_key)
        if cached is not None:
            try:
                m = await client.get_messages("me", ids=cached)
                matched = False
                if m is not None and not isinstance(m, types.MessageEmpty):
                    if getattr(m, "file", None) is not None:
                        if m.file.name == display_name and (
                            not check_size
                            or (getattr(m.file, "size", 0) or 0) == file_size
                        ):
                            matched = True
                if matched:
                    return cached
                _find_cache.pop(cache_key, None)
            except Exception:
                _find_cache.pop(cache_key, None)

        attempt = 0
        while True:
            try:
                found: int | None = None
                async for msg in client.iter_messages("me", limit=scan_limit):
                    if getattr(msg, "file", None) is None:
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
                    _find_miss_cache.pop(cache_key, None)
                    _find_cache_put(cache_key, found)
                else:
                    _find_miss_cache[cache_key] = time.monotonic()
                    _log(f"  🔍 _find_in_me({display_name}): не найдено — кэширую на {int(_FIND_MISS_TTL)}с")
                return found

            except FloodWaitError as fwe:
                attempt += 1
                if attempt > max_retries:
                    raise
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
    anomalous_done: list[tuple[int, dict, str]] = []

    if not os.path.isdir(DOWNLOAD_DIR):
        return {
            "manual": manual,
            "own_partial": own_partial,
            "own_ready": own_ready,
            "anomalous_done": anomalous_done,
        }

    for name in os.listdir(DOWNLOAD_DIR):
        full = os.path.join(DOWNLOAD_DIR, name)
        if not os.path.isfile(full):
            continue

        if name.endswith(".part"):
            base_name = name[:-5]
            entry = own_by_name.get(base_name) or own_by_name.get(name)
            if entry is not None:
                qid, it = entry
                own_partial.append((qid, it, full))
            else:
                try:
                    os.remove(full)
                    _log(f"[scan] удалён осиротевший .part: {name}")
                except Exception as e:
                    _log(f"[scan] не удалось удалить .part {name}: {e}")
            continue

        entry = own_by_name.get(name)
        if entry is None:
            manual.append(full)
            continue

        qid, it = entry
        status = it.get("status")

        if it.get("download_only"):
            manual.append(full)
            continue

        if status in ("pending", "downloading"):
            own_partial.append((qid, it, full))
        elif status in ("downloaded", "uploading"):
            own_ready.append((qid, it, full))
        else:
            _log(f"файл {name}: статус «{status}» в JSON, но файл на диске — считаю manual")
            anomalous_done.append((qid, it, full))

    return {
        "manual": manual,
        "own_partial": own_partial,
        "own_ready": own_ready,
        "anomalous_done": anomalous_done,
    }

async def _prompt_orphan_uploads(scan: dict, timeout: float = 30.0) -> bool:
    manual_files = scan.get("manual", [])
    if not manual_files:
        return False

    print("\n" + "=" * 60)
    print(f"📂 Найдено файлов в папке загрузки: {len(manual_files)}")
    print(f"   Папка: {DOWNLOAD_DIR}")
    print("=" * 60)

    if not sys.stdin or not sys.stdin.isatty():
        print("⏭  Нет интерактивного терминала — отправка пропущена.")
        return False

    prompt = f"❓ Отправить эти файлы в «Избранное»? [y/n] (авто-отказ через {int(timeout)}с): "
    try:
        answer = await asyncio.wait_for(
            asyncio.to_thread(input, prompt),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        print(f"\n⏳ Время ожидания ({int(timeout)}с) истекло — файлы остаются в папке загрузки.")
        return False
    except (EOFError, KeyboardInterrupt):
        print("\n⏭  Ввод прерван — отправка пропущена.")
        return False

    answer = (answer or "").strip().lower()
    if answer in ("y", "yes", "д", "да"):
        return True

    print("⏭  Файлы остаются в папке загрузки, отправка пропущена.")
    return False


async def _upload_orphans_directly(scan: dict):
    manual_files = scan.get("manual", [])
    if not manual_files:
        return
    for full_path in manual_files:
        name = os.path.basename(full_path)
        try:
            size = os.path.getsize(full_path)
            limit = LIMIT_PREMIUM if account_state.get("is_premium") else LIMIT_FREE
            if size > limit:
                _log(
                    f"⚠️  {name}: размер {human_size(size)} превышает лимит "
                    f"{human_size(limit)} — пропущено (файл оставлен на диске)"
                )
                continue
        except Exception:
            size = 0

        try:
            found = await _find_in_me(display_name=name, file_size=size)
        except Exception:
            found = None
        if found is not None:
            _log(
                f"✅ {name}: уже в «Избранном» (id={found}) — удаляю локальный"
            )
            try:
                os.remove(full_path)
            except Exception:
                pass
            continue

        try:
            sent = await _send_media("me", full_path, name)
            sent_msg = sent[0] if isinstance(sent, list) else sent
            sent_id = getattr(sent_msg, "id", None)
            if sent_id is None:
                _log(f"❌ {name}: send_file вернул id=None — оставляю файл")
                continue
            _log(f"📤 {name} ({human_size(size)}) → me (id={sent_id})")
            try:
                os.remove(full_path)
                _log(f"🧹 удалён локальный файл: {name}")
            except Exception as e:
                _log(f"⚠️  не удалось удалить {name}: {e}")
        except FloodWaitError as fwe:
            _log(
                f"⏳ {name}: FloodWait {fwe.seconds}s при отправке — "
                f"файл оставлен, повтор при следующем запуске"
            )
            continue
        except Exception as e:
            _log(f"❌ {name}: {type(e).__name__}: {e} — файл оставлен")

def _cleanup_partial(local_path: str, expected_size: int = 0):
    if not local_path:
        return False

    if local_path.endswith(".part"):
        local_path = local_path[:-5]

    removed = False
    for path in (local_path, local_path + ".part"):
        if not os.path.exists(path):
            continue
        try:
            if expected_size > 0:
                actual = os.path.getsize(path)
                is_part = path.endswith(".part")
                if actual >= expected_size and not is_part:
                    continue
            else:
                if not path.endswith(".part"):
                    continue
            os.remove(path)
            removed = True
            _log(f"[cleanup] удалён частичный файл: {os.path.basename(path)}")
        except Exception as e:
            _log(f"[cleanup] не удалось удалить {path}: {e}")
    return removed

async def _ensure_connected(check_interval: float = 4.0) -> bool:
    """
    Ждёт реального восстановления интернета и переподключает Telethon.
    Возвращает True, если был обрыв связи.
    """
    was_offline = False
    while True:
        try:
            if not client.is_connected():
                await asyncio.wait_for(client.connect(), timeout=8.0)
            # Реальная проверка прохождения трафика до серверов Telegram (обходит заглушки VPN)
            await asyncio.wait_for(client.get_me(), timeout=8.0)
            if was_offline:
                _log("🌐 Связь с Telegram восстановлена — продолжаю докачку...")
            return was_offline
        except asyncio.CancelledError:
            raise
        except Exception:
            if not was_offline:
                _log("📡 Пропал интернет — жду восстановления связи с Telegram...")
                was_offline = True
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(check_interval)

class StatusThrottler:
    """Ограничивает частоту редактирования статусного сообщения в Telegram."""

    def __init__(self, min_interval: float = 3.5):
        self.min_interval = min_interval
        self._last_edit: float = 0.0
        self._last_text: str = ""
        self._lock = asyncio.Lock()

    async def edit(self, status_msg, text: str, force: bool = False):
        """
        Редактирует сообщение не чаще раза в min_interval секунд.
        Если force=True (для финальных статусов 100% / Ошибка), ждёт окончания кулдауна и отправляет.
        """
        if not status_msg or text == self._last_text:
            return

        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_edit

            if elapsed < self.min_interval:
                if not force:
                    # Пропускаем промежуточный кадр прогресса (например, 12%, 13%)
                    return
                # Для финального статуса ждём остаток паузы, чтобы гарантированно обновить текст
                await asyncio.sleep(self.min_interval - elapsed)

            if text == self._last_text:
                return

            try:
                await status_msg.edit(text)
                self._last_edit = time.monotonic()
                self._last_text = text
            except FloodWaitError as e:
                # Если Telegram всё же попросил подождать — сдвигаем таймер вперёд
                self._last_edit = time.monotonic() + int(e.seconds)
            except Exception as e:
                # При обрыве сети или 429 увеличиваем интервал, чтобы не спамить в лог
                self._last_edit = time.monotonic() + self.min_interval
                _log(f"[status] edit пропущен: {type(e).__name__}: {e}")

throttler = StatusThrottler(min_interval=4.0)  # Не чаще 1 раза в 4 секунды

async def download_with_retry(
    chat_id, msg_id, progress_callback,
    size_holder: dict | None = None, max_retries: int = 3,
    on_path_known=None,
    existing_local_file: str | None = None,
):
    while True:
        try:
            await _ensure_connected()
            channel = await client.get_entity(chat_id)
            break
        except FloodWaitError as e:
            wait_s = int(e.seconds) + 2
            _log(f"⏳ FloodWait при get_entity({chat_id}) — ждём {wait_s} сек...")
            await asyncio.sleep(wait_s)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(3)

    while True:
        try:
            await _ensure_connected()
            target_msg = await client.get_messages(channel, ids=msg_id)
            break
        except FloodWaitError as e:
            wait_s = int(e.seconds) + 2
            _log(f"⏳ FloodWait при get_messages({msg_id}) — ждём {wait_s} сек...")
            await asyncio.sleep(wait_s)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(3)

    if not target_msg:
        raise ValueError(f"Сообщение {msg_id} не найдено")
    if not target_msg.media:
        raise ValueError(f"В сообщении {msg_id} нет медиа-вложения")

    expected_size = (getattr(target_msg.file, "size", 0) or 0) if target_msg.file else 0
    if size_holder is not None:
        size_holder["expected"] = expected_size

    if existing_local_file:
        local_path = os.path.join(DOWNLOAD_DIR, existing_local_file)
    else:
        suggested_filename = build_filename(target_msg)
        local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))
    part_path = local_path + ".part"

    if on_path_known is not None:
        try:
            on_path_known(local_path)
        except Exception:
            pass

    if os.path.exists(part_path):
        try:
            part_size = os.path.getsize(part_path)
            if expected_size and part_size >= expected_size:
                _cleanup_partial(local_path, expected_size)
            elif part_size > 0:
                _log(
                    f"♻️  Найден недокачанный файл ({human_size(part_size)}) — "
                    f"продолжаем докачку: {os.path.basename(part_path)}"
                )
        except OSError:
            pass

    attempt = 0
    flood_attempts = 0
    CHUNK_ALIGN = 4096
    CHUNK_SIZE = 512 * 1024

    while True:
        attempt += 1
        try:
            last_activity = [time.monotonic()]
            stalled = [False]

            def wrapped_progress(received, total):
                last_activity[0] = time.monotonic()
                if total and size_holder is not None and not size_holder.get("expected"):
                    size_holder["expected"] = int(total)
                if progress_callback:
                    progress_callback(received, total)

            async def _watchdog(task_to_cancel: asyncio.Task):
                while not task_to_cancel.done():
                    await asyncio.sleep(5)
                    if task_to_cancel.done():
                        return
                    if time.monotonic() - last_activity[0] > 15:
                        stalled[0] = True
                        task_to_cancel.cancel()
                        return

            async def _stream_download():
                current_offset = 0
                if os.path.exists(part_path):
                    raw_size = os.path.getsize(part_path)
                    current_offset = (raw_size // CHUNK_ALIGN) * CHUNK_ALIGN

                mode = "r+b" if current_offset > 0 else "wb"
                with open(part_path, mode) as f:
                    if current_offset > 0:
                        f.truncate(current_offset)
                        f.seek(current_offset)
                        wrapped_progress(current_offset, expected_size)

                    async for chunk in client.iter_download(
                        target_msg.media,
                        offset=current_offset,
                        chunk_size=CHUNK_SIZE,
                    ):
                        f.write(chunk)
                        current_offset += len(chunk)
                        wrapped_progress(current_offset, expected_size)
                return part_path

            dl_task = asyncio.create_task(_stream_download())
            wd_task = asyncio.create_task(_watchdog(dl_task))
            try:
                saved_path = await dl_task
            except asyncio.CancelledError:
                if stalled[0]:
                    raise asyncio.TimeoutError("Нет прогресса скачивания > 15 сек")
                raise
            finally:
                wd_task.cancel()
                try:
                    await wd_task
                except BaseException:
                    pass

            if not saved_path or not os.path.exists(saved_path):
                raise RuntimeError("Скачивание не создало файл")

            actual_size = os.path.getsize(saved_path)
            exp = (size_holder.get("expected") if size_holder else 0) or expected_size
            if exp and actual_size != exp:
                raise RuntimeError(
                    f"Несовпадение размера файла: скачано {actual_size} Б, "
                    f"ожидалось {exp} Б"
                )

            if os.path.exists(local_path):
                local_path = unique_path(local_path)
            os.replace(saved_path, local_path)

            if size_holder is not None:
                size_holder["actual"] = actual_size
            return local_path

        except asyncio.CancelledError:
            raise

        except FloodWaitError as e:
            flood_attempts += 1
            if flood_attempts > 10:
                raise RuntimeError(
                    f"Слишком много FloodWait ({flood_attempts}) при скачивании msg_id={msg_id}"
                )
            wait_s = int(e.seconds) + 2
            _log(f"⏳ FloodWait при скачивании msg_id={msg_id} — ждём {wait_s} сек...")
            await asyncio.sleep(wait_s)
            attempt = max(0, attempt - 1)

        except FileReferenceExpiredError:
            _log(f"🔄 Истёк file_reference для msg_id={msg_id}, обновляем сообщение...")
            await _ensure_connected()
            old_local_path = local_path
            target_msg = await client.get_messages(channel, ids=msg_id)
            if not target_msg or not target_msg.media:
                raise ValueError(f"Сообщение {msg_id} недоступно при обновлении file_reference")
            expected_size = (getattr(target_msg.file, "size", 0) or 0) if target_msg.file else 0
            if size_holder is not None:
                size_holder["expected"] = expected_size
                size_holder["actual"] = 0

            if not existing_local_file:
                suggested_filename = build_filename(target_msg)
                new_local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))
                if os.path.exists(old_local_path + ".part") and new_local_path != old_local_path:
                    try:
                        os.replace(old_local_path + ".part", new_local_path + ".part")
                    except OSError:
                        pass
                local_path = new_local_path
                part_path = local_path + ".part"
                if on_path_known is not None:
                    try:
                        on_path_known(local_path)
                    except Exception:
                        pass
            attempt = max(0, attempt - 1)

        except (asyncio.TimeoutError, ConnectionError, OSError, Exception) as e:
            # Сбрасываем зависшее соединение и ждём реального ответа от серверов Telegram
            had_outage = await _ensure_connected()
            if had_outage:
                # Интернет пропадал — не тратим попытку и продолжаем докачку
                attempt = max(0, attempt - 1)
                continue

            if attempt >= max_retries:
                raise RuntimeError(
                    f"Скачивание сорвалось после {max_retries} попыток: {type(e).__name__}: {e}"
                )
            delay = min(5 * (2 ** (attempt - 1)), 60)
            _log(
                f"⚠️  обрыв скачивания ({attempt}/{max_retries}): "
                f"{type(e).__name__}: {e} — докачка через {delay} сек"
            )
            await asyncio.sleep(delay)

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
        try:
            meta = extractMetadata(parser)
            if not meta:
                return 0, 0, 0
            d = _to_seconds(meta.get("duration"))
            w = int(meta.get("width") or 0)
            h = int(meta.get("height") or 0)
            return d, w, h
        finally:
            try:
                parser.close()
            except Exception:
                pass
    except Exception:
        return 0, 0, 0

def _audio_meta_sync(path: str) -> int:
    try:
        from hachoir.metadata import extractMetadata
        from hachoir.parser import createParser
        parser = createParser(path)
        if parser:
            try:
                meta = extractMetadata(parser)
                if meta:
                    d = _to_seconds(meta.get("duration"))
                    if d > 0:
                        return d
            finally:
                try:
                    parser.close()
                except Exception:
                    pass
    except Exception:
        pass
    try:
        from mutagen import File as MutagenFile
        m = MutagenFile(path)
        if m is not None and m.info is not None:
            length = getattr(m.info, "length", None)
            if length:
                return int(float(length))
    except Exception:
        pass

    return 0

def _get_tag(tags, *keys) -> str | None:
    for k in keys:
        try:
            v = tags.get(k)
        except Exception:
            v = None
        if not v:
            continue
        if isinstance(v, (list, tuple)):
            if not v:
                continue
            v = v[0]
        if isinstance(v, bytes):
            try:
                v = v.decode("utf-8", errors="ignore")
            except Exception:
                continue
        s = str(v).strip()
        if s:
            return s
    return None

def _audio_tags_sync(path: str) -> tuple[str | None, str | None]:
    try:
        from mutagen import File as MutagenFile
        m = MutagenFile(path)
        if m is None or m.tags is None:
            return (None, None)

        tags = m.tags
        title = _get_tag(
            tags,
            "TIT2", "title",       # ID3 / Vorbis
            "\xa9nam",             # MP4
            "Title",               # ASF
        )
        performer = _get_tag(
            tags,
            "TPE1", "artist",      # ID3 / Vorbis
            "\xa9ART",             # MP4
            "Author",              # ASF
        )
        return (title, performer)
    except Exception:
        return (None, None)
    
# ---------- Отправка медиа ----------
def _get_send_kind(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext in VIDEO_EXTS:
        return "video"
    if ext in AUDIO_EXTS:
        return "audio"
    if ext in PHOTO_EXTS:
        return "photo"
    return "document"

async def _send_media(
    chat,
    path: str,
    display_name: str,
    progress_callback=None,
    caption: str | None = None,
    max_retries: int = 3,
):
    kind = _get_send_kind(display_name or path)
    fname = display_name or os.path.basename(path)
    loop = asyncio.get_running_loop()

    # Заранее готовим атрибуты, чтобы не перечитывать метаданные при каждом повторе
    attrs = None
    force_document = False
    supports_streaming = False

    if kind == "video":
        dur, w, h = await loop.run_in_executor(None, _video_meta_sync, path)
        attrs = [
            DocumentAttributeVideo(
                duration=dur,
                w=w,
                h=h,
                supports_streaming=True,
            )
        ]
        if _is_meaningful_name(fname):
            attrs.append(DocumentAttributeFilename(file_name=fname))
        supports_streaming = True
    elif kind == "audio":
        dur = await loop.run_in_executor(None, _audio_meta_sync, path)
        tag_title, tag_performer = await loop.run_in_executor(None, _audio_tags_sync, path)
        fallback_title = os.path.splitext(fname)[0] if _is_meaningful_name(fname) else ""
        title = tag_title or fallback_title
        attrs = [
            DocumentAttributeAudio(
                duration=dur,
                title=title,
                performer=tag_performer or "",
            )
        ]
        if _is_meaningful_name(fname):
            attrs.append(DocumentAttributeFilename(file_name=fname))
    elif kind != "photo":
        attrs = [DocumentAttributeFilename(file_name=fname)] if fname else None
        force_document = True

    attempt = 0
    flood_attempts = 0

    while True:
        attempt += 1
        try:
            await _ensure_connected()

            last_activity = [time.monotonic()]
            stalled = [False]

            async def _wrapped_upload_cb(current, total):
                last_activity[0] = time.monotonic()
                if progress_callback:
                    res = progress_callback(current, total)
                    if asyncio.iscoroutine(res):
                        await res

            async def _upload_watchdog(task_to_cancel: asyncio.Task):
                while not task_to_cancel.done():
                    await asyncio.sleep(5)
                    if task_to_cancel.done():
                        return
                    # Для отправки даём 25 сек без прогресса (финализация больших файлов на сервере может занять 10–15 сек)
                    if time.monotonic() - last_activity[0] > 25:
                        stalled[0] = True
                        task_to_cancel.cancel()
                        return

            send_coro = client.send_file(
                chat,
                path,
                caption=caption or "",
                supports_streaming=supports_streaming,
                force_document=force_document,
                attributes=attrs,
                progress_callback=_wrapped_upload_cb,
            )
            ul_task = asyncio.create_task(send_coro)
            wd_task = asyncio.create_task(_upload_watchdog(ul_task))

            try:
                return await ul_task
            except asyncio.CancelledError:
                if stalled[0]:
                    raise asyncio.TimeoutError("Нет прогресса отправки > 25 сек")
                raise
            finally:
                wd_task.cancel()
                try:
                    await wd_task
                except BaseException:
                    pass

        except asyncio.CancelledError:
            raise

        except FloodWaitError as e:
            flood_attempts += 1
            if flood_attempts > 10:
                raise
            wait_s = int(e.seconds) + 2
            _log(f"⏳ FloodWait при отправке {fname} — ждём {wait_s} сек...")
            await asyncio.sleep(wait_s)
            attempt = max(0, attempt - 1)

        except Exception as e:
            # Проверяем, не пропал ли интернет во время отправки
            had_outage = await _ensure_connected()
            if had_outage:
                attempt = max(0, attempt - 1)
                continue

            if "429" in str(e) or "InvalidBufferError" in type(e).__name__:
                _log(f"⏳ HTTP 429 при отправке {fname} — пауза 10 сек перед повтором...")
                await asyncio.sleep(10.0)
                attempt = max(0, attempt - 1)
                continue

            if attempt >= max_retries:
                raise

            delay = min(5 * (2 ** (attempt - 1)), 60)
            _log(
                f"⚠️  обрыв отправки {fname} ({attempt}/{max_retries}): "
                f"{type(e).__name__}: {e} — повтор через {delay} сек"
            )
            await asyncio.sleep(delay)


# ---------- Статусы ----------
def _render_status(st: dict, full_link: str,
                   queue_pos: int | None = None,
                   queue_total: int | None = None,
                   force_status: str | None = None) -> str:
    status = force_status if force_status is not None else st["status"]
    if status == "fail":
        return f"❌ {full_link}"
    if status == "skip":
        return f"⚠️ {full_link} (пропущено)"
    if status == "downloaded":
        sz = st.get("up_total", 0) or st.get("file_size", 0) or 0
        parts = []
        if sz > 0:
            parts.append(human_size(sz))
        if queue_pos is not None and queue_total is not None:
            parts.append(f"{queue_pos} из {queue_total}")
        if parts:
            return f"⏳ ожидание ({', '.join(parts)}) {full_link}"
        return f"⏳ ожидание... {full_link}"
    if status == "uploading":
        cur = st.get("up_current", 0)
        tot = st.get("up_total", 0)
        pct = st.get("up_percent", 0.0)
        return (
            f"📤 {pct:.1f}% "
            f"({human_size(cur)} / {human_size(tot)}) "
            f"{full_link}"
        )
    if status == "downloading":
        cur = st.get("current", 0)
        tot = st.get("total", 0)
        return (
            f"⬇️ {st.get('percent', 0.0):.1f}% "
            f"({human_size(cur)} / {human_size(tot)}) "
            f"{full_link}"
        )
    return f"📋 {full_link}"

def _initial_status_text(it: dict) -> str:
    link = it.get("link", "")
    status = it.get("status", "pending")
    _fsize = it.get("file_size", 0) or 0
    st_stub = {
        "status": status,
        "percent": 0.0,
        "current": 0,
        "total": 0,
        "up_current": 0,
        "up_total": _fsize,
        "file_size": _fsize,
        "up_percent": 0.0,
    }
    return _render_status(st_stub, link, force_status=status)

_global_edit_lock = asyncio.Lock()
_global_edit_state = {"last_ts": 0.0, "cooldown_until": 0.0}
_GLOBAL_EDIT_MIN_INTERVAL = 1.5   # Общая пауза между любыми edit в аккаунте (защита от 429)
_PER_MSG_EDIT_INTERVAL = 4.0      # Минимальный интервал обновления одного и того же сообщения


async def _update_status_msg(batch: dict, key: tuple, full_link: str, force: bool = False):
    st = batch["link_states"].get(key)
    status_msg = batch["status_msgs"].get(key)
    if st is None or status_msg is None:
        return

    now = time.monotonic()

    # 1. Быстрая проверка до захвата блокировок: не слишком ли рано для этой ссылки
    if not force and now - st.get("last_edit", 0.0) < _PER_MSG_EDIT_INTERVAL:
        return

    # 2. Если сервер прислал 429 или FloodWait — не стучимся до окончания паузы
    if now < _global_edit_state["cooldown_until"]:
        if not force:
            return
        wait_cd = _global_edit_state["cooldown_until"] - now
        if wait_cd > 0:
            await asyncio.sleep(wait_cd)

    # 3. Собираем актуальный текст под локальной блокировкой батча
    async with batch["lock"]:
        now = time.monotonic()
        if not force and now - st.get("last_edit", 0.0) < _PER_MSG_EDIT_INTERVAL:
            return

        batch_id = batch.get("batch_id")
        json_status = None
        if batch_id is not None:
            item = _find_item(batch_id, key[0], key[1])
            if item is not None:
                json_status = item.get("status")

        if json_status == "uploading" and key not in batch.get("_uploading", set()):
            json_status = "downloaded"

        queue_pos = None
        queue_total = None
        if (json_status or st.get("status")) == "downloaded" and batch_id is not None:
            q = queues.get(batch_id)
            if q is not None:
                items = q.get("items", [])
                queue_total = len(items)
                for i, it in enumerate(items):
                    if it["chat_id"] == key[0] and it["msg_id"] == key[1]:
                        queue_pos = i + 1
                        break

        new_text = _render_status(
            st, full_link,
            queue_pos=queue_pos,
            queue_total=queue_total,
            force_status=json_status,
        )
        if new_text == st.get("last_text"):
            return

    # 4. Глобальный троттлинг: выстраиваем edit-запросы от всех воркеров в общую очередь
    async with _global_edit_lock:
        now = time.monotonic()
        if now < _global_edit_state["cooldown_until"]:
            if not force:
                return
            await asyncio.sleep(_global_edit_state["cooldown_until"] - now)

        elapsed = time.monotonic() - _global_edit_state["last_ts"]
        if elapsed < _GLOBAL_EDIT_MIN_INTERVAL:
            if not force:
                return
            await asyncio.sleep(_GLOBAL_EDIT_MIN_INTERVAL - elapsed)

        if not client.is_connected():
            return

        try:
            await status_msg.edit(new_text)
            done_ts = time.monotonic()
            st["last_edit"] = done_ts
            st["last_text"] = new_text
            _global_edit_state["last_ts"] = done_ts
        except MessageNotModifiedError:
            st["last_text"] = new_text
            st["last_edit"] = time.monotonic()
        except FloodWaitError as fwe:
            wait_s = int(fwe.seconds) + 2
            _global_edit_state["cooldown_until"] = time.monotonic() + wait_s
            _log(f"[flood] edit статуса {full_link}: пауза {wait_s}s")
        except Exception as e:
            err_str = str(e)
            # При HTTP 429 или обрыве сети ставим глобальную паузу на 10 секунд для всех воркеров
            if "429" in err_str or "InvalidBufferError" in type(e).__name__:
                _global_edit_state["cooldown_until"] = time.monotonic() + 10.0
                _log(f"[status] HTTP 429 для {full_link} — пауза обновлений статусов на 10с")
            else:
                st["last_edit"] = time.monotonic()
                _log(f"[status] edit пропущен для {full_link}: {type(e).__name__}: {e}")


def _release_processed(key: tuple, result: str) -> str:
    processed.discard(key)
    return result

async def process_link(chat_id, msg_id, link: str, batch_id: int) -> str:
    batch = batches.get(batch_id)
    if not batch:
        return "fail"

    key = (chat_id, msg_id)
    if key in processed:
        return "skip"
    processed.add(key)


    st = batch.get("link_states", {}).get(key)

    item_entry = _find_item(batch_id, chat_id, msg_id)
    download_only = bool(
        (item_entry.get("download_only") if item_entry else False)
        or batch.get("download_only")
        or account_state.get("download_only")
    )
    existing_local = item_entry.get("local_file") if item_entry else None
    size_holder: dict = {"expected": 0, "actual": 0}
    known_path = {"value": None}

    def _on_path(p: str):
        known_path["value"] = p
        _mark_item(
            batch_id, chat_id, msg_id,
            status="downloading",
            local_file=os.path.basename(p),
        )

    if st is not None:
        st["status"] = "downloading"
    _mark_item(batch_id, chat_id, msg_id, status="downloading")
    _log(f"⬇️  [{batch_id}] старт скачивания: {link}")

    last_console_pct = [-1]
    loop = asyncio.get_running_loop()

    def download_cb(received, total):
        pct = int(received * 100 / total) if total else 0
        if pct >= last_console_pct[0] + 10 or (total and received == total):
            last_console_pct[0] = pct
            _log(
                f"   ⬇️  [{batch_id}] msg_id={msg_id}: {pct}% "
                f"({human_size(received)}/{human_size(total)})"
            )

        if st is not None:
            st["status"] = "downloading"
            st["current"] = received
            st["total"] = total or 0
            st["percent"] = (received / total * 100.0) if total else 0.0
            if time.monotonic() - st.get("last_edit", 0.0) >= _PER_MSG_EDIT_INTERVAL or (total and received >= total):
                loop.create_task(_update_status_msg(batch, key, link))
    try:
        filename = None
        if existing_local:
            candidate = os.path.join(DOWNLOAD_DIR, existing_local)
            if os.path.exists(candidate) and not candidate.endswith(".part"):
                filename = candidate
                size_holder["actual"] = os.path.getsize(candidate)
                _log(f"♻️  [{batch_id}] файл уже скачан ранее: {existing_local}")

        if filename is None:
            filename = await download_with_retry(
                chat_id, msg_id, download_cb,
                size_holder=size_holder,
                on_path_known=_on_path,
                existing_local_file=existing_local,
            )

        base_name = os.path.basename(filename)
        final_size = size_holder.get("actual") or (
            os.path.getsize(filename) if os.path.exists(filename) else 0
        )
        if download_only:
            _mark_item(
                batch_id, chat_id, msg_id,
                status="done",
                local_file=base_name,
                file_size=final_size,
                downloaded_at=time.time(),
                download_only=True,
                status_msg_id=None,
            )
            if st is not None:
                st["status"] = "done"
            status_msg = batch.get("status_msgs", {}).pop(key, None)
            if status_msg is not None:
                try:
                    await status_msg.edit(
                        f"✅ {base_name} ({human_size(final_size)})"
                    )
                except Exception:
                    pass
            _log(
                f"✅ [{batch_id}] скачано без отправки: "
                f"{base_name} ({human_size(final_size)})"
            )
            return _release_processed(key, "ok")

        _mark_item(
            batch_id, chat_id, msg_id,
            status="downloaded",
            local_file=base_name,
            file_size=final_size,
            downloaded_at=time.time(),
        )
        if st is not None:
            st["status"] = "downloaded"
            st["file_size"] = final_size
            st["up_total"] = final_size
        await _update_status_msg(batch, key, link, force=True)
        _log(
            f"✅ [{batch_id}] скачано: {base_name} ({human_size(final_size)}) "
            f"— передано в очередь отправки"
        )
        return _release_processed(key, "downloaded")

    except asyncio.CancelledError:
        cur_local = (item_entry.get("local_file") if item_entry else None) or (
            os.path.basename(known_path["value"]) if known_path["value"] else None
        )
        keep_local = (
            cur_local
            if cur_local and os.path.exists(os.path.join(DOWNLOAD_DIR, cur_local + ".part"))
            else None
        )
        _mark_item(batch_id, chat_id, msg_id, status="pending", local_file=keep_local)
        if st is not None:
            st["status"] = "pending"
        _release_processed(key, "deferred")
        raise

    except Exception as e:
        err_text = f"{type(e).__name__}: {e}"
        _log(f"❌ [{batch_id}] {link}: {err_text}")

        fallback_path = None
        if item_entry and item_entry.get("local_file"):
            fallback_path = os.path.join(DOWNLOAD_DIR, item_entry["local_file"])
        elif known_path["value"]:
            fallback_path = known_path["value"]

        keep_local = None
        if fallback_path and os.path.exists(fallback_path + ".part"):
            keep_local = os.path.basename(fallback_path)
            _log(f"💾 [{batch_id}] {link}: недокачанный .part сохранён для докачки при перезапуске")

        _mark_item(
            batch_id, chat_id, msg_id,
            status="fail",
            error=err_text,
            local_file=keep_local,
        )
        if st is not None:
            st["status"] = "fail"
        await _update_status_msg(batch, key, link, force=True)
        return _release_processed(key, "fail")

# ---------- Воркер скачивания ----------
async def link_worker(worker_id: int):
    while True:
        chat_id, msg_id, full_link, batch_id = await link_queue.get()
        in_progress["count"] += 1
        try:
            task = asyncio.create_task(
                process_link(chat_id, msg_id, full_link, batch_id)
            )
            active_dl_tasks.add(task)
            try:
                result = await task
            finally:
                active_dl_tasks.discard(task)
        except asyncio.CancelledError:
            _log(f"🛑 [worker-{worker_id}] скачивание прервано по /clear: {full_link}")
            continue
        except Exception as e:
            _log(f"⚠️  [worker-{worker_id}] неожиданное исключение: {e}")
            result = "fail"
        finally:
            in_progress["count"] -= 1
            link_queue.task_done()

        batch = batches.get(batch_id)
        if not batch or batch.get("cancelled"):
            continue
        async with batch["lock"]:
            if result == "ok":
                batch["ok"] += 1
            elif result == "downloaded":
                pass
            elif result == "skip":
                batch["skip"] += 1
            elif result == "fail":
                batch["fail"] += 1
            elif result == "deferred":
                pass
        await _maybe_finish_batch(batch_id)

async def _maybe_finish_batch(batch_id: int):
    b = batches.get(batch_id)
    if b is None:
        return

    q = queues.get(batch_id)

    if q is not None:
        b["ok"] = sum(1 for it in q.get("items", []) if it.get("status") == "done")
        b["skip"] = sum(1 for it in q.get("items", []) if it.get("status") == "skip")
        b["fail"] = sum(1 for it in q.get("items", []) if it.get("status") == "fail")
        b["total"] = len(q.get("items", []))

    done = b["ok"] + b["skip"] + b["fail"]
    if done < b["total"]:
        return
    if b.get("finished"):
        return

    if b.get("_uploading"):
        _log(
            f"batch #{batch_id}: счётчики сошлись, но "
            f"{len(b['_uploading'])} upload-воркеров ещё работают — "
            f"откладываю завершение"
        )
        return

    if q is not None:
        active_statuses = {"pending", "downloading", "downloaded", "uploading"}
        stuck = [
            it for it in q.get("items", [])
            if it.get("status") in active_statuses
        ]
        if stuck:
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

    b["finished"] = True

    _log(
        f"batch #{batch_id}: завершение | "
        f"ok={b['ok']}, skip={b['skip']}, fail={b['fail']}, "
        f"total={b['total']}"
    )

    if b["to_delete"]:
        _log(f"batch #{batch_id}: к отложенному удалению: {len(b['to_delete'])} сообщ.")

    try:
        _schedule_delete(*b["to_delete"])
    finally:
        if q is not None:
            for it in q.get("items", []):
                lf = it.get("local_file")
                if lf:
                    _find_cache.pop(
                        (lf, it.get("file_size") or None), None
                    )
        batches.pop(batch_id, None)
        queues.pop(batch_id, None)
        try:
            os.remove(_queue_path(batch_id))
        except Exception:
            pass

    _log(f"batch #{batch_id}: пачка закрыта и удалена из памяти")

# ---------- Upload worker ----------
async def _upload_worker(worker_id: int = 0):
    UPLOAD_POLL_INTERVAL = 1.0

    while True:
        try:
            await asyncio.sleep(UPLOAD_POLL_INTERVAL)

            for batch_id, b in list(batches.items()):
                q = queues.get(batch_id)
                if q is None:
                    continue

                target = None
                failed_missing = False

                for it in q.get("items", []):
                    status = it.get("status")
                    if status in ("done", "fail", "skip"):
                        continue
                    key = (it["chat_id"], it["msg_id"])

                    if key in processed:
                        continue
                    if key in b.get("_uploading", set()):
                        continue
                    if status != "downloaded":
                        continue

                    retry_ts = it.get("next_retry_ts", 0)
                    if retry_ts and time.time() < retry_ts:
                        continue

                    lf = it.get("local_file")
                    if not lf:
                        _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                   status="fail", local_file=None, next_retry_ts=0)
                        b["fail"] = b.get("fail", 0) + 1
                        failed_missing = True
                        _log(
                            f"  ❌ upload-воркер #{worker_id}: "
                            f"{it['link']}: local_file=None → fail"
                        )
                        continue
                    full_path = os.path.join(DOWNLOAD_DIR, lf)
                    if not os.path.exists(full_path):
                        _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                   status="fail", local_file=None, next_retry_ts=0)
                        b["fail"] = b.get("fail", 0) + 1
                        failed_missing = True
                        _log(
                            f"  ❌ upload-воркер #{worker_id}: "
                            f"{it['link']}: файл пропал → fail"
                        )
                        continue
                    target = it
                    break

                if failed_missing:
                    await _maybe_finish_batch(batch_id)
                    break

                if target is None:
                    continue

                key = (target["chat_id"], target["msg_id"])
                b.setdefault("_uploading", set()).add(key)
                try:
                    ul_task = asyncio.create_task(_upload_one(batch_id, b, target))
                    active_ul_tasks.add(ul_task)
                    try:
                        result = await ul_task
                    finally:
                        active_ul_tasks.discard(ul_task)

                    if result == "ok":
                        b["ok"] = b.get("ok", 0) + 1
                        _log(
                            f"📤 upload-воркер #{worker_id}: "
                            f"{target['link']} → ok | "
                            f"batch #{batch_id}: ok={b['ok']}, "
                            f"skip={b['skip']}, fail={b['fail']}, "
                            f"total={b['total']}"
                        )
                        await _maybe_finish_batch(batch_id)
                    elif result == "fail":
                        b["fail"] = b.get("fail", 0) + 1
                        _log(
                            f"❌ upload-воркер #{worker_id}: "
                            f"{target['link']} → fail | "
                            f"batch #{batch_id}: ok={b['ok']}, "
                            f"skip={b['skip']}, fail={b['fail']}, "
                            f"total={b['total']}"
                        )
                        await _maybe_finish_batch(batch_id)
                    elif result == "deferred":
                        _log(
                            f"⏳ upload-воркер #{worker_id}: "
                            f"{target['link']} — отложено"
                        )
                except asyncio.CancelledError:
                    _log(f"🛑 upload-воркер #{worker_id}: отправка прервана по /clear ({target['link']})")
                    continue
                finally:
                    b.get("_uploading", set()).discard(key)
                    try:
                        await _maybe_finish_batch(batch_id)
                    except Exception:
                        _log(f"[_upload_worker #{worker_id}] _maybe_finish_batch упал для batch {batch_id}")


        except asyncio.CancelledError:
            return
        except Exception as e:
            _log(f"upload-воркер #{worker_id}: ошибка: {type(e).__name__}: {e}")

async def _upload_one(batch_id: int, batch: dict, it: dict) -> str:
    chat_id = it["chat_id"]
    msg_id = it["msg_id"]
    full_link = it["link"]
    local_name = it.get("local_file")

    batch["_last_upload_activity_ts"] = time.time()

    key = (chat_id, msg_id)

    if batch.get("cancelled"):
        _log(f"  ⏭  upload-воркер: {full_link}: batch отменён — пропуск")
        return "deferred"

    cur = _find_item(batch_id, chat_id, msg_id)
    if cur is not None and cur.get("status") in ("done", "fail", "skip"):
        _log(
            f"  ⏭  upload-воркер: {full_link}: статус уже "
            f"«{cur.get('status')}» — пропуск"
        )
        return "ok" if cur.get("status") == "done" else "fail"

    if not local_name:
        _mark_item(batch_id, chat_id, msg_id, status="fail", next_retry_ts=0)
        return "fail"

    filename = os.path.join(DOWNLOAD_DIR, local_name)
    if not os.path.exists(filename):
        _mark_item(batch_id, chat_id, msg_id, status="fail", local_file=None, next_retry_ts=0)
        _log(f"❌ upload-воркер: {full_link}: файл пропал ({filename}) → fail")
        return "fail"

    st = batch["link_states"].get(key)
    if st is None:
        _mark_item(batch_id, chat_id, msg_id, status="fail")
        return "fail"

    display_name = os.path.basename(filename)
    file_size = it.get("file_size") or os.path.getsize(filename)

    if account_state.get("download_only"):
        _log(
            f"📥 /nome: {full_link}: только скачивание"
        )
        _mark_item(
            batch_id, chat_id, msg_id,
            status="done",
            file_message_id=None,
            downloaded_at=time.time(),
            download_only=True,    
            upload_retries=0,
            next_retry_ts=0,
        )
        st["status"] = "done"

        status_msg = batch["status_msgs"].pop(key, None)
        if status_msg is not None:
            try:
                await status_msg.edit(
                    f"✅ {display_name} ({human_size(file_size)})"
                )
                _mark_item(batch_id, chat_id, msg_id,
                           status_msg_id=None)
            except MessageNotModifiedError:
                pass
            except Exception as e:
                _log(
                    f"⚠️  /nome: {full_link}: не удалось обновить "
                    f"статус: {e}"
                )
        else:
            _mark_item(batch_id, chat_id, msg_id,
                       status_msg_id=None)

        return "ok"

    limit = LIMIT_PREMIUM if account_state.get("is_premium") else LIMIT_FREE
    if file_size > limit:
        _log(
            f"⚠️  upload-воркер: {full_link}: размер {human_size(file_size)} "
            f"превышает лимит {human_size(limit)} — пропущено (файл оставлен на диске)"
        )
        _mark_item(
            batch_id, chat_id, msg_id,
            status="skip",
            next_retry_ts=0,
        )
        st["status"] = "skip"
        await _update_status_msg(batch, key, full_link, force=True)
        return "skip"

    _is_photo = os.path.splitext(display_name)[1].lower() in PHOTO_EXTS
    if _is_photo:
        found_id = None
        _log(
            f"  🔍 upload-воркер: {full_link}: фото — "
            f"проверку me пропускаю, отправляю"
        )
    else:
        try:
            found_id = await _find_in_me(
                display_name=display_name,
                file_size=file_size,
            )
        except FloodWaitError:
            _log(
                f"⚠️  upload-воркер: {full_link}: FloodWait при проверке me — "
                f"продолжаю отправку"
            )
            found_id = None
        except Exception as e:
            _log(
                f"⚠️  upload-воркер: {full_link}: ошибка проверки me "
                f"({type(e).__name__}: {e}) — продолжаю отправку"
            )
            found_id = None

    if found_id is not None:
        _log(
            f"✅ upload-воркер: {full_link}: уже в «Избранном» "
            f"(id={found_id}), пропускаю отправку"
        )
        _mark_item(
            batch_id, chat_id, msg_id,
            status="done",
            file_message_id=found_id,
            local_file=None,
            downloaded_at=None,
            upload_retries=0,
            next_retry_ts=0,
        )
        st["status"] = "done"

        status_msg = batch["status_msgs"].pop(key, None)
        if status_msg is not None:
            try:
                await status_msg.edit(
                    f"✅ {display_name} ({human_size(file_size)}) — уже в «Избранном»"
                )
                _mark_item(batch_id, chat_id, msg_id,
                           status_msg_id=None)
            except MessageNotModifiedError:
                pass
            except Exception as e:
                _log(
                    f"⚠️  upload-воркер: {full_link}: не удалось "
                    f"обновить статус: {e}"
                )
        else:
            _mark_item(batch_id, chat_id, msg_id,
                       status_msg_id=None)

        if os.path.exists(filename):
            try:
                os.remove(filename)
                _log(f"🧹 удалён локальный файл: {os.path.basename(filename)}")
            except Exception as e:
                _log(f"⚠️  не удалось удалить {os.path.basename(filename)}: {e}")

        return "ok"

    st["status"] = "uploading"
    st["up_current"] = 0
    st["up_total"] = file_size
    st["up_percent"] = 0.0
    st["last_up_cb"] = 0.0
    st["_last_logged_pct"] = -1
    _mark_item(batch_id, chat_id, msg_id, status="uploading")
    await _update_status_msg(batch, key, full_link, force=True)

    async def upload_cb(current, total):
        now_wall = time.time()
        now_mono = time.monotonic()
        batch["_last_upload_activity_ts"] = now_wall

        if now_mono - st.get("last_up_cb", 0.0) >= _PER_MSG_EDIT_INTERVAL or (total and current >= total):
            st["last_up_cb"] = now_mono
            st["up_current"] = current
            st["up_total"] = total
            st["up_percent"] = (current / total) * 100 if total else 0
            cur_item = _find_item(batch_id, chat_id, msg_id)
            if cur_item is None or cur_item.get("status") in ("fail", "done", "skip"):
                return

            pct = (current / total * 100) if total else 0
            last_pct = st.get("_last_logged_pct", -1)
            if pct - last_pct >= 10 or (total and current >= total):
                st["_last_logged_pct"] = pct
                _log(
                    f"📤 [upload] {display_name}: {pct:.1f}% "
                    f"({human_size(current)} / {human_size(total)})"
                )

            if key in batch["status_msgs"]:
                await _update_status_msg(batch, key, full_link)




    caption = None
    kind = _get_send_kind(filename)
    if kind == "video":
        caption = f"🎬 {display_name}"

    try:
        sent = await _send_media(
            "me", filename, display_name, upload_cb, caption=caption
        )
        
    except FloodWaitError as fwe:
        _log(
            f"⏳ upload-воркер: {full_link}: FloodWait {fwe.seconds}s "
            f"— откладываю до {time.strftime('%H:%M:%S', time.localtime(time.time() + fwe.seconds))}"
        )
        batch["_last_upload_activity_ts"] = time.time()
        _mark_item(
            batch_id, chat_id, msg_id,
            status="downloaded",
            next_retry_ts=time.time() + fwe.seconds,
        )
        st["status"] = "downloaded"
        await _update_status_msg(batch, key, full_link, force=True)
        return "deferred"
    except Exception as e:
        _log(f"❌ upload-воркер: {full_link}: {type(e).__name__}: {e}")
        try:
            actual = os.path.getsize(filename)
        except Exception:
            actual = 0
        if file_size > 0 and actual >= file_size:
            _log(
                f"♻️  upload-воркер: {full_link}: файл целый "
                f"({human_size(actual)}) — оставляю downloaded, повтор позже"
            )
            _mark_item(
                batch_id, chat_id, msg_id,
                status="downloaded",
                next_retry_ts=time.time() + 5,  
            )
            st["status"] = "downloaded"
            await _update_status_msg(batch, key, full_link, force=True)
            return "deferred"
        else:
            _cleanup_partial(filename, file_size)
            _mark_item(batch_id, chat_id, msg_id, status="fail", local_file=None, next_retry_ts=0)
            st["status"] = "fail"
            await _update_status_msg(batch, key, full_link, force=True)
            return "fail"

    sent_msg = sent[0] if isinstance(sent, list) else sent
    sent_id = getattr(sent_msg, "id", None)

    if sent_id is None:
        _log(
            f"❌ upload-воркер: {full_link}: send_file вернул "
            f"id=None (sent={sent!r}) — fail, файл оставляю"
        )
        _mark_item(
            batch_id, chat_id, msg_id,
            status="fail",
            next_retry_ts=0,
        )
        st["status"] = "fail"
        await _update_status_msg(batch, key, full_link, force=True)
        return "fail"

    if batch.get("cancelled"):
        _log(
            f"🛑 upload-воркер: {full_link}: batch отменён во время "
            f"отправки — удаляю только что загруженное (id={sent_id})"
        )
        if sent_id:
            try:
                await client.delete_messages("me", [sent_id])
            except Exception:
                pass
        try:
            os.remove(filename)
        except Exception:
            pass
        return "deferred"

    _log(
        f"✅ upload-воркер: файл загружен в «Избранное»: {full_link} "
        f"→ сообщение id={sent_id}, размер={human_size(file_size)}"
    )

    _mark_item(
        batch_id, chat_id, msg_id,
        status="done",
        file_message_id=sent_id,
        local_file=None,
        downloaded_at=None,
        upload_retries=0,
        next_retry_ts=0,
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
            _mark_item(batch_id, chat_id, msg_id,
                       status_msg_id=None)
        except Exception as e:
            _log(
                f"⚠️  не удалось удалить статус {full_link} "
                f"(id={old_id}): {e} — status_msg_id оставляем, "
                f"reconcile повторит"
            )
    else:
        _mark_item(batch_id, chat_id, msg_id,
                   status_msg_id=None)

    if os.path.exists(filename):
        try:
            os.remove(filename)
            _log(f"🧹 удалён локальный файл: {os.path.basename(filename)}")
        except Exception as e:
            _log(f"⚠️  не удалось удалить {os.path.basename(filename)}: {e}")

    return "ok"

# ---------- Watchdog ----------
async def _watchdog_stuck_batches(interval: float = 60.0):
    while True:
        try:
            await asyncio.sleep(interval)
            now = time.time()
            for batch_id, b in list(batches.items()):
                q = queues.get(batch_id)
                if q is None:
                    continue

                last_activity = b.get("_last_upload_activity_ts", 0)
                worker_progressing = (
                    last_activity > 0
                    and (now - last_activity) < 3 * interval
                )

                candidates = [
                    it for it in q.get("items", [])
                    if it.get("status") in ("downloaded", "uploading")
                ]
                if not candidates:
                    continue

                if worker_progressing:
                    continue

                key = tuple((id(it), it.get("status")) for it in candidates)
                wd = b.setdefault("_wd", {})
                if wd.get("key") != key:
                    wd["key"] = key
                    wd["ts"] = now
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
                    cur = _find_item(batch_id, it["chat_id"], it["msg_id"])
                    if cur is None:
                        continue
                    if cur.get("status") in ("done", "fail", "skip"):
                        if (it["chat_id"], it["msg_id"]) in b.get("_uploading", set()):
                            _log(
                                f"  🐕 #{batch_id} {it['link']}: "
                                f"уже {cur.get('status')} — пропуск"
                            )
                            continue

                        if (it["chat_id"], it["msg_id"]) in processed:
                            _log(
                                f"  🐕 #{batch_id} {it['link']}: "
                                f"уже в работе (process_link) — пропуск"
                            )
                            continue

                    retry_ts = cur.get("next_retry_ts", 0)
                    if retry_ts and now < retry_ts:
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"next_retry_ts через {retry_ts - now:.0f}s — пропуск"
                        )
                        continue

                    local = cur.get("local_file")
                    full_path = os.path.join(DOWNLOAD_DIR, local) if local else None
                    file_exists = bool(full_path and os.path.exists(full_path))

                    if cur.get("status") == "uploading" and not file_exists:
                        if not local:
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
                                upload_retries=3, next_retry_ts=0,
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
                                status="done",
                                file_message_id=found_id,
                                local_file=None,
                                upload_retries=0,
                                next_retry_ts=0,
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
                                upload_retries=3, next_retry_ts=0,
                            )
                            b["fail"] = b.get("fail", 0) + 1
                            any_fail = True
                            _log(f"  ❌ #{batch_id} {it['link']}: uploading → fail")
                        continue

                    if not file_exists:
                        if cur.get("upload_retries", 0) >= 3:
                            _log(
                                f"  🐕 #{batch_id} {it['link']}: "
                                f"файл пропал (уже {cur.get('upload_retries')} попыток) — пропуск"
                            )
                            continue
                        _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                   status="fail", local_file=None,
                                   upload_retries=3, next_retry_ts=0)
                        b["fail"] = b.get("fail", 0) + 1
                        any_fail = True
                        _log(f"  🐕 #{batch_id} {it['link']}: файл пропал → fail")
                        continue

                    retries = cur.get("upload_retries", 0)
                    if retries >= 3:
                        _mark_item(
                            batch_id, it["chat_id"], it["msg_id"],
                            status="fail", local_file=None, next_retry_ts=0,
                        )
                        b["fail"] = b.get("fail", 0) + 1
                        any_fail = True
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"превышено попыток ({retries}) → fail"
                        )
                        continue

                    if (it["chat_id"], it["msg_id"]) in processed or (it["chat_id"], it["msg_id"]) in b.get("_uploading", set()):
                        continue

                    _mark_item(
                        batch_id, it["chat_id"], it["msg_id"],
                        status="downloaded",
                        upload_retries=retries + 1,
                        next_retry_ts=0,
                    )
                    _log(
                        f"  🐕 #{batch_id} {it['link']}: "
                        f"разблокирован для отправки (попытка {retries + 1}/3)"
                    )


                if any_fail:
                    await _maybe_finish_batch(batch_id)
                wd["key"] = None
        except asyncio.CancelledError:
            return
        except Exception as e:
            _log(f"🐕 watchdog: ошибка: {type(e).__name__}: {e}")

async def _restore_batch_from_queue(qid: int, q: dict) -> bool:
    """
    Восстанавливает batches[qid] из JSON-очереди q, если батча ещё нет в памяти:
    - удаляет старые статус-сообщения из «Избранного»;
    - создаёт актуальные статус-сообщения для активных элементов;
    - заполняет link_states и регистрирует батч в batches[qid].
    Возвращает True, если батч существует или успешно создан.
    """
    if qid in batches:
        return True

    items = q.get("items", [])
    if not items:
        _log(f"  ⚠️  #{qid}: очередь пустая — batch не восстановлен")
        return False

    try:
        # 1. Чистим старые статус-сообщения из прошлой сессии
        old_ids = list(dict.fromkeys(
            it["status_msg_id"] for it in items if it.get("status_msg_id")
        ))
        if old_ids:
            try:
                await client.delete_messages("me", old_ids)
                _log(f"  🗑  #{qid}: удалено {len(old_ids)} старых статусов")
            except FloodWaitError as fwe:
                _log(f"  ⏳ #{qid}: FloodWait {fwe.seconds}s при удалении старых статусов")
                await asyncio.sleep(fwe.seconds)
                try:
                    await client.delete_messages("me", old_ids)
                except Exception:
                    pass
            except Exception as e:
                _log(f"  ⚠️  #{qid}: не удалось удалить старые ({len(old_ids)}): {e}")

        for it in items:
            it["status_msg_id"] = None

        queues[qid] = q
        _save_queue(qid)

        # 2. Создаём состояния и новые статус-сообщения
        status_msgs: dict[tuple, any] = {}
        link_states: dict[tuple, dict] = {}
        active_statuses = ("pending", "downloading", "downloaded", "uploading")

        for it in items:
            key = (it["chat_id"], it["msg_id"])
            status = it.get("status", "pending")
            fsize = it.get("file_size", 0) or 0

            link_states[key] = {
                "status": status,
                "percent": 0.0,
                "current": 0,
                "total": 0,
                "last_edit": 0.0,
                "up_current": 0,
                "up_total": fsize if status in ("downloaded", "uploading") else 0,
                "up_percent": 0.0,
                "last_up_cb": 0.0,
                "file_size": fsize,
            }

            if status not in active_statuses:
                continue

            for attempt in range(2):
                try:
                    m = await client.send_message("me", _initial_status_text(it))
                    status_msgs[key] = m
                    it["status_msg_id"] = m.id
                    _log(f"  ➕ #{qid} {it['link']}: создан статус (id={m.id})")
                    break
                except FloodWaitError as fwe:
                    _log(
                        f"  ⏳ #{qid} {it['link']}: FloodWait "
                        f"{fwe.seconds}s при создании статуса"
                    )
                    await asyncio.sleep(fwe.seconds)
                except Exception as e:
                    _log(f"  ⚠️  #{qid} {it['link']}: статус не создан: {e}")
                    break

        # 3. Регистрируем батч
        batches[qid] = {
            "batch_id": qid,
            "event": _make_fake_event(),
            "is_restored": True,
            "status_msgs": status_msgs,
            "link_states": link_states,
            "lock": asyncio.Lock(),
            "total": len(items),
            "ok": sum(1 for it in items if it.get("status") == "done"),
            "skip": sum(1 for it in items if it.get("status") == "skip"),
            "fail": sum(1 for it in items if it.get("status") == "fail"),
            "finished": False,
            "to_delete": [],
        }
        _save_queue(qid)
        _log(
            f"  🆕 #{qid}: batch восстановлен "
            f"(total={len(items)}, status_msgs={len(status_msgs)}, "
            f"ok={batches[qid]['ok']}, fail={batches[qid]['fail']})"
        )
        return True

    except Exception as e:
        _log(
            f"  ⚠️  #{qid}: не удалось восстановить batch: "
            f"{type(e).__name__}: {e}"
        )
        return False

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

        queues[qid] = q

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
            _save_queue(qid)

            b_existing = batches.get(qid)
            if b_existing is not None:
                for it in items:
                    key = (it["chat_id"], it["msg_id"])
                    st = b_existing["link_states"].get(key)
                    if st is not None:
                        st["status"] = it.get("status", st.get("status"))
        stuck_dl = 0
        for it in items:
            if it.get("status") == "downloading":
                it["status"] = "pending"
                stuck_dl += 1
        if stuck_dl:
            _log(f"  🔄 #{qid}: сброшено зависших downloading → pending: {stuck_dl}")
            _save_queue(qid)

        pending_items = [it for it in items if it.get("status") == "pending"]

        if pending_items:
            _log(
                f"  🔄 #{qid}: найдено pending-элементов для перекачки: "
                f"{len(pending_items)}"
            )

            if not await _restore_batch_from_queue(qid, q):
                continue

            b = batches[qid]
            for it in pending_items:
                key = (it["chat_id"], it["msg_id"])
                if key in b["link_states"]:
                    b["link_states"][key]["status"] = "pending"

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
                _log(f"  📤 #{qid}: поставлено pending в очередь: {placed}")

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
        return []
    _log(f"📥 Найдено уже скачанных, но не отправленных файлов: {len(items)} — передаю в отправку")

    ready_count = 0
    failed: list[tuple[int, dict, str]] = []

    missing_qids = {qid for qid, _ in items if qid not in batches}
    for qid in missing_qids:
        q = queues.get(qid) or _load_queue(qid)
        if not q:
            _log(f"  ⚠️  #{qid}: JSON не читается — batch не восстановлен")
            continue
        await _restore_batch_from_queue(qid, q)

    for qid, it in items:
        try:
            b = batches.get(qid)
            if b is None:
                _log(f"  ⚠️  #{qid} {it['link']}: batch не восстановлен — пропуск")
                failed.append((qid, it, "batch не восстановлен"))
                continue

            key = (it["chat_id"], it["msg_id"])
            if key not in b["status_msgs"]:
                failed.append((qid, it, "статус не создан"))
                continue

            ready_count += 1
            _log(f"  📤 #{qid} {it['link']}: готов к отправке (_upload_worker)")
        except Exception as e:
            _log(
                f"  ❌ #{qid} {it['link']}: подготовка не удалась: "
                f"{type(e).__name__}: {e}"
            )
            failed.append((qid, it, f"{type(e).__name__}: {e}"))

    _log(f"📤 Передано в отправку: {ready_count}")
    if failed:
        _log(f"⚠️  Не удалось подготовить: {len(failed)}")
    return failed

# ---------- Обработчик сообщений ----------
@client.on(events.NewMessage(chats="me"))
async def handle_saved_messages(event):
    text = event.raw_text
    cmd = text.strip().lower()

    if cmd in ("/quit", "/exit", "/shutdown"):
        await event.respond(
            "🛑 Работа прекращена."
        )
        shutdown_event.set()
        return
    
    if cmd == "/clear":
        _log("🧹 Получена команда /clear — полная остановка и очистка...")

        drained = 0
        while not link_queue.empty():
            try:
                link_queue.get_nowait()
                link_queue.task_done()
                drained += 1
            except asyncio.QueueEmpty:
                break

        cancelled_dl = 0
        for t in list(active_dl_tasks):
            if not t.done():
                t.cancel()
                cancelled_dl += 1

        cancelled_ul = 0
        for t in list(active_ul_tasks):
            if not t.done():
                t.cancel()
                cancelled_ul += 1

        if active_dl_tasks or active_ul_tasks:
            await asyncio.gather(
                *list(active_dl_tasks), *list(active_ul_tasks),
                return_exceptions=True,
            )

        msg_ids_to_del: list[int] = []
        for b in list(batches.values()):
            b["cancelled"] = True
            for m in b.get("status_msgs", {}).values():
                if getattr(m, "id", None):
                    msg_ids_to_del.append(m.id)
        batches.clear()
        processed.clear()

        removed_queues = 0
        for qid in _list_queue_files():
            q = _load_queue(qid)
            if q:
                for it in q.get("items", []):
                    if it.get("status_msg_id"): msg_ids_to_del.append(it["status_msg_id"])
            qpath = _queue_path(qid)
            if os.path.exists(qpath):
                try:
                    os.remove(qpath)
                    removed_queues += 1
                except OSError as e:
                    _log(f"⚠️  Не удалось удалить {os.path.basename(qpath)}: {e}")
        queues.clear()

        removed_parts = 0
        if os.path.isdir(DOWNLOAD_DIR):
            for fname in os.listdir(DOWNLOAD_DIR):
                if fname.endswith(".part"):
                    fpath = os.path.join(DOWNLOAD_DIR, fname)
                    try:
                        os.remove(fpath)
                        removed_parts += 1
                        _log(f"🗑  Удалён недокачанный файл: {fname}")
                    except OSError as e:
                        _log(f"⚠️  Не удалось удалить {fname}: {e}")

        msg_ids_to_del = list(dict.fromkeys(msg_ids_to_del))
        if msg_ids_to_del:
            try:
                await client.delete_messages("me", msg_ids_to_del)
            except Exception:
                pass

        _find_cache.clear()
        _find_miss_cache.clear()
        summary = (
            f"🧹 Очистка по /clear завершена:\n"
            f"• Прервано скачиваний: {cancelled_dl}\n"
            f"• Прервано отправок: {cancelled_ul}\n"
            f"• Снято из очереди: {drained}\n"
            f"• Удалено JSON-очередей: {removed_queues}\n"
            f"• Удалено .part файлов: {removed_parts}"
        )
        _log(summary.replace("\n", " | "))
        try:
            summary_msg = await event.reply(summary)
            _schedule_delete(event.message, summary_msg, delay=30)
        except Exception:
            _schedule_delete(event.message, delay=30)
        return

    if cmd == "/stats":
        nome_status = "🟢 ВКЛ" if account_state.get("download_only") else "🔴 ВЫКЛ"
        active_ul = len(active_ul_tasks)
        pending_ul = 0
        for q in queues.values():
            for it in q.get("items", []):
                if it.get("download_only"):
                    continue
                if it.get("status") in ("downloaded", "deferred"):
                    pending_ul += 1


        msg = await event.respond(
            f"📊 **Статистика**\n"
            f"├ Режим `/nome`: **{nome_status}**\n"
            f"├ Скачивается сейчас: **{in_progress['count']}** (в очереди: **{link_queue.qsize()}**)\n"
            f"├ Отправляется в me: **{active_ul}** (ожидают отправки: **{pending_ul}**)"
        )
        _schedule_delete(event.message, msg, delay=60)
        return

    if cmd == "/nome":
        account_state["download_only"] = not account_state["download_only"]
        state_txt = (
            "🟢 ВКЛ" if account_state["download_only"]
            else "🔴 ВЫКЛ"
        )
        await event.respond(
            f"📥 Режим «только скачивание»: {state_txt}\n"
            + (
                "Файлы НЕ будут отправляться в me."
                if account_state["download_only"]
                else "Файлы будут скачиваться и отправляться в me как обычно."
            )
        )
        return

    if "t.me/" not in text.lower():
        return

    links = extract_all_links(text)
    if not links:
        return

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

    if not links:
        return

    status_msgs: dict[tuple, any] = {}
    status_creation_failed = False
    for chat_id, msg_id, full_link in links:
        try:
            status_msg = await event.respond(f"📋 {full_link}")
        except FloodWaitError as fwe:
            _log(
                f"⏳ FloodWait {fwe.seconds}s при создании статуса "
                f"{full_link} — жду и повторяю"
            )
            await asyncio.sleep(fwe.seconds)
            try:
                status_msg = await event.respond(f"📋 {full_link}")
            except Exception as e:
                _log(
                    f"❌ не удалось создать статус {full_link}: "
                    f"{type(e).__name__}: {e} — пачка пропущена"
                )
                status_creation_failed = True
                break
        except Exception as e:
            _log(
                f"❌ не удалось создать статус {full_link}: "
                f"{type(e).__name__}: {e} — пачка пропущена"
            )
            status_creation_failed = True
            break
        status_msgs[(chat_id, msg_id)] = status_msg

    if status_creation_failed:
        for m in status_msgs.values():
            _schedule_delete(m, delay=0)
        return

    batch_id = None
    async with batch_create_lock:
        batch_counter["id"] += 1
        batch_id = batch_counter["id"]

        link_states: dict[tuple, dict] = {}
        items: list[dict] = []

        for chat_id, msg_id, full_link in links:
            key = (chat_id, msg_id)
            link_states[key] = {
                "status": "pending",
                "percent": 0.0,
                "current": 0,
                "total": 0,
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
                "status_msg_id": status_msgs[(chat_id, msg_id)].id,
                "file_message_id": None,
                "local_file": None,
                "downloaded_at": None,
                "download_only": account_state.get("download_only", False),
            })

        _create_queue(batch_id, items)

        batches[batch_id] = {
            "batch_id": batch_id,
            "event": event,
            "status_msgs": status_msgs,
            "link_states": link_states,
            "lock": asyncio.Lock(),
            "total": len(link_states),
            "ok": 0,
            "skip": 0,
            "fail": 0,
            "finished": False,
            "to_delete": [],
        }

        for chat_id, msg_id, full_link in links:
            await link_queue.put((chat_id, msg_id, full_link, batch_id))

    batch_suffix = f" (batch #{batch_id})" if batch_id is not None else ""
    try:
        await event.message.delete()
        _log(f"🗑  удалено исходное сообщение со ссылками{batch_suffix}")
    except FloodWaitError as fwe:
        _log(
            f"  ⏳ FloodWait {fwe.seconds}s при удалении исходного "
            f"сообщения{batch_suffix}"
        )
        await asyncio.sleep(fwe.seconds)
        try:
            await event.message.delete()
            _log(
                f"🗑  удалено исходное сообщение "
                f"(после FloodWait{batch_suffix})"
            )
        except Exception:
            pass
    except Exception:
        pass

def _print_scan_summary(scan: dict):
    if scan["manual"]:
        _log(f"📂 Найдено файлов без записи в очереди: {len(scan['manual'])}")
        for f in scan["manual"][:20]:
            try:
                sz = human_size(os.path.getsize(f))
            except Exception:
                sz = "?"
            _log(f"   • {os.path.basename(f)} ({sz})")
        if len(scan["manual"]) > 20:
            _log(f"   … и ещё {len(scan['manual']) - 20}")

    if scan.get("anomalous_done"):
        _log(
            f"📂 Найдено файлов с терминальным статусом в JSON, "
            f"но на диске: {len(scan['anomalous_done'])} — проверю в «Избранном»"
        )
        for _, it, f in scan["anomalous_done"][:20]:
            _log(f"   • {os.path.basename(f)} ← {it['link']}")
        if len(scan["anomalous_done"]) > 20:
            _log(f"   … и ещё {len(scan['anomalous_done']) - 20}")

    if scan["own_partial"]:
        _log(
            f"📂 Найдено недокачанных файлов (есть запись в очереди): "
            f"{len(scan['own_partial'])} — перекачиваю"
        )
        for _, it, f in scan["own_partial"][:20]:
            _log(f"   • {os.path.basename(f)} ← {it['link']}")
        if len(scan["own_partial"]) > 20:
            _log(f"   … и ещё {len(scan['own_partial']) - 20}")

    if scan["own_ready"]:
        _log(
            f"📂 Найдено готовых к отправке файлов (есть запись в очереди): "
            f"{len(scan['own_ready'])} — поставлю в очередь на отправку"
        )
        for _, it, f in scan["own_ready"][:20]:
            _log(f"   • {os.path.basename(f)} ← {it['link']}")
        if len(scan["own_ready"]) > 20:
            _log(f"   … и ещё {len(scan['own_ready']) - 20}")

# ---------- Запуск ----------
async def main():
    global link_queue, batch_create_lock, shutdown_event
    link_queue = asyncio.Queue()
    batch_create_lock = asyncio.Lock()
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig_name: str):
        if not shutdown_event.is_set():
            _log(f"🛑 Получен {sig_name} — завершаю работу...")
            shutdown_event.set()

    for sig in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(
                getattr(_signal, sig),
                _request_shutdown, sig,
            )
        except (NotImplementedError, AttributeError):
            pass

    existing_ids = _list_queue_files()
    if existing_ids:
        batch_counter["id"] = max(existing_ids)
        _log(f"🔢 batch_counter инициализирован: {batch_counter['id']}")

    # Единый стартовый проход по всем JSON-очередям:
    # 1) удаляем пустые очереди;
    # 2) сбрасываем счётчик повторов для downloaded;
    # 3) обнуляем next_retry_ts для завершённых элементов;
    # 4) воскрешаем fail -> downloaded, если файл на диске цел.
    for qid in list(_list_queue_files()):
        q = _load_queue(qid)
        if not q:
            continue
        items = q.get("items", [])
        if not items:
            queues.pop(qid, None)
            try:
                os.remove(_queue_path(qid))
            except Exception:
                pass
            continue

        changed = False
        for it in items:
            status = it.get("status")
            if status == "downloaded" and it.get("upload_retries", 0) > 0:
                it["upload_retries"] = 0
                changed = True
            elif status in ("done", "fail", "skip") and it.get("next_retry_ts"):
                it["next_retry_ts"] = 0
                changed = True
            elif status == "fail":
                lf = it.get("local_file")
                full_path = os.path.join(DOWNLOAD_DIR, lf) if lf else None
                if full_path and os.path.exists(full_path):
                    expected = it.get("file_size", 0) or 0
                    try:
                        actual = os.path.getsize(full_path)
                    except Exception:
                        actual = 0
                    if expected == 0 or actual >= expected:
                        it["status"] = "downloaded"
                        it["upload_retries"] = 0
                        it["file_message_id"] = None
                        changed = True
                        _log(
                            f"  ♻️  #{qid} {lf}: fail → downloaded "
                            f"(файл цел, {human_size(actual)}) — переотправлю"
                        )
        if changed:
            queues[qid] = q
            _save_queue(qid)

    scan = _scan_downloads()

    _print_scan_summary(scan)

    for qid, it, full in scan["own_partial"]:
        base_name = os.path.basename(full)
        if base_name.endswith(".part"):
            base_name = base_name[:-5]
        q = queues.get(qid) or _load_queue(qid)
        if q is not None:
            queues[qid] = q
            _mark_item(
                qid, it["chat_id"], it["msg_id"],
                status="pending",
                local_file=base_name,
                downloaded_at=None,
            )
            _save_queue(qid)
        _log(f"♻️  {it['link']}: найден .part ({base_name}.part) — докачаю с места остановки")

    session_file = os.path.join(BASE_DIR, "user_session.session")
    had_session_file = os.path.exists(session_file)

    _log("⏳ Подключение к Telegram...")
    await client.start()

    authorized = await client.is_user_authorized()
    if not had_session_file or not authorized:
        _log("📇 Первый запуск или сессия недействительна — загружаю диалоги...")
        await client.get_dialogs()
    else:
        _log("📇 Сессия в порядке — диалоги не перезагружаем.")

    _force_dialogs = os.getenv("FORCE_GET_DIALOGS", "0") == "1"
    if _force_dialogs:
        _log("📇 Принудительная загрузка диалогов (FORCE_GET_DIALOGS=1)...")
        try:
            dialogs = await client.get_dialogs(limit=None)
            _log(f"📇 Загружено диалогов: {len(dialogs)}")
        except Exception as e:
            _log(f"⚠️  get_dialogs упал: {e}")

    me = await client.get_me()
    account_state["is_premium"] = bool(getattr(me, "premium", False))
    limit = LIMIT_PREMIUM if account_state["is_premium"] else LIMIT_FREE
    _log(
        f"👤 Аккаунт: @{me.username or me.id} "
        f"({'Premium' if account_state['is_premium'] else 'Free'})"
    )
    _log(f"📦 Лимит на отправку: {human_size(limit)}")

    if scan.get("anomalous_done"):
        _log(
            f"🔍 Проверяю {len(scan['anomalous_done'])} аномальных файлов в «Избранном»..."
        )
        for qid, it, full in scan["anomalous_done"]:
            name = os.path.basename(full)
            try:
                size = os.path.getsize(full)
            except Exception:
                size = 0
            try:
                found_id = await _find_in_me(
                    display_name=name, file_size=size,
                )
            except FloodWaitError as fwe:
                _log(
                    f"⏳ FloodWait {fwe.seconds}s при проверке {name} — оставляю как manual"
                )
                await asyncio.sleep(fwe.seconds)
                scan["manual"].append(full)
                continue
            except Exception as e:
                _log(
                    f"⚠️  ошибка проверки {name}: {e} — оставляю как manual"
                )
                scan["manual"].append(full)
                continue

            if found_id is not None:
                q = queues.get(qid) or _load_queue(qid)
                if q is not None:
                    queues[qid] = q
                    _mark_item(
                        qid, it["chat_id"], it["msg_id"],
                        status="done",
                        file_message_id=found_id,
                        local_file=None,
                        downloaded_at=None,
                    )
                    _save_queue(qid)
                try:
                    os.remove(full)
                    _log(
                        f"♻️  {name}: уже в «Избранном» (id={found_id}) — "
                        f"помечен done, файл удалён"
                    )
                except Exception as e:
                    _log(f"⚠️  не удалось удалить {name}: {e}")
            else:
                _log(
                    f"📂 {name}: в «Избранном» не найден — помечаю fail в JSON (файл оставляю как manual)"
                )
                q = queues.get(qid) or _load_queue(qid)
                if q is not None:
                    queues[qid] = q
                    _mark_item(
                        qid, it["chat_id"], it["msg_id"],
                        status="fail",
                        local_file=None,
                    )
                    _save_queue(qid)
                scan["manual"].append(full)

    workers = [
        asyncio.create_task(link_worker(i + 1))
        for i in range(CONCURRENT_DOWNLOADS)
    ]
    _log(f"▶ Запущено воркеров скачивания: {CONCURRENT_DOWNLOADS}")

    upload_workers = [
        asyncio.create_task(_upload_worker(i + 1))
        for i in range(CONCURRENT_UPLOADS)
    ]
    _log(
        f"📤 Запущено upload-воркеров: {CONCURRENT_UPLOADS} "
    )

    watchdog_task = asyncio.create_task(_watchdog_stuck_batches(interval=120.0))
    _log("🐕 Запущен watchdog зависших пачек")

    if await _prompt_orphan_uploads(scan):
        await _upload_orphans_directly(scan)

    await resume_pending_queues()

    failed_send = await offer_send_downloaded()

    if failed_send:
        _log(
            f"⚠️  ВНИМАНИЕ: {len(failed_send)} файлов не удалось поставить в очередь на отправку."
        )
        for qid, it, reason in failed_send:
            _log(f"   • #{qid} {it.get('link', '?')}: {reason}")

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
                "Файлы остались в папке загрузки, статусы в JSON — `downloaded`."
            )
            await client.send_message("me", "\n".join(lines))
        except Exception as e:
            _log(f"⚠️  не удалось уведомить в me: {e}")
    else:
        _log("✅ Все найденные файлы поставлены в очередь на отправку")

    for qid in list(batches.keys()):
        await _maybe_finish_batch(qid)

    _log(
        f"🚀 Готово!\n"
        f"одновременно качается {CONCURRENT_DOWNLOADS}.\n"
        f"Команды:\n"
        f"• /nome - включить режим скачивания, без отправки в me\n"
        f"• /quit -  остановит работу скрипта\n"
        f"• /clear - остановить скачивание и отправку, очистить очередь и удалить недокачанные файлы\n"
        f"• /stats - показать текущую статистику"
    )

    nome_mode = "ВКЛ (только скачивание)" if account_state.get("download_only") else "ВЫКЛ (скачивание и отправка в me)"
    try:
        welcome_msg = await client.send_message(
            "me",
            f"🚀 **Xant запущен и готов к работе!**\n"
            f"• Папка загрузки: `{DOWNLOAD_DIR}`\n"
            f"• Режим `/nome`: **{nome_mode}**\n\n"
            f"**Доступные команды:**\n"
            f"`/nome` — включить режим скачивания, без отправки в me\n"
            f"`/quit` — остановит работу скрипта\n"
            f"`/clear` — остановить скачивание и отправку, очистить очередь и удалить недокачанные файлы\n"
            f"`/stats` — показать текущую статистику"
            
        )
        _schedule_delete(welcome_msg, delay=60)
    except Exception as e:
        _log(f"⚠️  Не удалось отправить приветствие в me: {e}")

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
        _log("🛑 Работа прекращена.")
    finally:
        watchdog_task.cancel()
        for uw in upload_workers:
            uw.cancel()
        for w in workers:
            w.cancel()
        await asyncio.gather(
            watchdog_task,
            *upload_workers,
            *workers,
            return_exceptions=True,
        )
        if _pending_deletions:
            for t in list(_pending_deletions):
                t.cancel()
            await asyncio.gather(*_pending_deletions, return_exceptions=True)

        await asyncio.sleep(0.5)
        try:
            await client.disconnect()
        except Exception:
            pass

if __name__ == "__main__":
    if sys.platform == "win32":
        def _win_ctrl_c(signum, frame):
            _log("🛑 Ctrl+C — завершаю работу...")
            try:
                if shutdown_event is not None and not shutdown_event.is_set():
                    shutdown_event.set()
                    return
            except Exception:
                pass
            raise KeyboardInterrupt
        _signal.signal(_signal.SIGINT, _win_ctrl_c)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _log("🛑 Остановлено пользователем (Ctrl+C).")
    except Exception as e:
        _log(f"❌ КРИТИЧЕСКИЙ СБОЙ: {type(e).__name__}: {e}")