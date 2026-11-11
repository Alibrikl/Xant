import os
import re
import json
import time
import asyncio
import mimetypes
import shutil
import sys
import select
import signal as _signal
import logging
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import (
    FileReferenceExpiredError,
    FloodWaitError,
    MessageNotModifiedError,
    ChannelPrivateError,
)
from telethon.tl import types
from telethon.tl.types import (
    DocumentAttributeFilename,
    DocumentAttributeAudio,
    DocumentAttributeVideo,
)
logger = logging.getLogger("xant")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ENV_FILE = os.path.join(BASE_DIR, ".env")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
QUEUE_DIR = os.path.join(DOWNLOAD_DIR, "queues")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(QUEUE_DIR, exist_ok=True)

LIMIT_FREE = 2 * 1024 ** 3
LIMIT_PREMIUM = 4 * 1024 ** 3

CONCURRENT_DOWNLOADS = 3
CONCURRENT_UPLOADS = 3

CLEANUP_DELAY = 10

REORDER_AFTER_EACH_FILE = True
REORDER_DEBOUNCE = 3.0

VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v", ".3gp"}
AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".flac", ".wav", ".ogg", ".opus", ".oga"}
PHOTO_EXTS = {
    ".jpg", ".jpeg", ".jpe", ".png", ".webp",
    ".bmp", ".tif", ".tiff", ".heic", ".heif",
}

_USELESS_BASENAMES = {"record", "audio", "voice", "file", "track", "sound",
    "audio_file", "voice_file", "record_file",
    "telegram_audio", "telegram_voice", "telegram_record",
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
except (TypeError, ValueError):
    print("❌ Ошибка: Неверный TELEGRAM_API_ID в файле .env.")
    exit(1)

client = TelegramClient(
    os.path.join(BASE_DIR, "user_session"), API_ID, API_HASH,
    connection_retries=10, retry_delay=3, timeout=60,
)

LINK_PATTERN = r"https://t\.me/(?:c/(\d+)|([\w_]+))/(\d+)"
LINK_REGEX = re.compile(LINK_PATTERN, re.IGNORECASE)

processed: set[tuple] = set()
in_progress = {"count": 0}

batches: dict[int, dict] = {}
batch_counter = {"id": 0}

account_state = {"is_premium": False}

queues: dict[int, dict] = {}

link_queue = None
batch_create_lock = None
upload_lock = None
shutdown_event = None
upload_queue = None
upload_worker_task = None

_pending_deletions: set[asyncio.Task] = set()
_pending_manual_uploads: set[asyncio.Task] = set()


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
                logger.warning(
                    "[FloodWait] пауза %ss перед удалением", fwe.seconds,
                )
                await asyncio.sleep(fwe.seconds)
            except Exception:
                break


def _schedule_delete(*messages, delay: float = CLEANUP_DELAY):
    task = asyncio.create_task(_delete_later(*messages, delay=delay))
    _pending_deletions.add(task)
    task.add_done_callback(_pending_deletions.discard)
    return task


def _log(msg: str, prefix: str | None = None, level: int = logging.INFO):
    """
    Совместимая обёртка. Все старые вызовы `_log("...")` работают.
    `prefix` добавляется к сообщению (например, "[download]").
    """
    text = f"{prefix} {msg}" if prefix else msg
    logger.log(level, text)


def _queue_snapshot(q: dict) -> str:
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


# ПАТЧ: добавлены C1-контролы (\x7f-\x9f) для Windows-совместимости
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
    """
    ПАТЧ: пытаемся получить оригинальное имя файла из разных мест:
      1. DocumentAttributeFilename.file_name (если отправлен как документ)
      2. DocumentAttributeAudio.title / performer (ID3-теги)
      3. message.text (caption) — первая строка, если похоже на имя
      4. Fallback: record_{id}.ogg / voice_{id}.ogg / file_{id}.ext
    """
    original = None
    source = None

    # 1. file.name (Telegram иногда даёт)
    if message.file and message.file.name:
        original = message.file.name
        source = "file.name"

    # 2. DocumentAttributeFilename.file_name
    if not original and isinstance(message.media, types.MessageMediaDocument):
        doc = message.media.document
        if isinstance(doc, types.Document):
            for attr in doc.attributes:
                if isinstance(attr, types.DocumentAttributeFilename):
                    if attr.file_name:
                        original = attr.file_name
                        source = "DocumentAttributeFilename"
                        break

    # 3. DocumentAttributeAudio.title
    if not original and isinstance(message.media, types.MessageMediaDocument):
        doc = message.media.document
        if isinstance(doc, types.Document):
            for attr in doc.attributes:
                if isinstance(attr, types.DocumentAttributeAudio):
                    if attr.title:
                        original = attr.title
                        source = "DocumentAttributeAudio.title"
                        break

    # 4. message.text (caption) — первая строка, если похоже на имя
    if not original and message.text:
        first_line = message.text.strip().split("\n")[0].strip()
        # ПАТЧ: caption должен выглядеть как имя файла:
        # - не URL, не пустое, длина 1-100
        # - содержит точку (расширение) ИЛИ не содержит эмодзи/пробелов подряд
        # - не содержит символов, которые обычно есть в обычном тексте
        _looks_like_filename = (
            first_line
            and 1 <= len(first_line) <= 100
            and "http" not in first_line.lower()
            and "www." not in first_line.lower()
            and (
                # есть расширение — точно имя файла
                bool(re.search(r"\.[A-Za-z0-9]{1,8}$", first_line))
                or (
                    # или: короткое, без эмодзи, без множественных пробелов
                    len(first_line) <= 60
                    and not re.search(r"[\U0001F300-\U0001FAFF]", first_line)
                    and "  " not in first_line
                )
            )
        )
        if _looks_like_filename:
            original = first_line
            source = "message.text"

    _log(
        f"[build_filename] id={message.id} kind={_detect_kind(message)} "
        f"source={source!r} original={original!r}"
    )

    # ПАТЧ: «бесполезные» имена (record.ogg, audio.ogg, voice.ogg) —
    # считаем, что оригинала нет. Иначе все файлы будут record.ogg,
    # record (1).ogg, record (2).ogg и т.д.
    if original:
        _base_check, _ = os.path.splitext(original)
        _base_norm = _base_check.strip().lower()
        _base_norm = re.sub(r"[_\s]*\(\d+\)$", "", _base_norm)
        _base_norm = re.sub(r"_\d+$", "", _base_norm)
        if _base_norm in _USELESS_BASENAMES:
            _log(
                f"  [build_filename] «бесполезное» имя {original!r} — "
                f"считаю, что оригинала нет"
            )
            original = None
            source = None

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


    filename = f"{base}{ext}"
    filename = _truncate_to_bytes(filename, _MAX_NAME_BYTES)
    return filename

def unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    reserve = 8  # ПАТЧ: запас на "(99999)" + возможные вариации
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
    # ПАТЧ: KB для маленьких значений
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
_FIND_CACHE_MAX = 1000


def _find_cache_put(key, value):
    """ПАТЧ: ограничиваем размер _find_cache (FIFO-вытеснение)."""
    if len(_find_cache) >= _FIND_CACHE_MAX:
        # Удаляем первый (самый старый) ключ
        oldest = next(iter(_find_cache))
        _find_cache.pop(oldest, None)
    _find_cache[key] = value


async def _find_in_me(display_name: str, file_size: int | None,
    scan_limit: int = 400, max_retries: int = 2) -> int | None:
    # ПАТЧ: для фото поиск по имени не работает (file.name=None).
    # Возвращаем None — фото отправится, дубли возможны, но не сломается.
    _ext = os.path.splitext(display_name)[1].lower()
    if _ext in PHOTO_EXTS:
        _log(
            f"  🔍 _find_in_me({display_name}): фото — "
            f"поиск по имени не поддерживается, не кэширую"
        )
        return None
    if not display_name:
        return None

    cache_key = (display_name, file_size if file_size and file_size > 0 else None)
    check_size = bool(file_size and file_size > 0)

    cached = _find_cache.get(cache_key)
    if cached is not None:
        try:
            m = await client.get_messages("me", ids=cached)
            matched = False
            if m is not None and not isinstance(m, types.MessageEmpty):
                if getattr(m, "photo", None) is not None:
                    # ПАТЧ: фото — у него нет file.name, сравниваем по размеру.
                    # MessageMediaPhoto → m.file.size = размер фотографии.
                    size_ok = (
                        not check_size
                        or (getattr(m.file, "size", 0) or 0) == file_size
                    )
                    if size_ok:
                        matched = True
                elif getattr(m, "file", None) is not None:
                    # Документ/аудио/видео — сравниваем по имени + размеру.
                    if m.file.name == display_name and (
                        not check_size
                        or (getattr(m.file, "size", 0) or 0) == file_size
                    ):
                        matched = True
            if matched:
                _log(f"  💾 _find_in_me({display_name}): из кэша (id={cached})")
                return cached
            _find_cache.pop(cache_key, None)
        except Exception:
            _find_cache.pop(cache_key, None)

    attempt = 0
    while True:
        try:
            found: int | None = None
            async for msg in client.iter_messages("me", limit=scan_limit):
                if getattr(msg, "photo", None) is not None:
                    # ПАТЧ: фото — сравниваем только по размеру.
                    if check_size:
                        size = getattr(msg.file, "size", 0) or 0
                        if size != file_size:
                            continue
                    found = msg.id
                    break
                elif getattr(msg, "file", None) is not None:
                    # Документ/аудио/видео.
                    if msg.file.name != display_name:
                        continue
                    if check_size:
                        size = getattr(msg.file, "size", 0) or 0
                        if size != file_size:
                            continue
                    found = msg.id
                    break

            if found is not None:
                _find_cache_put(cache_key, found)
                _log(f"  💾 _find_in_me({display_name}): найдено id={found} — кэширую")
            else:
                _log(f"  🔍 _find_in_me({display_name}): не найдено — не кэширую")
            return found

        except FloodWaitError as fwe:
            attempt += 1
            if attempt > max_retries:
                _log(f"  ⚠️  _find_in_me({display_name}): FloodWait — сдаюсь")
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

def _recover_queues_from_downloads():
    """
    Восстанавливает JSON-очереди из файлов в downloads/, если JSON'ов нет
    (или они повреждены/удалены), но файлы на диске есть.

    Логика:
      1. Собираем имена файлов, которые уже упомянуты в существующих JSON
         (own_by_name) — их НЕ трогаем.
      2. Для остальных файлов в downloads/ — создаём новую очередь,
         status="downloaded", local_file=<имя>, file_size=<размер>.
      3. Сохраняем JSON.

    Возвращает (created_count, skipped_existing).
    """
    existing_names: set[str] = set()
    for qid in _list_queue_files():
        q = _load_queue(qid)
        if not q:
            continue
        for it in q.get("items", []):
            lf = it.get("local_file")
            if lf:
                existing_names.add(lf)

    orphan_files: list[str] = []
    if os.path.isdir(DOWNLOAD_DIR):
        for name in os.listdir(DOWNLOAD_DIR):
            if name.endswith(".part"):
                continue
            full = os.path.join(DOWNLOAD_DIR, name)
            if not os.path.isfile(full):
                continue
            if name in existing_names:
                continue
            orphan_files.append(name)

    if not orphan_files:
        return (0, len(existing_names))

    # Определяем batch_id: max(existing) + 1 или 1, если очередей нет.
    existing_ids = _list_queue_files()
    batch_id = (max(existing_ids) + 1) if existing_ids else 1

    # Восстанавливаем batch_counter, чтобы новые очереди не пересекались.
    if batch_id > batch_counter["id"]:
        batch_counter["id"] = batch_id

    items: list[dict] = []
    for idx, name in enumerate(orphan_files):
        full = os.path.join(DOWNLOAD_DIR, name)
        try:
            size = os.path.getsize(full)
        except Exception:
            size = 0

        # ПАТЧ: уникальный msg_id, чтобы не конфликтовать с manual-очередями
        # (batch_id * 1_000_000 + idx, отрицательный — чтобы не пересечься
        # с реальными Telegram msg_id)
        unique_mid = -(batch_id * 1_000_000 + idx)
        items.append({
            "chat_id": "me",
            "msg_id": unique_mid,
            "link": name,
            "status": "downloaded",
            "status_msg_id": None,
            "slot_msg_id": None,
            "file_message_id": None,
            "local_file": name,
            "downloaded_at": time.time(),
            "file_size": size,
        })

    _create_queue(batch_id, items)
    _log(
        f"♻️  восстановлена очередь #{batch_id} из downloads/: "
        f"{len(items)} файлов (orphan), пропущено (уже в JSON): "
        f"{len(existing_names)}"
    )
    return (len(items), len(existing_names))

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

        # ---------- .part ----------
        if name.endswith(".part"):
            base_name = name[:-5]
            entry = own_by_name.get(base_name) or own_by_name.get(name)
            if entry is not None:
                qid, it = entry
                own_partial.append((qid, it, full))
            else:
                # осиротевший .part — удаляем, к отправке не предлагаем
                try:
                    os.remove(full)
                    _log(f"[scan] удалён осиротевший .part: {name}")
                except Exception as e:
                    _log(f"[scan] не удалось удалить .part {name}: {e}")
            continue

        # ---------- обычный файл ----------
        entry = own_by_name.get(name)          # ← ВОЗВРАЩЕНО
        if entry is None:
            manual.append(full)
            continue

        qid, it = entry
        status = it.get("status")

        if status in ("pending", "downloading"):
            own_partial.append((qid, it, full))
        elif status in ("downloaded", "uploading"):
            own_ready.append((qid, it, full))
        else:
            logger.warning(
                "файл %s: статус «%s» в JSON, но файл на диске — "
                "считаю manual",
                name, status,
            )
            anomalous_done.append((qid, it, full))

    return {
        "manual": manual,
        "own_partial": own_partial,
        "own_ready": own_ready,
        "anomalous_done": anomalous_done,
    }


# ---------- Скачивание ----------
def _cleanup_partial(local_path: str, expected_size: int = 0):
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
                    continue
            else:
                # ПАТЧ: если размер неизвестен — не трогаем финальный файл,
                # только .part. Иначе можно потерять уже скачанный целый файл.
                if not path.endswith(".part"):
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
    _log(f"[download] START {chat_id}/{msg_id}")

    # ---------- 1. Получаем сообщение (с timeout 60s) ----------
    target_msg = None
    try:
        _log(f"[download] get_messages {chat_id}/{msg_id}...")
        target_msg = await asyncio.wait_for(
            client.get_messages(chat_id, ids=msg_id),
            timeout=60,
        )
        _log(
            f"[download] get_messages {chat_id}/{msg_id}: "
            f"{'есть медиа' if target_msg and target_msg.media else 'НЕТ медиа'}"
        )
    except asyncio.TimeoutError:
        _log(f"[download] TIMEOUT get_messages {chat_id}/{msg_id} (60s)")
        return None
    except ChannelPrivateError:
        logger.warning(
            "[download] канал %s приватный или аккаунт не состоит в нём — "
            "пропуск %s/%s",
            chat_id, chat_id, msg_id,
        )
        return None
    except FloodWaitError as fwe:
        logger.warning(
            "[download] FloodWait %ss при get_messages — жду",
            fwe.seconds,
        )
        await asyncio.sleep(fwe.seconds)
        try:
            target_msg = await client.get_messages(chat_id, ids=msg_id)
        except ChannelPrivateError:
            logger.warning(
                "[download] канал %s приватный — пропуск %s/%s",
                chat_id, chat_id, msg_id,
            )
            return None
        except Exception as e:
            _log(f"[download] ошибка при перезапросе: {e}")
            return None
    except Exception as e:
        _log(f"[download] ошибка get_messages: {type(e).__name__}: {e}")
        return None

    if not target_msg or not target_msg.media:
        logger.warning(
            "[download] сообщение %s/%s не найдено или не содержит медиа — "
            "пропуск",
            chat_id, msg_id,
        )
        return None

    # ---------- 2. Размер, лимит Telegram и место на диске ----------
    if size_holder is not None:
        size_holder["size"] = target_msg.file.size if target_msg.file else 0

        if size_holder["size"] > 0:
            # ПАТЧ: проверяем лимит Telegram ДО скачивания, чтобы
            # не тратить трафик и диск на файл, который всё равно
            # не отправим.
            _limit = LIMIT_PREMIUM if account_state["is_premium"] else LIMIT_FREE
            if size_holder["size"] > _limit:
                logger.warning(
                    "[limit] %s/%s: %s > %s — пропуск без скачивания",
                    chat_id, msg_id,
                    human_size(size_holder["size"]),
                    human_size(_limit),
                )
                return None

            try:
                free = shutil.disk_usage(DOWNLOAD_DIR).free
                need = size_holder["size"] * 1.1
                if free < need:
                    logger.warning(
                        "[disk check] недостаточно места для %s/%s: "
                        "нужно %s, свободно %s — пропуск",
                        chat_id, msg_id, human_size(need), human_size(free),
                    )
                    return None
            except Exception as e:
                logger.warning("[disk check] не удалось проверить место: %s", e)

    # ---------- 3. Путь ----------
    suggested_filename = build_filename(target_msg)
    local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))
    # ПАТЧ: скачиваем во временный .part, переименовываем только при успехе
    part_path = local_path + ".part"
    if size_holder is not None:
        size_holder["path"] = local_path
        size_holder["part_path"] = part_path
    if on_path_known is not None:
        on_path_known(local_path)

    # ---------- 4. Скачивание с retry ----------
    attempt = 0
    while True:
        _log(
            f"⬇️  download_media: {chat_id}/{msg_id} → "
            f"{os.path.basename(local_path)} "
            f"(размер={human_size(size_holder.get('size', 0) if size_holder else 0)})"
        )

         # ПАТЧ: таймаут зависит от размера — 30 мин базово + время
        # на скачивание при скорости ~100 КБ/с (с запасом).
        _sz = size_holder.get("size", 0) if size_holder else 0
        _dl_timeout = max(1800, int(_sz / (100 * 1024)) + 600)

        try:
            result = await asyncio.wait_for(
                client.download_media(
                    target_msg, file=part_path,
                    progress_callback=progress_callback,
                ),
                timeout=_dl_timeout,
            )
            # Атомарно переименовываем .part → финальное имя
            if result and os.path.exists(part_path):
                try:
                    os.replace(part_path, local_path)
                    result = local_path
                except OSError as e:
                    _log(
                        f"[download] не удалось переименовать "
                        f"{os.path.basename(part_path)} → "
                        f"{os.path.basename(local_path)}: {e}"
                    )
                    _cleanup_partial(
                        local_path,
                        size_holder.get("size", 0) if size_holder else 0,
                    )
                    attempt += 1
                    if attempt >= max_retries:
                        return None
                    await asyncio.sleep(2 * attempt)
                    continue
            # ПАТЧ: проверяем, что скачался весь файл
            expected = size_holder.get("size", 0) if size_holder else 0
            if expected > 0 and result and os.path.exists(result):
                try:
                    actual = os.path.getsize(result)
                except Exception:
                    actual = 0
                if actual < expected:
                    attempt += 1
                    _log(
                        f"[download] неполный файл {chat_id}/{msg_id}: "
                        f"{human_size(actual)} из {human_size(expected)} "
                        f"(попытка {attempt}/{max_retries})"
                    )
                    _cleanup_partial(result, expected)
                    if attempt >= max_retries:
                        return None
                    await asyncio.sleep(2 * attempt)
                    try:
                        target_msg = await client.get_messages(
                            chat_id, ids=msg_id, use_cache=False,
                        )
                    except Exception as e:
                        _log(f"[download] не удалось перезапросить target_msg: {e}")
                        return None
                    if not target_msg or not target_msg.media:
                        return None
                    continue
            _log(
                f"✅ download_media: {chat_id}/{msg_id} → "
                f"{os.path.basename(local_path)} — готово"
            )
            return result

        except asyncio.TimeoutError:
            attempt += 1
            _log(
                f"[download] TIMEOUT download_media {chat_id}/{msg_id} "
                f"({_dl_timeout}s, попытка {attempt}/{max_retries})"
            )
            _cleanup_partial(
                local_path,
                size_holder.get("size", 0) if size_holder else 0,
            )
            if attempt >= max_retries:
                _log(f"[download] исчерпаны попытки — fail {chat_id}/{msg_id}")
                return None
            await asyncio.sleep(5)
            try:
                target_msg = await client.get_messages(
                    chat_id, ids=msg_id, use_cache=False,
                )
            except Exception as e:
                _log(f"[download] не удалось перезапросить target_msg: {e}")
                return None
            if not target_msg or not target_msg.media:
                return None
            continue

        except FileReferenceExpiredError:
            attempt += 1
            logger.warning(
                "[download] ссылка устарела (попытка %s) — перезапрашиваю",
                attempt,
            )
            _cleanup_partial(
                local_path,
                size_holder.get("size", 0) if size_holder else 0,
            )
            if attempt >= max_retries:
                _log("[download] исчерпаны попытки (FileRefExpired) — fail")
                return None
            try:
                target_msg = await client.get_messages(
                    chat_id, ids=msg_id, use_cache=False,
                )
            except Exception as e:
                _log(f"[download] не удалось перезапросить target_msg: {e}")
                return None
            if not target_msg or not target_msg.media:
                return None
            if size_holder is not None:
                size_holder["size"] = target_msg.file.size if target_msg.file else 0

            old_path = local_path
            suggested_filename = build_filename(target_msg)
            local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))

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

            part_path = local_path + ".part"
            if size_holder is not None:
                size_holder["path"] = local_path
                size_holder["part_path"] = part_path
            if on_path_known is not None:
                on_path_known(local_path)
            continue

        except FloodWaitError as fwe:
            _cleanup_partial(
                local_path,
                size_holder.get("size", 0) if size_holder else 0,
            )
            logger.warning("[FloodWait] ждём %ss", fwe.seconds)
            await asyncio.sleep(fwe.seconds)
            continue

        except asyncio.CancelledError:
            # ПАТЧ: чистим частичный файл при отмене
            _cleanup_partial(
                local_path,
                size_holder.get("size", 0) if size_holder else 0,
            )
            raise

        except Exception as e:
            attempt += 1
            logger.warning(
                "[download_with_retry] %s (попытка %s/%s): %s",
                type(e).__name__, attempt, max_retries, e,
            )
            _cleanup_partial(
                local_path,
                size_holder.get("size", 0) if size_holder else 0,
            )
            if attempt >= max_retries:
                _log(
                    f"[download] исчерпаны попытки ({attempt}/{max_retries}): "
                    f"{type(e).__name__}: {e}"
                )
                return None
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
    # ПАТЧ: сначала hachoir (mp3, m4a, flac — обычно ок).
    try:
        from hachoir.metadata import extractMetadata
        from hachoir.parser import createParser
        parser = createParser(path)
        if parser:
            meta = extractMetadata(parser)
            if meta:
                d = _to_seconds(meta.get("duration"))
                if d > 0:
                    return d
    except Exception:
        pass

    # ПАТЧ: fallback через mutagen (лучше для ogg/opus).
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

def _audio_tags_sync(path: str) -> tuple[str | None, str | None]:
    """
    Возвращает (title, performer) из ID3/тегов файла.
    Оба значения — str или None. Не бросает исключений.
    """
    try:
        from mutagen import File as MutagenFile
        m = MutagenFile(path)
        if m is None or m.tags is None:
            return (None, None)

        tags = m.tags

        def _get(*keys):
            """mutagen tags — это dict-like; ключи различаются по формату."""
            for k in keys:
                try:
                    v = tags.get(k)
                except Exception:
                    v = None
                if not v:
                    continue
                # mutagen может вернуть список (ID3) или строку
                if isinstance(v, (list, tuple)):
                    if not v:
                        continue
                    v = v[0]
                # ID3-объекты (TIT2, TPE1) — приводим к str
                s = str(v).strip()
                if s:
                    return s
            return None

        # ID3 (mp3), MP4 (m4a), Vorbis (flac/ogg), ASF (wma)
        title = _get(
            "TIT2", "title",       # ID3 / Vorbis
            "\xa9nam",             # MP4
            "Title",               # ASF
        )
        performer = _get(
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

async def _send_media(chat, path: str, display_name: str, progress_cb=None,
                      caption: str | None = None):
    kind = _get_send_kind(path)
    name_attr = DocumentAttributeFilename(display_name)

    if kind == "photo":
        _log(
            f"[_send_media] {display_name}: kind=photo, "
            f"отправляю как фото"
        )
        result = await client.send_file(
            chat, path,
            caption=caption,
            progress_callback=progress_cb,
        )
        _log(
            f"[_send_media] {display_name}: send_file вернул "
            f"type={type(result).__name__!r}, id={getattr(result, 'id', None)!r}"
        )
        if result is None:
            raise RuntimeError(
                f"_send_media: send_file вернул None для {display_name} (kind=photo)"
            )
        return result

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
                caption=caption,
                progress_callback=progress_cb,
            )
        return await client.send_file(
            chat, path,
            force_document=True,
            attributes=[name_attr],
            caption=caption,
            progress_callback=progress_cb,
        )

    elif kind == "audio":
        d = await asyncio.to_thread(_audio_meta_sync, path)

        ext_lower = os.path.splitext(path)[1].lower()

        try:
            fsize = os.path.getsize(path)
        except Exception:
            fsize = 0

        _log(
            f"[_send_media] {os.path.basename(path)}: kind=audio, "
            f"ext={ext_lower}, size={human_size(fsize)}, "
            f"duration={d}"
        )

        _MIME_BY_EXT = {
            ".mp3": "audio/mpeg",
            ".m4a": "audio/mp4",
            ".aac": "audio/aac",
            ".flac": "audio/flac",
            ".wav": "audio/wav",
            ".ogg": "audio/ogg",
            ".oga": "audio/ogg",
            ".opus": "audio/ogg",
        }
        mime = _MIME_BY_EXT.get(ext_lower, "audio/mpeg")

        title, performer = await asyncio.to_thread(_audio_tags_sync, path)

        if not title:
            base_no_ext = os.path.splitext(display_name)[0]
            title = base_no_ext or None
        if not performer:
            performer = None

        if title:
            title = title[:128]
        if performer:
            performer = performer[:128]

        # ПАТЧ: OGG/Opus отправляем как голосовое ТОЛЬКО если имя
        # «бесполезное» (record, voice, audio и т.п.). Если у файла
        # осмысленное имя — отправляем как обычный аудиофайл,
        # чтобы имя сохранилось (Telegram у voice имя не показывает).
        _base_check = os.path.splitext(display_name)[0].strip().lower()
        # убираем суффикс " (N)" / "_N", добавленный unique_path/_sanitize
        _base_norm = re.sub(r"[_\s]*\(\d+\)$", "", _base_check)
        _base_norm = re.sub(r"_\d+$", "", _base_norm)
        _has_meaningful_name = _base_norm not in _USELESS_BASENAMES

        is_voice = (
            ext_lower in (".ogg", ".opus", ".oga")
            and 0 < d <= 60
            and not _has_meaningful_name
        )

        _log(
            f"[_send_media] {display_name}: is_voice={is_voice} "
            f"(base={_base_check!r}, meaningful={_has_meaningful_name}, "
            f"dur={d})"
        )

        attrs = [
            DocumentAttributeAudio(
                duration=d,
                title=title,
                performer=performer,
                voice=is_voice,
            ),
            name_attr,
        ]

        return await client.send_file(
            chat, path,
            attributes=attrs,
            mime_type=mime,
            progress_callback=progress_cb,
            caption=caption,
        )

    # ПАТЧ: document (PDF, ZIP, DOCX и т.д.) — отправляем как документ.
    ext = os.path.splitext(path)[1].lower()
    if ext == ".gif":
        # GIF — как анимация (Telethon сам распознаёт по расширению).
        _log(f"[_send_media] {display_name}: gif → как анимация")
        result = await client.send_file(
            chat, path,
            attributes=[name_attr],   # ← сохраняем имя для дедупликации
            caption=caption,
            progress_callback=progress_cb,
        )
    else:
        _log(f"[_send_media] {display_name}: document → force_document=True")
        result = await client.send_file(
            chat, path,
            force_document=True,
            attributes=[name_attr],
            caption=caption,
            progress_callback=progress_cb,
        )
    # ПАТЧ: если Telethon вернул None (что не должен) — явно бросаем.
    if result is None:
        raise RuntimeError(
            f"_send_media: send_file вернул None для {display_name} "
            f"(kind={kind})"
        )
    return result
    

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
    st_stub = {
        "status": status,
        "percent": 0.0,
        "current": 0,
        "total": 0,
        "up_current": 0,
        "up_total": it.get("file_size", 0) or 0,
        "file_size": it.get("file_size", 0) or 0,
        "up_percent": 0.0,
    }
    # ПАТЧ 4: статус из JSON — источник истины
    return _render_status(st_stub, link, force_status=status)


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

        # ПАТЧ 4: берём статус из JSON, а не из st — он источник истины.
        # ПАТЧ 5: 📤 рисуем ТОЛЬКО если элемент реально в _uploading (сейчас
        # отправляется). Иначе, даже если в JSON застрял "uploading",
        # показываем ⏳.
        batch_id = batch.get("batch_id")
        json_status = None
        item = None
        if batch_id is not None:
            item = _find_item(batch_id, key[0], key[1])
            if item is not None:
                json_status = item.get("status")

        if json_status == "uploading" and key not in batch.get("_uploading", set()):
            json_status = "downloaded"

        queue_pos = None
        queue_total = None
        if (json_status or st.get("status")) == "downloaded":
            if batch_id is not None:
                q = queues.get(batch_id)
                if q is not None:
                    items = q.get("items", [])
                    queue_total = len(items)
                    for i, it in enumerate(items):
                        if (it["chat_id"] == key[0]
                                and it["msg_id"] == key[1]):
                            queue_pos = i + 1
                            break

        try:
            await status_msg.edit(_render_status(
                st, full_link,
                queue_pos=queue_pos,
                queue_total=queue_total,
                force_status=json_status,   # ← ПАТЧ 4
            ))
        except MessageNotModifiedError:
            pass
        except FloodWaitError as fwe:
            # ПАТЧ: логируем, чтобы видеть в тестах/проде
            _log(
                f"[flood] edit статуса {full_link}: {fwe.seconds}s — "
                f"пропускаю обновление"
            )
            # НЕ спим здесь — статус не критичен, следующее обновление
            # придёт через 3s throttle. Сон заблокировал бы воркер.
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
        to_delete_ids: list[int] = []
        for it in q["items"]:
            for k in ("status_msg_id", "slot_msg_id"):
                mid = it.get(k)
                if mid:
                    if it.get("status") == "done" and k == "slot_msg_id":
                        continue
                    to_delete_ids.append(mid)
        to_delete_ids = list(dict.fromkeys(to_delete_ids))

        _log(
            f"batch #{batch_id}: старт reconcile | "
            f"items={len(q['items'])}, к удалению={len(to_delete_ids)}"
        )

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

        for it in q["items"]:
            for k in ("status_msg_id", "slot_msg_id"):
                mid = it.get(k)
                if mid and mid in deleted_ok:
                    it[k] = None
        _save_queue(batch_id)

        new_status_msgs: dict[tuple, any] = {}
        for idx, it in enumerate(q["items"]):
            key = (it["chat_id"], it["msg_id"])
            full_link = it["link"]
            status = it["status"]
            tag = f"item #{idx} {full_link} ({status})"

            if status == "done" and it.get("file_message_id"):
                _log(
                    f"  ⏭  ПРОПУСК пересылки {tag}: "
                    f"файл id={it['file_message_id']} уже в чате "
                    f"(плейсхолдер не создаём)"
                )
                continue

            else:
                queue_pos = None
                queue_total = None
                if status == "downloaded":
                    queue_total = len(q["items"])
                    queue_pos = idx + 1

                # ПАТЧ UI: JSON — источник истины.
                # st["status"] мог застрять в uploading после deferred/ошибки —
                # перезаписываем статусом из JSON, чтобы downloaded всегда был ⏳.
                st_for_render = dict(
                    batch["link_states"].get(key) or {}
                )
                st_for_render["status"] = status
                if status == "downloaded":
                    st_for_render["up_current"] = 0
                    st_for_render["up_percent"] = 0.0
                if not st_for_render.get("up_total"):
                    st_for_render["up_total"] = it.get("file_size", 0) or 0
                if not st_for_render.get("file_size"):
                    st_for_render["file_size"] = it.get("file_size", 0) or 0

                # ПАТЧ 5: 📤 — только если элемент реально в _uploading.
                status_for_render = status
                if (status == "uploading"
                        and key not in batch.get("_uploading", set())):
                    status_for_render = "downloaded"

                text = _render_status(
                    st_for_render, full_link,
                    queue_pos=queue_pos,
                    queue_total=queue_total,
                    force_status=status_for_render,
                )
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
                st["up_percent"] = 0.0
                st["last_up_cb"] = 0.0
                if it.get("file_size"):
                    st["file_size"] = it["file_size"]
                    if not st.get("up_total"):
                        st["up_total"] = it["file_size"]
            else:
                st["last_edit"] = 0.0
                st["last_cb"] = 0.0

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
        except Exception:
            logger.exception(
                "[request_reconcile] ошибка реконсиляции пачки %s", batch_id,
            )
        finally:
            state["running"] = False

        if not state["pending"]:
            break


# ---------- Хелпер ----------
def _release_processed(key: tuple, result: str) -> str:
    processed.discard(key)
    return result


# ---------- Обработка одной ссылки ----------
async def process_link(chat_id, msg_id, full_link, batch_id):
    batch = batches.get(batch_id)
    if batch is None:
        return "fail"

    # ПАТЧ: batch отменён через /clear — не качаем
    if batch.get("cancelled"):
        _log(f"⏭  {full_link}: batch отменён — пропуск")
        return "fail"

    key = (chat_id, msg_id)
    if key in processed:
        return "deferred"

    st = batch["link_states"].get(key)
    if st is None:
        return "fail"

    item = _find_item(batch_id, chat_id, msg_id)
    if item is None:
        return "fail"

    if item.get("status") in ("done", "skip", "fail"):
        _log(
            f"⏭  пропуск {full_link}: уже {item.get('status')} "
            f"(повторный process_link)"
        )
        return "deferred"

    # ПАТЧ: processed.add — только после всех проверок
    processed.add(key)
    filename = None
    size_holder = {"size": 0}
    st["_last_logged_pct"] = -1

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
            cur_item = _find_item(batch_id, chat_id, msg_id)
            if cur_item is None:
                return
            if cur_item.get("status") in ("fail", "done", "skip"):
                return
            path = size_holder.get("path")
            if path and not item.get("local_file"):
                _mark_item(
                    batch_id, chat_id, msg_id,
                    status="downloading",
                    local_file=os.path.basename(path),
                )

            # ПАТЧ: прогресс в консоль, но не чаще, чем каждые 10%
            pct = (current / total * 100) if total else 0
            last_pct = st.get("_last_logged_pct", -1)
            if pct - last_pct >= 10 or current == total:
                st["_last_logged_pct"] = pct
                logger.info(
                    "⬇️  [download] %s: %.1f%% (%s / %s)",
                    full_link, pct,
                    human_size(current), human_size(total),
                )

            if key in batch["status_msgs"]:
                await _update_status_msg(batch, key, full_link)

    try:
        already = item.get("local_file")
        if already and os.path.exists(os.path.join(DOWNLOAD_DIR, already)):
            filename = os.path.join(DOWNLOAD_DIR, already)
            _log(f"📂 файл уже скачан ранее: {filename}")

            # ПАТЧ: НЕ проверяем me здесь — _upload_one сделает это
            # ближе к отправке. Убирает дублирующий iter_messages
            # для uploading-элементов. Просто ставим downloaded.
            if item.get("status") == "uploading":
                _log(
                    f"📂 {full_link}: статус uploading — "
                    f"передаю upload-воркеру (проверит me сам)"
                )

            # downloaded или uploading (не найден) — ставим downloaded
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
                _mark_item(batch_id,
                           chat_id, 
                           msg_id, 
                           status="fail",
                           next_retry_ts=0
                           )
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
                _mark_item(batch_id, 
                           chat_id, 
                           msg_id, 
                           status="fail",
                           next_retry_ts=0
                           )
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

        display_name = os.path.basename(filename)
        file_size = size_holder.get("size") or (
            os.path.getsize(filename) if os.path.exists(filename) else 0
        )
        st["up_current"] = 0
        st["up_total"] = file_size
        st["up_percent"] = 0.0
        st["last_up_cb"] = 0.0

        _mark_item(
            batch_id, chat_id, msg_id,
            status="downloaded",
            local_file=display_name,
            downloaded_at=time.time(),
            file_size=file_size,
        )
        await _update_status_msg(batch, key, full_link, force=True)

        _log(
            f"📥 скачан и поставлен в очередь на отправку: {full_link} "
            f"→ {display_name} ({human_size(file_size)})"
        )

        _release_processed(key, "downloaded")
        return "downloaded"

    except asyncio.CancelledError:
        # ПАТЧ: сбрасываем в pending, чтобы при следующем запуске
        # элемент был перекачан (а не остался в downloading)
        try:
            _mark_item(batch_id, chat_id, msg_id, status="pending")
            st["status"] = "pending"
        except Exception:
            pass
        _release_processed(key, "fail")
        raise

    except ChannelPrivateError:
        _log(
            f"❌ {full_link}: канал приватный или аккаунт не состоит в нём"
        )
        _mark_item(batch_id,
                   chat_id, 
                   msg_id, 
                   status="fail",
                   next_retry_ts=0
                   )
        st["status"] = "fail"
        await _update_status_msg(batch, key, full_link, force=True)
        _release_processed(key, "fail")
        if REORDER_AFTER_EACH_FILE:
            await request_reconcile(batch_id, force=False)
        return "fail"
    except Exception as e:
        _log(f"❌ {full_link}: {type(e).__name__}: {e}")

        fallback_path = size_holder.get("path")
        expected = size_holder.get("size", 0)

        if fallback_path and os.path.exists(fallback_path):
            try:
                actual = os.path.getsize(fallback_path)
            except Exception:
                actual = 0

            if expected > 0 and actual >= expected:
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
                return "deferred"
            else:
                _cleanup_partial(fallback_path, expected)
                _mark_item(batch_id, chat_id, msg_id,
                           status="fail", local_file=None, next_retry_ts=0)
                st["status"] = "fail"
                _log(
                    f"🗑  {full_link}: частичный файл "
                    f"({human_size(actual)} из {human_size(expected)}) "
                    f"удалён — fail"
                )
        else:
            _mark_item(batch_id, chat_id, msg_id, status="fail", next_retry_ts=0)
            st["status"] = "fail"

        await _update_status_msg(batch, key, full_link, force=True)
        _release_processed(key, "fail")
        if REORDER_AFTER_EACH_FILE:
            await request_reconcile(batch_id, force=False)
        return "fail"


# ---------- Воркер скачивания ----------
async def link_worker(worker_id: int):
    while True:
        try:
            chat_id, msg_id, full_link, batch_id = await link_queue.get()
        except asyncio.CancelledError:
            return

        in_progress["count"] += 1

        try:
            result = await process_link(chat_id, msg_id, full_link, batch_id)
            batch = batches.get(batch_id)
            if batch is not None:
                if result == "deferred":
                    _log(
                        f"worker #{worker_id}: {full_link} — отложено "
                        f"(не инкременчу счётчики)"
                    )
                elif result == "downloaded":
                    _log(
                        f"worker #{worker_id}: {full_link} — скачано, "
                        f"ждёт отправки (upload-воркер)"
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

    q = queues.get(batch_id)

    if q is not None:
        b["ok"] = sum(1 for it in q.get("items", []) if it.get("status") == "done")
        b["skip"] = sum(1 for it in q.get("items", []) if it.get("status") == "skip")
        b["fail"] = sum(1 for it in q.get("items", []) if it.get("status") == "fail")
        # ПАТЧ: total = len(items) из JSON — источник истины
        b["total"] = len(q.get("items", []))

    done = b["ok"] + b["skip"] + b["fail"]
    if done < b["total"]:
        return
    if b.get("finished"):
        return

    # ПАТЧ: не завершаем batch, пока есть активные upload-воркеры
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
        # ПАТЧ: чистим _find_cache для элементов этого batch
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
    """
    ПАТЧ U1: параллельная отправка до CONCURRENT_UPLOADS файлов.
    Каждый воркер берёт СВОЙ downloaded-элемент (первый, ещё не занятый
    другими), грузит его, и СРАЗУ коммитит (ok += 1).

    Перетусовка (reconcile_batch) вызывается ТОЛЬКО в _maybe_finish_batch,
    когда ВСЕ файлы в batch достигли done/fail/skip. Пока идёт отправка,
    статус-сообщения не пересоздаются — только edit (через _update_status_msg).
    """
    UPLOAD_POLL_INTERVAL = 1.0

    while True:
        try:
            await asyncio.sleep(UPLOAD_POLL_INTERVAL)

            for batch_id, b in list(batches.items()):
                q = queues.get(batch_id)
                if q is None:
                    continue
                if b.get("is_manual"):
                    continue

                # ПАТЧ U3: свободный порядок — берём ЛЮБОЙ downloaded,
                # не занятый другими воркерами и не в processed.
                # pending/downloading/uploading — просто пропускаем,
                # НЕ ставим барьер. Файлы в чате — по готовности.
                target = None
                failed_missing = False

                for it in q.get("items", []):
                    status = it.get("status")
                    if status in ("done", "fail", "skip"):
                        continue
                    key = (it["chat_id"], it["msg_id"])

                    # Занят process_link'ом — пропускаем
                    if key in processed:
                        continue
                    # Занят другим upload-воркером — пропускаем
                    if key in b.get("_uploading", set()):
                        continue
                    # pending / downloading / uploading — не барьер, пропускаем
                    if status != "downloaded":
                        continue

                    # status == "downloaded" — берём
                    # ПАТЧ: уважаем next_retry_ts (FloodWait)
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
                    continue

                if target is None:
                    continue  # всё занято, всё done или всё ещё качается

                key = (target["chat_id"], target["msg_id"])
                b.setdefault("_uploading", set()).add(key)
                try:
                    result = await _upload_one(batch_id, b, target)
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
                finally:
                    b.get("_uploading", set()).discard(key)
                    # ПАТЧ: после снятия _uploading проверяем завершение batch —
                    # иначе batch закроется только на следующем тике (через 1 сек)
                    try:
                        await _maybe_finish_batch(batch_id)
                    except Exception:
                        logger.exception(
                            "[_upload_worker #%s] _maybe_finish_batch упал "
                            "для batch %s", worker_id, batch_id,
                        )

        except asyncio.CancelledError:
            return
        except Exception as e:
            _log(f"upload-воркер #{worker_id}: ошибка: {type(e).__name__}: {e}")

async def _upload_one(batch_id: int, batch: dict, it: dict) -> str:
    chat_id = it["chat_id"]
    msg_id = it["msg_id"]
    full_link = it["link"]
    local_name = it.get("local_file")

    # ПАТЧ 1: heartbeat — воркер начал работу над элементом.
    batch["_last_upload_activity_ts"] = time.time()

    key = (chat_id, msg_id)

    # ПАТЧ: batch отменён через /clear — не отправляем
    if batch.get("cancelled"):
        _log(f"  ⏭  upload-воркер: {full_link}: batch отменён — пропуск")
        return "deferred"

    # ПАТЧ 4: если process_link уже взял элемент — НЕ отправляем параллельно.
    if key in processed:
        _log(
            f"  ⏭  upload-воркер: {full_link}: уже в processed "
            f"(process_link работает) — откладываю"
        )
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

    # ПАТЧ U7: проверяем, есть ли файл уже в «Избранном» ДО _update_status_msg.
    # Если да — НЕ ставим uploading (📤 не мигнёт), сразу done + ✅.
    # ПАТЧ: для фото _find_in_me не работает (нет file.name) — пропускаем.
    _is_photo = display_name.lower().endswith(
        (".jpg", ".jpeg", ".jpe", ".png", ".webp", ".bmp",
         ".tif", ".tiff", ".heic", ".heif")
    )
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

        # Редактируем статус в «✅ уже в Избранном»
        status_msg = batch["status_msgs"].pop(key, None)
        if status_msg is not None:
            try:
                await status_msg.edit(
                    f"✅ {display_name} ({human_size(file_size)}) — уже в «Избранном»"
                )
                _mark_item(batch_id, chat_id, msg_id,
                           status_msg_id=None, slot_msg_id=None)
            except MessageNotModifiedError:
                pass
            except Exception as e:
                _log(
                    f"⚠️  upload-воркер: {full_link}: не удалось "
                    f"обновить статус: {e}"
                )
        else:
            _mark_item(batch_id, chat_id, msg_id,
                       status_msg_id=None, slot_msg_id=None)

        # Удаляем локальный файл
        if os.path.exists(filename):
            try:
                os.remove(filename)
                _log(f"🧹 удалён локальный файл: {os.path.basename(filename)}")
            except Exception as e:
                _log(f"⚠️  не удалось удалить {os.path.basename(filename)}: {e}")

        return "ok"

    # Не найден — теперь ставим uploading и отправляем
    st["status"] = "uploading"
    st["up_current"] = 0
    st["up_total"] = file_size
    st["up_percent"] = 0.0
    st["last_up_cb"] = 0.0
    st["_last_logged_pct"] = -1
    _mark_item(batch_id, chat_id, msg_id, status="uploading")
    await _update_status_msg(batch, key, full_link, force=True)

    async def upload_cb(current, total):
        now = time.time()
        # ПАТЧ 1: heartbeat — БЕЗ throttle, только запись в память.
        batch["_last_upload_activity_ts"] = now
        if now - st.get("last_up_cb", 0) > 3 or current == total:
            st["last_up_cb"] = now
            st["up_current"] = current
            st["up_total"] = total
            st["up_percent"] = (current / total) * 100 if total else 0
            cur_item = _find_item(batch_id, chat_id, msg_id)
            if cur_item is None:
                return
            if cur_item.get("status") in ("fail", "done", "skip"):
                return
            _mark_item(batch_id, chat_id, msg_id, status="uploading")

            # ПАТЧ: прогресс в консоль, но не чаще, чем каждые 10%
            pct = (current / total * 100) if total else 0
            last_pct = st.get("_last_logged_pct", -1)
            if pct - last_pct >= 10 or current == total:
                st["_last_logged_pct"] = pct
                logger.info(
                    "📤 [upload] %s: %.1f%% (%s / %s)",
                    display_name, pct,
                    human_size(current), human_size(total),
                )

            if key in batch["status_msgs"]:
                await _update_status_msg(batch, key, full_link)

    caption = None
    kind = _get_send_kind(filename)
    if kind == "video":
        caption = f"🎬 {display_name}"

    try:
        # ПАТЧ U1: убираем upload_lock — 3 воркера должны отправлять параллельно.
        # Telegram сам обрабатывает параллельные send_file.
        sent = await _send_media(
            "me", filename, display_name, upload_cb, caption=caption
        )
        
    except FloodWaitError as fwe:
        _log(
            f"⏳ upload-воркер: {full_link}: FloodWait {fwe.seconds}s "
            f"— откладываю до {time.strftime('%H:%M:%S', time.localtime(time.time() + fwe.seconds))}"
        )
        # ПАТЧ: НЕ спим в воркере — сохраняем next_retry_ts в item.
        # Воркер освобождается и берёт другие downloaded-элементы.
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
                next_retry_ts=time.time() + 5,   # ← задержка 5 сек
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

    # ПАТЧ: если send_file вернул None (например, document без ветки в
    # _send_media) — считаем отправку неуспешной, не удаляем файл,
    # не помечаем done. Иначе файл теряется.
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

    # ПАТЧ: batch отменили, пока файл летел — удаляем только что залитое
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
                       status_msg_id=None, slot_msg_id=None)
        except Exception as e:
            _log(
                f"⚠️  не удалось удалить статус {full_link} "
                f"(id={old_id}): {e} — status_msg_id оставляем, "
                f"reconcile повторит"
            )
    else:
        _mark_item(batch_id, chat_id, msg_id,
                   status_msg_id=None, slot_msg_id=None)

    if os.path.exists(filename):
        try:
            os.remove(filename)
            _log(f"🧹 удалён локальный файл: {os.path.basename(filename)}")
        except Exception as e:
            _log(f"⚠️  не удалось удалить {os.path.basename(filename)}: {e}")

    return "ok"


# ---------- Watchdog ----------
async def _watchdog_stuck_batches(interval: float = 60.0):
    """
    ПАТЧ 2: вместо «таск upload-воркера жив» проверяем heartbeat
    b["_last_upload_activity_ts"] — воркер РЕАЛЬНО прогрессирует.
    Иначе зависший на FloodWait/сети воркер навсегда заблокирует watchdog.
    ПАТЧ 5: барьер перед downloaded проверяется от ПЕРВОГО не-терминального
    элемента.
    """
    while True:
        try:
            await asyncio.sleep(interval)
            now = time.time()
            for batch_id, b in list(batches.items()):
                q = queues.get(batch_id)
                if q is None:
                    continue

                if b.get("is_manual"):
                    uploading_keys = b.get("_uploading", set())
                    stuck_manual = [
                        it for it in q.get("items", [])
                        if it.get("status") in ("downloaded", "uploading")
                        and (it["chat_id"], it["msg_id"]) not in uploading_keys
                    ]
                    if not stuck_manual:
                        continue
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
                                       status="fail", local_file=None, next_retry_ts=0)
                            b["fail"] = b.get("fail", 0) + 1
                            _log(f"  🐕 #{batch_id} {it['link']}: файл пропал → fail")
                            continue
                        retries = it.get("upload_retries", 0)
                        if retries >= 3:
                            _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                       status="fail", local_file=None, next_retry_ts=0)
                            b["fail"] = b.get("fail", 0) + 1
                            _log(
                                f"  🐕 #{batch_id} {it['link']}: "
                                f"превышено попыток ({retries}) → fail"
                            )
                            continue
                        _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                   upload_retries=retries + 1,
                                   status="downloaded")
                        task = asyncio.create_task(
                            _run_manual_upload(
                                batch_id, it,
                                os.path.join(DOWNLOAD_DIR, local)
                            )
                        )
                        _pending_manual_uploads.add(task)
                        task.add_done_callback(_pending_manual_uploads.discard)
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"повторно в manual upload (попытка {retries + 1}/3)"
                        )
                    await _maybe_finish_batch(batch_id)
                    mwd["key"] = None
                    continue

                # ПАТЧ 2: heartbeat-based прогресс.
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

                # ПАТЧ 2: воркер прогрессирует — пропускаем.
                if worker_progressing:
                    _log(
                        f"🐕 watchdog: batch #{batch_id} — "
                        f"upload-воркер прогрессирует "
                        f"(активность {now - last_activity:.0f}s назад), "
                        f"{len(candidates)} downloaded в очереди — пропускаем"
                    )
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

                    # ПАТЧ: уважаем next_retry_ts (FloodWait)
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
                        _mark_item(batch_id, it["chat_id"], it["msg_id"],
                                   status="fail", local_file=None, next_retry_ts=0)
                        b["fail"] = b.get("fail", 0) + 1
                        any_fail = True
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"превышено попыток ({retries}) → fail"
                        )
                        continue

                    cur2 = _find_item(batch_id, it["chat_id"], it["msg_id"])
                    if cur2 is None:
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"элемент исчез из JSON — пропуск"
                        )
                        continue
                    if cur2.get("status") in ("done", "fail", "skip", "uploading"):
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"статус изменился на «{cur2.get('status')}» — "
                            f"не ставим в link_queue (пропуск)"
                        )
                        continue
                    if (it["chat_id"], it["msg_id"]) in processed:
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"уже в processed — пропуск"
                        )
                        continue
                    if (it["chat_id"], it["msg_id"]) in b.get("_uploading", set()):
                        _log(
                            f"  🐕 #{batch_id} {it['link']}: "
                            f"уже в _uploading — пропуск"
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

            # ПАТЧ: синхронизируем link_states для существующего batch
            b_existing = batches.get(qid)
            if b_existing is not None:
                for it in items:
                    key = (it["chat_id"], it["msg_id"])
                    st = b_existing["link_states"].get(key)
                    if st is not None:
                        st["status"] = it.get("status", st.get("status"))

        pending_items = [
            it for it in items
            if it.get("status") == "pending"
        ]

        if pending_items:
            _log(
                f"  🔄 #{qid}: найдено pending-элементов для перекачки: "
                f"{len(pending_items)}"
            )

            b = batches.get(qid)
            if b is None:
                status_msgs: dict[tuple, any] = {}
                link_states: dict[tuple, dict] = {}

                for it in items:
                    key = (it["chat_id"], it["msg_id"])

                    st = {
                        "status": it.get("status", "pending"),
                        "percent": 0.0,
                        "current": 0,
                        "total": 0,
                        "last_cb": 0.0,
                        "last_edit": 0.0,
                        "up_current": 0,
                        "up_total": (
                            it.get("file_size", 0) or 0
                            if it.get("status") in ("downloaded", "uploading")
                            else 0
                        ),
                        "up_percent": 0.0,
                        "last_up_cb": 0.0,
                        "file_size": it.get("file_size", 0) or 0,
                    }
                    link_states[key] = st

                    # ПАТЧ U9: не восстанавливаем из старых id —
                    # могли быть удалены. Создаём новый статус.
                    if it.get("status") in (
                        "pending", "downloading", "downloaded", "uploading"
                    ):
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

                batches[qid] = {
                    "batch_id": qid,
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

        _close_queue_if_finished(qid)


def _close_queue_if_finished(qid: int) -> bool:
    b = batches.get(qid)
    if b is not None:
        if not b.get("finished"):
            return False
        # ПАТЧ: не удаляем JSON, пока есть активные upload-воркеры
        if b.get("_uploading"):
            return False

    q = queues.get(qid) or _load_queue(qid)
    if q is None:
        return False

    items = q.get("items", [])
    if not items:
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

    logger.info(
        "📥 Найдено уже скачанных, но не отправленных файлов: %d — отправляю",
        len(items),
    )

    placed = 0
    failed: list[tuple[int, dict, str]] = []

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

            # ПАТЧ U9: удаляем ВСЕ старые status_msg_id/slot_msg_id пачкой,
            # затем создаём статусы заново для downloaded/uploading.
            # Старые сообщения могли быть удалены (тобой или reconcile) —
            # edit на них молча падает, и статусов в чате нет.
            old_ids: list[int] = []
            for it in q_items:
                for k in ("status_msg_id", "slot_msg_id"):
                    mid = it.get(k)
                    if mid:
                        old_ids.append(mid)
            old_ids = list(dict.fromkeys(old_ids))

            if old_ids:
                try:
                    await client.delete_messages("me", old_ids)
                    _log(
                        f"  🗑  #{qid}: удалено {len(old_ids)} "
                        f"старых статусов"
                    )
                except FloodWaitError as fwe:
                    _log(
                        f"  ⏳ #{qid}: FloodWait {fwe.seconds}s при удалении "
                        f"старых статусов"
                    )
                    await asyncio.sleep(fwe.seconds)
                    try:
                        await client.delete_messages("me", old_ids)
                    except Exception:
                        pass
                except Exception as e:
                    _log(
                        f"  ⚠️  #{qid}: не удалось удалить старые "
                        f"({len(old_ids)}): {e}"
                    )

            # Обнуляем id и создаём новые статусы
            for it in q_items:
                it["status_msg_id"] = None
                it["slot_msg_id"] = None

            # ПАТЧ: сохраняем сразу — если упадём при создании статусов,
            # JSON не будет ссылаться на уже удалённые сообщения
            queues[qid] = q
            _save_queue(qid)

            for it in q_items:
                key = (it["chat_id"], it["msg_id"])

                link_states[key] = {
                    "status": it.get("status", "downloaded"),
                    "percent": 0.0,
                    "current": 0,
                    "total": 0,
                    "last_cb": 0.0,
                    "last_edit": 0.0,
                    "up_current": 0,
                    "up_total": (
                        it.get("file_size", 0) or 0
                        if it.get("status") in ("downloaded", "uploading")
                        else 0
                    ),
                    "up_percent": 0.0,
                    "last_up_cb": 0.0,
                    "file_size": it.get("file_size", 0) or 0,
                }

                if it.get("status") in (
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
                "batch_id": qid,
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

    for qid, it in items:
        try:
            b = batches.get(qid)
            if b is None:
                _log(
                    f"  ⚠️  #{qid} {it['link']}: batch не восстановлен — "
                    f"пропуск"
                )
                failed.append((qid, it, "batch не восстановлен"))
                continue

            key = (it["chat_id"], it["msg_id"])

            if key not in b["status_msgs"]:
                # ПАТЧ U9: пробуем старый id, но проверяем что он валиден
                # (не MessageEmpty). Если edit упадёт — создаём новый.
                # Проще: создаём новый сразу — старые уже удалены пачкой выше.
                status_created = False
                for attempt in range(2):
                    try:
                        m = await client.send_message(
                            "me", f"⏳ {it['link']} (ожидание...)"
                        )
                        _mark_item(qid, it["chat_id"], it["msg_id"],
                                   status_msg_id=m.id)
                        b["status_msgs"][key] = m
                        status_created = True
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

                if not status_created:
                    # ПАТЧ: не ставим в очередь без статуса — иначе
                    # _update_status_msg будет тихо падать.
                    _log(
                        f"  ⚠️  #{qid} {it['link']}: статус не создан — "
                        f"пропускаю постановку в очередь (повтор при следующем запуске)"
                    )
                    failed.append((qid, it, "статус не создан"))
                    continue

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
            _log(
                f"  ❌ #{qid} {it['link']}: постановка не удалась: "
                f"{type(e).__name__}: {e}"
            )
            failed.append((qid, it, f"{type(e).__name__}: {e}"))

    logger.info("📤 Поставлено в очередь на отправку: %d", placed)
    if failed:
        logger.warning("⚠️  Не удалось поставить: %d", len(failed))
    return failed


# ---------- Отправка «ручных» файлов ----------
async def upload_manual_file(batch_id: int, item: dict, path: str) -> bool:
    chat_id, msg_id = item["chat_id"], item["msg_id"]
    batch = batches.get(batch_id)
    if batch is None:
        raise RuntimeError(
            f"upload_manual_file: batch #{batch_id} не в памяти"
        )
    key = (chat_id, msg_id)
    st = batch["link_states"].get(key)
    if st is None:
        raise RuntimeError(
            f"upload_manual_file: link_states[{key}] отсутствует "
            f"в batch #{batch_id}"
        )

    if not os.path.exists(path):
        _log(f"⚠️  manual-файл пропал: {path} → fail")
        _mark_item(batch_id, chat_id, msg_id,
                   status="fail", next_retry_ts=0)
        st["status"] = "fail"
        raise FileNotFoundError(path)

    full_link = item["link"]
    display_name = os.path.basename(path)
    file_size = os.path.getsize(path)

    # ПАТЧ U7: проверяем, есть ли файл уже в «Избранном» ДО _update_status_msg.
    try:
        found_id = await _find_in_me(
            display_name=display_name,
            file_size=file_size,
        )
    except Exception as e:
        _log(
            f"⚠️  manual: {full_link}: ошибка проверки me "
            f"({type(e).__name__}: {e}) — продолжаю отправку"
        )
        found_id = None

    if found_id is not None:
        _log(
            f"✅ manual: {full_link}: уже в «Избранном» "
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
                           status_msg_id=None, slot_msg_id=None)
            except Exception as e:
                _log(f"⚠️  manual: не удалось обновить статус: {e}")
        else:
            _mark_item(batch_id, chat_id, msg_id,
                       status_msg_id=None, slot_msg_id=None)

        try:
            if os.path.exists(path):
                os.remove(path)
                _log(f"🧹 manual: удалён локальный файл: {display_name}")
        except Exception as e:
            _log(f"⚠️  manual: не удалось удалить {display_name}: {e}")

        return True

    # Не найден — ставим uploading и отправляем
    st["status"] = "uploading"
    st["up_current"] = 0
    st["up_total"] = file_size
    st["up_percent"] = 0.0
    st["last_up_cb"] = 0.0
    st["_last_logged_pct"] = -1
    _mark_item(batch_id, chat_id, msg_id, status="uploading")
    await _update_status_msg(batch, key, full_link, force=True)

    async def upload_cb(current, total):
        now = time.time()
        if now - st.get("last_up_cb", 0) > 3 or current == total:
            st["last_up_cb"] = now
            st["up_current"] = current
            st["up_total"] = total
            st["up_percent"] = (current / total) * 100 if total else 0

            # ПАТЧ: прогресс в консоль, но не чаще, чем каждые 10%
            pct = (current / total * 100) if total else 0
            last_pct = st.get("_last_logged_pct", -1)
            if pct - last_pct >= 10 or current == total:
                st["_last_logged_pct"] = pct
                logger.info(
                    "📤 [manual upload] %s: %.1f%% (%s / %s)",
                    display_name, pct,
                    human_size(current), human_size(total),
                )

            if key in batch["status_msgs"]:
                await _update_status_msg(batch, key, full_link)

    manual_caption = None
    kind = _get_send_kind(path)
    if kind == "video":
        manual_caption = f"🎬 {display_name}"

    sent = await _send_media(
        "me", path, display_name, upload_cb,
        caption=manual_caption,
    )

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
        next_retry_ts=0,
    )
    st["status"] = "done"

    status_msg = batch["status_msgs"].pop(key, None)
    if status_msg is not None:
        try:
            await status_msg.delete()
            _mark_item(batch_id, chat_id, msg_id,
                       status_msg_id=None, slot_msg_id=None)
        except Exception as e:
            _log(f"⚠️  не удалось удалить статус файла: {e}")
    else:
        _mark_item(batch_id, chat_id, msg_id,
                   status_msg_id=None, slot_msg_id=None)

    try:
        if os.path.exists(path):
            os.remove(path)
            _log(f"🧹 удалён локальный файл: {display_name}")
    except Exception as e:
        _log(f"⚠️  не удалось удалить {display_name}: {e}")

    return True


async def _run_manual_upload(batch_id: int, item: dict, path: str):
    key = (item["chat_id"], item["msg_id"])

    b_init = batches.get(batch_id)
    if b_init is not None:
        b_init.setdefault("_uploading", set()).add(key)

    try:
        ok = await upload_manual_file(batch_id, item, path)
        b = batches.get(batch_id)
        if b is not None:
            if ok:
                b["ok"] += 1
            else:
                _log(
                    f"⚠️  _run_manual_upload: upload_manual_file вернул "
                    f"{ok!r} вместо True для {os.path.basename(path)} — fail"
                )
                b["fail"] += 1
    except Exception as e:
        _log(f"❌  Файл {os.path.basename(path)}: {type(e).__name__}: {e}")
        _mark_item(batch_id, item["chat_id"], item["msg_id"], status="fail", next_retry_ts=0)
        b = batches.get(batch_id)
        if b is not None:
            b["fail"] += 1
    finally:
        b_fin = batches.get(batch_id)
        if b_fin is not None:
            b_fin.get("_uploading", set()).discard(key)
        try:
            await _maybe_finish_batch(batch_id)
        except Exception:
            logger.exception(
                "[_run_manual_upload] _maybe_finish_batch упал для batch %s",
                batch_id,
            )


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
            # ПАТЧ: уникальный ключ на основе batch_id
            unique_mid = -(batch_id * 1_000_000 + idx)
            key = ("me", unique_mid)
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
                "msg_id": unique_mid,
                "link": name,
                "status": "downloaded",
                "status_msg_id": status_msg.id,
                "slot_msg_id": None,
                "file_message_id": None,
                "local_file": name,
                "downloaded_at": time.time(),
                "file_size": _fsize,
            })

        q = {"batch_id": batch_id, "path": _queue_path(batch_id), "items": items}
        queues[batch_id] = q

        batches[batch_id] = {
            "batch_id": batch_id,
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
            task = asyncio.create_task(_run_manual_upload(batch_id, it, p))
            _pending_manual_uploads.add(task)
            task.add_done_callback(_pending_manual_uploads.discard)


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
        if in_progress["count"] > 0:
            await event.respond(
                f"⏳ В работе: {in_progress['count']} загрузок — "
                f"они завершатся, новые отменены."
            )

        # ПАТЧ: чистим кэш поиска в me — файлы могли быть удалены
        _find_cache.clear()

        async with batch_create_lock:
            cleared = 0
            while not link_queue.empty():
                try:
                    link_queue.get_nowait()
                    cleared += 1
                except asyncio.QueueEmpty:
                    break

            # NEW: отменяем ручные аплоады
            for t in list(_pending_manual_uploads):
                t.cancel()
            if _pending_manual_uploads:
                await asyncio.gather(
                    *_pending_manual_uploads, return_exceptions=True
                )

            # ПАТЧ: отменяем отложенные удаления
            for t in list(_pending_deletions):
                t.cancel()
            if _pending_deletions:
                await asyncio.gather(
                    *_pending_deletions, return_exceptions=True
                )

            cleared_batches = 0
            for batch_id, b in list(batches.items()):
                if b.get("finished"):
                    continue
                # ПАТЧ: помечаем как отменённый ДО удаления —
                # воркеры увидят флаг и прервутся
                b["finished"] = True
                b["cancelled"] = True

                # NEW: чистим processed и собираем id статусов/слотов
                to_delete_ids: list[int] = []
                q = queues.get(batch_id)
                if q:
                    for it in q.get("items", []):
                        processed.discard((it["chat_id"], it["msg_id"]))
                        for k in ("status_msg_id", "slot_msg_id"):
                            mid = it.get(k)
                            if mid:
                                to_delete_ids.append(mid)

                to_delete_ids = list(dict.fromkeys(to_delete_ids))
                if to_delete_ids:
                    try:
                        await client.delete_messages("me", to_delete_ids)
                    except FloodWaitError as fwe:
                        await asyncio.sleep(fwe.seconds)
                        try:
                            await client.delete_messages("me", to_delete_ids)
                        except Exception:
                            pass
                    except Exception:
                        pass

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
            f"└ Обработано уникальных: {len(processed)}"
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
                "status_msg_id": status_msgs[(chat_id, msg_id)].id,
                "slot_msg_id": None,
                "file_message_id": None,
                "local_file": None,
                "downloaded_at": None,
            })

        _create_queue(batch_id, items)

        batches[batch_id] = {
            "batch_id": batch_id,
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
        logger.info(
            "📂 Найдено файлов без записи в очереди: %d",
            len(scan["manual"]),
        )
        for f in scan["manual"][:20]:
            try:
                sz = human_size(os.path.getsize(f))
            except Exception:
                sz = "?"
            logger.info("   • %s (%s)", os.path.basename(f), sz)
        if len(scan["manual"]) > 20:
            logger.info("   … и ещё %d", len(scan["manual"]) - 20)

    if scan.get("anomalous_done"):
        logger.info(
            "📂 Найдено файлов с терминальным статусом в JSON, "
            "но на диске: %d — проверю в «Избранном»",
            len(scan["anomalous_done"]),
        )
        for _, it, f in scan["anomalous_done"][:20]:
            logger.info("   • %s ← %s", os.path.basename(f), it["link"])
        if len(scan["anomalous_done"]) > 20:
            logger.info("   … и ещё %d", len(scan["anomalous_done"]) - 20)

    if scan["own_partial"]:
        logger.info(
            "📂 Найдено недокачанных файлов (есть запись в очереди): %d — "
            "перекачиваю",
            len(scan["own_partial"]),
        )
        for _, it, f in scan["own_partial"][:20]:
            logger.info("   • %s ← %s", os.path.basename(f), it["link"])
        if len(scan["own_partial"]) > 20:
            logger.info("   … и ещё %d", len(scan["own_partial"]) - 20)

    if scan["own_ready"]:
        logger.info(
            "📂 Найдено готовых к отправке файлов (есть запись в очереди): "
            "%d — поставлю в очередь на отправку",
            len(scan["own_ready"]),
        )
        for _, it, f in scan["own_ready"][:20]:
            logger.info("   • %s ← %s", os.path.basename(f), it["link"])
        if len(scan["own_ready"]) > 20:
            logger.info("   … и ещё %d", len(scan["own_ready"]) - 20)

# ---------- Запуск ----------
async def main():
    global link_queue, batch_create_lock, shutdown_event, upload_lock
    global upload_queue, upload_worker_task
    link_queue = asyncio.Queue()
    batch_create_lock = asyncio.Lock()
    shutdown_event = asyncio.Event()
    upload_lock = asyncio.Lock()

    # ПАТЧ: SIGINT/SIGTERM → shutdown_event, а не KeyboardInterrupt
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig_name: str):
        if not shutdown_event.is_set():
            logger.warning("🛑 Получен %s — завершаю работу...", sig_name)
            shutdown_event.set()

    for sig in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(
                getattr(_signal, sig),
                _request_shutdown, sig,
            )
        except (NotImplementedError, AttributeError):
            # Windows: add_signal_handler не поддерживается для SIGTERM
            pass
    existing_ids = _list_queue_files()
    if existing_ids:
        batch_counter["id"] = max(existing_ids)
        _log(f"🔢 batch_counter инициализирован: {batch_counter['id']}")

    # ПАТЧ 8: если JSON-очередей нет (или файлы в downloads/ не упомянуты
    # ни в одной), но файлы на диске есть — создаём JSON из downloads/.
    # Это лечит случай, когда queues/ пуста, но downloads/ полна: без JSON
    # reconcile_batch не работает, и статусы в чате не пересоздаются.
    recovered, skipped = _recover_queues_from_downloads()
    if recovered:
        _log(
            f"♻️  ПАТЧ 8: восстановлено файлов из downloads/ в JSON: "
            f"{recovered} (пропущено, уже в JSON: {skipped})"
        )

    # ПАТЧ 6: сбрасываем upload_retries у downloaded-элементов, чтобы старые
    # «попытки 1/3» от предыдущих watchdog-срабатываний не привели к fail.
    for qid in _list_queue_files():
        q = _load_queue(qid)
        if not q:
            continue
        changed = 0
        for it in q.get("items", []):
            if (it.get("status") == "downloaded"
                    and it.get("upload_retries", 0) > 0):
                it["upload_retries"] = 0
                changed += 1
        if changed:
            queues[qid] = q
            _save_queue(qid)
            _log(
                f"  ♻️  #{qid}: сброшено upload_retries у {changed} "
                f"downloaded-элементов (были ложные инкременты watchdog)"
            )

    # ПАТЧ 9: реабилитируем fail-элементы, у которых файл есть на диске.
    # Если fail из-за прошлой ошибки (ValueError и т.п.), но локальный файл
    # цел — ставим downloaded, чтобы _upload_worker переотправил.
    for qid in _list_queue_files():
        q = _load_queue(qid)
        if not q:
            continue
        revived = 0
        for it in q.get("items", []):
            if it.get("status") != "fail":
                continue
            lf = it.get("local_file")
            if not lf:
                # fail без local_file — файла нет, нечего восстанавливать
                continue
            full_path = os.path.join(DOWNLOAD_DIR, lf)
            if not os.path.exists(full_path):
                continue
            expected = it.get("file_size", 0) or 0
            try:
                actual = os.path.getsize(full_path)
            except Exception:
                actual = 0
            if expected > 0 and actual < expected:
                # частичный файл — не восстанавливаем
                _log(
                    f"  ⚠️  #{qid} {lf}: fail, но файл частичный "
                    f"({human_size(actual)} из {human_size(expected)}) — пропуск"
                )
                continue
            it["status"] = "downloaded"
            it["upload_retries"] = 0
            it["file_message_id"] = None
            revived += 1
            _log(
                f"  ♻️  #{qid} {lf}: fail → downloaded "
                f"(файл цел, {human_size(actual)}) — переотправлю"
            )
        if revived:
            queues[qid] = q
            _save_queue(qid)
    # ПАТЧ A: чистим next_retry_ts у терминальных статусов (done/fail/skip).
    # next_retry_ts остаётся в JSON после успешной отправки или fail —
    # это мусор, который не читается, но засоряет файл очереди.
    for qid in _list_queue_files():
        q = _load_queue(qid)
        if not q:
            continue
        changed = 0
        for it in q.get("items", []):
            if (it.get("status") in ("done", "fail", "skip")
                    and it.get("next_retry_ts")):
                it["next_retry_ts"] = 0
                changed += 1
        if changed:
            queues[qid] = q
            _save_queue(qid)
            _log(
                f"  ♻️  #{qid}: сброшено next_retry_ts у {changed} "
                f"терминальных элементов"
            )

    scan = _scan_downloads()

    _print_scan_summary(scan)

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

    manual_send = False
    if scan["manual"]:
        # ПАТЧ: таймаут на ввод — 60 секунд, потом считаем «n»
        _INPUT_TIMEOUT = 60.0

        def _input_with_timeout(prompt: str, timeout: float):
            if sys.platform != "win32":
                print(prompt, end="", flush=True)
                rlist, _, _ = select.select([sys.stdin], [], [], timeout)
                if rlist:
                    return sys.stdin.readline().strip()
                print()
                return ""
            # Windows fallback — обычный input (без таймаута)
            return input(prompt).strip()

        try:
            ans = (await asyncio.to_thread(
                _input_with_timeout,
                "\nОтправить их в Избранное? [y/n, 60s]: ",
                _INPUT_TIMEOUT,
            )).lower()
        except Exception:
            ans = ""
        manual_send = ans in ("y", "yes", "да", "д")
        if not manual_send:
            logger.info("⏭  Пропущено — спрошу при следующем запуске.")

    session_file = os.path.join(BASE_DIR, "user_session.session")
    had_session_file = os.path.exists(session_file)

    logger.info("⏳ Подключение к Telegram...")
    await client.start()

    authorized = await client.is_user_authorized()
    if not had_session_file or not authorized:
        logger.info(
            "📇 Первый запуск или сессия недействительна — загружаю диалоги..."
        )
        await client.get_dialogs()
    else:
        logger.info("📇 Сессия в порядке — диалоги не перезагружаем.")

    me = await client.get_me()
    account_state["is_premium"] = bool(getattr(me, "premium", False))
    limit = LIMIT_PREMIUM if account_state["is_premium"] else LIMIT_FREE
    logger.info(
        "👤 Аккаунт: @%s (%s)",
        me.username or me.id,
        "Premium" if account_state["is_premium"] else "Free",
    )
    logger.info("📦 Лимит на отправку: %s", human_size(limit))

    if scan.get("anomalous_done"):
        logger.info(
            "🔍 Проверяю %d аномальных файлов в «Избранном»...",
            len(scan["anomalous_done"]),
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
                logger.warning(
                    "⏳ FloodWait %ss при проверке %s — оставляю как manual",
                    fwe.seconds, name,
                )
                await asyncio.sleep(fwe.seconds)
                scan["manual"].append(full)
                continue
            except Exception as e:
                logger.warning(
                    "⚠️  ошибка проверки %s: %s — оставляю как manual",
                    name, e,
                )
                scan["manual"].append(full)
                continue

            if found_id is not None:
                it["status"] = "done"
                it["file_message_id"] = found_id
                it["local_file"] = None
                it["downloaded_at"] = None
                q = queues.get(qid) or _load_queue(qid)
                if q is not None:
                    for entry in q.get("items", []):
                        if (entry.get("chat_id") == it["chat_id"]
                                and entry.get("msg_id") == it["msg_id"]):
                            entry.update({
                                "status": "done",
                                "file_message_id": found_id,
                                "local_file": None,
                                "downloaded_at": None,
                            })
                            break
                    queues[qid] = q
                    _save_queue(qid)
                try:
                    os.remove(full)
                    logger.info(
                        "♻️  %s: уже в «Избранном» (id=%s) — "
                        "помечен done, файл удалён",
                        name, found_id,
                    )
                except Exception as e:
                    logger.warning("⚠️  не удалось удалить %s: %s", name, e)
            else:
                # ПАТЧ 6: файл есть, но в me нет — помечаем fail в JSON,
                # чтобы не возвращаться к нему при каждом запуске
                # (бесконечный цикл anomalous_done).
                logger.warning(
                    "📂 %s: в «Избранном» не найден — "
                    "помечаю fail в JSON (файл оставляю как manual)",
                    name,
                )
                q = queues.get(qid) or _load_queue(qid)
                if q is not None:
                    for entry in q.get("items", []):
                        if (entry.get("chat_id") == it["chat_id"]
                                and entry.get("msg_id") == it["msg_id"]):
                            entry["status"] = "fail"
                            entry["local_file"] = None
                            break
                    queues[qid] = q
                    _save_queue(qid)
                scan["manual"].append(full)

    workers = [
        asyncio.create_task(link_worker(i + 1))
        for i in range(CONCURRENT_DOWNLOADS)
    ]
    logger.info("▶ Запущено воркеров скачивания: %d", CONCURRENT_DOWNLOADS)

    upload_workers = [
        asyncio.create_task(_upload_worker(i + 1))
        for i in range(CONCURRENT_UPLOADS)
    ]
    logger.info(
        "📤 Запущено upload-воркеров: %d (параллельно, commit по порядку)",
        CONCURRENT_UPLOADS,
    )

    watchdog_task = asyncio.create_task(_watchdog_stuck_batches(interval=60.0))
    logger.info("🐕 Запущен watchdog зависших пачек")

    await resume_pending_queues()

    failed_send = await offer_send_downloaded()

    if failed_send:
        logger.warning(
            "⚠️  ВНИМАНИЕ: %d файлов не удалось поставить в очередь "
            "на отправку:",
            len(failed_send),
        )
        for qid, it, reason in failed_send:
            logger.warning("   • #%s %s: %s", qid, it.get("link", "?"), reason)

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
            logger.warning("⚠️  не удалось уведомить в me: %s", e)
    else:
        logger.info("✅ Все найденные файлы поставлены в очередь на отправку")

    for qid in _list_queue_files():
        _close_queue_if_finished(qid)

    if manual_send:
        await enqueue_manual_files(scan["manual"])

    logger.info(
        "🚀 Готово! Каждая ссылка — отдельное сообщение, "
        "одновременно качается %d, перетусовка: %s. "
        "Команды: /quit /clear /stats",
        CONCURRENT_DOWNLOADS,
        "после закачки всей пачки" if REORDER_AFTER_EACH_FILE
        else "по завершении пачки",
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
        logger.info("🛑 Завершение работы...")
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
        if _pending_manual_uploads:
            for t in list(_pending_manual_uploads):
                t.cancel()
            await asyncio.gather(*_pending_manual_uploads, return_exceptions=True)
        if _pending_deletions:
            await asyncio.gather(*_pending_deletions, return_exceptions=True)
        await asyncio.sleep(0.5)
        try:
            await client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # Telethon — только WARNING, чтобы не засорять консоль
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("telethon.network").setLevel(logging.WARNING)

    # На Windows add_signal_handler не работает — ставим fallback
    if sys.platform == "win32":
        def _win_ctrl_c(signum, frame):
            logger.warning("🛑 Ctrl+C — завершаю работу...")
            try:
                if shutdown_event is not None and not shutdown_event.is_set():
                    shutdown_event.set()
                    return
            except Exception:
                pass
            raise KeyboardInterrupt
        _signal.signal(_signal.SIGINT, _win_ctrl_c)
    # На Unix полагаемся на loop.add_signal_handler в main().

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("🛑 Остановлено пользователем (Ctrl+C).")
    except Exception:
        logger.exception("❌ КРИТИЧЕСКИЙ СБОЙ")