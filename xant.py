[29.10.2026 21:42] Aleh: import os
import re
import sys
import time
import shutil
import asyncio
import logging
import mimetypes
import subprocess
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FileReferenceExpiredError, FloodWaitError
from telethon.tl import types

# Локальный файл FastTelethon.py (используем только upload_file)
import FastTelethon

ENV_FILE = ".env"
DOWNLOAD_DIR = "downloads"
NOTIFY_SOUND = "notify.ogg"   # опциональный звук для отправки в TG в конце пачки
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

LIMIT_FREE = 2 * 1024 ** 3      # 2 ГБ
LIMIT_PREMIUM = 4 * 1024 ** 3   # 4 ГБ

# ---------- Первичная настройка ----------
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
    print("❌ Ошибка: Неверный TELEGRAM_API_ID в файле .env (должен содержать только цифры).")
    print("Пожалуйста, удалите .env и перезапустите скрипт.")
    exit(1)

client = TelegramClient(
    "user_session",
    API_ID,
    API_HASH,
    connection_retries=10,
    retry_delay=3,
    timeout=60,
)

LINK_PATTERN = r"https://t\.me/(?:c/(\d+)|([\w_]+))/(\d+)"

task_queue = asyncio.Queue(maxsize=50)
processed: set[tuple] = set()
cancel_flag = {"cancel": False}


# ---------- Звуковое оповещение (локальное) ----------
def _play_sound_sync():
    """Синхронно проигрывает системный звук (вызывается в отдельном потоке)."""
    try:
        if sys.platform == "win32":
            import winsound
            winsound.MessageBeep(winsound.MB_ICONASTERISK)
            return
        if sys.platform == "darwin":
            if shutil.which("afplay"):
                subprocess.run(
                    ["afplay", "/System/Library/Sounds/Glass.aiff"],
                    check=False, timeout=3,
                )
                return
        for player, sound in (
            ("paplay", "/usr/share/sounds/freedesktop/stereo/complete.oga"),
            ("aplay",  "/usr/share/sounds/alsa/Front_Center.wav"),
        ):
            if shutil.which(player) and os.path.exists(sound):
                subprocess.run([player, sound], check=False, timeout=3)
                return
    except Exception:
        pass
    finally:
        try:
            sys.stdout.write("\a")
            sys.stdout.flush()
        except Exception:
            pass


async def play_sound():
    """Асинхронная обёртка — не блокирует event loop."""
    await asyncio.to_thread(_play_sound_sync)


# ---------- Утилиты: имена файлов ----------
_INVALID_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_MULTI_UNDERSCORE = re.compile(r'_+')
_MAX_NAME_LEN = 200


def _sanitize(name: str) -> str:
    name = _INVALID_CHARS.sub("_", name)
    name = _MULTI_UNDERSCORE.sub("_", name)
    return name.strip(" ._")


def _detect_extension(message) -> str:
    mime = getattr(message.file, "mime_type", None) if message.file else None
    if mime:
        ext = mimetypes.guess_extension(mime.split(";")[0].strip())
        if ext:
            if ext == ".jpe":
                ext = ".jpg"
            return ext

    if isinstance(message.media, types.MessageMediaPhoto):
        return ".jpg"
    if isinstance(message.media, types.MessageMediaDocument):
        doc = message.media.document
        if isinstance(doc, types.Document):
            for attr in doc.attributes:
                if isinstance(attr, types.DocumentAttributeVideo):
                    return ".mp4"
                if isinstance(attr, types.DocumentAttributeAudio):
                    if getattr(attr, "voice", False):
                        return ".ogg"
                    return ".mp3"
                if isinstance(attr, types.DocumentAttributeSticker):
                    return ".webp"
                if isinstance(attr, types.DocumentAttributeAnimated):
                    return ".mp4"
                if isinstance(attr, types.DocumentAttributeFilename):
                    return ""
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


def build_filename(message, chat_title: str) -> str:
    """
    Формат: <channel>__<base>_<msgid><ext>
    Пример: durov__video_12345.mp4
    """
    channel = _sanitize(chat_title or "chat")[:40]

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

    filename = f"{channel}__{base}_{message.id}{ext}"

    if len(filename) > _MAX_NAME_LEN:
        suffix = f"_{message.id}{ext}"
        keep = _MAX_NAME_LEN - len(suffix) - len(channel) - 2
        base = base[:max(keep, 10)]
        filename = f"{channel}__{base}{suffix}"

    return filename


def unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    i = 1
    while os.path.exists(f"{base} ({i}){ext}"):
        i += 1
    return f"{base} ({i}){ext}"


async def get_chat_title(chat_id) -> str:
    try:
        entity = await client.get_entity(chat_id)
        return (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or getattr(entity, "first_name", None)
            or str(chat_id)
        )
    except Exception:
        return str(chat_id)


def extract_all_links(text: str):
    matches = re.finditer(LINK_PATTERN, text)
    results = []
    for match in matches:
        full_link = match.group(0)
        channel_part, username, message_id = match.groups()
        chat_id = int(f"-100{channel_part}") if channel_part else username
        results.append((chat_id, int(message_id), full_link))
    return results


def human_size(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024:
            return f"{num:.2f} {unit}"
        num /= 1024
    return f"{num:.2f} PB"


# ---------- Скачивание (Telethon) ----------
async def download_with_retry(chat_id, msg_id, progress_callback, max_retries=3):
    """Скачивание стандартным методом Telethon (с cryptg скорость достаточная)."""
    target_msg = await client.get_messages(chat_id, ids=msg_id)
    if not target_msg or not target_msg.media:
        return None

    chat_title = await get_chat_title(chat_id)
    suggested_filename = build_filename(target_msg, chat_title)
    local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))
[29.10.2026 21:42] Aleh: for attempt in range(1, max_retries + 1):
        try:
            return await client.download_media(
                target_msg,
                file=local_path,
                progress_callback=progress_callback,
            )
        except FileReferenceExpiredError:
            print(f"\n[Внимание] Ссылка устарела (попытка {attempt}). Перезапрашиваем...")
            target_msg = await client.get_messages(chat_id, ids=msg_id, use_cache=False)
            suggested_filename = build_filename(target_msg, chat_title)
            local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))
            if attempt == max_retries:
                raise
        except FloodWaitError as fwe:
            print(f"\n[FloodWait] Ждём {fwe.seconds} сек...")
            await asyncio.sleep(fwe.seconds)
        except Exception as e:
            if attempt == max_retries:
                raise e
            await asyncio.sleep(2 * attempt)


# ---------- Воркер очереди ----------
async def task_worker():
    while True:
        event, links = await task_queue.get()
        cancel_flag["cancel"] = False
        total_count = len(links)
        ok_count = skipped_count = failed_count = 0

        status_msg = await event.respond(
            f"⏳ Пакет ссылок ({total_count} шт.). Начинаю последовательную обработку..."
        )

        for index, (chat_id, msg_id, full_link) in enumerate(links, start=1):
            if cancel_flag["cancel"]:
                await event.respond("⛔ Обработка пачки отменена пользователем.")
                break

            key = (chat_id, msg_id)
            if key in processed:
                await event.respond(
                    f"♻️ [{index}/{total_count}] Уже обработано ранее, пропуск: {full_link}"
                )
                skipped_count += 1
                continue

            progress_data = {"last_update": 0}
            upload_progress_data = {"last_update": 0}
            filename = None

            async def download_cb(current, total):
                now = time.time()
                if now - progress_data["last_update"] > 3 or current == total:
                    progress_data["last_update"] = now
                    percent = (current / total) * 100
                    try:
                        await status_msg.edit(
                            f"⏳ Скачивание [{index}/{total_count}]\n"
                            f"🔗 {full_link}\n"
                            f"📥 {percent:.1f}% "
                            f"({human_size(current)} / {human_size(total)})"
                        )
                    except FloodWaitError as fwe:
                        await asyncio.sleep(fwe.seconds)
                    except Exception:
                        pass

            async def upload_cb(current, total):
                now = time.time()
                if now - upload_progress_data["last_update"] > 3 or current == total:
                    upload_progress_data["last_update"] = now
                    percent = (current / total) * 100 if total else 0
                    try:
                        await status_msg.edit(
                            f"📤 Отправка [{index}/{total_count}]\n"
                            f"📁 {os.path.basename(filename or '')}\n"
                            f"📊 {percent:.1f}% "
                            f"({human_size(current)} / {human_size(total)})"
                        )
                    except FloodWaitError as fwe:
                        await asyncio.sleep(fwe.seconds)
                    except Exception:
                        pass

            try:
                await status_msg.edit(
                    f"⏳ Обработка [{index}/{total_count}]\n"
                    f"🔗 {full_link}\n"
                    f"🔎 Проверка доступа..."
                )

                target_msg = await client.get_messages(chat_id, ids=msg_id)
                if not target_msg:
                    await event.respond(
                        f"⚠️ [{index}/{total_count}] Сообщение недоступно: {full_link}"
                    )
                    failed_count += 1
                    continue

                if not target_msg.media:
                    await event.respond(
                        f"⚠️ [{index}/{total_count}] В сообщении нет медиа: {full_link}"
                    )
                    failed_count += 1
                    continue

                file_size = target_msg.file.size if target_msg.file else 0
                if file_size > LIMIT_PREMIUM:
                    await event.respond(
                        f"⚠️ [{index}/{total_count}] Файл слишком большой для Telegram "
                        f"({human_size(file_size)} > 4 ГБ). Пропуск: {full_link}"
                    )
                    failed_count += 1
                    continue
                if file_size > LIMIT_FREE:
                    await event.respond(
                        f"ℹ️ [{index}/{total_count}] Файл {human_size(file_size)} > 2 ГБ — "
                        f"для отправки нужен Premium. Скачаю, но перезалить не смогу."
                    )

                filename = await download_with_retry(chat_id, msg_id, download_cb)

                if not filename:
                    await event.respond(
                        f"⚠️ [{index}/{total_count}] Не удалось скачать: {full_link}"
                    )
                    failed_count += 1
                    continue

                file_size_gb = os.path.getsize(filename) / (1024 ** 3)

                # Upload через FastTelethon.upload_file
                with open(filename, "rb") as f:
                    file_handle = await FastTelethon.upload_file(
                        client=client,
                        file=f,
                        progress_callback=upload_cb,
                    )
                    await client.send_file("me", file_handle, force_document=True)

                await event.respond(
                    f"✅ [{index}/{total_count}] {os.path.basename(filename)} "
                    f"({file_size_gb:.2f} GB) перезалит в Избранное!"
                )
                processed.add(key)
                ok_count += 1

                # 🔔 локальный звук после каждой успешной отправки
                asyncio.create_task(play_sound())

                if os.path.exists(filename):
                    os.remove(filename)

            except Exception as e:
                await event.respond(
                    f"❌ [{index}/{total_count}] Ошибка {full_link}:\n{e}"
                )
                failed_count += 1
                if filename and os.path.exists(filename):
                    try:
                        os.remove(filename)
                    except Exception:
                        pass

        await status_msg.edit(
            f"✅ Пакет обработан!\n"
            f"├ Успешно: {ok_count}\n"
            f"├ Пропущено (дубли): {skipped_count}\n"
            f"└ Ошибок: {failed_count}"
        )

        # 🔔 Звук в Telegram в конце пачки
        if ok_count > 0:
            if os.path.exists(NOTIFY_SOUND):
                try:
                    await client.send_file(
                        "me",
                        NOTIFY_SOUND,
                        voice_note=True,
                        caption=f"🔔 Пачка обработана: {ok_count} файл(ов)",
                    )
                except Exception as e:
                    print(f"[notify] Не удалось отправить {NOTIFY_SOUND}: {e}")
            asyncio.create_task(play_sound())

        task_queue.task_done()


# ---------- Обработчик сообщений ----------
@client.on(events.NewMessage(chats="me"))
async def handle_saved_messages(event):
    text = event.raw_text
    cmd = text.strip().lower()

    if cmd == "/stop":
        cancel_flag["cancel"] = True
        await event.respond(
            "⛔ Команда отмены получена — текущая пачка будет остановлена после текущего файла."
        )
        return
[29.10.2026 21:42] Aleh: if cmd == "/clear":
        cleared = 0
        while not task_queue.empty():
            try:
                task_queue.get_nowait()
                task_queue.task_done()
                cleared += 1
            except asyncio.QueueEmpty:
                break
        await event.respond(f"🧹 Очищено задач в очереди: {cleared}")
        return

    if cmd == "/stats":
        await event.respond(
            f"📊 Статистика\n"
            f"├ В очереди пачек: {task_queue.qsize()}\n"
            f"└ Обработано уникальных файлов: {len(processed)}"
        )
        return

    if "t.me/" not in text:
        return

    links = extract_all_links(text)
    if not links:
        return

    try:
        task_queue.put_nowait((event, links))
        await event.respond(
            f"📥 Ссылки добавлены в очередь ({len(links)} шт.). "
            f"Текущая длина очереди: {task_queue.qsize()}"
        )
    except asyncio.QueueFull:
        await event.respond("⚠️ Очередь переполнена (50 пачек). Дождитесь обработки текущих.")


# ---------- Запуск ----------
async def main():
    print("⏳ Подключение к Telegram...")
    await client.start()
    await client.get_dialogs()

    asyncio.create_task(task_worker())

    print(
        "\n🚀 Готово! Отправьте ссылки в Избранное.\n"
        "Команды:\n"
        "  /stop  — остановить текущую пачку\n"
        "  /clear — очистить очередь\n"
        "  /stats — статистика\n"
    )
    await client.run_until_disconnected()


if name == "main":
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.WARNING,
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)

    try:
        client.start()
        client.loop.run_until_complete(main())
    except Exception as crash_error:
        print(f"\n❌ КРИТИЧЕСКИЙ СБОЙ: {crash_error}")