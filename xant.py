import os
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
from telethon.tl.types import DocumentAttributeFilename

ENV_FILE = ".env"
DOWNLOAD_DIR = "downloads"
NOTIFY_SOUND = "notify.ogg"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

LIMIT_FREE = 2 * 1024 ** 3
LIMIT_PREMIUM = 4 * 1024 ** 3

CONCURRENT_DOWNLOADS = 3
CLEANUP_DELAY = 10

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

client = TelegramClient("user_session", API_ID, API_HASH, connection_retries=10, retry_delay=3, timeout=60)

LINK_PATTERN = r"https://t\.me/(?:c/(\d+)|([\w_]+))/(\d+)"

link_queue: asyncio.Queue = asyncio.Queue()
processed: set[tuple] = set()
cancel_flag = {"cancel": False}

batches: dict[int, dict] = {}
batch_counter = {"id": 0}

upload_lock = asyncio.Lock()


# ---------- Звук ----------
def _play_sound_sync():
    try:
        if sys.platform == "win32":
            import winsound
            winsound.MessageBeep(winsound.MB_ICONASTERISK)
            return
        if sys.platform == "darwin":
            if shutil.which("afplay"):
                subprocess.run(["afplay", "/System/Library/Sounds/Glass.aiff"], check=False, timeout=3)
                return
        for player, sound in (
            ("paplay", "/usr/share/sounds/freedesktop/stereo/complete.oga"),
            ("aplay", "/usr/share/sounds/alsa/Front_Center.wav"),
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
    await asyncio.to_thread(_play_sound_sync)


# ---------- Отложенное удаление ----------
async def _delete_later(*messages, delay: float = CLEANUP_DELAY):
    if delay > 0:
        await asyncio.sleep(delay)
    for m in messages:
        if m is None:
            continue
        try:
            await m.delete()
        except Exception:
            pass


# ---------- Имена файлов ----------
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
            return ".jpg" if ext == ".jpe" else ext
    if isinstance(message.media, types.MessageMediaPhoto):
        return ".jpg"
    if isinstance(message.media, types.MessageMediaDocument):
        doc = message.media.document
        if isinstance(doc, types.Document):
            for attr in doc.attributes:
                if isinstance(attr, types.DocumentAttributeVideo):
                    return ".mp4"
                if isinstance(attr, types.DocumentAttributeAudio):
                    return ".ogg" if getattr(attr, "voice", False) else ".mp3"
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
    if len(filename) > _MAX_NAME_LEN:
        suffix = f"_{message.id}{ext}"
        keep = _MAX_NAME_LEN - len(suffix)
        base = base[:max(keep, 10)]
        filename = f"{base}{suffix}"
    return filename


def unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    i = 1
    while os.path.exists(f"{base} ({i}){ext}"):
        i += 1
    return f"{base} ({i}){ext}"


def extract_all_links(text: str):
    results = []
    for match in re.finditer(LINK_PATTERN, text):
        channel_part, username, message_id = match.groups()
        chat_id = int(f"-100{channel_part}") if channel_part else username
        results.append((chat_id, int(message_id), match.group(0)))
    return results


def human_size(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024:
            return f"{num:.2f} {unit}"
        num /= 1024
    return f"{num:.2f} PB"


# ---------- Скачивание ----------
async def download_with_retry(chat_id, msg_id, progress_callback, max_retries=3):
    target_msg = await client.get_messages(chat_id, ids=msg_id)
    if not target_msg or not target_msg.media:
        return None

    suggested_filename = build_filename(target_msg)
    local_path = unique_path(os.path.join(DOWNLOAD_DIR, suggested_filename))

    for attempt in range(1, max_retries + 1):
        try:
            return await client.download_media(
                target_msg, file=local_path, progress_callback=progress_callback,
            )
        except FileReferenceExpiredError:
            print(f"\n[Внимание] Ссылка устарела (попытка {attempt}). Перезапрашиваем...")
            target_msg = await client.get_messages(chat_id, ids=msg_id, use_cache=False)
            suggested_filename = build_filename(target_msg)
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


# ---------- Обработка одной ссылки ----------
async def process_link(chat_id, msg_id, full_link, batch_id):
    batch = batches[batch_id]

    key = (chat_id, msg_id)
    if key in processed:
        await batch["event"].respond(f"♻️ Уже обработано: `{full_link}`")
        return "skip"

    status_msg = await batch["event"].respond(f"⏳ Старт: `{full_link}`")

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
                    f"⏳ **Скачивание**\n"
                    f"🔗 `{full_link}`\n"
                    f"📥 **{percent:.1f}%** ({human_size(current)} / {human_size(total)})"
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
                    f"📤 **Отправка**\n"
                    f"📁 `{os.path.basename(filename or '')}`\n"
                    f"📊 **{percent:.1f}%** ({human_size(current)} / {human_size(total)})"
                )
            except FloodWaitError as fwe:
                await asyncio.sleep(fwe.seconds)
            except Exception:
                pass

    try:
        target_msg = await client.get_messages(chat_id, ids=msg_id)
        if not target_msg or not target_msg.media:
            await status_msg.edit(f"⚠️ Нет медиа: `{full_link}`")
            return "fail"

        file_size = target_msg.file.size if target_msg.file else 0
        if file_size > LIMIT_PREMIUM:
            await status_msg.edit(
                f"⚠️ Файл слишком большой ({human_size(file_size)} > 4 ГБ). Пропуск."
            )
            return "fail"
        if file_size > LIMIT_FREE:
            await status_msg.edit(
                f"ℹ️ Файл {human_size(file_size)} > 2 ГБ — нужен Premium для отправки."
            )

        filename = await download_with_retry(chat_id, msg_id, download_cb)
        if not filename:
            await status_msg.edit(f"⚠️ Не удалось скачать: `{full_link}`")
            return "fail"

        file_size_gb = os.path.getsize(filename) / (1024 ** 3)
        display_name = os.path.basename(filename)

        async with upload_lock:
            await status_msg.edit(
                f"📤 **Отправка**\n"
                f"📁 `{display_name}`\n"
                f"⏸ Ожидание очереди upload..."
            )
            await client.send_file(
                "me",
                filename,
                force_document=True,
                attributes=[DocumentAttributeFilename(display_name)],
                progress_callback=upload_cb,
            )

        await status_msg.edit(f"✅ `{display_name}` ({file_size_gb:.2f} GB) перезалит!")
        processed.add(key)
        asyncio.create_task(play_sound())
        if os.path.exists(filename):
            os.remove(filename)
        return "ok"

    except Exception as e:
        await status_msg.edit(f"❌ Ошибка `{full_link}`:\n`{e}`")
        if filename and os.path.exists(filename):
            try:
                os.remove(filename)
            except Exception:
                pass
        return "fail"


# ---------- Воркер ----------
async def link_worker(worker_id: int):
    while True:
        try:
            chat_id, msg_id, full_link, batch_id = await link_queue.get()
        except asyncio.CancelledError:
            return

        try:
            if cancel_flag["cancel"]:
                result = "skip"
            else:
                result = await process_link(chat_id, msg_id, full_link, batch_id)

            batch = batches.get(batch_id)
            if batch is not None:
                batch[result] += 1
                await _maybe_finish_batch(batch_id)

        except Exception as e:
            print(f"[worker #{worker_id}] Необработанная ошибка: {e}")
            batch = batches.get(batch_id)
            if batch is not None:
                batch["fail"] += 1
                await _maybe_finish_batch(batch_id)

        finally:
            link_queue.task_done()


async def _maybe_finish_batch(batch_id: int):
    b = batches.get(batch_id)
    if b is None:
        return
    done = b["ok"] + b["skip"] + b["fail"]
    if done < b["total"]:
        return

    summary_msg = await b["event"].respond(
        f"✅ **Пачка обработана!**\n"
        f"├ Успешно: {b['ok']}\n"
        f"├ Пропущено (дубли): {b['skip']}\n"
        f"└ Ошибок: {b['fail']}"
    )
    b["to_delete"].append(summary_msg)

    if b["ok"] > 0:
        if os.path.exists(NOTIFY_SOUND):
            try:
                notify_msg = await client.send_file(
                    "me", NOTIFY_SOUND, voice_note=True,
                    caption=f"🔔 Пачка обработана: {b['ok']} файл(ов)",
                )
                b["to_delete"].append(notify_msg)
            except Exception as e:
                print(f"[notify] Не удалось отправить {NOTIFY_SOUND}: {e}")
        asyncio.create_task(play_sound())

    asyncio.create_task(_delete_later(*b["to_delete"]))
    batches.pop(batch_id, None)


# ---------- Отправка старых файлов из downloads/ ----------
async def upload_existing_downloads():
    """Отправляет все файлы из downloads в Избранное по одному."""
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
        size_gb = os.path.getsize(path) / (1024 ** 3)
        progress_data = {"last_update": 0}

        async def cb(current, total_bytes):
            now = time.time()
            if now - progress_data["last_update"] > 3 or current == total_bytes:
                progress_data["last_update"] = now
                percent = (current / total_bytes) * 100 if total_bytes else 0
                try:
                    await status.edit(
                        f"📤 **Отправка файлов [{i}/{total}]**\n"
                        f"📁 `{name}`\n"
                        f"📊 **{percent:.1f}%** "
                        f"({human_size(current)} / {human_size(total_bytes)})"
                    )
                except FloodWaitError as fwe:
                    await asyncio.sleep(fwe.seconds)
                except Exception:
                    pass

        try:
            async with upload_lock:
                await client.send_file(
                    "me",
                    path,
                    force_document=True,
                    attributes=[DocumentAttributeFilename(name)],
                    progress_callback=cb,
                )
            os.remove(path)
            ok += 1
            print(f"  ✅ [{i}/{total}] {name} ({size_gb:.2f} GB)")
        except Exception as e:
            fail += 1
            print(f"  ❌ [{i}/{total}] {name}: {e}")

    await status.edit(
        f"✅ **Скачанные файлы обработаны**\n"
        f"├ Успешно: {ok}\n"
        f"└ Ошибок: {fail}"
    )
    asyncio.create_task(play_sound())


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

    if cmd == "/clear":
        cleared = 0
        while not link_queue.empty():
            try:
                link_queue.get_nowait(); link_queue.task_done(); cleared += 1
            except asyncio.QueueEmpty:
                break
        await event.respond(f"🧹 Очищено ссылок в очереди: {cleared}")
        return

    if cmd == "/stats":
        await event.respond(
            f"📊 **Статистика**\n"
            f"├ Ссылок в очереди: {link_queue.qsize()}\n"
            f"├ Активных пачек: {len(batches)}\n"
            f"├ Одновременных скачиваний: {CONCURRENT_DOWNLOADS}\n"
            f"├ Upload: по одному файлу (Lock)\n"
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
    batches[batch_id] = {
        "event": event, "total": len(links),
        "ok": 0, "skip": 0, "fail": 0,
        "to_delete": [event],
    }
    cancel_flag["cancel"] = False

    for chat_id, msg_id, full_link in links:
        await link_queue.put((chat_id, msg_id, full_link, batch_id))

    info_msg = await event.respond(
        f"📥 Добавлено ссылок: {len(links)}. "
        f"Скачивание: до {CONCURRENT_DOWNLOADS} параллельно, upload: по одному. "
        f"Всего в очереди: {link_queue.qsize()}"
    )
    batches[batch_id]["to_delete"].append(info_msg)


# ---------- Запуск ----------
async def main():
    # [NEW] проверяем downloads/ ДО подключения (input не блокирует loop)
    files_in_downloads = [
        f for f in os.listdir(DOWNLOAD_DIR)
        if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))
    ]
    send_old = False
    if files_in_downloads:
        print(f"\n📂 В папке `{DOWNLOAD_DIR}/` найдено {len(files_in_downloads)} файл(ов):")
        for f in files_in_downloads[:20]:
            size_mb = os.path.getsize(os.path.join(DOWNLOAD_DIR, f)) / (1024 ** 2)
            print(f"   • {f} ({size_mb:.1f} MB)")
        if len(files_in_downloads) > 20:
            print(f"   … и ещё {len(files_in_downloads) - 20}")
        answer = input("\nОтправить их в Избранное? [y/n]: ").strip().lower()
        send_old = answer in ("y", "yes", "да", "д")

    print("⏳ Подключение к Telegram...")
    await client.start()
    await client.get_dialogs()

    # [NEW] отправляем старые файлы, если пользователь согласился
    if send_old:
        await upload_existing_downloads()

    workers = [
        asyncio.create_task(link_worker(i + 1))
        for i in range(CONCURRENT_DOWNLOADS)
    ]
    print(f"  ▶ Запущено воркеров: {CONCURRENT_DOWNLOADS} (upload — по одному)")

    print(
        f"\n🚀 Готово!\n"
        f"├ Скачивание: до {CONCURRENT_DOWNLOADS} параллельно\n"
        f"├ Отправка: строго по одному файлу\n"
        f"└ Служебные сообщения удаляются через {CLEANUP_DELAY} сек\n"
        f"Команды: /stop, /clear, /stats"
    )
    try:
        await client.run_until_disconnected()
    finally:
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


if __name__ == "__main__":
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