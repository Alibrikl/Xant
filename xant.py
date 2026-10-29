import os
import re
import time
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FileReferenceExpiredError, FloodWaitError

# Импортируем оптимизированные методы из fast_telethon
from FastTelethon import download_file, upload_file

ENV_FILE = ".env"

if not os.path.exists(ENV_FILE):
    print("==================================================")
    print("🚀 Добро пожаловать в Xant")
    print("Получите свои учетные данные API https://telegram.org")
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
    print("❌ Ошибка: Неверный идентификатор TELEGRAM_API_ID в файле .env (должен содержать только цифры).")
    print("Пожалуйста, удалите env-файл и перезапустите скрипт.")
    exit(1)

client = TelegramClient(
    "user_session",
    API_ID,
    API_HASH,
    connection_retries=10,
    retry_delay=3,
    timeout=120
)

LINK_PATTERN = r"https://t\.me/(?:c/(\d+)|([\w_]+))/(\d+)"
task_queue = asyncio.Queue()


def extract_all_links(text: str):
    if not text:
        return []
    matches = re.finditer(LINK_PATTERN, text)
    results = []

    for match in matches:
        full_link = match.group(0)
        channel_part, username, message_id = match.groups()

        if channel_part:
            chat_id = int(f"-100{channel_part}")
        else:
            chat_id = username

        results.append((chat_id, int(message_id), full_link))

    return results

def play_sound_notification():
    """Воспроизводит системный звуковой сигнал (BEEP)"""
    try:
        print("\a", end="", flush=True)
    except Exception:
        pass


def get_original_filename(message):
    if not message or not message.media:
        return None
    if message.file and message.file.name:
        return message.file.name
    ext = message.file.ext if message.file and message.file.ext else ".dat"
    return f"telegram_file_{message.chat_id}_{message.id}{ext}"


async def download_with_retry(chat_id, msg_id, progress_callback, max_retries=3):
    try:
        target_msg = await client.get_messages(chat_id, ids=msg_id)
    except Exception as e:
        print(f"Ошибка получения сообщения {msg_id} из {chat_id}: {e}")
        return None
    
    if not target_msg or not target_msg.media:
        return None

    suggested_filename = get_original_filename(target_msg)

    for attempt in range(1, max_retries + 1):
        try:
            with open(suggested_filename, "wb") as f:
                await download_file(
                    client, 
                    target_msg.media.document, 
                    f, 
                    progress_callback=progress_callback
                )
            return suggested_filename
        except FileReferenceExpiredError:
            print(f"\n[Внимание] Ссылка устарела на попытке {attempt}. Перезапрашиваем сообщение без кэша...")
            target_msg = await client.get_messages(chat_id, ids=msg_id, use_cache=False)
            if attempt == max_retries:
                raise
        except Exception as e:
            if attempt == max_retries:
                raise e
            await asyncio.sleep(2 * attempt)


async def task_worker():
    while True:
        event, links = await task_queue.get()
        total_count = len(links)
        
        status_msg = await event.respond(
            f"⏳ В очереди обнаружен пакет ссылок ({total_count} шт.). Начинаю обработку через fast_telethon..."
        )

        for index, (chat_id, msg_id, full_link) in enumerate(links, start=1):
            # Контейнеры для отслеживания прогресса скачивания и загрузки
            download_progress = {"last_update": 0, "percent": 0.0}
            upload_progress = {"last_update": 0, "percent": 0.0}

            # 1. Колбэк для отслеживания СКАЧИВАНИЯ
            def download_callback(current, total):
                now = time.time()
                if total > 0:
                    download_progress["percent"] = (current / total) * 100
                
                if now - download_progress["last_update"] > 3 or current == total:
                    download_progress["last_update"] = now
                    mb_current = current / (1024 * 1024)
                    mb_total = total / (1024 * 1024)
                    
                    asyncio.create_task(
                        status_msg.edit(
                            f"⏳ **Обработка [{index}/{total_count}]**\n"
                            f"🔗 `{full_link}`\n"
                            f"📥 Скачивание: **{download_progress['percent']:.1f}%** ({mb_current:.1f} MB / {mb_total:.1f} MB)"
                        )
                    )

            # 2. Колбэк для отслеживания ЗАГРУЗКИ (ОТПРАВКИ)
            def upload_callback(current, total):
                now = time.time()
                if total > 0:
                    upload_progress["percent"] = (current / total) * 100
                
                if now - upload_progress["last_update"] > 3 or current == total:
                    upload_progress["last_update"] = now
                    mb_current = current / (1024 * 1024)
                    mb_total = total / (1024 * 1024)
                    
                    asyncio.create_task(
                        status_msg.edit(
                            f"⏳ **Обработка [{index}/{total_count}]**\n"
                            f"🔗 `{full_link}`\n"
                            f"📤 Отправка в Избранное: **{upload_progress['percent']:.1f}%** ({mb_current:.1f} MB / {mb_total:.1f} MB)"
                        )
                    )

            filename = None
            try:
                await status_msg.edit(
                    f"⏳ **Обработка [{index}/{total_count}]**\n"
                    f"🔗 `{full_link}`\n"
                    f"🔎 Проверка доступа к чату..."
                )

                try:
                    await client.get_entity(chat_id)
                except Exception:
                    await event.respond(
                        f"⚠️ **[{index}/{total_count}]** Нет доступа к закрытому чату `{chat_id}`. Пропуск."
                    )
                    continue

                # Шаг 1: Скачивание файла
                filename = await download_with_retry(chat_id, msg_id, download_callback)

                if not filename or not os.path.exists(filename):
                    await event.respond(
                        f"⚠️ **[{index}/{total_count}]** Файл не найден или сообщение недоступно: `{full_link}`."
                    )
                    continue

                file_size_gb = os.path.getsize(filename) / (1024 ** 3)

                # Шаг 2: Отгрузка файла на сервера Telegram (с отслеживанием процентов)
                await status_msg.edit(
                    f"⏳ **[{index}/{total_count}]** Подготовка к отправке файла `{os.path.basename(filename)}` ({file_size_gb:.2f} GB)..."
                )
                
                try:
                    with open(filename, "rb") as f:
                        uploaded_file_handle = await upload_file(
                            client, 
                            f, 
                            filename=os.path.basename(filename),
                            progress_callback=upload_callback  # Добавили отслеживание прогресса отгрузки
                        )
                    
                    # Финальная отправка структуры сообщения в "Избранное"
                    await client.send_file("me", uploaded_file_handle, force_document=True)
                    
                    # ЗВУКОВОЙ СИГНАЛ: Воспроизводится строго ПОСЛЕ успешной отправки в Избранное
                    play_sound_notification()
                    
                    await event.respond(f"✅ **[{index}/{total_count}]** Файл `{os.path.basename(filename)}` успешно перезалит в Избранное!")
                    
                except Exception as upload_err:
                    await event.respond(
                        f"⚠️ **[{index}/{total_count}]** Файл скачан локально, но Telegram отклонил загрузку.\n"
                        f"Ошибка: `{upload_err}`\n"
                        f"Размер: {file_size_gb:.2f} GB. Ограничение аккаунта (2 ГБ обычный / 4 ГБ Premium).\n"
                        f"📁 Файл сохранен локально: `{filename}`"
                    )
                    filename = None  # Не удаляем файл, оставляем на сервере/ПК пользователя

            except Exception as e:
                await event.respond(
                    f"❌ **[{index}/{total_count}]** Ошибка обработки ссылки `{full_link}`:\n`{e}`"
                )
            finally:
                if filename and os.path.exists(filename):
                    try:
                        os.remove(filename)
                    except Exception:
                        pass
                        
        await status_msg.delete()
        task_queue.task_done()


@client.on(events.NewMessage(pattern=r'.*https://t\.me/.*'))
async def handle_new_links(event):
    if not event.is_private:
        return
        
    links = extract_all_links(event.text)
    if links:
        await task_queue.put((event, links))
        await event.respond(f"📥 Добавлено в очередь пакетов. Обнаружено ссылок: {len(links)}")


async def main():
    client.loop.create_task(task_worker())
    await client.start()
    print("🤖 Скрипт Xant (Fast) успешно запущен и использует многопоточную загрузку!")
    await client.run_until_disconnected()

if __name__ == "__main__":
    import logging
    # Включаем подробный вывод логов Telethon в консоль
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
        level=logging.INFO
    )
    
    try:
        client.loop.run_until_complete(main())
    except Exception as crash_error:
        print(f"\n❌ КРИТИЧЕСКИЙ СБОЙ ПРИ ЗАПУСКЕ: {crash_error}")