import os
import re
import time
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FileReferenceExpiredError, FloodWaitError

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
    timeout=60  # Увеличенный таймаут для стабильного скачивания файлов > 2 ГБ
)

LINK_PATTERN = r"https://t\.me/(?:c/(\d+)|([\w_]+))/(\d+)"

# Глобальная асинхронная очередь задач для последовательной обработки
task_queue = asyncio.Queue()


def extract_all_links(text: str):
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


def get_original_filename(message):
    """Извлекает оригинальное имя файла или генерирует его на основе типа медиа"""
    if not message or not message.media:
        return None
        
    # Попытка получить имя из атрибутов файла Telegram
    if message.file and message.file.name:
        return message.file.name
        
    # Если имени нет (например, видео без названия), генерируем понятное имя с правильным расширением
    ext = message.file.ext if message.file and message.file.ext else ".dat"
    return f"telegram_file_{message.chat_id}_{message.id}{ext}"


async def download_with_retry(chat_id, msg_id, progress_callback, max_retries=3):
    """
    Функция скачивания медиафайла.
    Принимает chat_id и msg_id для возможности безопасного 
    перезапроса file_reference при долгой загрузке.
    """
    target_msg = await client.get_messages(chat_id, ids=msg_id)
    
    if not target_msg or not target_msg.media:
        return None

    # Получаем оригинальное имя файла
    suggested_filename = get_original_filename(target_msg)

    for attempt in range(1, max_retries + 1):
        try:
            return await client.download_media(
                target_msg,
                file=suggested_filename,  # Передаем оригинальное имя файла для сохранения на диск
                progress_callback=progress_callback
            )
        except FileReferenceExpiredError:
            print(f"\n[Внимание] Ссылка устарела на попытке {attempt}. Перезапрашиваем сообщение без кэша...")
            # ВАЖНО: use_cache=False принудительно обновляет file_reference для больших файлов
            target_msg = await client.get_messages(chat_id, ids=msg_id, use_cache=False)
            if attempt == max_retries:
                raise
        except Exception as e:
            if attempt == max_retries:
                raise e
            await asyncio.sleep(2 * attempt)


async def task_worker():
    """Фоновый воркер, выполняющий задачи из очереди строго по очереди (один за другим)"""
    while True:
        event, links = await task_queue.get()
        total_count = len(links)
        
        status_msg = await event.respond(
            f"⏳ В очереди обнаружен пакет ссылок ({total_count} шт.). Начинаю последовательную обработку..."
        )

        for index, (chat_id, msg_id, full_link) in enumerate(links, start=1):
            progress_data = {"last_update": 0}

            async def progress_callback(current, total):
                now = time.time()
                if now - progress_data["last_update"] > 3 or current == total:
                    progress_data["last_update"] = now
                    percent = (current / total) * 100
                    mb_current = current / (1024 * 1024)
                    mb_total = total / (1024 * 1024)
                    
                    try:
                        await status_msg.edit(
                            f"⏳ **Обработка [{index}/{total_count}]**\n"
                            f"🔗 `{full_link}`\n"
                            f"📥 Загрузка: **{percent:.1f}%** ({mb_current:.1f} MB / {mb_total:.1f} MB)"
                        )
                    except FloodWaitError as fwe:
                        await asyncio.sleep(fwe.seconds)
                    except Exception:
                        pass

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

                # Скачиваем файл с оригинальным именем
                filename = await download_with_retry(chat_id, msg_id, progress_callback)

                if not filename:
                    await event.respond(
                        f"⚠️ **[{index}/{total_count}]** Файл не найден или сообщение недоступно: `{full_link}`."
                    )
                    continue

                file_size_gb = os.path.getsize(filename) / (1024 ** 3)

                await status_msg.edit(
                    f"📤 **[{index}/{total_count}]** Отправка файла `{os.path.basename(filename)}` ({file_size_gb:.2f} GB) в Избранное..."
                )
                
                try:
                    # Отправляем файл обратно с оригинальным именем файла на диске
                    await client.send_file("me", filename, force_document=True)
                    await event.respond(f"✅ **[{index}/{total_count}]** Файл `{os.path.basename(filename)}` успешно перезалиты!")
                    
                    if os.path.exists(filename):
                        os.remove(filename)
                        
                except Exception as upload_err:
                    await event.respond(
                        f"⚠️ **[{index}/{total_count}]** Файл `{os.path.basename(filename)}` успешно скачан локально, но Telegram отклонил отправку "
                        f"(Размер: {file_size_gb:.2f} GB).\n"
                        f"Это ограничение размера вашего аккаунта (2 ГБ обычный / 4 ГБ Premium).\n"
                        f"📁 Файл сохранен на сервере под именем: `{filename}`"
                    )

            except Exception as e:
                await event.respond(
                    f"❌ **[{index}/{total_count}]** Ошибка обработки ссылки `{full_link}`:\n`{e}`"
                )
                if filename and os.path.exists(filename):
                    try:
                        os.remove(filename)
                    except Exception:
                        pass

        await status_msg.edit(
            f"✅ **Пакет ссылок успешно обработан! ({total_count}/{total_count})**"
        )
        task_queue.task_done()


@client.on(events.NewMessage(chats="me"))
async def handle_saved_messages(event):
    text = event.raw_text

    if "t.me/" not in text:
        return

    links = extract_all_links(text)
    if not links:
        return

    # Добавляем задачу (событие и извлеченные ссылки) в асинхронную очередь задач
    await task_queue.put((event, links))
    await event.respond("📥 Ссылки успешно добавлены в очередь обработки. Скрипт скачает файлы последовательно один за другим.")


async def main():
    print("⏳ Подключение к Telegram...")
    await client.start()
    await client.get_dialogs()
    
    # Запускаем бесконечный фоновый воркер для последовательной обработки задач
    asyncio.create_task(task_worker())
    
    print("\n🚀 Все готово! Отправьте ссылку на закрытые/открытые сообщения в свое Избранное (Saved Messages).")
    await client.run_until_disconnected()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
