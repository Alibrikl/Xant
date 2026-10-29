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
    print("❌ Ошибка: Неверный идентификатор TELEGRAM_API_ID в файле .env (должен содержать только цифры).")
    print("Пожалуйста, удалите env-файл и перезапустите скрипт.")
    exit(1)

client = TelegramClient(
    "user_session",
    API_ID,
    API_HASH,
    connection_retries=10,
    retry_delay=3,
    timeout=60  # Увеличили таймаут для больших файлов
)

LINK_PATTERN = r"https://t\.me/(?:c/(\d+)|([\w_]+))/(\d+)"


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


async def download_with_retry(chat_id, msg_id, progress_callback, max_retries=3):
    """Теперь функция принимает chat_id и msg_id для возможности обновления ссылки"""
    # Сначала получаем актуальное сообщение
    target_msg = await client.get_messages(chat_id, ids=msg_id)
    
    if not target_msg or not target_msg.media:
        return None

    for attempt in range(1, max_retries + 1):
        try:
            # Передаем именно весь объект target_msg, чтобы Telethon мог автоматически обновлять file_reference
            return await client.download_media(
                target_msg,
                progress_callback=progress_callback
            )
        except FileReferenceExpiredError:
            print(f"\n[Внимание] Ссылка устарела на попытке {attempt}. Обновляем сообщение...")
            # Перезапрашиваем сообщение заново с серверов Telegram, чтобы получить новый file_reference
            target_msg = await client.get_messages(chat_id, ids=msg_id)
            if attempt == max_retries:
                raise
        except Exception as e:
            if attempt == max_retries:
                raise e
            await asyncio.sleep(2 * attempt)


@client.on(events.NewMessage(chats="me"))
async def handle_saved_messages(event):
    text = event.raw_text

    if "t.me/" not in text:
        return

    links = extract_all_links(text)
    if not links:
        return

    total_count = len(links)
    status_msg = await event.respond(
        f"⏳ Найденные ссылки: {total_count}. Начало обработки..."
    )

    for index, (chat_id, msg_id, full_link) in enumerate(links, start=1):
        last_update_time = [0]

        async def progress_callback(current, total):
            now = time.time()
            if now - last_update_time[0] > 3 or current == total:  # Увеличили шаг до 3 секунд, чтобы не ловить флуд-вейты на изменении текста сообщения
                last_update_time[0] = now
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
                f"🔎 Получение сообщения..."
            )

            # Проверяем доступность чата, активируя entity в кэше Telethon
            try:
                await client.get_entity(chat_id)
            except Exception as entity_err:
                await event.respond(
                    f"⚠️ **[{index}/{total_count}]** Нет доступа к закрытому чату `{chat_id}`. Вы состоите в нём? Скип."
                )
                continue

            # Вызываем исправленную функцию загрузки
            filename = await download_with_retry(chat_id, msg_id, progress_callback)

            if not filename:
                await event.respond(
                    f"⚠️ **[{index}/{total_count}]** Медиафайл не найден по адресу `{full_link}`."
                )
                continue

            # Проверяем размер скачанного файла перед отправкой
            file_size_bytes = os.path.getsize(filename)
            file_size_gb = file_size_bytes / (1024 ** 3)

            await status_msg.edit(
                f"📤 **[{index}/{total_count}]** Отправка файла ({file_size_gb:.2f} GB) в сохраненные сообщения..."
            )
            
            try:
                await client.send_file("me", filename)
                await event.respond(f"✅ **[{index}/{total_count}]** Файл успешно перезалит из `{full_link}`")
            except Exception as upload_err:
                # Если файл > 2ГБ (или > 4ГБ для Premium), Telegram не даст его отправить назад через send_file
                await event.respond(
                    f"⚠️ **[{index}/{total_count}]** Файл скачан локально как `{filename}`, но Telegram "
                    f"отклонил его отправку назад (Размер: {file_size_gb:.2f} GB). Ошибка: `{upload_err}`.\n"
                    f"Пожалуйста, заберите файл прямо с сервера/компьютера, где запущен скрипт."
                )

        except Exception as e:
            await event.respond(
                f"❌ **[{index}/{total_count}]** Ошибка обработки ссылки `{full_link}`:\n`{e}`"
            )
        finally:
            # Удаляем файл ТОЛЬКО если он успешно отправился. 
            # Если отправка упала из-за лимита размера, файл останется на диске, чтобы вы его не потеряли.
            if filename and os.path.exists(filename):
                try:
                    # Раскомментируйте строку ниже, если хотите удалять файл в любом случае:
                    # os.remove(filename)
                    pass 
                except Exception:
                    pass

    await status_msg.edit(
        f"✅ **Все ссылки обработаны! ({total_count}/{total_count})**"
    )


async def main():
    print("⏳ Подключение к Telegram...")
    await client.start()
    await client.get_dialogs()
    print("\n🚀 Все готово! Отправьте ссылку на закрытые/открытые сообщения в свое Избранное.")
    await client.run_until_disconnected()


if __name__ == "__main__":
    client.loop.run_until_complete(main())