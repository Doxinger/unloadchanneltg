from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import csv
import sqlite3
import json

api_id = '565347354'
api_hash = '553656fewoomn0i34932j'
phone_number = '+79999333333'  

session_name = 'my_session'
client = TelegramClient(session_name, api_id, api_hash)

channel_username_or_id = -1002555424292  # тут айди канала

processed_message_ids = set()


async def get_all_messages(channel):
    all_messages = []
    offset_id = 0
    limit = 100  # колво сообщений за один запрос

    while True:
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break

        for message in history.messages:
            if message.id not in processed_message_ids:
                all_messages.append(message)
                processed_message_ids.add(message.id)  # Добавляем ID в множество

        offset_id = history.messages[-1].id
        print(f"Получено {len(all_messages)} сообщений...")

    return all_messages

def save_to_csv(messages, filename='channel_messages.csv'):
    with open(filename, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        # Заголовки CSV
        writer.writerow(['ID', 'Дата', 'Текст сообщения', 'Отправитель'])

        for message in messages:
            writer.writerow([
                message.id,
                message.date,
                message.message,
                message.sender_id if message.sender_id else 'Нет данных'
            ])
    print(f"Сообщения сохранены в {filename}")


def save_to_sqlite(messages, db_filename='messages.db'):
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    # Создание таблицы, если она не существует
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            date TEXT,
            text TEXT,
            sender_id INTEGER
        )
    ''')


    for message in messages:
        cursor.execute('''
            INSERT OR IGNORE INTO messages (id, date, text, sender_id)
            VALUES (?, ?, ?, ?)
        ''', (
            message.id,
            str(message.date),
            message.message,
            message.sender_id if message.sender_id else None
        ))

    conn.commit()
    conn.close()
    print(f"Сообщения сохранены в базу данных {db_filename}")


def save_to_json(messages, filename='channel_messages.json'):
    data = []
    for message in messages:
        data.append({
            'id': message.id,
            'date': str(message.date),
            'text': message.message,
            'sender_id': message.sender_id if message.sender_id else None
        })

    with open(filename, mode='w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"Сообщения сохранены в {filename}")


async def main():
    await client.start(phone_number)
    print("Клиент запущен.")
    try:
        channel = await client.get_entity(channel_username_or_id)
        print(f"Канал найден: {channel.title}")
    except Exception as e:
        print(f"Ошибка: {e}")
        return

    messages = await get_all_messages(channel)
    print(f"Всего сообщений: {len(messages)}")

    save_to_csv(messages)

    save_to_sqlite(messages)
  
    save_to_json(messages)

with client:
    client.loop.run_until_complete(main())
