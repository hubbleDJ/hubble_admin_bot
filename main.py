from pathlib import Path
from TgApi import TgApi
from typing import Any
import sqlite3 as sq

import re
import json
import asyncio


BASE_DIR = Path(__file__).resolve().parent
TG_KEYS_DIR = Path(BASE_DIR, '.keys', 'tg.json')
DB_PATH = Path(BASE_DIR, 'my_db.db')


def get_token() -> str:
    """Достает токен"""
        
    with open(TG_KEYS_DIR, 'r') as f:
        token = json.load(f)['token']
    
    return token


async def db_run_query(query: str) -> Any:
    """Делает запрос в базу данных"""
    
    with sq.connect(DB_PATH) as connect:
        answer = connect.cursor().execute(query).fetchall()
    return answer


def db_create_table_users() -> str:
    """Создает таблицу с пользователями"""
    
    query = (
        'create table if not exists users (\n'
        '\tchat_id integer,\n'
        '\tuser_id integer,\n'
        '\tuser_name text\n'
        ')'
    )
    return str(asyncio.run(db_run_query(query)))


def db_save_user_info(chat_id: int, user_id: int, user_name: str) -> None:
    """Сохраняет инфу о пользователе в базе данных"""
    
    query = (
        'select count(1) from users\n'
        f'\twhere chat_id = {chat_id}\n'
        f'\tand user_id = {user_id}\n'
        f'\tand user_name = "{user_name}"'
    )
    
    if asyncio.run(db_run_query(query))[0][0] == 0:
        query = (
            'insert into users (chat_id, user_id, user_name) '
            f'values({chat_id}, {user_id}, "{user_name}")'
        )
        asyncio.run(db_run_query(query))
        

def db_get_all_user_name(chat_id: int) -> list[str]:
    """Достает name пользователей в базе данных с @"""
    
    query = (
        'select user_name from users\n'
        f'\twhere chat_id = {chat_id}'
    )
    
    return [f'@{row[0]}' for row in asyncio.run(db_run_query(query))]
    

def get_admins(tg_bot: TgApi, chat_id: int) -> list[int]:
    """Получаем список админов"""

    return {
        admin['user']['id']
        for admin in asyncio.run(tg_bot.get_admins(chat_id))['users']
    }


def get_commands(text: str) -> list[str]:
    """Ищет команду в тексте сообщения по маске @"""
    
    pattern = r'@([^ \n]+)'
    return re.findall(pattern, text)

    
def tag_all_users(tg_bot: TgApi, message: dict) -> None:
    """Тегает всех в чате"""
    
    users_str = ' '.join(db_get_all_user_name(message['chat']['id']))
    answer_text = message['text'].replace('@all', users_str)
    asyncio.run(tg_bot.delete_message(
        chat_id=message['chat']['id'],
        message_id=message['message_id']
    ))
    asyncio.run(tg_bot.send_message(
        text=answer_text,
        chat_id=message['chat']['id'],
        message_thread_id=message['message_thread_id'] if 'message_thread_id' in message else None
    ))


def main() -> None:
    while True:
        messages = asyncio.run(bot.get_messages())['messages']
        if len(messages) > 0:
            for message in messages:
                db_save_user_info(
                    chat_id=message['chat']['id'],
                    user_id=message['from']['id'],
                    user_name=message['from']['username'],
                )
                if 'text' in message:
                    commands = get_commands(message['text'])
                    if message['from']['id'] in get_admins(bot, message['chat']['id'])\
                        and len(commands) == 1\
                        and commands[0] in ADMIN_FUNCTIONS:
                        
                        ADMIN_FUNCTIONS[commands[0]](bot, message)
        # break

ADMIN_FUNCTIONS = {
    'all': tag_all_users
}

if __name__ == '__main__':
    TOKEN = get_token()    
    bot = TgApi(TOKEN)
    main()
