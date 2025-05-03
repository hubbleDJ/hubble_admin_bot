from pathlib import Path
from TgApi import TgApi
from typing import Any
import sqlite3 as sq

import re
import sys
import json
import asyncio
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
TG_KEYS_DIR = Path(BASE_DIR, '.keys', 'tg.json')
GS_KEYS_DIR = Path(BASE_DIR, '.keys', 'google_creds.json')
DB_PATH = Path(BASE_DIR, 'my_db.db')

SHEET_ID = '1qgSySoi2qepPO841gTLkSdwXNmmbVlagw0ldsNSFXRo'

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    GS_KEYS_DIR,
    ['https://www.googleapis.com/auth/spreadsheets',
     'https://www.googleapis.com/auth/drive'])
service = apiclient.discovery.build('sheets', 'v4', http=credentials.authorize(httplib2.Http()))

IS_TEST = len(sys.argv) > 1 and sys.argv[1] == 'test'
print(f'is test == {IS_TEST}')

def get_token() -> str:
    """Достает токен"""
        
    with open(TG_KEYS_DIR, 'r') as f:
        if not IS_TEST:
            token = json.load(f)['token']
        else:
            token = json.load(f)['test_bot_token']
    
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


def db_create_table_messages() -> None:
    """Создает таблицу с сообщениями"""

    query = (
        'create table if not exists messages (\n'
        '\tchat_id integer,\n'
        '\tuser_id integer,\n'
        '\tmessage_id integet,\n'
        '\ttext text,\n'
        '\tthread_id integer\n'
        ')'
    )

    asyncio.run(db_run_query(query))


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


def db_save_message(chat_id: int, user_id: int, message_id: int, text: str, thread_id: int=-1) -> None:
    """Сохраняет сообщение пользователя"""

    query = (
        'insert into messages (chat_id, user_id, message_id, text, thread_id) '
        f'values({chat_id}, {user_id}, {message_id}, "{text}", {thread_id})'
    )
    try:
        asyncio.run(db_run_query(query))
    except Exception as err:
        print(err)
        print(query)

def db_get_all_user_name(chat_id: int) -> list[str]:
    """Достает name пользователей в базе данных с @"""
    
    query = (
        'select distinct user_name from users\n'
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

def datettime_table_statistic(tg_bot: TgApi, message: dict) -> None:
    values = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range='A1:X1000',
        majorDimension='COLUMNS'
    ).execute()['values']

    def get_empty_values_str(count: int) -> list:
        return ['' for i in range(count)]

    len_table = max(*[len(column) - 1 for column in values])

    columns = {column[0]: column[1:] + get_empty_values_str(len_table - len(column[1:])) for column in values}

    table_md = (
        pd.melt(
            pd.DataFrame(columns),
            id_vars=['Отметка времени', 'Как тебя зовут(ФИО или ник в ТГ)?'],
            value_vars=['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'],
            var_name='День',
            value_name='Время'
        ).assign(
            Время=lambda df: df['Время'].apply(lambda x: x.split(', ')),
            count=1
        )
        .explode('Время')
        .groupby(['День', 'Время'], as_index=False).count()
        [['День', 'Время', 'count']]
        .sort_values('count', ascending=False)
        .query('Время != ""')
        .assign(perc=lambda df: (100 * df['count']/len_table).round(2))
        .to_markdown(index=False)
    )

    message_text = (
        f'Проголосовало: {len_table}\n\n'
        'Статистика:\n'
        '```Markdown\n'
        f'{table_md}\n'
        '```'
    )
    
    asyncio.run(tg_bot.send_message(
        text=message_text,
        chat_id=message['chat']['id'],
        message_thread_id=message['message_thread_id'] if 'message_thread_id' in message else None,
        parse_mode='MarkdownV2',
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
                    db_save_message(
                        chat_id=message['chat']['id'],
                        user_id=message['from']['id'],
                        message_id=message['message_id'],
                        text=message['text'],
                        thread_id=message['thread_id'] if 'thread_id' in message else -1
                    )

                if 'text' in message and message['from']['id'] != message['chat']['id']:
                    commands = get_commands(message['text'])
                    if message['from']['id'] in get_admins(bot, message['chat']['id'])\
                        and len(commands) == 1\
                        and commands[0] in ADMIN_FUNCTIONS:
                        
                        ADMIN_FUNCTIONS[commands[0]](bot, message)
        # break

ADMIN_FUNCTIONS = {
    'all': tag_all_users,
    'table_statistic': datettime_table_statistic,
}

if __name__ == '__main__':
    TOKEN = get_token()    
    bot = TgApi(TOKEN)
    db_create_table_users()
    db_create_table_messages()
    main()
