import telebot
from telebot import types
import psycopg2
import boto3
import uuid
import botocore
import re
from vars import *


text_messages = {
    'welcome':
        'Привет {}!\n\n'
        'Ты можешь загружать сюда фотографии и картинки (картинки сможет увидеть любой)\n'
        'А командой /random ты получишь рандомную картинку из всех загруженных картинок.',

    'info':
        'Я - домашнее задание по курсу облачные вычисления.\n'
        'Выполнили: Иванов Александр и другие.',

    'saved':
        'Картинка сохранена.',

    'random':
        'Держи рандомную картинку..',

    'history':
        'Было загружено {} фото. Выбери количество фото, которые нужно показать',
}

bot = telebot.AsyncTeleBot(BOT_TOKEN)


@bot.message_handler(commands=['start'])
def on_start(message):
    keyboard = types.InlineKeyboardMarkup()
    import_button = types.InlineKeyboardButton(text='Посмотреть рандомную картинку', callback_data='random')
    history_button = types.InlineKeyboardButton(text='Посмотреть свои картинки', callback_data='/history')

    keyboard.add(import_button)
    keyboard.add(history_button)

    bot.send_message(message.chat.id,
                     text_messages['welcome'].format(message.from_user.first_name),
                     reply_markup=keyboard)


@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)

    downloaded_file = bot.download_file(file_info.file_path)
    db.insert(wh.insert(downloaded_file), message.chat.id)
    bot.reply_to(message, text_messages['saved'])



@bot.callback_query_handler(func=lambda reply: bool(re.search('random', reply.data)))
def get_random_photo_reply(reply):
    bot.send_message(chat_id=reply.message.chat.id, text=text_messages['random'])
    bot.send_photo(chat_id=reply.message.chat.id, photo=(wh.read(db.random_pic()[0][0])['Body']))


@bot.message_handler(commands=['random'])
def get_random_photo(message):
    bot.send_message(chat_id=message.chat.id, text=text_messages['random'])
    bot.send_photo(chat_id=message.chat.id, photo=(wh.read(db.random_pic()[0][0])['Body']))


@bot.message_handler(commands=['history'])
def get_history(message):
    pics = db.user_pics(message.chat.id)
    bot.send_message(message.chat.id, text_messages['history'].format(len(pics)),
                     reply_markup=history_pages_keyboard(0, 3, pics))


@bot.message_handler(regexp='history_(\d+)')
@bot.callback_query_handler(func=lambda reply: bool(re.search('history_(\d+)', reply.data)))
def get_history_photo(message):
    text = message.text
    pics = db.user_pics(message.chat.id)

    bot.send_photo(chat_id=message.chat.id,
                   photo=(wh.read(pics[int(re.search('(\d+)', text).group(0))][0])['Body']))


@bot.callback_query_handler(func=lambda reply: bool(re.search('history_(\d+)', reply.data)))
def get_history_photo(reply):
    text = reply.data
    pics = db.user_pics(reply.message.chat.id)

    bot.send_photo(chat_id=reply.message.chat.id,
                   photo=(wh.read(pics[int(re.search('(\d+)', text).group(0))][0])['Body']))


@bot.callback_query_handler(func=lambda reply: bool(re.search('to_(\d+)', reply.data)))
def get_history_photos(reply):
    text = reply.data
    pics = db.user_pics(reply.message.chat.id)

    page = int(re.search('(\d+)', text).group(0))
    bot.edit_message_text(chat_id=reply.message.chat.id, message_id=reply.message.message_id,
                          text=text_messages['history'].format(len(pics)),
                          reply_markup=history_pages_keyboard(page, page+3, pics))


class DB:
    def __init__(self,
                 dsn_hostname=HOSTNAME,
                 dsn_port=PORT,
                 dsn_database=DATABASE,
                 dsn_uid=UID,
                 dsn_pwd=PWD):

        self.hostname = HOSTNAME
        self.port = PORT
        self.database = DATABASE
        self.uid = UID
        self.pwd = PWD

    def insert(self, name_id, chat_id):
        conn = psycopg2.connect(dbname=self.database,
                                host=self.hostname,
                                port=self.port,
                                user=self.uid,
                                password=self.pwd)
        cur = conn.cursor()
        cur.execute("INSERT INTO pics (pic_id, chat_id) VALUES (%s, %s)", (str(name_id), chat_id))
        conn.commit()
        cur.close()
        conn.close()

    def user_pics(self, chat_id):
        conn = psycopg2.connect(dbname=self.database,
                                host=self.hostname,
                                port=self.port,
                                user=self.uid,
                                password=self.pwd)
        cur = conn.cursor()
        cur.execute('SELECT *, ROW_NUMBER() OVER () as rn FROM pics where chat_id={};'.format(chat_id))
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return rows

    def random_pic(self):
        conn = psycopg2.connect(dbname=self.database,
                                host=self.hostname,
                                port=self.port,
                                user=self.uid,
                                password=self.pwd)
        cur = conn.cursor()
        cur.execute('SELECT * FROM pics ORDER BY random() LIMIT 1;')
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return rows

    def create_table(self):
        conn = psycopg2.connect(dbname=self.database,
                                host=self.hostname,
                                port=self.port,
                                user=self.uid,
                                password=self.pwd)
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS pics (pic_id UUID PRIMARY KEY, chat_id BIGINT NOT NULL);')
        conn.commit()
        cur.close()
        conn.close()


class S3:
    def __init__(self, access_key=ACCESS_KEY, secret_key=SECRET_KEY, bucket_name=BUCKET_NAME):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        self.bucket_name = bucket_name

    def insert(self, filename):
        name_id = uuid.uuid1()
        self.s3.put_object(Bucket=self.bucket_name,
                           Key=str(name_id) + '.jpg',
                           Body=filename)

        return name_id

    def read(self, filename):
        try:
            return self.s3.get_object(Bucket=self.bucket_name, Key=filename+'.jpg')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise


def history_pages_keyboard(start, stop, pics):
    check = lambda x: 0 if x < 0 else x
    keyboard = types.InlineKeyboardMarkup()
    if (stop < len(pics)) & (start > 0):
        for i in range(start+1, stop+1):
            keyboard.add(types.InlineKeyboardButton(text=str(i), callback_data='history_{}'.format(i)))
        keyboard.add(types.InlineKeyboardButton(text='⬅',
                                                callback_data='to_{}'.format(check(start-3))))
        keyboard.add(types.InlineKeyboardButton(text='➡',
                                                callback_data='to_{}'.format(stop)))
    elif (stop < len(pics)) & (start <= 0):
        for i in range(start+1, stop + 1):
            keyboard.add(types.InlineKeyboardButton(text=str(i),
                                                    callback_data='history_{}'.format(i)))
        keyboard.add(types.InlineKeyboardButton(text='➡',
                                                callback_data='to_{}'.format(stop)))

    elif (stop >= len(pics)) & (start > 0):
        for i in range(start+1, len(pics)+1):
            keyboard.add(types.InlineKeyboardButton(text=str(i),
                                                    callback_data='history_{}'.format(i)))
        keyboard.add(types.InlineKeyboardButton(text='⬅',
                                                callback_data='to_{}'.format(check(start-3))))
    elif (stop >= len(pics)) & (start <= 0):
        for i in range(start, len(pics)+1):
            keyboard.add(types.InlineKeyboardButton(text=str(i), callback_data='history_{}'.format(i)))

    return keyboard


wh = S3()
db = DB()
bot.polling()

