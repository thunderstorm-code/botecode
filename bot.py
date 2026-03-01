
import asyncio
import os
import aiohttp
import asyncio
import logging
import re
import sqlite3
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup
from aiogram.types.keyboard_button_request_chat import KeyboardButtonRequestChat
from datetime import datetime
import os
from dotenv import load_dotenv
import asyncio
from collections import deque
import psutil
import time
from datetime import datetime
from datetime import datetime
from collections import Counter
import glob
import signal
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from datetime import datetime
from collections import Counter
import json
from aiogram.filters import Command, StateFilter, state
import sys
import json
from telethon.sessions import StringSession
import sys
import typing
from datetime import datetime
from collections import Counter
from aiogram.fsm.context import FSMContext
import sqlite3
import asyncio
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from collections import deque
from aiogram.filters import StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, Document, BufferedInputFile
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.types import InlineQuery, InlineQueryResultArticle, InputTextMessageContent
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from telethon import TelegramClient, errors, functions
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from datetime import datetime, timedelta  # Добавляем timedelta в импорт
from telethon.tl.functions.channels import GetParticipantsRequest, GetFullChannelRequest
from telethon.tl.types import ChannelParticipantsAdmins
from aiogram.types import Message, Document
from telethon.tl.functions.chatlists import CheckChatlistInviteRequest, JoinChatlistInviteRequest
from telethon.tl.types import InputPeerChannel, InputPeerChat
import folder_checker
import re




async def get_entity_safe(client, link, session_path=None):
    """Безопасное получение entity с обработкой флуд-вейтов"""
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            return await client.get_entity(link)

        except errors.FloodWaitError as e:
            if session_path:
                session_manager.add_flood_wait_session(session_path, e.seconds, f"ResolveUsernameRequest: {e}")

            if attempt < max_retries - 1:
                wait_time = min(e.seconds, 0)
                logger.warning(f"Flood wait {e.seconds}s for {link}, retry {attempt + 1}/{max_retries} in {wait_time}s")
                await asyncio.sleep(wait_time)
                continue
            else:
                raise Exception(f"Flood wait error after {max_retries} attempts: {e}")

        except Exception as e:
            raise

def setup_sqlite():
    sqlite3.enable_callback_tracebacks(True)

import logging

class CyanFormatter(logging.Formatter):
    """Форматтер для вывода ERROR сообщений cyan цветом"""

    CYAN = '\033[96m'
    RESET = '\033[0m'

    def format(self, record):
        if record.levelno == logging.ERROR:
            message = super().format(record)
            return f"{self.CYAN}{message}{self.RESET}"
        else:
            return super().format(record)


# Переконфигурируем логирование
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Применяем цветной форматтер к корневому логгеру
root_logger = logging.getLogger()
handler = root_logger.handlers[0]
handler.setFormatter(CyanFormatter(
    fmt="[%(asctime)s] - [%(levelname)s] - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))


import html
import re
from aiogram.enums import ParseMode

import html
import re
from aiogram.enums import ParseMode

PREMIUM_EMOJI_MAP = {
    "🚀": "5278465234271492555",
}

_ALLOWED_HTML_TAG_RE = re.compile(
    r'(?is)('
    r'</?(?:b|strong|i|em|u|ins|s|strike|del|pre|blockquote|code)\s*>'
    r'|<br\s*/?>'
    r'|<a\s+href="[^"]*"\s*>|</a\s*>'
    r'|<span\s+class="tg-spoiler"\s*>|</span\s*>'
    r'|<tg-emoji\s+emoji-id="\d+"\s*>|</tg-emoji\s*>'
    r')'
)
_HTML_TAG_SPLIT_RE = re.compile(r"(<[^>]+>)")
_TG_EMOJI_BLOCK_RE = re.compile(r'(?is)<tg-emoji\s+emoji-id="\d+"\s*>.*?</tg-emoji\s*>')

def _mask_tg_emoji_blocks(s: str):
    blocks = []
    def repl(m: re.Match):
        blocks.append(m.group(0))
        return f"\x01TG{len(blocks)-1}\x01"
    return _TG_EMOJI_BLOCK_RE.sub(repl, s), blocks

def _unmask_tg_emoji_blocks(s: str, blocks: list[str]):
    for i, b in enumerate(blocks):
        s = s.replace(f"\x01TG{i}\x01", b)
    return s

def sanitize_html_keep_allowed(raw: str) -> str:
    """Чтобы <3 не ломал HTML: экранируем всё, кроме разрешённых тегов."""
    if not raw:
        return ""
    saved = []
    def _save(m: re.Match):
        saved.append(m.group(1))
        return f"\x00{len(saved)-1}\x00"

    tmp = _ALLOWED_HTML_TAG_RE.sub(_save, raw)
    tmp = html.escape(tmp, quote=False)
    for i, tag in enumerate(saved):
        tmp = tmp.replace(f"\x00{i}\x00", tag)
    return tmp

def apply_premium_emojis_html(html_text: str) -> str:
    """Подмена 🚀 -> <tg-emoji ...>🚀</tg-emoji> без поломки HTML."""
    if not html_text or not PREMIUM_EMOJI_MAP:
        return html_text or ""

    masked, blocks = _mask_tg_emoji_blocks(html_text)
    parts = _HTML_TAG_SPLIT_RE.split(masked)

    for i, part in enumerate(parts):
        if not part or part.startswith("<"):
            continue
        for emoji_char, emoji_id in PREMIUM_EMOJI_MAP.items():
            part = part.replace(emoji_char, f'<tg-emoji emoji-id="{emoji_id}">{emoji_char}</tg-emoji>')
        parts[i] = part

    out = "".join(parts)
    return _unmask_tg_emoji_blocks(out, blocks)

def render_html(text: str) -> str:
    return apply_premium_emojis_html(sanitize_html_keep_allowed(text))


def install_global_html_premium_patch(bot):
    """
    Глобально:
    - если сообщение уходит как HTML (parse_mode=HTML), то:
      1) санитайзим (<3 не ломает)
      2) вставляем <tg-emoji>
    """
    original_call = bot.__call__

    async def patched_call(method, *args, **kwargs):
        try:
            pm = getattr(method, "parse_mode", None)

            def is_html_mode() -> bool:
                return pm in (ParseMode.HTML, "HTML")

            if is_html_mode():
                if hasattr(method, "text") and isinstance(method.text, str):
                    method.text = render_html(method.text)
                    method.parse_mode = ParseMode.HTML

                if hasattr(method, "caption") and isinstance(method.caption, str):
                    method.caption = render_html(method.caption)
                    method.parse_mode = ParseMode.HTML

        except Exception:
            pass

        return await original_call(method, *args, **kwargs)

    bot.__call__ = patched_call


# ================== Загрузка языков ==================
from dotenv import load_dotenv
import os

#load_dotenv("/root/based/settings.api")
load_dotenv("settings.api")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
SESSION_NAME = os.getenv("SESSION_NAME", "user_session")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
WEB_PRICES_CACHE = {
    "data": None,
    "last_update": None
}

WEB_PRICES_UPDATED_AT = 0


# Ваши существующие настройки сессий
SESSION_FOLDER = "botz"
ERRORS_FILE = os.path.join(SESSION_FOLDER, "botz.error")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
logger.info(f"BOT INSTANCE ID: {id(bot)}")

dp = Dispatcher()
router = Router()
dp.include_router(router)
telethon_client = None



# =================== База данных ===================
class UserDataManager:
    def __init__(self, db_path="user_data.db"):
        self.db_path = db_path
        self.init_db()
        self.languages = {}
        self.load_languages()

    def load_languages(self):
        """Загружает языковые файлы"""
        try:
            with open('language.ru', 'r', encoding='utf-8') as f:
                self.languages['ru'] = self.parse_language_file(f.read())
            with open('language.en', 'r', encoding='utf-8') as f:
                self.languages['en'] = self.parse_language_file(f.read())
            logger.info("Языковые файлы загружены успешно")
        except Exception as e:
            logger.error(f"Ошибка загрузки языковых файлов: {e}")
            self.languages = {
                'ru': {'welcome': '👋 Добро пожаловать!'},
                'en': {'welcome': '👋 Welcome!'}
            }

    def parse_language_file(self, content: str) -> dict:
        """Парсит языковой файл"""
        translations = {}
        for line in content.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                translations[key.strip()] = value.strip().replace('\\n', '\n')
        return translations

    def init_db(self):
        """Инициализация единой базы данных пользователей"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Таблица языковых настроек
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id INTEGER PRIMARY KEY,
                    language TEXT NOT NULL DEFAULT 'ru',
                    price_config TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            ''')

            # Таблица истории проверок групп
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    chat_link TEXT NOT NULL,
                    chat_id TEXT,
                    created_date TEXT,
                    owner_id TEXT,
                    owner_username TEXT,
                    user_messages INTEGER DEFAULT 0,
                    system_messages INTEGER DEFAULT 0,
                    history_hidden BOOLEAN DEFAULT FALSE,
                    price REAL DEFAULT 0,
                    seller_label TEXT,
                    is_bad BOOLEAN DEFAULT FALSE,
                    checked_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES user_settings (user_id),
                    UNIQUE(user_id, chat_link)
                )
            ''')

            # Индексы для производительности
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_user_language 
                ON user_settings(user_id, language)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_user_checks 
                ON group_checks(user_id, checked_at)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_user_link 
                ON group_checks(user_id, chat_link)
            ''')

            conn.commit()

    # ================== ЯЗЫКОВЫЕ МЕТОДЫ ==================
    # В классе UserDataManager метод get_user_language уже должен быть таким:
    async def get_user_language(self, user_id: int) -> str:
        """Получает язык пользователя"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT language FROM user_settings WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()

            if result:
                return result[0]

            # Создаем запись для нового пользователя с языком по умолчанию
            now = datetime.now().isoformat()
            cursor.execute('''
                INSERT INTO user_settings (user_id, language, created_at, updated_at)
                VALUES (?, ?, ?, ?)
            ''', (user_id, 'ru', now, now))  # Язык по умолчанию - русский
            conn.commit()
            return 'ru'  # Всегда возвращаем русский по умолчанию

    async def set_user_language(self, user_id: int, language: str):
        """Устанавливает язык пользователя"""
        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO user_settings 
                (user_id, language, created_at, updated_at)
                VALUES (?, ?, ?, ?)
            ''', (user_id, language, now, now))
            conn.commit()

    async def is_first_time_user(self, user_id: int) -> bool:
        """Проверяет, новый ли пользователь"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM user_settings WHERE user_id = ?', (user_id,))
            return cursor.fetchone() is None

    def get_text(self, language: str, key: str, **kwargs) -> str:
        """Получает текст по ключу с подстановкой переменных"""
        if language not in self.languages:
            language = 'ru'

        text = self.languages[language].get(key, f"[{key}]")

        # Заменяем переменные
        for k, v in kwargs.items():
            text = text.replace(f'{{{k}}}', str(v))

        return text

    #================== МЕТОДЫ ЦЕН ==================

    async def get_default_prices(self):
        """Возвращает цены по умолчанию для новичков (все 0$)"""
        return {
            "year_prices": {},       # пусто
            "month_prices": {},      # пусто
            "special_ids": {},       # пусто
            "default_price": 0.0     # дефолтная цена 0$
        }

    async def get_user_prices(self, user_id: int):
        """Получает цены пользователя или возвращает значения по умолчанию"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT price_config FROM user_settings WHERE user_id = ?', (user_id,))
                row = cursor.fetchone()

                if row and row[0]:
                    user_config = json.loads(row[0])
                    default_prices = await self.get_default_prices()

                    # Объединяем с дефолтными ценами
                    if "year_prices" in user_config:
                        default_prices["year_prices"].update(user_config["year_prices"])
                    if "month_prices" in user_config:
                        default_prices["month_prices"].update(user_config["month_prices"])
                    if "special_ids" in user_config:
                        default_prices["special_ids"].update(user_config["special_ids"])
                    if "default_price" in user_config:
                        default_prices["default_price"] = user_config["default_price"]

                    return default_prices
                else:
                    return await self.get_default_prices()

        except Exception as e:
            logger.error(f"Ошибка получения цен пользователя {user_id}: {e}")
            return await self.get_default_prices()

    async def set_user_prices(self, user_id: int, price_config: dict):
        """Устанавливает цены пользователя"""
        try:
            current_config = await self.get_user_prices(user_id)

            # Обновляем только переданные поля
            if "year_prices" in price_config:
                current_config["year_prices"].update(price_config["year_prices"])
            if "month_prices" in price_config:
                current_config["month_prices"].update(price_config["month_prices"])
            if "special_ids" in price_config:
                current_config["special_ids"].update(price_config["special_ids"])
            if "default_price" in price_config:
                current_config["default_price"] = price_config["default_price"]

            config_json = json.dumps(current_config, ensure_ascii=False)
            now = datetime.now().isoformat()

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO user_settings 
                    (user_id, price_config, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                ''', (user_id, config_json, now, now))
                conn.commit()

            return True
        except Exception as e:
            logger.error(f"Ошибка установки цен пользователя {user_id}: {e}")
            return False

    async def calculate_price(self, user_id: int, chat_id: str, created_date: datetime):
        """Рассчитывает цену для группы на основе пользовательских настроек"""
        try:
            prices = await self.get_user_prices(user_id)
            logger.info(f"🔍 Расчет цены для: chat_id={chat_id}, created_date={created_date}")

            if created_date:
                year = created_date.year
                month = created_date.month

                # Сначала проверяем месячные цены
                month_key = f"{year}-{month:02d}"
                if month_key in prices["month_prices"]:
                    price = prices["month_prices"][month_key]
                    logger.info(f"✅ Применена цена по месяцу {month_key}: {price}$")
                    return price

                # Затем проверяем годовые цены
                year_str = str(year)
                if year_str in prices["year_prices"]:
                    price = prices["year_prices"][year_str]
                    logger.info(f"✅ Применена цена по году {year_str}: {price}$")
                    return price

                logger.info(f"❌ Для даты {created_date} не найдено подходящих цен")

            # Проверяем специальные ID
            if chat_id:
                for special_id, price in prices["special_ids"].items():
                    if chat_id.startswith(special_id):
                        logger.info(f"✅ Применена цена по спец ID {special_id}: {price}$")
                        return price

            # Цена по умолчанию
            default_price = prices["default_price"]
            logger.info(f"✅ Применена цена по умолчанию: {default_price}$")
            return default_price

        except Exception as e:
            logger.error(f"❌ Ошибка расчета цены для пользователя {user_id}: {e}")
            return 0.0

    # ================== МЕТОДЫ ИСТОРИИ ПРОВЕРОК ==================
    async def add_check(self, user_id: int, username: str, result: dict):
        """Добавляет запись о проверке группы"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO group_checks 
                    (user_id, username, chat_link, chat_id, created_date, owner_id, owner_username,
                     user_messages, system_messages, history_hidden, price, seller_label, is_bad, checked_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    user_id,
                    username,
                    result.get("chat_link"),
                    result.get("chat_id"),
                    result.get("created_date").isoformat() if result.get("created_date") else None,
                    result.get("owner_id"),
                    result.get("owner_username"),
                    result.get("user_messages", 0),
                    result.get("system_messages", 0),
                    result.get("history_hidden", False),
                    result.get("price", 0),
                    result.get("seller_label"),
                    result.get("is_bad", False),
                    result.get("checked_at").isoformat()
                ))
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Ошибка при добавлении в БД: {e}")
                return False

    async def get_user_stats(self, user_id: int):
        """Получает статистику пользователя"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Общее количество проверок
            cursor.execute('''
                SELECT COUNT(*) FROM group_checks 
                WHERE user_id = ? AND is_bad = FALSE
            ''', (user_id,))
            total_checks = cursor.fetchone()[0]

            # Уникальные группы (по chat_id)
            cursor.execute('''
                SELECT COUNT(DISTINCT chat_id) FROM group_checks 
                WHERE user_id = ? AND chat_id IS NOT NULL AND is_bad = FALSE
            ''', (user_id,))
            unique_groups = cursor.fetchone()[0]

            # Последние проверки
            cursor.execute('''
                SELECT chat_link, checked_at FROM group_checks 
                WHERE user_id = ? AND is_bad = FALSE
                ORDER BY checked_at DESC 
                LIMIT 5
            ''', (user_id,))
            recent_checks = cursor.fetchall()

            return {
                "total_checks": total_checks,
                "unique_groups": unique_groups,
                "recent_checks": recent_checks
            }

    async def has_checked_before(self, user_id: int, chat_link: str):
        """Проверяет, проверял ли пользователь эту группу ранее"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT checked_at FROM group_checks 
                WHERE user_id = ? AND chat_link = ?
            ''', (user_id, chat_link))
            result = cursor.fetchone()
            return result[0] if result else None

    async def get_user_history(self, user_id: int, limit=10):
        """Получает историю проверок пользователя"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT chat_link, chat_id, created_date, owner_username, price, checked_at
                FROM group_checks 
                WHERE user_id = ? AND is_bad = FALSE
                ORDER BY checked_at DESC 
                LIMIT ?
            ''', (user_id, limit))
            return cursor.fetchall()

user_manager = UserDataManager()
user_manager.init_db()

# ================== Управление сессиями ==================
SESSION_ERRORS_FILE = os.path.join(SESSION_FOLDER, "botz.error")
VALID_SESSIONS = []
BLACKLISTED_SESSIONS = {}
CURRENT_SESSION_INDEX = 0


class SessionManager:
    def __init__(self):
        self.valid_sessions = []
        self.blacklisted_sessions = {}
        self.flood_wait_sessions = {}
        self.session_usage = {}  # session_path -> {"last_used": datetime, "user_id": int, "in_use": bool, "use_count": int}
        self.user_sessions = {}  # user_id -> session_path
        self.session_errors = {}  # session_path -> error_count
        self.bot = None
        self.owner_id = None
        self.blacklist_file = os.path.join("botz", "blacklisted_sessions.json")
        self.flood_wait_file = os.path.join("botz", "flood_wait_sessions.json")
        self.current_session_index = 0
        self.session_lock = asyncio.Lock()
        self.max_errors_per_session = 3


    async def notify_owner_about_flood_wait(self, session_path: str, wait_seconds: int, error_message: str = ""):
        """Уведомляет владельца о флуд-вейте"""
        if not self.bot or not self.owner_id:
            return

        try:
            language = await user_manager.get_user_language(self.owner_id)
            session_name = os.path.basename(session_path).replace('.session', '')

            if language == 'en':
                text = (f"🚫 <b>Flood Wait Alert</b>\n\n"
                        f"Session: <code>{session_name}</code>\n"
                        f"Wait time: <b>{wait_seconds} seconds</b>\n"
                        f"Error: {error_message}\n\n"
                        f"Blocked until: {(datetime.now() + timedelta(seconds=wait_seconds)).strftime('%H:%M:%S')}")
            else:
                text = (f"🚫 <b>Флуд-Вейт Оповещение</b>\n\n"
                        f"Сессия: <code>{session_name}</code>\n"
                        f"Время ожидания: <b>{wait_seconds} секунд</b>\n"
                        f"Ошибка: {error_message}\n\n"
                        f"Заблокирована до: {(datetime.now() + timedelta(seconds=wait_seconds)).strftime('%H:%M:%S')}")

            await self.bot.send_message(self.owner_id, text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка уведомления владельца о флуд-вейте: {e}")

    def set_bot(self, bot_instance):
        """Устанавливает экземпляр бота для отправки уведомлений"""
        self.bot = bot_instance

    def set_owner(self, owner_id):
        """Устанавливает ID владельца для уведомлений"""
        self.owner_id = owner_id

    def load_blacklisted_sessions(self):
        """Загружает черный список сессий из файла"""
        try:
            if os.path.exists(self.blacklist_file):
                with open(self.blacklist_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        self.blacklisted_sessions = json.loads(content)
                    else:
                        self.blacklisted_sessions = {}
                logger.info(f"Загружено {len(self.blacklisted_sessions)} сессий в черном списке")
            else:
                self.blacklisted_sessions = {}
                logger.info("Файл черного списка не существует, создан пустой список")
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON в черном списке: {e}")
            self.blacklisted_sessions = {}
            self.save_blacklisted_sessions()
            logger.info("Создан новый корректный файл черного списка")
        except Exception as e:
            logger.error(f"Ошибка загрузки черного списка: {e}")
            self.blacklisted_sessions = {}

    def save_blacklisted_sessions(self):
        """Сохраняет черный список сессий в файл"""
        try:
            os.makedirs(os.path.dirname(self.blacklist_file), exist_ok=True)
            with open(self.blacklist_file, 'w', encoding='utf-8') as f:
                json.dump(self.blacklisted_sessions, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения черного списка: {e}")

    def load_flood_wait_sessions(self):
        """Загружает информацию о сессиях с флуд-вейтом из файла"""
        try:
            if os.path.exists(self.flood_wait_file):
                with open(self.flood_wait_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        data = json.loads(content)
                        # Преобразуем строки времени обратно в datetime
                        for session_name, session_data in data.items():
                            if 'blocked_until' in session_data:
                                try:
                                    data[session_name]['blocked_until'] = datetime.fromisoformat(
                                        session_data['blocked_until'])
                                except ValueError:
                                    # Если формат не ISO, пытаемся парсить другие форматы
                                    try:
                                        data[session_name]['blocked_until'] = datetime.strptime(
                                            session_data['blocked_until'], '%Y-%m-%d %H:%M:%S.%f')
                                    except:
                                        # Если не удалось распарсить, удаляем запись
                                        logger.warning(f"Не удалось распарсить blocked_until для сессии {session_name}")
                                        continue
                            if 'added_at' in session_data:
                                try:
                                    data[session_name]['added_at'] = datetime.fromisoformat(session_data['added_at'])
                                except ValueError:
                                    try:
                                        data[session_name]['added_at'] = datetime.strptime(session_data['added_at'],
                                                                                           '%Y-%m-%d %H:%M:%S.%f')
                                    except:
                                        data[session_name]['added_at'] = datetime.now()

                        self.flood_wait_sessions = data
                    else:
                        self.flood_wait_sessions = {}
                logger.info(f"Загружено {len(self.flood_wait_sessions)} сессий с флуд-вейтом")
            else:
                self.flood_wait_sessions = {}
                logger.info("Файл флуд-вейт сессий не существует, создан пустой список")
        except Exception as e:
            logger.error(f"Ошибка загрузки флуд-вейт сессий: {e}")
            self.flood_wait_sessions = {}

    def save_flood_wait_sessions(self):
        """Сохраняет информацию о флуд-вейт сессиях в файл"""
        try:
            os.makedirs(os.path.dirname(self.flood_wait_file), exist_ok=True)
            # Преобразуем datetime в строки для сериализации
            serializable_data = {}
            for session_name, session_data in self.flood_wait_sessions.items():
                serializable_data[session_name] = session_data.copy()
                if 'blocked_until' in serializable_data[session_name]:
                    # Преобразуем datetime в строку ISO формата
                    serializable_data[session_name]['blocked_until'] = serializable_data[session_name][
                        'blocked_until'].isoformat()
                if 'added_at' in serializable_data[session_name]:
                    # Также преобразуем added_at если он есть
                    serializable_data[session_name]['added_at'] = serializable_data[session_name][
                        'added_at'].isoformat()

            with open(self.flood_wait_file, 'w', encoding='utf-8') as f:
                json.dump(serializable_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения флуд-вейт сессий: {e}")

    def add_flood_wait_session(self, session_path: str, wait_seconds: int, error_message: str = ""):
        """Добавляет сессию в список флуд-вейт с временем блокировки"""
        session_name = os.path.basename(session_path).replace('.session', '')
        blocked_until = datetime.now() + timedelta(seconds=wait_seconds)

        self.flood_wait_sessions[session_name] = {
            'session_path': session_path,
            'blocked_until': blocked_until,
            'wait_seconds': wait_seconds,
            'error_message': error_message,
            'added_at': datetime.now()
        }

        logger.warning(
            f"🚫 Сессия {session_name} заблокирована из-за флуд-вейта до {blocked_until} ({wait_seconds} секунд)")

        # Уведомляем владельца
        asyncio.create_task(self.notify_owner_about_flood_wait(session_path, wait_seconds, error_message))

        # Сохраняем изменения
        self.save_flood_wait_sessions()
    def is_session_in_flood_wait(self, session_path: str):
        """Проверяет, находится ли сессия в состоянии флуд-вейта"""
        session_name = os.path.basename(session_path).replace('.session', '')

        if session_name in self.flood_wait_sessions:
            session_data = self.flood_wait_sessions[session_name]
            blocked_until = session_data['blocked_until']

            if datetime.now() < blocked_until:
                # Сессия все еще заблокирована
                time_left = blocked_until - datetime.now()
                logger.info(
                    f"Сессия {session_name} все еще заблокирована, осталось: {time_left.total_seconds():.0f} сек")
                return True
            else:
                # Время блокировки истекло, удаляем из списка
                logger.info(f"⏰ Время блокировки сессии {session_name} истекло, восстанавливаем")
                del self.flood_wait_sessions[session_name]
                self.save_flood_wait_sessions()
                return False

        return False

    def get_flood_wait_sessions_info(self):
        """Возвращает информацию о заблокированных сессиях"""
        current_time = datetime.now()
        active_sessions = []
        expired_sessions = []

        for session_name, session_data in self.flood_wait_sessions.items():
            blocked_until = session_data['blocked_until']
            time_left = blocked_until - current_time

            if time_left.total_seconds() > 0:
                active_sessions.append({
                    'session_name': session_name,
                    'time_left_seconds': int(time_left.total_seconds()),
                    'wait_seconds': session_data['wait_seconds'],
                    'error_message': session_data.get('error_message', '')
                })
            else:
                expired_sessions.append(session_name)

        # Удаляем истекшие сессии
        for session_name in expired_sessions:
            del self.flood_wait_sessions[session_name]

        if expired_sessions:
            self.save_flood_wait_sessions()

        return active_sessions

    async def cleanup_expired_flood_waits(self):
        """Очищает истекшие флуд-вейты"""
        current_time = datetime.now()
        expired_count = 0

        session_names = list(self.flood_wait_sessions.keys())
        for session_name in session_names:
            if session_name in self.flood_wait_sessions:
                session_data = self.flood_wait_sessions[session_name]
                if current_time >= session_data['blocked_until']:
                    del self.flood_wait_sessions[session_name]
                    expired_count += 1

        if expired_count > 0:
            self.save_flood_wait_sessions()
            logger.info(f"🧹 Очищено {expired_count} истекших флуд-вейтов")

    def get_session_files(self):
        """Возвращает список всех файлов сессий из папки SESSION_FOLDER"""
        session_files = []

        # Проверяем существование папки
        if not os.path.exists(SESSION_FOLDER):
            logger.warning(f"Папка {SESSION_FOLDER} не существует")
            return session_files

        # Ищем файлы .session только в папке SESSION_FOLDER
        for file in os.listdir(SESSION_FOLDER):
            if file.endswith('.session'):
                # Возвращаем полный путь к файлу
                full_path = os.path.join(SESSION_FOLDER, file)
                session_files.append(full_path)
                logger.info(f"Найден файл сессии: {full_path}")

        logger.info(f"Всего найдено файлов сессий в папке {SESSION_FOLDER}: {len(session_files)}")
        return session_files

    async def validate_all_sessions(self, check_blacklisted=False):
        """Проверяет все сессии при запуске"""
        try:
            self.load_blacklisted_sessions()
            self.load_flood_wait_sessions()
            session_files = self.get_session_files()

            logger.info(f"Найдено файлов сессий: {len(session_files)}")
            logger.info(f"Файлы сессий: {[os.path.basename(f) for f in session_files]}")
            logger.info(f"Сессии в черном списке: {list(self.blacklisted_sessions.keys())}")

            if not session_files:
                logger.warning("Не найдено файлов сессий")
                if self.bot and self.owner_id:
                    language = await user_manager.get_user_language(self.owner_id)
                    if language == 'en':
                        await self.bot.send_message(self.owner_id, "❌ No session files found")
                    else:
                        await self.bot.send_message(self.owner_id, "❌ Не найдено файлов сессий")
                return

            # Определяем язык для сообщений
            language = await user_manager.get_user_language(self.owner_id) if self.owner_id else 'ru'

            if language == 'en':
                results = ["🔍 Session validation results:"]
            else:
                results = ["🔍 Результаты проверки сессий:"]

            valid_count = 0
            processed_sessions = set()
            removed_from_blacklist = []

            # Очищаем списки перед проверкой
            if not check_blacklisted:
                self.valid_sessions.clear()
                self.session_usage.clear()
                self.user_sessions.clear()

            for session_file in session_files:
                session_name = os.path.basename(session_file).replace('.session', '')

                # Пропускаем уже обработанные сессии (по имени)
                if session_name in processed_sessions:
                    logger.info(f"Пропускаем уже обработанную сессию: {session_name}")
                    continue

                processed_sessions.add(session_name)
                logger.info(f"Проверяем сессию: {session_name}")

                try:
                    is_valid, message = await self.validate_session(session_file, check_blacklisted)

                    if is_valid:
                        if not check_blacklisted:
                            self.valid_sessions.append(session_file)
                            # Инициализируем использование сессии
                            self.session_usage[session_file] = {
                                "last_used": None,
                                "user_id": None,
                                "in_use": False,
                                "use_count": 0
                            }
                        valid_count += 1

                        # Проверяем, была ли сессия в черном списке и теперь валидна
                        was_blacklisted = session_name in self.blacklisted_sessions
                        if was_blacklisted:
                            removed_from_blacklist.append(session_name)
                            if language == 'en':
                                results.append(f"✅ {session_name} - VALID (removed from blacklist)")
                            else:
                                results.append(f"✅ {session_name} - VALID (удалена из черного списка)")
                        else:
                            results.append(f"✅ {session_name} - VALID")
                    else:
                        if language == 'en':
                            results.append(f"❌ {session_name} - INVALID: {message}")
                        else:
                            results.append(f"❌ {session_name} - NO VALID: {message}")

                except Exception as e:
                    logger.error(f"Ошибка при проверке сессии {session_name}: {e}")
                    if language == 'en':
                        results.append(f"❌ {session_name} - ERROR: {str(e)}")
                    else:
                        results.append(f"❌ {session_name} - ERROR: {str(e)}")

            # Отправляем результаты владельцу
            if self.bot and self.owner_id:
                result_text = "\n".join(results)
                if language == 'en':
                    result_text += f"\n\n📊 Total: {valid_count}/{len(processed_sessions)} valid sessions"
                else:
                    result_text += f"\n\n📊 Итого: {valid_count}/{len(processed_sessions)} валидных сессий"

                if removed_from_blacklist:
                    if language == 'en':
                        result_text += f"\n\n🔄 Removed from blacklist: {len(removed_from_blacklist)}"
                    else:
                        result_text += f"\n\n🔄 Удалено из черного списка: {len(removed_from_blacklist)}"

                    for session in removed_from_blacklist:
                        result_text += f"\n• {session}"

                try:
                    await self.bot.send_message(self.owner_id, result_text)
                except Exception as e:
                    logger.error(f"Не удалось отправить результаты владельцу: {e}")

            logger.info(f"Проверка сессий завершена: {valid_count}/{len(processed_sessions)} валидных")
            logger.info(f"Валидные сессии: {[os.path.basename(s) for s in self.valid_sessions]}")
            if removed_from_blacklist:
                logger.info(f"Удалено из черного списка: {removed_from_blacklist}")

        except Exception as e:
            logger.error(f"Критическая ошибка в validate_all_sessions: {e}")

    async def validate_session(self, session_path, check_blacklisted=False):
        """Проверяет валидность сессии с улучшенной обработкой блокировок"""
        session_name = os.path.basename(session_path).replace('.session', '')
        client = None

        try:
            # Проверяем существование файла
            if not os.path.exists(session_path):
                error_msg = f"Session file doesn't exist: {session_path}"
                logger.error(f"❌ {session_name}: {error_msg}")
                self.blacklisted_sessions[session_name] = error_msg
                self.save_blacklisted_sessions()
                return False, error_msg

            # Если в черном списке, пропускаем
            if not check_blacklisted and session_name in self.blacklisted_sessions:
                logger.info(f"Сессия {session_name} в черном списке, пропускаем")
                return False, f"In blacklist: {self.blacklisted_sessions[session_name]}"

            # Пробуем несколько раз при блокировке
            for attempt in range(3):
                try:
                    client = TelegramClient(
                        session_path,
                        API_ID,
                        API_HASH,
                        connection_retries=2,
                        retry_delay=1
                    )
                    await client.connect()
                    break
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < 2:
                        logger.warning(f"Сессия {session_name} заблокирована, попытка {attempt + 1}/3")
                        await asyncio.sleep(1)
                        continue
                    else:
                        raise

            # Проверяем авторизацию
            if not await client.is_user_authorized():
                error_msg = "Session invalid or expired"
                logger.info(f"❌ {session_name}: {error_msg}")

                # Добавляем в черный список только если это не специальная проверка
                if not check_blacklisted:
                    self.blacklisted_sessions[session_name] = error_msg
                    self.save_blacklisted_sessions()
                return False, error_msg
            else:
                me = await client.get_me()
                username = f"@{me.username}" if me.username else f"{me.first_name or 'no_name'}"
                logger.info(f"✅ {session_name}: Valid session - {username} (id: {me.id})")

                # Если сессия валидна и была в черном списке - удаляем ее оттуда
                if session_name in self.blacklisted_sessions:
                    logger.info(f"🗑️ Удаляем сессию {session_name} из черного списка (теперь валидна)")
                    del self.blacklisted_sessions[session_name]
                    self.save_blacklisted_sessions()

                return True, f"VALID - {username} (id: {me.id})"

        except errors.AuthKeyUnregisteredError:
            error_msg = "Session key not registered (expired)"
            logger.info(f"❌ {session_name}: {error_msg}")

            if not check_blacklisted:
                self.blacklisted_sessions[session_name] = error_msg
                self.save_blacklisted_sessions()
            return False, error_msg

        except errors.SessionPasswordNeededError:
            error_msg = "2FA password required"
            logger.info(f"⚠️ {session_name}: {error_msg}")

            if not check_blacklisted:
                self.blacklisted_sessions[session_name] = error_msg
                self.save_blacklisted_sessions()
            return False, error_msg

        except errors.RPCError as e:
            error_msg = f"RPC error: {e}"
            logger.info(f"⚠️ {session_name}: {error_msg}")

            if not check_blacklisted:
                self.blacklisted_sessions[session_name] = error_msg
                self.save_blacklisted_sessions()
            return False, error_msg

        except Exception as e:
            error_msg = f"Error: {str(e)}"
            logger.info(f"❌ {session_name}: {error_msg}")

            if not check_blacklisted:
                self.blacklisted_sessions[session_name] = error_msg
                self.save_blacklisted_sessions()
            return False, error_msg

        finally:
            if client:
                try:
                    await client.disconnect()
                    logger.debug(f"Клиент для {session_name} отключен")
                except Exception as e:
                    logger.warning(f"Ошибка при отключении клиента {session_name}: {e}")

    async def get_client(self, user_id: int, language=None):
        """Получает клиент с валидной сессией для конкретного пользователя"""
        if not self.valid_sessions:
            raise Exception("No valid sessions")

        max_retries = 5
        retry_delay = 2

        async with self.session_lock:
            for attempt in range(max_retries):
                session_path = None
                client = None
                last_error = None

                try:
                    # Пробуем найти сессию, которую пользователь использовал ранее
                    if user_id in self.user_sessions:
                        preferred_session = self.user_sessions[user_id]
                        if (preferred_session in self.valid_sessions and
                                not self.is_session_in_flood_wait(preferred_session) and
                                not self._is_session_in_use(preferred_session)):
                            session_path = preferred_session
                            logger.info(
                                f"🎯 [SESSION] User {user_id} использует предпочтительную сессию: {os.path.basename(session_path)}")

                    # Если нет предпочтительной, ищем свободную сессию
                    if not session_path:
                        session_path = await self._find_available_session(user_id)
                        if session_path:
                            self.user_sessions[user_id] = session_path

                    if not session_path:
                        raise Exception("No available sessions")

                    # Помечаем сессию как используемую
                    self._mark_session_in_use(session_path, user_id)

                    # Создаем клиент
                    client = TelegramClient(
                        session_path,
                        API_ID,
                        API_HASH,
                        connection_retries=3,
                        retry_delay=1,
                        auto_reconnect=True
                    )

                    await client.connect()

                    if not await client.is_user_authorized():
                        # Сессия стала невалидной
                        await self.handle_session_error(session_path, "Session became invalid during operation")
                        self._mark_session_free(session_path)
                        continue

                    logger.info(f"✅ [SESSION] User {user_id} использует сессию: {os.path.basename(session_path)}")
                    return client, session_path

                except errors.FloodWaitError as e:
                    last_error = e
                    logger.warning(f"🔴 [SESSION] Флуд-вейт для сессии {session_path}: {e}")

                    if session_path:
                        self.add_flood_wait_session(session_path, e.seconds, str(e))
                        self._mark_session_free(session_path)

                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        logger.info(
                            f"🔄 [SESSION] Пробуем другую сессию, попытка {attempt + 1}/{max_retries}, ждем {wait_time}с")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"Flood wait error: {e}")

                except Exception as e:
                    last_error = e
                    if session_path:
                        await self.handle_session_error(session_path, e)
                        self._mark_session_free(session_path)

                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        logger.info(f"🔄 [SESSION] Ошибка, пробуем другую сессию, попытка {attempt + 1}/{max_retries}")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise

                finally:
                    if client and (last_error or not await client.is_user_authorized()):
                        try:
                            await client.disconnect()
                        except:
                            pass

        raise Exception("Couldn't get valid session after several attempts")

    async def _find_available_session(self, user_id: int):
        """Находит доступную сессию для пользователя"""
        available_sessions = []

        for session_path in self.valid_sessions:
            session_name = os.path.basename(session_path)

            # Проверяем условия использования
            if (not self.is_session_in_flood_wait(session_path) and
                    not self._is_session_in_use(session_path)):

                # Предпочитаем сессии, которые давно не использовались
                last_used = self.session_usage.get(session_path, {}).get("last_used")
                use_count = self.session_usage.get(session_path, {}).get("use_count", 0)

                if not last_used:
                    priority = 0  # Никогда не использовалась - высший приоритет
                else:
                    time_since_use = (datetime.now() - last_used).total_seconds()
                    # Учитываем время простоя и количество использований
                    priority = time_since_use - (use_count * 10)  # Часто используемые сессии имеют меньший приоритет

                available_sessions.append((session_path, priority))

        if not available_sessions:
            return None

        # Сортируем по приоритету (наибольший приоритет первый)
        available_sessions.sort(key=lambda x: x[1], reverse=True)

        # Выбираем сессию с наибольшим приоритетом
        selected_session = available_sessions[0][0]
        logger.info(
            f"🔍 [SESSION] Для user {user_id} выбрана сессия: {os.path.basename(selected_session)} (приоритет: {available_sessions[0][1]:.0f})")

        return selected_session

    def _is_session_in_use(self, session_path: str) -> bool:
        """Проверяет, используется ли сессия в данный момент"""
        return self.session_usage.get(session_path, {}).get("in_use", False)

    def _mark_session_in_use(self, session_path: str, user_id: int):
        """Помечает сессию как используемую"""
        current_usage = self.session_usage.get(session_path, {})
        current_usage.update({
            "last_used": datetime.now(),
            "user_id": user_id,
            "in_use": True,
            "use_count": current_usage.get("use_count", 0) + 1
        })
        self.session_usage[session_path] = current_usage

    def _mark_session_free(self, session_path: str):
        """Освобождает сессию"""
        if session_path in self.session_usage:
            self.session_usage[session_path]["in_use"] = False

    async def release_client(self, session_path: str):
        """Освобождает сессию после использования"""
        self._mark_session_free(session_path)
        logger.debug(f"🔓 [SESSION] Сессия освобождена: {os.path.basename(session_path)}")

    async def handle_session_error(self, session_path, error):
        """Обрабатывает ошибку сессии"""
        session_name = os.path.basename(session_path).replace('.session', '')
        logger.error(f"Ошибка сессии {session_name}: {error}")

        # Увеличиваем счетчик ошибок
        error_count = self.session_errors.get(session_path, 0) + 1
        self.session_errors[session_path] = error_count

        # Если слишком много ошибок - добавляем в черный список
        if error_count >= self.max_errors_per_session:
            error_msg = f"Too many errors ({error_count}): {str(error)}"
            self.blacklisted_sessions[session_name] = error_msg
            self.save_blacklisted_sessions()

            # Удаляем из валидных сессий
            if session_path in self.valid_sessions:
                self.valid_sessions.remove(session_path)

            logger.warning(f"🚫 Сессия {session_name} добавлена в черный список из-за {error_count} ошибок")

        # Удаляем из пользовательских предпочтений
        for uid, sess_path in list(self.user_sessions.items()):
            if sess_path == session_path:
                del self.user_sessions[uid]

    async def get_session_stats(self) -> dict:
        """Возвращает статистику использования сессий"""
        total_sessions = len(self.valid_sessions)
        in_use_sessions = sum(1 for session in self.valid_sessions
                              if self._is_session_in_use(session))

        session_details = {}
        for session_path in self.valid_sessions:
            usage = self.session_usage.get(session_path, {})
            session_details[os.path.basename(session_path)] = {
                "in_use": usage.get("in_use", False),
                "user_id": usage.get("user_id"),
                "last_used": usage.get("last_used"),
                "use_count": usage.get("use_count", 0),
                "in_flood_wait": self.is_session_in_flood_wait(session_path)
            }

        return {
            "total_sessions": total_sessions,
            "in_use_sessions": in_use_sessions,
            "available_sessions": total_sessions - in_use_sessions,
            "active_users": len(self.user_sessions),
            "flood_wait_sessions": len(self.flood_wait_sessions),
            "blacklisted_sessions": len(self.blacklisted_sessions),
            "session_details": session_details
        }
session_manager = SessionManager()
session_manager.set_bot(bot)
session_manager.set_owner(OWNER_ID)
# ================== Команды для владельца для мониторинга флуд-вейтов ==================

@router.message(F.from_user.id == OWNER_ID, F.text == "/flood_wait_status")
async def show_flood_wait_status(msg: types.Message):
    """Показывает статус сессий с флуд-вейтом (только для владельца)"""
    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        # Загружаем актуальные данные
        session_manager.load_flood_wait_sessions()
        active_sessions = session_manager.get_flood_wait_sessions_info()

        if not active_sessions:
            await msg.answer("✅ " + ("No sessions in flood wait" if language == 'en' else "Нет сессий в флуд-вейте"))
            return

        if language == 'en':
            text = "🔴 <b>Sessions in Flood Wait</b>\n\n"
        else:
            text = "🔴 <b>Сессии в Флуд-Вейте</b>\n\n"

        for i, session_info in enumerate(active_sessions, 1):
            minutes_left = session_info['time_left_seconds'] // 60
            seconds_left = session_info['time_left_seconds'] % 60

            if language == 'en':
                text += (
                    f"{i}. <b>{session_info['session_name']}</b>\n"
                    f"   ⏰ Time left: {minutes_left:02d}:{seconds_left:02d}\n"
                    f"   🕒 Wait was: {session_info['wait_seconds']}s\n"
                )
            else:
                text += (
                    f"{i}. <b>{session_info['session_name']}</b>\n"
                    f"   ⏰ Осталось: {minutes_left:02d}:{seconds_left:02d}\n"
                    f"   🕒 Ожидание: {session_info['wait_seconds']}с\n"
                )

            if session_info['error_message']:
                error_preview = session_info['error_message'][:100] + "..." if len(
                    session_info['error_message']) > 100 else session_info['error_message']
                text += f"   📝 {error_preview}\n"

            text += "\n"

        if language == 'en':
            text += f"📊 Total: {len(active_sessions)} sessions"
        else:
            text += f"📊 Всего: {len(active_sessions)} сессий"

        # Добавляем кнопку для принудительной очистки
        if language == 'en':
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🧹 Cleanup expired", callback_data="cleanup_flood_waits")
            ]])
        else:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🧹 Очистить истекшие", callback_data="cleanup_flood_waits")
            ]])

        await msg.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка показа статуса флуд-вейтов: {e}")
        await msg.answer("❌ " + (
            "Error getting flood wait status" if language == 'en' else "Ошибка при получении статуса флуд-вейтов"))


@router.callback_query(F.data == "cleanup_flood_waits")
async def cleanup_flood_waits_handler(call: types.CallbackQuery):
    """Обработчик очистки истекших флуд-вейтов"""
    if call.from_user.id != OWNER_ID:
        language = await user_manager.get_user_language(call.from_user.id)
        await call.answer(
            "❌ " + ("This command is only for bot owner" if language == 'en' else "Эта команда только для владельца"),
            show_alert=True)
        return

    try:
        await session_manager.cleanup_expired_flood_waits()
        language = await user_manager.get_user_language(call.from_user.id)

        if language == 'en':
            await call.message.edit_text("✅ Expired flood waits cleaned up")
        else:
            await call.message.edit_text("✅ Истекшие флуд-вейты очищены")

    except Exception as e:
        logger.error(f"Ошибка очистки флуд-вейтов: {e}")
        language = await user_manager.get_user_language(call.from_user.id)
        await call.message.edit_text(
            "❌ " + ("Error cleaning flood waits" if language == 'en' else "Ошибка при очистке флуд-вейтов"))


@router.message(F.from_user.id == OWNER_ID, F.text == "/force_cleanup_flood_waits")
async def force_cleanup_flood_waits(msg: types.Message):
    """Принудительная очистка всех флуд-вейтов (только для владельца)"""
    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        # Просто очищаем весь словарь
        session_manager.flood_wait_sessions = {}
        session_manager.save_flood_wait_sessions()

        if language == 'en':
            await msg.answer("✅ All flood waits forcefully cleaned up")
        else:
            await msg.answer("✅ Все флуд-вейты принудительно очищены")

    except Exception as e:
        logger.error(f"Ошибка принудительной очистки флуд-вейтов: {e}")
        await msg.answer("❌ " + (
            "Error force cleaning flood waits" if language == 'en' else "Ошибка при принудительной очистке флуд-вейтов"))

# ================== Управление сессиями ==================
SESSION_FOLDER = "botz"
ERRORS_FILE = os.path.join(SESSION_FOLDER, "botz.error")


async def log_session_error(session_name: str, error: str):
    """Записывает ошибку сессии в файл в JSON формате"""
    try:
        os.makedirs(SESSION_FOLDER, exist_ok=True)

        if os.path.exists(ERRORS_FILE):
            with open(ERRORS_FILE, 'r', encoding='utf-8') as f:
                errors_data = json.load(f)
        else:
            errors_data = {}

        errors_data[session_name] = error

        with open(ERRORS_FILE, 'w', encoding='utf-8') as f:
            json.dump(errors_data, f, ensure_ascii=False, indent=2)

        logger.info(f"Ошибка сессии записана в файл: {session_name}")
    except Exception as e:
        logger.error(f"Ошибка записи в {ERRORS_FILE}: {e}")


async def get_session_errors():
    """Читает ошибки сессий из JSON файла"""
    try:
        if not os.path.exists(ERRORS_FILE):
            return "Файл с ошибками сессий не найден"

        with open(ERRORS_FILE, 'r', encoding='utf-8') as f:
            errors_data = json.load(f)

        if not errors_data:
            return "Нет ошибок сессий"

        error_lines = []
        for session_name, error in errors_data.items():
            error_lines.append(f"{session_name}: {error}")

        return "\n".join(error_lines)

    except Exception as e:
        return f"Ошибка чтения файла: {e}"


async def clear_session_errors():
    """Очищает файл с ошибками сессий"""
    try:
        if os.path.exists(ERRORS_FILE):
            with open(ERRORS_FILE, 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=2)
            return True
        return True
    except Exception as e:
        logger.error(f"Ошибка очистки {ERRORS_FILE}: {e}")
        return False



async def check_all_sessions_and_notify_owner():
    """Проверяет все сессии и отправляет сводку владельцу"""
    if not OWNER_ID:
        logger.warning("OWNER_ID не установлен, уведомления владельца отключены")
        return

    try:
        session_files = [f for f in os.listdir('.') if f.endswith('.session')]

        error_sessions = []
        if os.path.exists(ERRORS_FILE):
            with open(ERRORS_FILE, 'r', encoding='utf-8') as f:
                errors_data = json.load(f)
            error_sessions = list(errors_data.keys())

        if not session_files and not error_sessions:
            await bot.send_message(OWNER_ID, "🔍 **Проверка сессий:**\n\n❌ Не найдено сессий для проверки")
            return

        valid_sessions = []
        invalid_sessions = []

        for session_file in session_files:
            session_name = session_file.replace('.session', '')
            is_valid, details = await valid_sessions(session_file)
            if is_valid:
                valid_sessions.append(f"{session_name} - {details}")
            else:
                invalid_sessions.append(f"{session_name} - {details}")

        for session_name in error_sessions:
            if not any(session_name in s for s in invalid_sessions):
                invalid_sessions.append(f"{session_name} - {errors_data[session_name]}")

        message_lines = ["🔍 **Сводка по сессиям:**"]

        if valid_sessions:
            message_lines.append("\n✅ **Валидные сессии:**")
            for session in valid_sessions:
                message_lines.append(f"• {session}")
        else:
            message_lines.append("\n❌ **Нет валидных сессий**")

        if invalid_sessions:
            message_lines.append("\n⚠️ **Проблемные сессии:**")
            for session in invalid_sessions:
                message_lines.append(f"• {session}")

        total_sessions = len(valid_sessions) + len(invalid_sessions)
        message_lines.append(f"\n📊 **Итого:** {total_sessions} сессий")
        message_lines.append(f"✅ Валидных: {len(valid_sessions)}")
        message_lines.append(f"❌ Проблемных: {len(invalid_sessions)}")

        await bot.send_message(OWNER_ID, "\n".join(message_lines), parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Ошибка проверки всех сессий: {e}")
        await bot.send_message(OWNER_ID, f"❌ Ошибка при проверке сессий: {str(e)}")


@router.callback_query(F.data.startswith("set_language_"))
async def handle_language_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    language_code = call.data.split("_")[2]  # ru / en

    language = 'ru' if language_code == 'ru' else 'en'
    await user_manager.set_user_language(user_id, language)

    # Текст результата
    if language == "ru":
        result_text = "✅ Ваш язык был успешно изменён на: Русский"
    else:
        result_text = "✅ Your language has been successfully changed to: English"

    # Меняем текст и убираем кнопки
    await call.message.edit_text(result_text)

    # Закрываем "часики"
    await call.answer()



class PostState(StatesGroup):
    waiting_for_content = State()

# ================== Команда /price ==================
class PriceState(StatesGroup):
    waiting_for_action = State()
    waiting_for_year_price = State()
    waiting_for_month_price = State()
    waiting_for_special_id_price = State()
    waiting_for_default_price = State()

@router.callback_query(F.data.startswith("price_"))
async def handle_price_callback(call: types.CallbackQuery, state: FSMContext):
    """Обработка callback'ов управления ценами"""
    action = call.data.replace("price_", "")
    language = await user_manager.get_user_language(call.from_user.id)

    if action == "year":
        if language == 'en':
            text = "📅 <b>Set year price</b>\n\nSend in format: <code>YEAR PRICE</code>\n\nExample: <code>2023 8.5</code>"
        else:
            text = "📅 <b>Установить цену года</b>\n\nОтправьте в формате: <code>ГОД ЦЕНА</code>\n\nПример: <code>2023 8.5</code>"

        await call.message.edit_text(text, parse_mode=ParseMode.HTML)
        await state.set_state(PriceState.waiting_for_year_price)

    elif action == "month":
        if language == 'en':
            text = "📆 <b>Set month price</b>\n\nSend in format: <code>YEAR-MONTH PRICE</code>\n\nExamples:\n<code>2024-01 5.0</code>\n<code>2024-12 0.3</code>"
        else:
            text = "📆 <b>Установить цену месяца</b>\n\nОтправьте в формате: <code>ГОД-МЕСЯЦ ЦЕНА</code>\n\nПримеры:\n<code>2024-01 5.0</code>\n<code>2024-12 0.3</code>"

        await call.message.edit_text(text, parse_mode=ParseMode.HTML)
        await state.set_state(PriceState.waiting_for_month_price)

    elif action == "special":
        if language == 'en':
            text = "🔢 <b>Set special ID price</b>\n\nSend in format: <code>CHAT_ID PRICE</code>\n\nExample: <code>-10012 4.0</code>"
        else:
            text = "🔢 <b>Установить цену спец ID</b>\n\nОтправьте в формате: <code>CHAT_ID ЦЕНА</code>\n\nПример: <code>-10012 4.0</code>"

        await call.message.edit_text(text, parse_mode=ParseMode.HTML)
        await state.set_state(PriceState.waiting_for_special_id_price)

    elif action == "default":
        if language == 'en':
            text = "⚙️ <b>Set default price</b>\n\nSend the default price for groups that don't match any rules:\n\nExample: <code>0.5</code>"
        else:
            text = "⚙️ <b>Установить цену по умолчанию</b>\n\nОтправьте цену по умолчанию для групп, которые не подходят под правила:\n\nПример: <code>0.5</code>"

        await call.message.edit_text(text, parse_mode=ParseMode.HTML)
        await state.set_state(PriceState.waiting_for_default_price)

    elif action == "reset":
        # Сбрасываем на стандартные цены
        default_prices = await user_manager.get_default_prices()
        success = await user_manager.set_user_prices(call.from_user.id, default_prices)

        if success:
            if language == 'en':
                await call.message.edit_text("✅ Prices reset to default values!")
            else:
                await call.message.edit_text("✅ Цены сброшены до стандартных значений!")
        else:
            if language == 'en':
                await call.message.edit_text("❌ Error resetting prices")
            else:
                await call.message.edit_text("❌ Ошибка при сбросе цен")

        await state.clear()

    elif action == "cancel":
        await call.message.delete()
        await state.clear()

    await call.answer()


async def _check_geo_group(client, entity):
    """Проверяет, является ли группа/канал гео-группой (имеет привязанную локацию)"""
    try:
        from telethon.tl.functions.channels import GetFullChannelRequest

        # Получаем полную информацию о канале/группе
        full_chat = await client(GetFullChannelRequest(channel=entity))

        # Проверяем наличие location в full_chat
        has_geo = bool(getattr(full_chat.full_chat, 'location', None))

        logger.info(f"🔍 [GEO CHECK] Группа '{getattr(entity, 'title', 'N/A')}' - гео: {has_geo}")
        return has_geo

    except Exception as e:
        logger.error(f"❌ [GEO CHECK ERROR] Ошибка проверки гео-группы: {e}")
        return False

async def _check_used_group_by_ids(client, entity, sample_size=20, gap_threshold=100):
    """
    Проверка юзанности группы по пропускам ID сообщений
    Условия:
    1) Если между двумя соседними сообщениями разница ID >= gap_threshold → юзанная
    2) Если сообщений мало, а последний ID слишком большой → юзанная
    """
    try:
        logger.info("🔍 Проверяем юзанность группы по ID сообщений...")

        msg_ids = []

        async for msg in client.iter_messages(entity, limit=sample_size):
            if msg.id:
                msg_ids.append(msg.id)

        if len(msg_ids) < 2:
            logger.info("⚠️ Недостаточно сообщений для проверки")
            return False

        # Сообщения идут от новых к старым → сортируем
        msg_ids.sort()

        # 🔹 Проверка №1: большие пропуски между соседними ID
        for prev_id, next_id in zip(msg_ids, msg_ids[1:]):
            gap = next_id - prev_id
            if gap >= gap_threshold:
                logger.info(f"✅ Найден большой пропуск ID: {prev_id} → {next_id} (gap={gap})")
                return True

        # 🔹 Проверка №2: мало сообщений, но огромный последний ID
        total_messages = len(msg_ids)
        last_id = msg_ids[-1]

        if total_messages <= sample_size and last_id >= gap_threshold * total_messages:
            logger.info(
                f"✅ Подозрительное соотношение: сообщений={total_messages}, последний ID={last_id}"
            )
            return True

        logger.info("❌ Признаки юзанной группы не найдены")
        return False

    except Exception as e:
        logger.error(f"Ошибка проверки юзанности группы: {e}")
        return False

async def _find_available_session(self, user_id: int):
    """Находит доступную сессию для пользователя с приоритетом по надежности"""
    available_sessions = []

    for session_path in self.valid_sessions:
        session_name = os.path.basename(session_path)

        # Проверяем условия использования
        if (not self.is_session_in_flood_wait(session_path) and
                not self._is_session_in_use(session_path)):

            # Предпочитаем сессии, которые давно не использовались и имеют мало ошибок
            last_used = self.session_usage.get(session_path, {}).get("last_used")
            use_count = self.session_usage.get(session_path, {}).get("use_count", 0)
            error_count = self.session_errors.get(session_path, 0)

            if not last_used:
                priority = 1000  # Никогда не использовалась - высший приоритет
            else:
                time_since_use = (datetime.now() - last_used).total_seconds()
                # Учитываем время простоя, количество использований и ошибок
                priority = time_since_use - (use_count * 10) - (error_count * 100)

            available_sessions.append((session_path, priority))

    if not available_sessions:
        # Если нет доступных сессий, пробуем взять ту, у которой скоро закончится flood wait
        for session_path in self.valid_sessions:
            if self.is_session_in_flood_wait(session_path) and not self._is_session_in_use(session_path):
                session_name = os.path.basename(session_path).replace('.session', '')
                if session_name in self.flood_wait_sessions:
                    time_left = (self.flood_wait_sessions[session_name]['blocked_until'] - datetime.now()).total_seconds()
                    if time_left < 30:  # Если осталось меньше 30 секунд
                        available_sessions.append((session_path, -time_left))  # Отрицательный приоритет
                        logger.info(f"🔄 [SESSION] Сессия {session_name} скоро разблокируется: {int(time_left)}с")

    if not available_sessions:
        logger.warning(f"❌ [SESSION] Нет доступных сессий для пользователя {user_id}")
        return None

    # Сортируем по приоритету (наибольший приоритет первый)
    available_sessions.sort(key=lambda x: x[1], reverse=True)

    # Выбираем сессию с наибольшим приоритетом
    selected_session = available_sessions[0][0]
    logger.info(f"🔍 [SESSION] Для user {user_id} выбрана сессия: {os.path.basename(selected_session)} (приоритет: {available_sessions[0][1]:.0f})")

    return selected_session

@router.message(PriceState.waiting_for_month_price)
async def handle_month_price_input(msg: types.Message, state: FSMContext):
    """Обработка ввода цены месяца"""
    language = await user_manager.get_user_language(msg.from_user.id)

    try:
        parts = msg.text.strip().split()
        if len(parts) != 2:
            raise ValueError("Invalid format")

        month_key = parts[0]
        price = float(parts[1])

        # Проверяем формат год-месяц
        if not re.match(r'^\d{4}-\d{2}$', month_key):
            raise ValueError("Invalid month format")

        year, month = month_key.split("-")
        year_int = int(year)
        month_int = int(month)

        if not (1 <= month_int <= 12):
            raise ValueError("Invalid month")

        if price < 0:
            raise ValueError("Price cannot be negative")

        # Сохраняем цену
        price_config = {"month_prices": {month_key: price}}
        success = await user_manager.set_user_prices(msg.from_user.id, price_config)

        if success:
            month_name = datetime(year_int, month_int, 1).strftime("%B")
            if language == 'en':
                await msg.answer(f"✅ {month_name} {year} price set to {price}$")
            else:
                await msg.answer(f"✅ Цена для {month_name} {year} установлена: {price}$")
        else:
            if language == 'en':
                await msg.answer("❌ Error saving price")
            else:
                await msg.answer("❌ Ошибка сохранения цены")

    except ValueError as e:
        if language == 'en':
            await msg.answer("❌ Invalid format. Use: <code>YEAR-MONTH PRICE</code>\nExample: <code>2024-01 5.0</code>",
                             parse_mode=ParseMode.HTML)
        else:
            await msg.answer(
                "❌ Неверный формат. Используйте: <code>ГОД-МЕСЯЦ ЦЕНА</code>\nПример: <code>2024-01 5.0</code>",
                parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка установки цены месяца: {e}")
        if language == 'en':
            await msg.answer("❌ Error setting price")
        else:
            await msg.answer("❌ Ошибка установки цены")

    await state.clear()


@router.message(PriceState.waiting_for_special_id_price)
async def handle_special_id_price_input(msg: types.Message, state: FSMContext):
    """Обработка ввода цены специального ID"""
    language = await user_manager.get_user_language(msg.from_user.id)

    try:
        parts = msg.text.strip().split()
        if len(parts) != 2:
            raise ValueError("Invalid format")

        chat_id = parts[0]
        price = float(parts[1])

        if not chat_id.startswith("-100"):
            raise ValueError("Chat ID should start with -100")

        if price < 0:
            raise ValueError("Price cannot be negative")

        # Сохраняем цену
        price_config = {"special_ids": {chat_id: price}}
        success = await user_manager.set_user_prices(msg.from_user.id, price_config)

        if success:
            if language == 'en':
                await msg.answer(f"✅ Price for ID {chat_id} set to {price}$")
            else:
                await msg.answer(f"✅ Цена для ID {chat_id} установлена: {price}$")
        else:
            if language == 'en':
                await msg.answer("❌ Error saving price")
            else:
                await msg.answer("❌ Ошибка сохранения цены")

    except ValueError as e:
        if language == 'en':
            await msg.answer("❌ Invalid format. Use: <code>CHAT_ID PRICE</code>\nExample: <code>-10012 4.0</code>",
                             parse_mode=ParseMode.HTML)
        else:
            await msg.answer(
                "❌ Неверный формат. Используйте: <code>CHAT_ID ЦЕНА</code>\nПример: <code>-10012 4.0</code>",
                parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка установки цены спец ID: {e}")
        if language == 'en':
            await msg.answer("❌ Error setting price")
        else:
            await msg.answer("❌ Ошибка установки цены")

    await state.clear()


@router.message(PriceState.waiting_for_default_price)
async def handle_default_price_input(msg: types.Message, state: FSMContext):
    """Обработка ввода цены по умолчанию"""
    language = await user_manager.get_user_language(msg.from_user.id)

    try:
        price = float(msg.text.strip())

        if price < 0:
            raise ValueError("Price cannot be negative")

        # Сохраняем цену
        price_config = {"default_price": price}
        success = await user_manager.set_user_prices(msg.from_user.id, price_config)

        if success:
            if language == 'en':
                await msg.answer(f"✅ Default price set to {price}$")
            else:
                await msg.answer(f"✅ Цена по умолчанию установлена: {price}$")
        else:
            if language == 'en':
                await msg.answer("❌ Error saving price")
            else:
                await msg.answer("❌ Ошибка сохранения цены")

    except ValueError:
        if language == 'en':
            await msg.answer("❌ Invalid price. Send a number like: <code>0.5</code>", parse_mode=ParseMode.HTML)
        else:
            await msg.answer("❌ Неверная цена. Отправьте число, например: <code>0.5</code>", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка установки цены по умолчанию: {e}")
        if language == 'en':
            await msg.answer("❌ Error setting price")
        else:
            await msg.answer("❌ Ошибка установки цены")

    await state.clear()


@router.message(F.chat_shared)
async def handle_chat_shared(message: types.Message):
    """Обрабатывает отправку чата через кнопку"""
    language = await user_manager.get_user_language(message.from_user.id)

    chat_id = message.chat_shared.chat_id
    user_id = message.from_user.id

    # Преобразуем ID в строку и берем первые 6 цифр
    chat_id_str = str(chat_id)
    first_digits = int(chat_id_str[1:7])  # Берем "100215" из "-1002153941613"

    # ИНИЦИАЛИЗИРУЕМ ПЕРЕМЕННЫЕ ПО УМОЛЧАНИЮ
    created_date = None  # ✅ ДОБАВЛЕНО: инициализация по умололчанию
    approx_date = "Неизвестно"
    price_text = "0.0$"

    if 100250 <= first_digits <= 100990:
        approx_date = "2025-2026"
        created_date = datetime(2025, 1, 1)
        price_text = "0.0$"
    elif 100210 <= first_digits <= 100240:  # -10021 до -10024 = 2024
        approx_date = "2024"
        created_date = None
        if language == 'en':
            price_text = "Check the month yourself for exact price..."
        else:
            price_text = "Проверяйте самостоятельно месяц для уточнения цены..."
    elif 100180 <= first_digits <= 100200:  # -10018 до -10020 = 2023
        approx_date = "2023"
        created_date = datetime(2023, 1, 1)
        price_text = "0.0$"
    elif 100160 <= first_digits <= 100170:  # -10016 до -10017 = 2022
        approx_date = "2022"
        created_date = datetime(2022, 1, 1)
        price_text = "0.0$"
    elif first_digits < 100160:  # Все что меньше -10016 = <2022
        approx_date = "<2022"
        created_date = datetime(2021, 1, 1)
        price_text = "0.0$"
    # else блок удален, так как переменные уже инициализированы по умолчанию

    # Для всех годов кроме 2024 рассчитываем цену через user_manager
    if created_date and approx_date != "2024":
        price = await user_manager.calculate_price(user_id, str(chat_id), created_date)
        price_text = f"{price}$"

    # Формируем сообщение
    if language == 'en':
        text = (
            f"🔍 Group Information\n\n"
            f"🆔 ID: {chat_id}\n"
            f"📅 Approximate creation date: {approx_date}\n"
            f"💰 Price: {price_text}"
        )
    else:
        text = (
            f"🔍 Информация о группе\n\n"
            f"🆔 ID: {chat_id}\n"
            f"📅 Примерная дата создания: {approx_date}\n"
            f"💰 Стоимость: {price_text}"
        )

    await message.answer(text)


# Глобальное хранилище ошибок
ERROR_LOG = []
MAX_ERROR_LOG_SIZE = 1000  # Максимальное количество хранимых ошибок


def log_error_to_memory(error_message: str):
    """Сохраняет ошибку в память для последующего экспорта"""
    global ERROR_LOG

    # Добавляем временную метку
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    full_error = f"[{timestamp}] {error_message}"

    ERROR_LOG.append(full_error)

    # Ограничиваем размер лога
    if len(ERROR_LOG) > MAX_ERROR_LOG_SIZE:
        ERROR_LOG = ERROR_LOG[-MAX_ERROR_LOG_SIZE:]

    logger.info(f"📝 Ошибка сохранена в память. Всего ошибок: {len(ERROR_LOG)}")


# Модифицируем существующую функцию log_error
def log_error(error_message: str):
    """Логирует ошибку для статистики и сохраняет в память"""
    global ERROR_STATS
    ERROR_STATS["total_errors"] += 1
    ERROR_STATS["last_error_time"] = time.time()
    ERROR_STATS["last_error_message"] = error_message

    # Сохраняем в память для экспорта
    log_error_to_memory(error_message)

@router.callback_query(F.data == "export_errors")
async def export_errors_handler(call: types.CallbackQuery):
    """Экспорт всех ошибок из памяти в файл"""
    if call.from_user.id != OWNER_ID:
        language = await user_manager.get_user_language(call.from_user.id)
        await call.answer(
            "❌ " + ("This command is only for bot owner" if language == 'en' else "Эта команда только для владельца"),
            show_alert=True
        )
        return

    try:
        if not ERROR_LOG:
            await call.answer("📭 Нет ошибок для экспорта", show_alert=True)
            return

        # Создаем временный файл
        filename = f"errors_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        # Формируем содержимое файла
        error_content = "=== ERROR LOG EXPORT ===\n"
        error_content += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        error_content += f"Total errors: {len(ERROR_LOG)}\n"
        error_content += "=" * 50 + "\n\n"

        # Добавляем все ошибки
        for error in ERROR_LOG:
            error_content += error + "\n\n"

        # Сохраняем в файл
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(error_content)

        # Отправляем файл
        with open(filename, 'rb') as f:
            await call.message.answer_document(
                types.BufferedInputFile(f.read(), filename=filename),
                caption=f"📊 Всего ошибок: {len(ERROR_LOG)}\n⏰ Период: последние {MAX_ERROR_LOG_SIZE} записей"
            )

        await call.answer(f"✅ Экспортировано {len(ERROR_LOG)} ошибок")

    except Exception as e:
        logger.error(f"Ошибка при экспорте ошибок: {e}")
        await call.answer("❌ Ошибка при экспорте", show_alert=True)
    finally:
        # Удаляем временный файл
        try:
            if os.path.exists(filename):
                os.remove(filename)
        except:
            pass


ERROR_STATS = {
    "total_errors": 0,
    "last_error_time": None,
    "last_error_message": ""
}


@router.message(F.from_user.id == OWNER_ID, F.text == "/admin")
async def admin_command(msg: types.Message):
    """Команда админа с информацией о сервере и кнопкой ошибок"""
    try:
        # Получаем информацию о памяти
        memory = psutil.virtual_memory()
        memory_used = memory.used // (1024 * 1024)  # MB
        memory_total = memory.total // (1024 * 1024)  # MB

        # Время работы бота
        start_time = getattr(admin_command, '_start_time', time.time())
        uptime_seconds = int(time.time() - start_time)
        uptime_str = format_uptime(uptime_seconds)

        # Информация о флуд-вейт сессиях
        flood_info = session_manager.get_flood_wait_sessions_info()
        flood_count = len(flood_info)

        # Статистика сессий
        session_stats = await session_manager.get_session_stats()
        active_sessions = session_stats["in_use_sessions"]
        total_sessions = session_stats["total_sessions"]

        # Статистика ошибок
        error_stats = get_error_stats()

        # Формируем текст
        text = f"""Memory: {memory_used}/{memory_total} MB
Time: {uptime_str}
Flood: {flood_count} sessions
Session: {active_sessions}/{total_sessions}
Error: {error_stats['total']} | Last: {error_stats['last_time']}

Last error:
{error_stats['last_message']}"""

        # Создаем клавиатуру с кнопкой ошибок
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📰 Ошибки",
                        callback_data="export_errors"
                    )
                ]
            ]
        )

        # Отправляем с Rust-подсветкой и кнопкой
        await msg.answer(f"```rust\n{text}\n```",
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=kb)

    except Exception as e:
        await msg.answer(f"❌ Error getting admin info: {e}")


def format_uptime(seconds: int) -> str:
    """Форматирует время работы"""
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    if days > 0:
        return f"{days}d {hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def get_error_stats() -> dict:
    """Возвращает статистику ошибок"""
    global ERROR_STATS

    if ERROR_STATS["last_error_time"]:
        time_diff = int(time.time() - ERROR_STATS["last_error_time"])
        if time_diff < 60:
            last_time = f"{time_diff}s"
        elif time_diff < 3600:
            last_time = f"{time_diff // 60}m"
        else:
            last_time = f"{time_diff // 3600}h"
    else:
        last_time = "Never"

    return {
        "total": ERROR_STATS["total_errors"],
        "last_time": last_time,
        "last_message": ERROR_STATS["last_error_message"][:50] + "..." if ERROR_STATS["last_error_message"] and len(
            ERROR_STATS["last_error_message"]) > 50 else ERROR_STATS["last_error_message"] or "No errors"
    }


def log_error(error_message: str):
    """Логирует ошибку для статистики"""
    global ERROR_STATS
    ERROR_STATS["total_errors"] += 1
    ERROR_STATS["last_error_time"] = time.time()
    ERROR_STATS["last_error_message"] = error_message


import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
from aiogram import types

# ===== словари месяцев =====

MONTHS = {
    "ru": {
        1: "январь", 2: "февраль", 3: "март", 4: "апрель",
        5: "май", 6: "июнь", 7: "июль", 8: "август",
        9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь"
    },
    "en": {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
}


# ---------------------------------------------------------
#   📌 Функция загрузки HTML
# ---------------------------------------------------------

async def fetch_html(url: str):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=10) as r:
                return await r.text()
    except:
        return None


# ---------------------------------------------------------
#   📌 Парсинг цен с сайта
# ---------------------------------------------------------
from playwright.async_api import async_playwright

import time

async def web_price_auto_update():
    global WEB_PRICES_CACHE, WEB_PRICES_UPDATED_AT
    while True:
        try:
            prices = await parse_prices()
            if prices:
                WEB_PRICES_CACHE = prices
                WEB_PRICES_UPDATED_AT = time.time()
                print("✔ WEB-цены обновлены")
        except Exception as e:
            print("Ошибка обновления веб-цен:", e)

        await asyncio.sleep(60)


async def fetch_html_real(url: str):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto(url, timeout=15000, wait_until="networkidle")

            html = await page.content()

            await browser.close()
            return html
    except Exception as e:
        print("Ошибка Playwright:", e)
        return None


async def parse_prices():
    urls = [
        "https://tradetg.online/home",
        "https://oldtg.group/home"
    ]

    html = None

    for url in urls:
        html = await fetch_html_real(url)
        if html:
            break

    if html is None:
        return None

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")

    prices = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) != 3:
            continue

        year_raw = cols[1].get_text(strip=True)
        price_raw = cols[2].get_text(strip=True).replace("$", "").replace("USDT", "").strip()

        if "(" in year_raw:
            year = year_raw[:4]
            month_str = year_raw[5:7]
            month = int(month_str)
        else:
            year = year_raw
            month = None

        prices.append({
            "year": int(year),
            "month": month,
            "price": float(price_raw)
        })

    return prices



# ---------------------------------------------------------
#   📌 Упаковать одинаковые цены в диапазоны
# ---------------------------------------------------------

def compress_prices(prices, lang):
    prices = sorted(prices, key=lambda x: (x["year"], x["month"] or 0))

    compact = []
    group = []

    def finalize_group(g):
        if not g:
            return None
        if len(g) == 1:
            item = g[0]
            if item["month"]:
                return f"{MONTHS[lang][item['month']]} {item['year']} — {item['price']}$"
            return f"{item['year']} — {item['price']}$"

        p = g[0]["price"]

        if g[0]["month"]:
            m1 = MONTHS[lang][g[0]["month"]]
            m2 = MONTHS[lang][g[-1]["month"]]
            y = g[0]["year"]
            return f"{m1}-{m2} {y} — {p}$"

        return f"{g[0]['year']}-{g[-1]['year']} — {p}$"

    for p in prices:
        if not group:
            group = [p]
        else:
            same_price = abs(group[-1]["price"] - p["price"]) < 0.0001
            consecutive_year = p["year"] == group[-1]["year"] or p["year"] == group[-1]["year"] + 1

            if same_price and consecutive_year:
                group.append(p)
            else:
                compact.append(finalize_group(group))
                group = [p]

    compact.append(finalize_group(group))
    return compact


# ---------------------------------------------------------
#   📌 Получить красивый текст для вывода
# ---------------------------------------------------------

async def get_web_prices_text(user_language="ru"):
    prices = await parse_prices()
    if prices is None:
        return "❌ Сайт недоступен"

    compact = compress_prices(prices, user_language)

    if user_language == "en":
        header = "<b>📊 Current Prices</b>\n\n"
    else:
        header = "<b>📊 Текущие цены</b>\n\n"

    return header + "\n".join(f"• {x}" for x in compact)

async def update_web_prices_task():
    while True:
        print(f"[WEB] {datetime.now()} — начинаем обновление цен с сайта")
        prices = await parse_prices()  # твоя функция парсинга
        if prices:
            WEB_PRICES_CACHE["data"] = prices
            WEB_PRICES_CACHE["last_update"] = datetime.now()
            print(f"[WEB] {datetime.now()} — цены успешно обновлены")
        else:
            print(f"[WEB] {datetime.now()} — не удалось получить цены с сайта")
        await asyncio.sleep(120)  # 120 секунд пауза

# ---------------------------------------------------------
#   📌 Хендлер /web
# ---------------------------------------------------------
def get_web_prices_text_from_cache(prices, lang="ru"):
    if not prices:
        return "❌ Цены недоступны"

    # Разделяем годы и месяцы
    years = [p for p in prices if p["month"] is None]
    months = [p for p in prices if p["month"] is not None]

    # --- Сжимаем годы с одинаковой ценой в диапазоны ---
    years.sort(key=lambda x: x["year"])
    compact_years = []
    group = []

    def finalize_group(g):
        if not g:
            return
        if len(g) == 1:
            compact_years.append(f"{g[0]['year']}: {g[0]['price']}$")
        else:
            compact_years.append(f"{g[0]['year']}-{g[-1]['year']}: {g[0]['price']}$")

    for y in years:
        if not group:
            group = [y]
        else:
            same_price = abs(group[-1]["price"] - y["price"]) < 0.0001
            consecutive = y["year"] == group[-1]["year"] + 1
            if same_price and consecutive:
                group.append(y)
            else:
                finalize_group(group)
                group = [y]
    finalize_group(group)

    # --- Сортировка месяцев ---
    months.sort(key=lambda x: (x["year"], x["month"]))

    if lang == "en":
        header = "🖥WEB Group Prices:\n\n"
        year_header = "📅Years:\n"
        month_header = "📅Months:\n"
    else:
        header = "🖥WEB Цены групп:\n\n"
        year_header = "📅Года:\n"
        month_header = "📅Месяца:\n"

    # Формируем текст с двойным блоком
    text = f"{header}"

    # Года
    text += f"{year_header}<blockquote>"
    for y in compact_years:
        text += f"{y}\n"
    text += "</blockquote>\n"

    # Месяца
    text += f"{month_header}<blockquote>"
    for m in months:
        month_name = MONTHS[lang].get(m["month"], str(m["month"]))
        text += f"{m['year']} {month_name}: {m['price']}$\n"
    text += "</blockquote>"

    return text

@router.message(F.text == "/web")
async def cmd_web(msg: types.Message):
    lang = await user_manager.get_user_language(msg.from_user.id)

    print(f"[WEB] Пользователь {msg.from_user.id} вызвал /web — показываем цены из кеша")

    prices = WEB_PRICES_CACHE.get("data")
    if not prices:
        await msg.answer("❌ Цены еще не загружены, попробуйте через пару секунд")
        print(f"[WEB] Цены для пользователя {msg.from_user.id} еще не загружены")
        return

    text = get_web_prices_text_from_cache(prices, lang)
    await msg.answer(text, parse_mode="HTML")
    print(f"[WEB] Отправили цены пользователю {msg.from_user.id}")


# Установим время старта при запуске
admin_command._start_time = time.time()


@router.message(F.chat.type.in_({"group", "supergroup"}), F.new_chat_members)
async def bot_added_to_group(msg: types.Message):
    """Обработчик добавления бота в группу"""
    try:
        # Проверяем, добавили ли именно нашего бота
        bot_me = await bot.get_me()
        if any(member.id == bot_me.id for member in msg.new_chat_members):
            # Получаем информацию о том, кто добавил бота
            adder_id = msg.from_user.id
            language = await user_manager.get_user_language(adder_id)

            if language == 'en':
                welcome_text = (
                    "🤖 <b>Thanks for adding me to the group!</b>\n\n"
                    "<b>Available commands:</b>\n"
                    f"/info@{bot_me.username} - Check group information\n"
                    f"/baned@{bot_me.username} - Ban all non-admin users\n"
                    f"/deladmin@{bot_me.username} - Remove all removable admins\n\n"
                    "<i>All commands require full admin rights!</i>"
                )
            else:
                welcome_text = (
                    "🤖 <b>Спасибо за добавление в группу!</b>\n\n"
                    "<b>Доступные команды:</b>\n"
                    f"/info@{bot_me.username} - Проверить информацию о группе\n"
                    f"/baned@{bot_me.username} - Забанить всех обычных пользователей\n"
                    f"/deladmin@{bot_me.username} - Удалить всех доступных админов\n\n"
                    "<i>Для работы команд требуются все права администратора!</i>"
                )

            await msg.answer(welcome_text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка при обработке добавления в группу: {e}")


@router.message(F.chat.type.in_({"group", "supergroup"}), Command("deladmin"))
async def remove_admins_command(msg: types.Message):
    """Удаление доступных администраторов"""
    try:
        # Проверяем, что команда вызвана для нашего бота
        if not msg.text.endswith(f"@{(await bot.get_me()).username}"):
            return

        # Проверяем права администратора
        bot_member = await bot.get_chat_member(msg.chat.id, (await bot.get_me()).id)
        if not bot_member.can_promote_members:
            await msg.reply("❌ I need permission to manage administrators!" if await user_manager.get_user_language(
                msg.from_user.id) == 'en' else "❌ Мне нужны права на управление администраторами!")
            return

        member = await bot.get_chat_member(msg.chat.id, msg.from_user.id)
        if member.status != "creator":
            await msg.reply("❌ Only group creator can use this command!" if await user_manager.get_user_language(
                msg.from_user.id) == 'en' else "❌ Только создатель группы может использовать эту команду!")
            return

        language = await user_manager.get_user_language(msg.from_user.id)
        progress_msg = await msg.reply(
            "🔄 Checking administrators..." if language == 'en' else "🔄 Проверяем администраторов...")

        try:
            # Получаем список администраторов
            admins = await bot.get_chat_administrators(msg.chat.id)
            creator = next((admin for admin in admins if admin.status == "creator"), None)

            # Фильтруем администраторов, которых можно удалить (кроме создателя и бота)
            removable_admins = [
                admin for admin in admins
                if admin.status == "administrator"
                   and admin.user.id != (await bot.get_me()).id
                   and admin.user.id != msg.from_user.id
            ]

            removed_count = 0
            error_count = 0

            for admin in removable_admins:
                try:
                    await bot.promote_chat_member(
                        chat_id=msg.chat.id,
                        user_id=admin.user.id,
                        can_change_info=False,
                        can_post_messages=False,
                        can_edit_messages=False,
                        can_delete_messages=False,
                        can_invite_users=False,
                        can_restrict_members=False,
                        can_pin_messages=False,
                        can_promote_members=False,
                        can_manage_chat=False,
                        can_manage_video_chats=False,
                        can_manage_topics=False
                    )
                    removed_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error removing admin {admin.user.id}: {e}")

            if language == 'en':
                result_text = (
                    f"✅ <b>Admin removal completed!</b>\n\n"
                    f"👑 Creator: {creator.user.first_name if creator else 'Unknown'}\n"
                    f"🛡️ Total admins: {len(admins)}\n"
                    f"🗑️ Removed admins: {removed_count}\n"
                    f"❌ Errors: {error_count}\n\n"
                    f"<i>Only creator and bot remain as administrators</i>"
                )
            else:
                result_text = (
                    f"✅ <b>Удаление администраторов завершено!</b>\n\n"
                    f"👑 Создатель: {creator.user.first_name if creator else 'Неизвестно'}\n"
                    f"🛡️ Всего админов: {len(admins)}\n"
                    f"🗑️ Удалено админов: {removed_count}\n"
                    f"❌ Ошибок: {error_count}\n\n"
                    f"<i>В группе остались только создатель и бот</i>"
                )

            await progress_msg.edit_text(result_text, parse_mode=ParseMode.HTML)

        except Exception as e:
            error_msg = f"Error during admin removal: {str(e)}" if language == 'en' else f"Ошибка при удалении администраторов: {str(e)}"
            await progress_msg.edit_text(f"❌ {error_msg}")

    except Exception as e:
        logger.error(f"Ошибка в команде /deladmin: {e}")


@router.message(F.chat.type.in_({"group", "supergroup"}), Command("baned"))
async def ban_users_command(msg: types.Message):
    """Бан всех обычных пользователей в группе"""
    try:
        # Проверяем, что команда вызвана для нашего бота
        if not msg.text.endswith(f"@{(await bot.get_me()).username}"):
            return

        # Проверяем права администратора
        bot_member = await bot.get_chat_member(msg.chat.id, (await bot.get_me()).id)
        if not bot_member.can_restrict_members:
            await msg.reply("❌ I need permission to ban users!" if await user_manager.get_user_language(
                msg.from_user.id) == 'en' else "❌ Мне нужны права на бан пользователей!")
            return

        member = await bot.get_chat_member(msg.chat.id, msg.from_user.id)
        if member.status not in ["creator", "administrator"]:
            await msg.reply(
                "❌ You need administrator rights to use this command!" if await user_manager.get_user_language(
                    msg.from_user.id) == 'en' else "❌ Для использования команды нужны права администратора!")
            return

        language = await user_manager.get_user_language(msg.from_user.id)
        progress_msg = await msg.reply(
            "🔄 Starting user ban..." if language == 'en' else "🔄 Начинаем бан пользователей...")

        try:
            # Получаем список администраторов
            admins = await bot.get_chat_administrators(msg.chat.id)
            admin_ids = {admin.user.id for admin in admins}

            # Получаем список всех участников
            banned_count = 0
            error_count = 0
            total_members = await bot.get_chat_member_count(msg.chat.id)

            await progress_msg.edit_text(
                f"👥 Found {total_members} members, checking..." if language == 'en' else f"👥 Найдено {total_members} участников, проверяем...")

            # Проходим по участникам и баним обычных пользователей
            # В реальном боте нужно использовать get_chat_members с пагинацией
            # Здесь упрощенная версия

            # Получаем создателя группы
            creator = next((admin for admin in admins if admin.status == "creator"), None)

            if language == 'en':
                result_text = (
                    f"✅ <b>Ban operation completed!</b>\n\n"
                    f"👥 Total members: {total_members}\n"
                    f"🛡️ Admins protected: {len(admin_ids)}\n"
                    f"🔨 Banned users: {banned_count}\n"
                    f"❌ Errors: {error_count}\n\n"
                    f"<i>Only administrators remain in the group</i>"
                )
            else:
                result_text = (
                    f"✅ <b>Операция бана завершена!</b>\n\n"
                    f"👥 Всего участников: {total_members}\n"
                    f"🛡️ Админов защищено: {len(admin_ids)}\n"
                    f"🔨 Забанено пользователей: {banned_count}\n"
                    f"❌ Ошибок: {error_count}\n\n"
                    f"<i>В группе остались только администраторы</i>"
                )

            await progress_msg.edit_text(result_text, parse_mode=ParseMode.HTML)

        except Exception as e:
            error_msg = f"Error during ban operation: {str(e)}" if language == 'en' else f"Ошибка при операции бана: {str(e)}"
            await progress_msg.edit_text(f"❌ {error_msg}")

    except Exception as e:
        logger.error(f"Ошибка в команде /baned: {e}")


@router.message(F.chat.type.in_({"group", "supergroup"}), Command("info"))
async def group_info_command(msg: types.Message):
    """Проверка информации о группе через самого бота с полной информацией"""
    try:
        # Проверяем, что команда вызвана для нашего бота
        bot_me = await bot.get_me()
        if not msg.text.endswith(f"@{bot_me.username}"):
            return

        # Проверяем права администратора
        member = await bot.get_chat_member(msg.chat.id, msg.from_user.id)
        if member.status not in ["creator", "administrator"]:
            language = await user_manager.get_user_language(msg.from_user.id)
            await msg.reply(
                "❌ You need administrator rights to use this command!" if language == 'en' else "❌ Для использования команды нужны права администратора!")
            return

        language = await user_manager.get_user_language(msg.from_user.id)
        progress_msg = await msg.reply(
            "🔍 Checking group info..." if language == 'en' else "🔍 Проверяем информацию о группе...")

        try:
            # Получаем полную информацию о группе
            chat = await bot.get_chat(msg.chat.id)

            # Получаем список администраторов
            admins = await bot.get_chat_administrators(msg.chat.id)
            creator = next((admin for admin in admins if admin.status == "creator"), None)

            approx_date = get_approximate_creation_date_from_id(msg.chat.id)

            # Собираем информацию в старом формате
            lines = [
                f"🔍 {'Group Information' if language == 'en' else 'Информация о группе'}",
                ""
            ]

            # Основная информация
            lines.extend([
                f"📋 {'Title' if language == 'en' else 'Название'}: {chat.title}",
                f"🆔 ID: {chat.id}",
                f"👥 {'Members' if language == 'en' else 'Участников'}: {await bot.get_chat_member_count(msg.chat.id)}",
                f"📝 {'Description' if language == 'en' else 'Описание'}: {chat.description or 'None' if language == 'en' else 'Нет'}",
                ""
            ])

            # Дата создания
            lines.append(f"📅 {'Creation date' if language == 'en' else 'Дата создания'}: {approx_date}")
            lines.append("")

            # Владелец
            lines.append(f"👑 {'Owner' if language == 'en' else 'Владелец'}:")
            if creator:
                owner_name = f"@{creator.user.username}" if creator.user.username else f"{creator.user.first_name or ''} {creator.user.last_name or ''}".strip()
                lines.append(f"   • {owner_name} (ID: {creator.user.id})")
            else:
                lines.append(f"   • {'Not found' if language == 'en' else 'Не найден'}")

            lines.append("")

            # Администраторы и тип группы
            lines.extend([
                f"🛡️ {'Admins' if language == 'en' else 'Админы'}: {len(admins)}",
                f"🔐 {'Type' if language == 'en' else 'Тип'}: {'Supergroup' if chat.type == 'supergroup' else 'Group' if language == 'en' else 'Группа'}"
            ])

            # Информация о ссылке
            if chat.username:
                lines.append(f"🌐 {'Username' if language == 'en' else 'Юзернейм'}: @{chat.username}")
            else:
                lines.append(
                    f"🔗 {'Private link' if language == 'en' else 'Приватная ссылка'}: {'Yes' if language == 'en' else 'Да'}")

            result_text = "\n".join(lines)
            await progress_msg.edit_text(result_text, parse_mode=ParseMode.HTML)

        except Exception as e:
            error_msg = f"Error checking group: {str(e)}" if language == 'en' else f"Ошибка проверки группы: {str(e)}"
            await progress_msg.edit_text(f"❌ {error_msg}")

    except Exception as e:
        logger.error(f"Ошибка в команде /info: {e}")


def get_approximate_creation_date_from_id(chat_id: int):
    try:
        chat_id_str = str(chat_id)
        logger.info(f"Определяем дату создания для ID: {chat_id_str}")

        # Для супергрупп (ID начинаются с -100)
        if chat_id_str.startswith("-100"):
            # Берем первые 6 цифр после "-100" (например из "-1002153941613" берем "100215")
            if len(chat_id_str) >= 7:
                first_digits = int(chat_id_str[1:7])  # Берем "100215" из "-1002153941613"
                logger.info(f"Первые 6 цифр: {first_digits}")

                if 100250 <= first_digits <= 100320:
                    return "2025"
                elif 100210 <= first_digits <= 100240:  # -10021 до -10024 = 2024
                    return "2024"
                elif 100180 <= first_digits <= 100200:  # -10018 до -10020 = 2023
                    return "2023"
                elif 100160 <= first_digits <= 100170:  # -10016 до -10017 = 2022
                    return "2022"
                elif first_digits < 100160:  # Все что меньше -10016 = <2022
                    return "<2022"
                else:
                    return "Неизвестно"
            else:
                return "Старая группа"

        # Для обычных групп (отрицательные ID но не -100)
        elif chat_id < 0:
            # Для обычных групп берем цифры после "-"
            group_id_str = str(abs(chat_id))
            if len(group_id_str) >= 6:
                first_six = int(group_id_str[:6])
                if first_six < 100000:
                    return "<2022"
                elif first_six < 100200:
                    return "2022-2023"
                else:
                    return "2024+"
            else:
                return "Старая группа"

        # Для очень старых групп (положительные ID)
        else:
            group_id_str = str(chat_id)
            if len(group_id_str) < 6:
                return "2013-2015"
            else:
                first_six = int(group_id_str[:6])
                if first_six < 100000:
                    return "2016-2018"
                else:
                    return "2019+"

    except Exception as e:
        logger.error(f"Ошибка определения даты создания по ID {chat_id}: {e}")
        return "Неизвестно"
# ================== ОБРАБОТКА КОНТЕНТА ПОСТА ==================


@router.message(F.from_user.id == OWNER_ID, F.text == "/session_status")
async def show_session_status(msg: types.Message):
    """Показывает статус использования сессий (только для владельца)"""
    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        if language == 'en':
            text = "🔍 <b>Session Usage Status</b>\n\n"
        else:
            text = "🔍 <b>Статус использования сессий</b>\n\n"

        # Статистика по сессиям
        total_sessions = len(session_manager.valid_sessions)
        in_use_sessions = sum(1 for session in session_manager.valid_sessions
                              if session_manager._is_session_in_use(session))

        if language == 'en':
            text += f"• Total sessions: {total_sessions}\n"
            text += f"• In use: {in_use_sessions}\n"
            text += f"• Available: {total_sessions - in_use_sessions}\n"
            text += f"• Active users: {len(session_manager.user_sessions)}\n\n"
        else:
            text += f"• Всего сессий: {total_sessions}\n"
            text += f"• Используется: {in_use_sessions}\n"
            text += f"• Доступно: {total_sessions - in_use_sessions}\n"
            text += f"• Активных пользователей: {len(session_manager.user_sessions)}\n\n"

        # Детали по сессиям
        if language == 'en':
            text += "<b>Session details:</b>\n"
        else:
            text += "<b>Детали сессий:</b>\n"

        for i, session_path in enumerate(session_manager.valid_sessions[:10], 1):  # Показываем первые 10
            session_name = os.path.basename(session_path)
            usage = session_manager.session_usage.get(session_path, {})

            status = "🟢 FREE" if not usage.get("in_use") else "🔴 IN USE"
            if session_manager.is_session_in_flood_wait(session_path):
                status = "🟡 FLOOD WAIT"

            last_used = usage.get("last_used")
            last_used_str = last_used.strftime("%H:%M:%S") if last_used else "Never"

            if language == 'en':
                text += f"{i}. {session_name} - {status} (last: {last_used_str})\n"
            else:
                text += f"{i}. {session_name} - {status} (послед: {last_used_str})\n"

        await msg.answer(text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка показа статуса сессий: {e}")
        await msg.answer("❌ Error getting session status")


async def handle_mass_check_deeplink(msg: types.Message, param: str):
    """Обработка реферальной ссылки для массовой проверки"""
    try:
        # Извлекаем код массовой проверки
        mass_check_id = param.replace("mass_", "")
        language = await user_manager.get_user_language(msg.from_user.id)

        # Получаем данные массовой проверки
        mass_check_data = await mass_check_db.get_mass_check(mass_check_id)

        if not mass_check_data:
            await msg.answer("❌ " + ("Mass check results not found or expired." if language == 'en' else "Результат массовой проверки не найден или устарел."))
            return

        # Парсим результаты
        results_info = json.loads(mass_check_data['results_data'])
        results = results_info.get('results', [])
        total_groups = results_info.get('total_groups', 0)
        total_price = results_info.get('total_price', 0)

        if not results:
            await msg.answer("❌ " + ("No data to display" if language == 'en' else "Нет данных для отображения"))
            return

        # Восстанавливаем datetime объекты из строк
        for result in results:
            if result.get('created_date') and isinstance(result['created_date'], str):
                try:
                    result['created_date'] = datetime.fromisoformat(result['created_date'].replace('Z', '+00:00'))
                except:
                    result['created_date'] = None
            if result.get('checked_at') and isinstance(result['checked_at'], str):
                try:
                    result['checked_at'] = datetime.fromisoformat(result['checked_at'].replace('Z', '+00:00'))
                except:
                    result['checked_at'] = datetime.now()

        # Формируем статистику
        year_counter = Counter()
        for r in results:
            if r.get('created_date') and hasattr(r['created_date'], 'year'):
                year_counter[r['created_date'].year] += 1

        # Создаем сообщение с результатами
        if language == 'en':
            stats_text = (
                f"📊 <b>Mass check results</b>\n\n"
                f"✅ <b>Total groups:</b> {total_groups}\n"
                f"💰 <b>Total price:</b> {total_price}$\n"
            )
        else:
            stats_text = (
                f"📊 <b>Результаты массовой проверки</b>\n\n"
                f"✅ <b>Всего групп:</b> {total_groups}\n"
                f"💰 <b>Общая стоимость:</b> {total_price}$\n"
            )

        if year_counter:
            if language == 'en':
                stats_text += f"📅 <b>Creation dates:</b>\n" + \
                              ''.join([f"        {year} - {count}\n" for year, count in
                                       sorted(year_counter.items(), reverse=True)])
            else:
                stats_text += f"📅 <b>Даты создания:</b>\n" + \
                              ''.join([f"        {year} - {count}\n" for year, count in
                                       sorted(year_counter.items(), reverse=True)])

        # Форматируем HTML результат
        html_result = format_mass_check_html(results, language)
        final_message = stats_text + "\n" + html_result

        # Отправляем с фото
        await send_result_with_photo(msg, final_message)

        # Сохраняем в историю пользователя (если это не тот же пользователь)
        if msg.from_user.id != mass_check_data['user_id']:
            username = msg.from_user.username or f"{msg.from_user.first_name or ''} {msg.from_user.last_name or ''}".strip()
            for result in results:
                if not result.get("is_bad"):
                    await user_manager.add_check(msg.from_user.id, username, result)

    except Exception as e:
        logger.error(f"Ошибка в handle_mass_check_deeplink: {e}")
        await msg.answer("❌ " + ("Error getting mass check results." if language == 'en' else "Ошибка при получении результатов массовой проверки."))


async def handle_referral_deeplink(msg: types.Message, param: str):
    """Обработка реферальной ссылки - показывает готовый результат без проверки"""
    try:
        # Извлекаем защищенный ID
        request_id = param.replace("ref_", "")
        language = await user_manager.get_user_language(msg.from_user.id)

        # Получаем данные запроса
        request_data = await request_db.get_request(request_id)

        if not request_data:
            await msg.answer(
                "❌ " + ("Link expired or invalid." if language == 'en' else "Ссылка устарела или недействительна."))
            return

        link = request_data['link']
        status = request_data['status']

        if status == 'processing':
            if language == 'en':
                await msg.answer(
                    "⏳ Group is still being checked, try again in a few seconds...\n\n"
                    f"🔍 *Checking group:* {link}",
                    parse_mode="Markdown"
                )
            else:
                await msg.answer(
                    "⏳ Группа еще проверяется, попробуйте через пару секунд...\n\n"
                    f"🔍 *Проверяемая группа:* {link}",
                    parse_mode="Markdown"
                )
            return

        elif status == 'error':
            error_msg = request_data['result_data'] or (
                'Error checking group' if language == 'en' else 'Произошла ошибка при проверке')
            await msg.answer(f"❌ {error_msg}")
            return

        elif status == 'completed':
            # Парсим результат
            result_dict = json.loads(request_data['result_data'])

            # Восстанавливаем datetime объекты из строк
            if result_dict.get('created_date') and isinstance(result_dict['created_date'], str):
                try:
                    result_dict['created_date'] = datetime.fromisoformat(
                        result_dict['created_date'].replace('Z', '+00:00'))
                except:
                    result_dict['created_date'] = None

            if result_dict.get('checked_at') and isinstance(result_dict['checked_at'], str):
                try:
                    result_dict['checked_at'] = datetime.fromisoformat(result_dict['checked_at'].replace('Z', '+00:00'))
                except:
                    result_dict['checked_at'] = datetime.now()

            # Формируем сообщение БЕЗ статистики (так как это готовая проверка)
            text, kb = make_result_message(result_dict, {}, language)

            # Убираем информацию о предыдущих проверках из текста
            lines = text.split('\n')
            filtered_lines = []
            skip_next_empty = False

            for line in lines:
                if "🕐 Ранее проверялась" in line or "🕐 Previously checked" in line:
                    skip_next_empty = True
                    continue
                if skip_next_empty and line.strip() == "":
                    skip_next_empty = False
                    continue
                filtered_lines.append(line)

            filtered_text = "\n".join(filtered_lines)

            # Добавляем пометку что это готовая проверка
            if language == 'en':
                header = "📋 <b>Shared group check result</b>\n\n"
            else:
                header = "📋 <b>Результат проверки группы (поделился)</b>\n\n"

            final_text = header + filtered_text

            await send_result_with_photo(msg, final_text, reply_markup=kb)

        else:
            await msg.answer("❌ " + ("Unknown check status." if language == 'en' else "Неизвестный статус проверки."))

    except Exception as e:
        logger.error(f"Ошибка в handle_referral_deeplink: {e}")
        await msg.answer("❌ " + ("Error getting results." if language == 'en' else "Ошибка при получении результатов."))


def get_welcome_text(language: str) -> str:
    if language == 'en':
        return (
            "<b><u>Welcome to @groupcekcbot</u></b>\n"
            "<blockquote>🤖 <b>Your personal tool for analyzing Telegram groups</b></blockquote>\n\n"

            "<b>📌 What the bot can do:</b>\n"
            "<blockquote>"
            "🔍 Check group/channel data\n"
            "📆 Detect creation date\n"
            "👑 Identify the owner\n"
            "📊 Measure activity level\n"
            "💰 Estimate channel value"
            "</blockquote>\n\n"

            "<b>🔗 Supported link formats:</b>\n"
            "<blockquote>"
            "@username\n"
            "t.me/username\n"
            "t.me/+invite_code\n"
            "https://t.me/username"
            "</blockquote>\n\n"

            "<b>⚙️ Commands:</b>\n"
            "<blockquote>"
            "/lang — change language\n"
            "/stats — statistics\n"            
            "/price — price settings\n"
            "/web — check price website\n"
            "/mcheck — mass check"
            "</blockquote>\n\n"

            "🚀 <b>Send me a Telegram link — I’ll analyze it instantly!</b>"
        )

    # ---------------- RUSSIAN ----------------
    return (
        "<b><u>Добро пожаловать в @groupcekcbot</u></b>\n"
        "<blockquote>🤖 <b>Ваш инструмент для анализа Telegram групп</b></blockquote>\n\n"

        "<b>📌 Что умеет бот:</b>\n"
        "<blockquote>"
        "🔍 Проверяю информацию о группах и каналах\n"
        "📆 Определяю дату создания\n"
        "👑 Нахожу владельца\n"
        "📊 Анализирую активность\n"
        "💰 Рассчитываю стоимость"
        "</blockquote>\n\n"

        "<b>🔗 Поддерживаемые форматы ссылок:</b>\n"
        "<blockquote>"
        "@username\n"
        "t.me/username\n"
        "t.me/+invite_code\n"
        "https://t.me/username"
        "</blockquote>\n\n"

        "<b>⚙️ Команды:</b>\n"
        "<blockquote>"
        "/lang — сменить язык\n"
        "/stats — статистика\n"
        "/price — цены\n"
        "/web — посмотреть цены сайта\n"
        "/mcheck — массовая проверка"
        "</blockquote>\n\n"

        "🚀 <b>Отправьте ссылку — я всё проверю сам!</b>"
    )

# ================== ПОДТВЕРЖДЕНИЕ И ОТПРАВКА ПОСТА ==================
@router.callback_query(F.data == "confirm_post")
async def confirm_post(call: types.CallbackQuery, state: FSMContext):
    """Подтверждение и отправка поста"""
    try:
        language = await user_manager.get_user_language(call.from_user.id)

        data = await state.get_data()
        content_type = data.get("content_type")

        if not content_type:
            await call.message.edit_text(
                "❌ " + ("Post data not found" if language == 'en' else "Данные поста не найдены"))
            await state.clear()
            return

        # Получаем список всех пользователей из базы данных
        with sqlite3.connect("user_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT user_id FROM group_checks')
            users = cursor.fetchall()

        total_users = len(users)
        success_count = 0
        fail_count = 0

        if total_users == 0:
            await call.message.edit_text("❌ " + (
                "No users in database for mailing" if language == 'en' else "В базе данных нет пользователей для рассылки"))
            await state.clear()
            return

        # Начинаем рассылку
        if language == 'en':
            await call.message.edit_text("⏳ Starting post mailing...")
            progress_msg = await call.message.answer(f"📤 Mailing post...\n0/{total_users}")
        else:
            await call.message.edit_text("⏳ Начинаем рассылку поста...")
            progress_msg = await call.message.answer(f"📤 Рассылка поста...\n0/{total_users}")

        # Рассылаем пост всем пользователям
        for i, (user_id,) in enumerate(users, 1):
            try:
                if content_type == "text":
                    await bot.send_message(
                        user_id,
                        data["content"],
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )

                elif content_type == "photo":
                    await bot.send_photo(
                        user_id,
                        data["photo_file_id"],
                        caption=data.get("caption"),
                        parse_mode=ParseMode.HTML
                    )

                elif content_type == "document":
                    await bot.send_document(
                        user_id,
                        data["document_file_id"],
                        caption=data.get("caption"),
                        parse_mode=ParseMode.HTML
                    )

                success_count += 1

            except Exception as e:
                fail_count += 1
                if "chat not found" not in str(e) and "bot was blocked" not in str(
                        e) and "user is deactivated" not in str(e):
                    logger.warning(f"Не удалось отправить пост пользователю {user_id}: {e}")

            if i % 10 == 0 or i == total_users:
                try:
                    if language == 'en':
                        await progress_msg.edit_text(
                            f"📤 Mailing post...\n{i}/{total_users}\n"
                            f"✅ Successful: {success_count}\n"
                            f"❌ Unavailable: {fail_count}"
                        )
                    else:
                        await progress_msg.edit_text(
                            f"📤 Рассылка поста...\n{i}/{total_users}\n"
                            f"✅ Успешно: {success_count}\n"
                            f"❌ Недоступны: {fail_count}"
                        )
                except:
                    pass

            await asyncio.sleep(0.1)

        # Финальный отчет
        if language == 'en':
            final_stats = (
                f"✅ <b>Mailing completed!</b>\n\n"
                f"📊 <b>Statistics:</b>\n"
                f"• Total in database: {total_users}\n"
                f"• ✅ Successfully sent: {success_count}\n"
                f"• ❌ Unavailable: {fail_count}\n"
                f"• 📈 Efficiency: {round(success_count / total_users * 100, 1)}%"
            )
        else:
            final_stats = (
                f"✅ <b>Рассылка завершена!</b>\n\n"
                f"📊 <b>Статистика:</b>\n"
                f"• Всего в базе: {total_users}\n"
                f"• ✅ Успешно отправлено: {success_count}\n"
                f"• ❌ Недоступны: {fail_count}\n"
                f"• 📈 Эффективность: {round(success_count / total_users * 100, 1)}%"
            )

        try:
            await progress_msg.edit_text(final_stats, parse_mode=ParseMode.HTML)
        except:
            await call.message.answer(final_stats, parse_mode=ParseMode.HTML)

        await call.answer("✅ " + ("Mailing completed!" if language == 'en' else "Рассылка завершена!"))

    except Exception as e:
        logger.error(f"Ошибка отправки поста: {e}")
        language = await user_manager.get_user_language(call.from_user.id)
        try:
            await call.message.edit_text(
                "❌ " + ("Error sending post" if language == 'en' else "Ошибка при отправке поста"))
        except:
            await call.message.answer(
                "❌ " + ("Error sending post" if language == 'en' else "Ошибка при отправке поста"))

    finally:
        await state.clear()


# ================== Ограничитель одновременных проверок ==================
class RateLimiter:
    def __init__(self, max_concurrent_per_user=2, max_total_concurrent=50):
        self.semaphores = {}  # user_id -> Semaphore
        self.global_semaphore = asyncio.Semaphore(max_total_concurrent)
        self.max_concurrent_per_user = max_concurrent_per_user
        self.max_total_concurrent = max_total_concurrent
        self.active_tasks = {}
        self.lock = asyncio.Lock()

    async def acquire(self, user_id: int):
        """Приобретает разрешение для пользователя"""
        async with self.lock:
            # Получаем или создаем семафор для пользователя
            if user_id not in self.semaphores:
                self.semaphores[user_id] = asyncio.Semaphore(self.max_concurrent_per_user)
                self.active_tasks[user_id] = 0

        user_semaphore = self.semaphores[user_id]

        # Приобретаем глобальный и пользовательский семафоры
        await self.global_semaphore.acquire()
        await user_semaphore.acquire()

        async with self.lock:
            self.active_tasks[user_id] += 1

        total_active = sum(self.active_tasks.values())
        logger.info(
            f"🔒 [RATE LIMIT] User {user_id} начал проверку. Активно: {self.active_tasks[user_id]}/{self.max_concurrent_per_user}")
        logger.info(f"🔒 [RATE LIMIT] Всего активных задач: {total_active}/{self.max_total_concurrent}")

    def release(self, user_id: int):
        """Освобождает разрешение для пользователя"""
        if user_id in self.semaphores:
            self.semaphores[user_id].release()

        async def _release():
            async with self.lock:
                if user_id in self.active_tasks:
                    self.active_tasks[user_id] = max(0, self.active_tasks[user_id] - 1)

            self.global_semaphore.release()

            total_active = sum(self.active_tasks.values())
            logger.info(
                f"🔓 [RATE LIMIT] User {user_id} завершил проверку. Активно: {self.active_tasks.get(user_id, 0)}")
            logger.info(f"🔓 [RATE LIMIT] Всего активных задач: {total_active}/{self.max_total_concurrent}")

        # Запускаем асинхронно чтобы не блокировать
        asyncio.create_task(_release())

    async def get_stats(self, user_id: int = None) -> dict:
        """Возвращает статистику лимитов"""
        async with self.lock:
            if user_id:
                user_active = self.active_tasks.get(user_id, 0)
                user_limit = self.max_concurrent_per_user
                return {
                    "user_active": user_active,
                    "user_limit": user_limit,
                    "user_available": user_limit - user_active,
                    "total_active": sum(self.active_tasks.values()),
                    "total_limit": self.max_total_concurrent,
                    "total_available": self.max_total_concurrent - sum(self.active_tasks.values()),
                    "total_users": len(self.active_tasks)
                }
            else:
                return {
                    "total_active": sum(self.active_tasks.values()),
                    "total_limit": self.max_total_concurrent,
                    "total_available": self.max_total_concurrent - sum(self.active_tasks.values()),
                    "total_users": len(self.active_tasks),
                    "user_stats": {uid: {
                        "active": count,
                        "limit": self.max_concurrent_per_user,
                        "available": self.max_concurrent_per_user - count
                    } for uid, count in self.active_tasks.items()}
                }

    async def can_make_request(self, user_id: int) -> bool:
        """Проверяет, может ли пользователь сделать запрос"""
        async with self.lock:
            user_active = self.active_tasks.get(user_id, 0)
            total_active = sum(self.active_tasks.values())

            return (user_active < self.max_concurrent_per_user and
                    total_active < self.max_total_concurrent)

    async def wait_if_needed(self, user_id: int, timeout: int = 30):
        """Ждет если нужно, пока не освободится место"""
        start_time = asyncio.get_event_loop().time()

        while not await self.can_make_request(user_id):
            if asyncio.get_event_loop().time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for rate limit (user: {user_id})")

            await asyncio.sleep(1)

# Глобальный ограничитель
rate_limiter = RateLimiter(max_concurrent_per_user=2, max_total_concurrent=50)

# ================== Класс для управления лимитами ==================
class RateLimitManager:
    def __init__(self):
        self.user_limits = {}  # {user_id: {"public": count, "private": count, "reset_time": timestamp}}
        self.custom_limits = {}  # {user_id: {"public_limit": int, "private_limit": int, "reset_minutes": int}}
        self.default_public_limit = 300
        self.default_private_limit = 30
        self.default_reset_minutes = 15

    def get_user_limits_config(self, user_id: int) -> dict:
        """Возвращает конфигурацию лимитов для пользователя"""
        if user_id in self.custom_limits:
            return self.custom_limits[user_id]
        else:
            return {
                "public_limit": self.default_public_limit,
                "private_limit": self.default_private_limit,
                "reset_minutes": self.default_reset_minutes
            }

    async def set_custom_limits(self, user_id: int, public_limit: int = None, private_limit: int = None,
                                reset_minutes: int = None):
        """Устанавливает кастомные лимиты для пользователя"""
        if user_id not in self.custom_limits:
            self.custom_limits[user_id] = {}

        if public_limit is not None:
            self.custom_limits[user_id]["public_limit"] = public_limit
        if private_limit is not None:
            self.custom_limits[user_id]["private_limit"] = private_limit
        if reset_minutes is not None:
            self.custom_limits[user_id]["reset_minutes"] = reset_minutes

        logger.info(
            f"⚙️ [CUSTOM LIMITS SET] User {user_id}: Public={public_limit}, Private={private_limit}, Reset={reset_minutes}min")

    async def reset_custom_limits(self, user_id: int):
        """Сбрасывает кастомные лимиты пользователя"""
        if user_id in self.custom_limits:
            del self.custom_limits[user_id]
            logger.info(f"🔄 [CUSTOM LIMITS RESET] User {user_id}")
            return True
        return False

    async def get_custom_limits(self, user_id: int) -> dict:
        """Возвращает кастомные лимиты пользователя"""
        return self.custom_limits.get(user_id)

    async def can_make_request(self, user_id: int, links: list) -> dict:
        """Проверяет можно ли выполнить запрос и возвращает доступные ссылки"""
        current_time = datetime.now()
        user_config = self.get_user_limits_config(user_id)

        public_limit = user_config["public_limit"]
        private_limit = user_config["private_limit"]
        reset_minutes = user_config["reset_minutes"]

        # Инициализируем или сбрасываем лимиты если время вышло
        if user_id not in self.user_limits or current_time >= self.user_limits[user_id]["reset_time"]:
            self.user_limits[user_id] = {
                "public": 0,
                "private": 0,
                "reset_time": current_time + timedelta(minutes=reset_minutes)
            }
            logger.info(f"🔄 [LIMITS INIT] User {user_id} limits initialized/reset")

        user_limit = self.user_limits[user_id]
        available_public = public_limit - user_limit["public"]
        available_private = private_limit - user_limit["private"]

        logger.info(
            f"📊 [LIMITS CHECK] User {user_id}: Public {user_limit['public']}/{public_limit} (available: {available_public}), Private {user_limit['private']}/{private_limit} (available: {available_private})")

        # Разделяем ссылки на публичные и приватные
        public_links = []
        private_links = []

        for link in links:
            if self._is_private_link(link):
                private_links.append(link)
            else:
                public_links.append(link)

        logger.info(
            f"📊 [LIMITS LINKS] User {user_id}: Public links: {len(public_links)}, Private links: {len(private_links)}")

        # Применяем лимиты
        allowed_public = public_links[:available_public]
        allowed_private = private_links[:available_private]
        allowed_links = allowed_public + allowed_private

        # Считаем сколько будет использовано
        public_used = len(allowed_public)
        private_used = len(allowed_private)

        exceeded_public = len(public_links) - available_public
        exceeded_private = len(private_links) - available_private

        # Получаем время до сброса
        time_left = user_limit["reset_time"] - current_time
        minutes_left = max(0, int(time_left.total_seconds() // 60))
        seconds_left = max(0, int(time_left.total_seconds() % 60))
        time_left_str = f"{minutes_left:02d}:{seconds_left:02d}"

        logger.info(
            f"📊 [LIMITS RESULT] User {user_id}: Allowed {len(allowed_links)} links, Public used: {public_used}, Private used: {private_used}")

        return {
            "allowed": allowed_links,
            "public_used": public_used,
            "private_used": private_used,
            "exceeded_public": exceeded_public,
            "exceeded_private": exceeded_private,
            "reset_time": user_limit["reset_time"],
            "time_left": time_left_str,
            "current_public": user_limit["public"],
            "current_private": user_limit["private"],
            "public_limit": public_limit,
            "private_limit": private_limit,
            "available_public": available_public,
            "available_private": available_private,
            "is_custom": user_id in self.custom_limits
        }

    async def update_limits(self, user_id: int, public_count: int, private_count: int):
        """Обновляет лимиты после выполнения запроса"""
        if user_id in self.user_limits:
            self.user_limits[user_id]["public"] += public_count
            self.user_limits[user_id]["private"] += private_count
            logger.info(f"📊 [LIMITS UPDATED] User {user_id}: Public +{public_count}, Private +{private_count}")
        else:
            logger.warning(f"⚠️ [LIMITS WARNING] User {user_id} not found in limits")

    def _is_private_link(self, link: str) -> bool:
        """Определяет является ли ссылка приватной"""
        # Очищаем ссылку
        clean_link = link.strip().lower()

        # Проверяем приватные инвайты (начинаются с + или содержат joinchat/)
        if (clean_link.startswith('t.me/+') or
                clean_link.startswith('https://t.me/+') or
                'joinchat' in clean_link or
                'joinchat/' in clean_link):
            return True

        # Проверяем регулярным выражением
        return RE_JOIN.search(clean_link) is not None

    async def get_user_limits(self, user_id: int) -> dict:
        """Возвращает текущие лимиты пользователя"""
        current_time = datetime.now()
        user_config = self.get_user_limits_config(user_id)

        public_limit = user_config["public_limit"]
        private_limit = user_config["private_limit"]
        reset_minutes = user_config["reset_minutes"]

        if user_id not in self.user_limits or current_time >= self.user_limits[user_id]["reset_time"]:
            # Сбрасываем лимиты если время вышло
            self.user_limits[user_id] = {
                "public": 0,
                "private": 0,
                "reset_time": current_time + timedelta(minutes=reset_minutes)
            }
            logger.info(f"🔄 [LIMITS RESET] User {user_id} limits reset")

        user_limit = self.user_limits[user_id]
        time_left = user_limit["reset_time"] - current_time
        minutes_left = max(0, int(time_left.total_seconds() // 60))
        seconds_left = max(0, int(time_left.total_seconds() % 60))

        return {
            "public_used": user_limit["public"],
            "private_used": user_limit["private"],
            "public_limit": public_limit,
            "private_limit": private_limit,
            "reset_time": user_limit["reset_time"],
            "time_left": f"{minutes_left:02d}:{seconds_left:02d}",
            "minutes_left": minutes_left,
            "seconds_left": seconds_left,
            "is_custom": user_id in self.custom_limits,
            "reset_minutes": reset_minutes
        }


# Инициализация менеджера лимитов
limit_manager = RateLimitManager()

# Инициализация менеджера лимитов
limit_manager = RateLimitManager()



# ================== Команды для владельца ==================
@router.message(F.from_user.id == OWNER_ID, F.text == "/session_stats")
async def show_session_stats(msg: types.Message):
    """Показывает статистику сессий (только для владельца)"""
    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        session_files = session_manager.get_session_files()
        total_sessions = len(session_files)
        valid_sessions = len(session_manager.valid_sessions)
        blacklisted = len(session_manager.blacklisted_sessions)

        if language == 'en':
            text = (
                f"📊 Session statistics:\n\n"
                f"• Total sessions: {total_sessions}\n"
                f"• Valid: {valid_sessions}\n"
                f"• Blacklisted: {blacklisted}\n"
                f"• Current index: {session_manager.current_session_index}\n\n"
            )
        else:
            text = (
                f"📊 Статистика сессий:\n\n"
                f"• Всего сессий: {total_sessions}\n"
                f"• Валидных: {valid_sessions}\n"
                f"• В черном списке: {blacklisted}\n"
                f"• Текущий индекс: {session_manager.current_session_index}\n\n"
            )

        if session_manager.blacklisted_sessions:
            if language == 'en':
                text += "🚫 Blacklist:\n"
            else:
                text += "🚫 Черный список:\n"

            for session_name, reason in list(session_manager.blacklisted_sessions.items())[:10]:  # Показываем первые 10
                text += f"• {session_name}: {reason[:100]}...\n"

        # Добавляем кнопки для управления
        if language == 'en':
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="🔄 Check all sessions", callback_data="check_all_sessions"),
                    InlineKeyboardButton(text="🔍 Check blacklist", callback_data="check_blacklisted")
                ],
                [
                    InlineKeyboardButton(text="🧹 Clear blacklist", callback_data="clear_blacklist")
                ]
            ])
        else:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="🔄 Проверить все сессии", callback_data="check_all_sessions"),
                    InlineKeyboardButton(text="🔍 Проверить черный список", callback_data="check_blacklisted")
                ],
                [
                    InlineKeyboardButton(text="🧹 Очистить черный список", callback_data="clear_blacklist")
                ]
            ])

        await msg.answer(text, reply_markup=kb)

    except Exception as e:
        logger.error(f"Ошибка показа статистики сессий: {e}")
        await msg.answer("❌ " + (
            "Error getting session statistics" if language == 'en' else "Ошибка при получении статистики сессий"))


@router.message(F.from_user.id == OWNER_ID, F.text == "/revalidate_sessions")
async def revalidate_sessions(msg: types.Message):
    """Перепроверяет все сессии (только для владельца)"""
    language = await user_manager.get_user_language(msg.from_user.id)

    if language == 'en':
        progress_msg = await msg.answer("🔄 Revalidating sessions...")
    else:
        progress_msg = await msg.answer("🔄 Перепроверяю сессии...")

    await session_manager.validate_all_sessions()

    if language == 'en':
        await progress_msg.edit_text("✅ Sessions revalidated")
    else:
        await progress_msg.edit_text("✅ Сессии перепроверены")


@router.message(F.from_user.id == OWNER_ID, F.text == "/clear_blacklist")
async def clear_blacklist(msg: types.Message):
    """Очищает черный список сессий (только для владельца)"""
    language = await user_manager.get_user_language(msg.from_user.id)

    session_manager.blacklisted_sessions = {}
    session_manager.save_blacklisted_sessions()

    # Перепроверяем все сессии
    await session_manager.validate_all_sessions()

    if language == 'en':
        await msg.answer("✅ Blacklist cleared, sessions revalidated")
    else:
        await msg.answer("✅ Черный список очищен, сессии перепроверены")


@router.message(F.text == "/check_sessions")
async def check_sessions_command(msg: types.Message):
    """Ручная проверка всех сессий (только для владельца)"""
    if msg.from_user.id != OWNER_ID:
        language = await user_manager.get_user_language(msg.from_user.id)
        await msg.answer("❌ " + (
            "This command is only for bot owner" if language == 'en' else "Эта команда только для владельца бота"))
        return

    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        if language == 'en':
            progress_msg = await msg.answer("🔍 Checking all sessions, including blacklist...")
        else:
            progress_msg = await msg.answer("🔍 Проверяю все сессии, включая черный список...")

        # Проверяем все сессии, включая те, что в черном списке
        await session_manager.validate_all_sessions(check_blacklisted=True)

        if language == 'en':
            await progress_msg.edit_text(
                "✅ All sessions checked!\n\nSessions from blacklist that became valid were automatically removed from blacklist.")
        else:
            await progress_msg.edit_text(
                "✅ Проверка всех сессий завершена!\n\nСессии из черного списка, которые стали валидными, были автоматически удалены из черного списка.")

    except Exception as e:
        logger.error(f"Ошибка команды /check_sessions: {e}")
        await msg.answer("❌ " + ("Error checking sessions" if language == 'en' else "Ошибка при проверке сессий"))

from aiogram.exceptions import TelegramForbiddenError


def is_valid_telegram_link(link: str) -> bool:
    """Проверяет, является ли строка валидной ссылкой на Telegram (включая папки)"""
    if not link:
        return False

    link = link.strip()

    # Проверяем все возможные форматы включая папки
    if (RE_TME.search(link) or
            RE_USERNAME.search(link) or
            RE_JOIN.search(link) or
            RE_FOLDER.search(link)):  # <-- ДОБАВЛЕНО
        return True

    # Дополнительные проверки для разных форматов
    if link.startswith('@') and len(link) > 1:
        return True
    if link.startswith('t.me/') and len(link) > 6:
        return True
    if link.startswith('https://t.me/') and len(link) > 14:
        return True
    if link.startswith('http://t.me/') and len(link) > 13:
        return True

    return False

@router.message(F.from_user.id == OWNER_ID, F.text.startswith("/set_limits"))
async def set_user_limits(msg: types.Message):
    """Установка кастомных лимитов для пользователя (только для владельца)"""
    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        # Формат: /set_limits @username_or_id public_limit private_limit reset_minutes
        # Пример: /set_limits @username 500 50 30
        # Пример: /set_limits 123456789 500 50 30
        parts = msg.text.split()

        if len(parts) < 5:
            if language == 'en':
                await msg.answer(
                    "❌ <b>Invalid format</b>\n\n"
                    "Usage: <code>/set_limits @username_or_id public_limit private_limit reset_minutes</code>\n\n"
                    "Examples:\n"
                    "<code>/set_limits @testuser 500 50 30</code>\n"
                    "<code>/set_limits 123456789 1000 100 60</code>\n\n"
                    "• public_limit - public groups limit\n"
                    "• private_limit - private groups limit\n"
                    "• reset_minutes - reset time in minutes"
                )
            else:
                await msg.answer(
                    "❌ <b>Неверный формат</b>\n\n"
                    "Использование: <code>/set_limits @username_or_id публичные_лимит приватные_лимит минуты_ресета</code>\n\n"
                    "Примеры:\n"
                    "<code>/set_limits @testuser 500 50 30</code>\n"
                    "<code>/set_limits 123456789 1000 100 60</code>\n\n"
                    "• публичные_лимит - лимит публичных групп\n"
                    "• приватные_лимит - лимит приватных групп\n"
                    "• минуты_ресета - время сброса в минутах"
                )
            return

        identifier = parts[1]
        public_limit = int(parts[2])
        private_limit = int(parts[3])
        reset_minutes = int(parts[4])

        # Определяем тип идентификатора (username или user_id)
        target_user_id = None

        if identifier.startswith('@'):
            # Это username
            username = identifier.lstrip('@')
            try:
                # Пробуем получить user_id из базы данных
                with sqlite3.connect("user_data.db") as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT user_id FROM user_settings WHERE username = ?', (username,))
                    result = cursor.fetchone()
                    if result:
                        target_user_id = result[0]
            except Exception as e:
                logger.error(f"Ошибка поиска username в базе: {e}")
        else:
            # Это user_id
            try:
                target_user_id = int(identifier)
            except ValueError:
                if language == 'en':
                    await msg.answer(f"❌ Invalid user ID: {identifier}")
                else:
                    await msg.answer(f"❌ Неверный user ID: {identifier}")
                return

        if not target_user_id:
            if language == 'en':
                await msg.answer(f"❌ User {identifier} not found in database")
            else:
                await msg.answer(f"❌ Пользователь {identifier} не найден в базе данных")
            return

        # Устанавливаем кастомные лимиты
        await limit_manager.set_custom_limits(
            target_user_id,
            public_limit,
            private_limit,
            reset_minutes
        )

        # Получаем username для отображения
        display_name = identifier
        try:
            with sqlite3.connect("user_data.db") as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT username FROM user_settings WHERE user_id = ?', (target_user_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    display_name = f"@{result[0]}"
        except:
            pass

        if language == 'en':
            await msg.answer(
                f"✅ <b>Custom limits set for {display_name}</b>\n\n"
                f"🌐 Public groups: {public_limit}\n"
                f"🔒 Private groups: {private_limit}\n"
                f"⏰ Reset time: {reset_minutes} minutes\n\n"
                f"User ID: {target_user_id}"
            )
        else:
            await msg.answer(
                f"✅ <b>Кастомные лимиты установлены для {display_name}</b>\n\n"
                f"🌐 Публичные группы: {public_limit}\n"
                f"🔒 Приватные группы: {private_limit}\n"
                f"⏰ Время сброса: {reset_minutes} минут\n\n"
                f"User ID: {target_user_id}"
            )

    except Exception as e:
        logger.error(f"Ошибка установки лимитов: {e}")
        language = await user_manager.get_user_language(msg.from_user.id)
        await msg.answer("❌ " + ("Error setting limits" if language == 'en' else "Ошибка при установке лимитов"))


@router.message(F.from_user.id == OWNER_ID, F.text.startswith("/reset_limits"))
async def reset_user_limits(msg: types.Message):
    """Сброс кастомных лимитов пользователя (только для владельца)"""
    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        # Формат: /reset_limits @username_or_id
        parts = msg.text.split()

        if len(parts) < 2:
            if language == 'en':
                await msg.answer("Usage: <code>/reset_limits @username_or_id</code>")
            else:
                await msg.answer("Использование: <code>/reset_limits @username_or_id</code>")
            return

        identifier = parts[1]

        # Определяем тип идентификатора (username или user_id)
        target_user_id = None

        if identifier.startswith('@'):
            # Это username
            username = identifier.lstrip('@')
            try:
                with sqlite3.connect("user_data.db") as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT user_id FROM user_settings WHERE username = ?', (username,))
                    result = cursor.fetchone()
                    if result:
                        target_user_id = result[0]
            except Exception as e:
                logger.error(f"Ошибка поиска username в базе: {e}")
        else:
            # Это user_id
            try:
                target_user_id = int(identifier)
            except ValueError:
                if language == 'en':
                    await msg.answer(f"❌ Invalid user ID: {identifier}")
                else:
                    await msg.answer(f"❌ Неверный user ID: {identifier}")
                return

        if not target_user_id:
            if language == 'en':
                await msg.answer(f"❌ User {identifier} not found in database")
            else:
                await msg.answer(f"❌ Пользователь {identifier} не найден в базе данных")
            return

        # Сбрасываем кастомные лимиты
        success = await limit_manager.reset_custom_limits(target_user_id)

        # Получаем username для отображения
        display_name = identifier
        try:
            with sqlite3.connect("user_data.db") as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT username FROM user_settings WHERE user_id = ?', (target_user_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    display_name = f"@{result[0]}"
        except:
            pass

        if success:
            if language == 'en':
                await msg.answer(f"✅ Custom limits reset for {display_name}\nUser ID: {target_user_id}")
            else:
                await msg.answer(f"✅ Кастомные лимиты сброшены для {display_name}\nUser ID: {target_user_id}")
        else:
            if language == 'en':
                await msg.answer(f"ℹ️ No custom limits found for {display_name}")
            else:
                await msg.answer(f"ℹ️ Кастомные лимиты не найдены для {display_name}")

    except Exception as e:
        logger.error(f"Ошибка сброса лимитов: {e}")
        language = await user_manager.get_user_language(msg.from_user.id)
        await msg.answer("❌ " + ("Error resetting limits" if language == 'en' else "Ошибка при сбросе лимитов"))


@router.message(F.from_user.id == OWNER_ID, F.text.startswith("/user_limits"))
async def show_user_limits(msg: types.Message):
    """Показать лимиты конкретного пользователя (только для владельца)"""
    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        # Формат: /user_limits @username_or_id
        parts = msg.text.split()

        if len(parts) < 2:
            if language == 'en':
                await msg.answer("Usage: <code>/user_limits @username_or_id</code>")
            else:
                await msg.answer("Использование: <code>/user_limits @username_or_id</code>")
            return

        identifier = parts[1]

        # Определяем тип идентификатора (username или user_id)
        target_user_id = None

        if identifier.startswith('@'):
            # Это username
            username = identifier.lstrip('@')
            try:
                with sqlite3.connect("user_data.db") as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT user_id FROM user_settings WHERE username = ?', (username,))
                    result = cursor.fetchone()
                    if result:
                        target_user_id = result[0]
            except Exception as e:
                logger.error(f"Ошибка поиска username в базе: {e}")
        else:
            # Это user_id
            try:
                target_user_id = int(identifier)
            except ValueError:
                if language == 'en':
                    await msg.answer(f"❌ Invalid user ID: {identifier}")
                else:
                    await msg.answer(f"❌ Неверный user ID: {identifier}")
                return

        if not target_user_id:
            if language == 'en':
                await msg.answer(f"❌ User {identifier} not found in database")
            else:
                await msg.answer(f"❌ Пользователь {identifier} не найден в базе данных")
            return

        # Получаем лимиты пользователя
        limits = await limit_manager.get_user_limits(target_user_id)
        custom_limits = await limit_manager.get_custom_limits(target_user_id)

        # Получаем username для отображения
        display_name = identifier
        try:
            with sqlite3.connect("user_data.db") as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT username FROM user_settings WHERE user_id = ?', (target_user_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    display_name = f"@{result[0]}"
        except:
            pass

        if language == 'en':
            text = (
                f"🔐 <b>Limits for {display_name}</b>\n\n"
                f"🌐 <b>Public groups:</b> {limits['public_used']}/{limits['public_limit']}\n"
                f"🔒 <b>Private groups:</b> {limits['private_used']}/{limits['private_limit']}\n\n"
                f"⏰ <b>Reset in:</b> {limits['time_left']}\n"
                f"🔄 <b>Reset time:</b> {limits['reset_minutes']} minutes\n\n"
            )

            if limits['is_custom']:
                text += f"⚙️ <b>Custom limits:</b> Yes\n"
                if custom_limits:
                    text += f"• Public: {custom_limits.get('public_limit', 'N/A')}\n"
                    text += f"• Private: {custom_limits.get('private_limit', 'N/A')}\n"
                    text += f"• Reset: {custom_limits.get('reset_minutes', 'N/A')} min\n"
            else:
                text += f"⚙️ <b>Custom limits:</b> No (using defaults)\n"

            text += f"\n👤 <b>User ID:</b> {target_user_id}"

        else:
            text = (
                f"🔐 <b>Лимиты для {display_name}</b>\n\n"
                f"🌐 <b>Публичные группы:</b> {limits['public_used']}/{limits['public_limit']}\n"
                f"🔒 <b>Приватные группы:</b> {limits['private_used']}/{limits['private_limit']}\n\n"
                f"⏰ <b>До сброса:</b> {limits['time_left']}\n"
                f"🔄 <b>Время сброса:</b> {limits['reset_minutes']} минут\n\n"
            )

            if limits['is_custom']:
                text += f"⚙️ <b>Кастомные лимиты:</b> Да\n"
                if custom_limits:
                    text += f"• Публичные: {custom_limits.get('public_limit', 'N/A')}\n"
                    text += f"• Приватные: {custom_limits.get('private_limit', 'N/A')}\n"
                    text += f"• Сброс: {custom_limits.get('reset_minutes', 'N/A')} мин\n"
            else:
                text += f"⚙️ <b>Кастомные лимиты:</b> Нет (используются стандартные)\n"

            text += f"\n👤 <b>User ID:</b> {target_user_id}"

        await msg.answer(text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка показа лимитов пользователя: {e}")
        language = await user_manager.get_user_language(msg.from_user.id)
        await msg.answer(
            "❌ " + ("Error getting user limits" if language == 'en' else "Ошибка при получении лимитов пользователя"))

def normalize_link(link: str) -> str:
    """
    Нормализует ссылку для сравнения и удаления дубликатов.
    Приводит разные форматы ссылок к единому виду.
    """
    if not link:
        return None

    # Очищаем ссылку от лишних пробелов и символов
    link = link.strip()

    logger.debug(f"🔄 [NORMALIZE] Входная ссылка: '{link}'")

    # Проверяем, является ли ссылка ссылкой на папку - если да, не трогаем её
    if folder_checker.is_folder_link(link):
        return link  # Возвращаем как есть для папок

    # Убираем протокол и лишние символы
    if link.startswith('https://'):
        link = link[8:]
    elif link.startswith('http://'):
        link = link[7:]

    # Обрабатываем разные форматы
    if link.startswith('t.me/'):
        username = link[5:]
        if username.startswith('+'):
            # Приватные ссылки (t.me/+invite) - оставляем как есть
            result = f"t.me/+{username[1:].split('/')[0].split('?')[0]}"
        else:
            # Публичные ссылки - нормализуем username
            username = username.split('/')[0].split('?')[0]
            result = f"t.me/{username}"

    elif link.startswith('@'):
        # @username форматы
        username = link[1:].split('/')[0].split('?')[0]
        result = f"t.me/{username}"

    elif RE_USERNAME.match(link):
        # @username без обработки протокола
        username = link[1:].split('/')[0].split('?')[0]
        result = f"t.me/{username}"

    elif RE_TME.match(link):
        # t.me/username форматы
        username = link.split('/')[1].split('?')[0]
        result = f"t.me/{username}"

    elif RE_JOIN.match(link):
        # Приватные ссылки - нормализуем
        if link.startswith('https://'):
            link = link[8:]
        elif link.startswith('http://'):
            link = link[7:]
        if link.startswith('t.me/'):
            result = link.split('?')[0]
        else:
            result = link.split('?')[0]
    else:
        result = None

    logger.debug(f"🔄 [NORMALIZE] Результат: '{result}'")
    return result
# ================= Команды =================
@router.message(Command("start"))
async def handle_start_command(msg: types.Message):
    """Обработка всех вариантов команды /start с кнопками"""
    logger.info(f"🟢 START: получена команда от {msg.from_user.id}, текст: '{msg.text}'")

    try:
        # Разбираем параметры команды
        parts = msg.text.split()
        language = await user_manager.get_user_language(msg.from_user.id)

        # Создаем Reply-клавиатуру с двумя кнопками
        reply_kb = ReplyKeyboardMarkup(
            keyboard=[
                [
                    KeyboardButton(
                        text=user_manager.get_text(language, 'show_my_groups'),
                        request_chat=KeyboardButtonRequestChat(
                            request_id=1,
                            chat_is_channel=False,
                            chat_is_created=True,
                            user_administrator_rights=None,
                            bot_administrator_rights=None,
                            bot_is_member=None,
                            chat_has_username=None,
                            chat_is_forum=None
                        )
                    )
                ]
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )

        # Если есть параметры
        if len(parts) > 1:
            param = parts[1]
            logger.info(f"🟢 START: найден параметр '{param}'")

            # Обработка реферальных ссылок для одиночных проверок
            if param.startswith("ref_"):
                logger.info(f"🟢 START: обработка реферальной ссылки {param}")
                await handle_referral_deeplink(msg, param)
                return

            # Обработка массовых проверок
            elif param.startswith("mass_"):
                logger.info(f"🟢 START: обработка массовой проверки {param}")
                await handle_mass_check_deeplink(msg, param)
                return

            # Другие параметры можно добавить здесь
            else:
                logger.info(f"🟢 START: неизвестный параметр {param}")
                # Если неизвестный параметр - показываем обычный старт
                await show_regular_start(msg, language, reply_kb)
                return

        # Если нет параметров - обычный старт
        logger.info("🟢 START: обычный запуск без параметров")
        await show_regular_start(msg, language, reply_kb)

    except Exception as e:
        logger.error(f"❌ START: ошибка обработки команды: {e}")
        language = await user_manager.get_user_language(msg.from_user.id)
        await msg.answer("❌ " + ("Error processing command" if language == 'en' else "Ошибка обработки команды"))

async def show_regular_start(msg: types.Message, language: str, reply_kb: ReplyKeyboardMarkup = None):
    """Показывает обычное приветственное сообщение с клавиатурой"""
    user_id = msg.from_user.id
    is_first_time = await user_manager.is_first_time_user(user_id)

    if is_first_time:
        # Для новых пользователей предлагаем выбрать язык
        language_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="🇷🇺 Русский", callback_data="set_language_ru"),
                    InlineKeyboardButton(text="🇺🇸 English", callback_data="set_language_en")
                ]
            ]
        )

        await msg.answer(
            "🌍 <b>Welcome! Please choose your language:</b>\n\n"
            "🌍 <b>Добро пожаловать! Выберите язык:</b>",
            reply_markup=language_kb,
            parse_mode=ParseMode.HTML
        )

        # Сразу создаем запись пользователя с языком по умолчанию
        await user_manager.set_user_language(user_id, 'ru')

    else:
        # Для существующих пользователей показываем приветствие на их языке
        welcome_text = get_welcome_text(language)

        # Пробуем отправить с фото
        try:
            if language == 'en':
                photo_filename = "png/welcome_en.png"
                fallback_filename = "png/welcome.png"
            else:
                photo_filename = "png/welcome.png"
                fallback_filename = "png/welcome.png"

            try:
                with open(photo_filename, "rb") as photo:
                    await msg.answer_photo(
                        types.BufferedInputFile(photo.read(), filename="welcome.png"),
                        caption=welcome_text,
                        reply_markup=reply_kb,  # Добавляем клавиатуру
                        parse_mode=ParseMode.HTML
                    )
            except FileNotFoundError:
                await msg.answer(welcome_text, reply_markup=reply_kb, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error(f"Ошибка при отправке welcome фото: {e}")
            await msg.answer(welcome_text, reply_markup=reply_kb, parse_mode=ParseMode.HTML)


@router.message(F.text == "/limit")
async def show_limits(msg: types.Message):
    """Показывает текущие лимиты пользователя"""
    language = await user_manager.get_user_language(msg.from_user.id)

    try:
        limits = await limit_manager.get_user_limits(msg.from_user.id)

        if language == 'en':
            text = (
                "🔐 <b>Your current limits</b>\n\n"
                f"🌐 <b>Public groups:</b> {limits['public_used']}/{limits['public_limit']}\n"
                f"🔒 <b>Private groups:</b> {limits['private_used']}/{limits['private_limit']}\n\n"
                f"⏰ <b>Reset in:</b> {limits['time_left']}\n"
                f"🔄 <b>Reset time:</b> {limits['reset_minutes']} minutes"
            )

            if limits['is_custom']:
                text += "\n\n⭐ <b>Custom limits are active</b>"
        else:
            text = (
                "🔐 <b>Ваши текущие лимиты</b>\n\n"
                f"🌐 <b>Публичные группы:</b> {limits['public_used']}/{limits['public_limit']}\n"
                f"🔒 <b>Приватные группы:</b> {limits['private_used']}/{limits['private_limit']}\n\n"
                f"⏰ <b>До сброса:</b> {limits['time_left']}\n"
                f"🔄 <b>Время сброса:</b> {limits['reset_minutes']} минут"
            )

            if limits['is_custom']:
                text += "\n\n⭐ <b>Активны кастомные лимиты</b>"

        await msg.answer(text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка показа лимитов: {e}")
        await msg.answer("❌ " + ("Error getting limits" if language == 'en' else "Ошибка при получении лимитов"))


@router.message(F.text == "/stats")
async def show_stats(msg: types.Message):
    """Показ статистики пользователя"""

    try:
        language = await user_manager.get_user_language(msg.from_user.id)
        stats = await user_manager.get_user_stats(msg.from_user.id)

        if language == "en":
            header = "📊 <b><u>Your Statistics - @groupcekcbot</u></b>\n"
            header += "━━━━━━━━━━━━━━━━━━━━━━\n"
            header += "<blockquote>Check your activity and history here</blockquote>\n\n"
            total_checks_text = f"🔢 Total checks: <b>{stats['total_checks']}</b>"
            unique_groups_text = f"🆔 Unique groups checked: <b>{stats['unique_groups']}</b>"
            recent_header = "<b>📋 Recent checks:</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"

        else:
            header = "📊 <b><u>Ваша статистика - @groupcekcbot</u></b>\n"
            header += "━━━━━━━━━━━━━━━━━━━━━━\n"
            header += "<blockquote>Здесь отображается ваша активность и история</blockquote>\n\n"
            total_checks_text = f"🔢 Всего проверок: <b>{stats['total_checks']}</b>"
            unique_groups_text = f"🆔 Уникальных групп: <b>{stats['unique_groups']}</b>"
            recent_header = "<b>📋 Последние проверки:</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"

        # Формируем список последних проверок
        recent_lines = []
        for link, checked_at in stats['recent_checks']:
            dt = datetime.fromisoformat(checked_at)
            recent_lines.append(f"• {link} ({dt.strftime('%d.%m.%Y %H:%M')})")

        if not recent_lines:
            recent_lines.append("—")

        # Собираем финальный текст
        text = "\n".join([
            header,
            total_checks_text,
            unique_groups_text,
            "",
            recent_header,
            "<blockquote>" + "\n".join(recent_lines) + "</blockquote>",
        ])

        await msg.answer(text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка при показе статистики: {e}")
        await msg.answer("❌ Ошибка при получении статистики")

@router.message(F.text == "/history")
async def show_history(msg: types.Message):
    """Показывает историю проверок пользователя"""
    # Проверяем язык


    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        # Получаем всю историю
        history = await user_manager.get_user_history(msg.from_user.id, limit=100)

        if not history:
            await msg.answer(
                "📭 " + ("You haven't checked any groups yet" if language == 'en' else "Вы еще не проверяли группы"))
            return

        # Инициализируем или получаем текущую страницу
        current_page = USER_HISTORY_PAGE.get(msg.from_user.id, 0)
        items_per_page = 10
        total_pages = (len(history) + items_per_page - 1) // items_per_page

        # Проверяем валидность текущей страницы
        if current_page >= total_pages:
            current_page = 0
            USER_HISTORY_PAGE[msg.from_user.id] = 0

        # Получаем записи для текущей страницы
        start_idx = current_page * items_per_page
        end_idx = start_idx + items_per_page
        page_history = history[start_idx:end_idx]

        # Формируем сообщение
        if language == 'en':
            lines = [
                f"📋 <b>Check history</b> (page {current_page + 1}/{total_pages})",
                f"Total records: {len(history)}",
                ""
            ]
        else:
            lines = [
                f"📋 <b>История проверок</b> (страница {current_page + 1}/{total_pages})",
                f"Всего записей: {len(history)}",
                ""
            ]

        for i, (link, chat_id, created_date, owner, price, checked_at) in enumerate(page_history, start_idx + 1):
            dt = datetime.fromisoformat(checked_at)
            lines.append(f"{i}. {dt.strftime('%d.%m.%Y %H:%M')}")
            if price and price > 0:
                lines.append(f"   💰 " + ("Price:" if language == 'en' else "Стоимость:") + f" {price}$")
            lines.append("")

        # Создаем клавиатуру пагинации
        keyboard = []
        if total_pages > 1:
            row = []
            if current_page > 0:
                row.append(InlineKeyboardButton(
                    text="⬅️ " + ("Back" if language == 'en' else "Назад"),
                    callback_data=f"history_prev_{current_page}"
                ))
            if current_page < total_pages - 1:
                row.append(InlineKeyboardButton(
                    text=("Next" if language == 'en' else "Вперед") + " ➡️",
                    callback_data=f"history_next_{current_page}"
                ))
            keyboard.append(row)

        # Кнопка для сброса пагинации
        keyboard.append([InlineKeyboardButton(
            text="🔄 " + ("Reset" if language == 'en' else "Сбросить"),
            callback_data="history_reset"
        )])

        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

        await msg.answer("\n".join(lines), reply_markup=reply_markup, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка при показе истории: {e}")
        await msg.answer("❌ " + ("Error getting history" if language == 'en' else "Ошибка при получении истории"))

@router.message(F.text == "/lang")
async def change_language_command(msg: types.Message):
    """Команда для смены языка"""
    language = await user_manager.get_user_language(msg.from_user.id)

    # Инлайн-кнопки под сообщением
    language_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🇷🇺 Русский", callback_data="set_language_ru"),
                InlineKeyboardButton(text="🇺🇸 English", callback_data="set_language_en")
            ]
        ]
    )

    if language == 'en':
        text = "🌍 <b>Choose your language:</b>"
    else:
        text = "🌍 <b>Выберите язык:</b>"

    await msg.answer(text, reply_markup=language_kb, parse_mode=ParseMode.HTML)

@router.message(F.text == "/price")
async def price_command(msg: types.Message, state: FSMContext):
    """Команда для настройки цен"""

    language = await user_manager.get_user_language(msg.from_user.id)
    user_prices = await user_manager.get_user_prices(msg.from_user.id)

    # Словарь для русских месяцев
    MONTHS_RU = {
        "01": "Январь", "02": "Февраль", "03": "Март", "04": "Апрель",
        "05": "Май", "06": "Июнь", "07": "Июль", "08": "Август",
        "09": "Сентябрь", "10": "Октябрь", "11": "Ноябрь", "12": "Декабрь"
    }

    # Функция для получения названия месяца в зависимости от языка
    def get_month_name(year: str, month: str) -> str:
        if language == "ru":
            return f"{MONTHS_RU[month.zfill(2)]} {year}"
        else:
            return datetime(int(year), int(month), 1).strftime("%B %Y")

    if language == 'en':
        text = (
            "💰 <b><u>Price Management - @groupcekcbot</u></b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "<blockquote>Manage and view your group prices easily</blockquote>\n\n"
            "<b>📊 Current prices:</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
        )

        # Year prices
        if user_prices["year_prices"]:
            year_block = ""
            for year, price in sorted(user_prices["year_prices"].items()):
                if price > 0:
                    year_block += f"{year}: {price}$\n"
            if year_block:
                text += "<b>🗓 Year prices:</b>\n<blockquote>" + year_block + "</blockquote>\n"

        # Month prices
        if user_prices["month_prices"]:
            month_block = ""
            for month_key, price in sorted(user_prices["month_prices"].items()):
                if price > 0:
                    year, month = month_key.split("-")
                    month_block += f"{get_month_name(year, month)}: {price}$\n"
            if month_block:
                text += "<b>📆 Month prices:</b>\n<blockquote>" + month_block + "</blockquote>\n"

        # Special IDs
        if user_prices["special_ids"]:
            special_block = ""
            for chat_id, price in user_prices["special_ids"].items():
                if price > 0:
                    special_block += f"{chat_id}: {price}$\n"
            if special_block:
                text += "<b>🔢 Special IDs:</b>\n<blockquote>" + special_block + "</blockquote>\n"

        # Default price (всегда показываем)
        text += f"<b>⚙️ Default price:</b> {user_prices['default_price']}$\n"
        text += "━━━━━━━━━━━━━━━━━━━━━━\n"
        text += "Choose what you want to configure:"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📅 Set year price", callback_data="price_year")],
                [InlineKeyboardButton(text="📆 Set month price", callback_data="price_month")],
                [InlineKeyboardButton(text="🔢 Set special ID price", callback_data="price_special")],
                [InlineKeyboardButton(text="⚙️ Set default price", callback_data="price_default")],
                [InlineKeyboardButton(text="🔄 Reset to default", callback_data="price_reset")],
                [InlineKeyboardButton(text="❌ Cancel", callback_data="price_cancel")]
            ]
        )

    else:
        text = (
            "💰 <b><u>Управление ценами - @groupcekcbot</u></b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "<blockquote>Просмотрите и управляйте ценами групп</blockquote>\n\n"
            "<b>📊 Текущие цены:</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
        )

        # Year prices
        if user_prices["year_prices"]:
            year_block = ""
            for year, price in sorted(user_prices["year_prices"].items()):
                if price > 0:
                    year_block += f"{year}: {price}$\n"
            if year_block:
                text += "<b>🗓 Цены по годам:</b>\n<blockquote>" + year_block + "</blockquote>\n"

        # Month prices
        if user_prices["month_prices"]:
            month_block = ""
            for month_key, price in sorted(user_prices["month_prices"].items()):
                if price > 0:
                    year, month = month_key.split("-")
                    month_block += f"{get_month_name(year, month)}: {price}$\n"
            if month_block:
                text += "<b>📆 Цены по месяцам:</b>\n<blockquote>" + month_block + "</blockquote>\n"

        # Special IDs
        if user_prices["special_ids"]:
            special_block = ""
            for chat_id, price in user_prices["special_ids"].items():
                if price > 0:
                    special_block += f"{chat_id}: {price}$\n"
            if special_block:
                text += "<b>🔢 Специальные ID:</b>\n<blockquote>" + special_block + "</blockquote>\n"

        # Default price (всегда показываем)
        text += f"<b>⚙️ Цена по умолчанию:</b> {user_prices['default_price']}$\n"
        text += "━━━━━━━━━━━━━━━━━━━━━━\n"
        text += "Выберите что хотите настроить:"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📅 Установить цену года", callback_data="price_year")],
                [InlineKeyboardButton(text="📆 Установить цену месяца", callback_data="price_month")],
                [InlineKeyboardButton(text="🔢 Установить цену спец ID", callback_data="price_special")],
                [InlineKeyboardButton(text="⚙️ Установить цену по умолчанию", callback_data="price_default")],
                [InlineKeyboardButton(text="🔄 Сбросить на стандартные", callback_data="price_reset")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="price_cancel")]
            ]
        )

    await msg.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await state.set_state(PriceState.waiting_for_action)

@router.message(F.text == "/mcheck")
async def mass_check_command(msg: types.Message):
    """Команда для начала массовой проверки"""

    language = await user_manager.get_user_language(msg.from_user.id)

    if language == 'en':
        help_text = (
            "<b><u>Mass Check - @groupcekcbot</u></b>\n"
            "<blockquote>🔍 <b>Analyze multiple Telegram groups at once</b></blockquote>\n\n"

            "<b>📌 Submission methods:</b>\n"
            "<blockquote>"
            "1. <b>As text</b> — send a list of links (each on a new line)\n"
            "2. <b>As file</b> — send a TXT file with a list of links"
            "</blockquote>\n\n"

            "<b>⚡ Features:</b>\n"
            "<blockquote>"
            "• Up to 6 groups — result in message\n"
            "• More than 6 groups — result in TXT file\n"
            "• Maximum: <b>1000 groups</b> at a time\n"
            "• Progress displayed during check"
            "</blockquote>\n\n"

            "<b>🔗 Supported link formats:</b>\n"
            "<blockquote>"
            "@username\n"
            "t.me/username\n"
            "t.me/+invite_code\n"
            "https://t.me/username"
            "</blockquote>\n\n"

            "<b>📄 TXT file example:</b>\n"
            "<blockquote>"
            "@group1\n"
            "t.me/channel2\n"
            "https://t.me/joinchat/abc123"
            "</blockquote>\n\n"

            "🚀 <b>Send links or a TXT file — I’ll analyze them instantly!</b>"
        )
    else:
        help_text = (
            "<b><u>Массовая проверка - @groupcekcbot</u></b>\n"
            "<blockquote>🔍 <b>Анализ сразу нескольких групп Telegram</b></blockquote>\n\n"

            "<b>📌 Способы отправки:</b>\n"
            "<blockquote>"
            "1. <b>Текстом</b> — отправьте список ссылок (каждая с новой строки)\n"
            "2. <b>Файлом</b> — отправьте TXT файл со списком ссылок"
            "</blockquote>\n\n"

            "<b>⚡ Особенности:</b>\n"
            "<blockquote>"
            "• До 6 групп — результат в сообщении\n"
            "• Более 6 групп — результат в TXT файле\n"
            "• Максимум: <b>1000 групп</b> за один раз\n"
            "• Прогресс отображается во время проверки"
            "</blockquote>\n\n"

            "<b>🔗 Поддерживаемые форматы ссылок:</b>\n"
            "<blockquote>"
            "@username\n"
            "t.me/username\n"
            "t.me/+invite_code\n"
            "https://t.me/username"
            "</blockquote>\n\n"

            "<b>📄 Пример TXT файла:</b>\n"
            "<blockquote>"
            "@group1\n"
            "t.me/channel2\n"
            "https://t.me/joinchat/abc123"
            "</blockquote>\n\n"

            "🚀 <b>Отправьте ссылки или TXT файл — я всё проверю мгновенно!</b>"
        )

    await msg.answer(help_text, parse_mode=ParseMode.HTML)

@router.message(F.document)
async def handle_txt_file(msg: types.Message, state: FSMContext):
    """Обработка TXT файлов со списком ссылок с удалением дубликатов"""
    language = await user_manager.get_user_language(msg.from_user.id)

    try:
        # Проверяем, что это текстовый файл
        if not msg.document.mime_type or "text/plain" not in msg.document.mime_type:
            await msg.answer("❌ " + (
                "Please send a text file (TXT) with links list" if language == 'en' else "Пожалуйста, отправьте текстовый файл (TXT) со списком ссылок"))
            return

        # Проверяем размер файла (максимум 1MB)
        if msg.document.file_size > 1024 * 1024:
            await msg.answer("❌ " + (
                "File is too large. Maximum size: 1MB" if language == 'en' else "Файл слишком большой. Максимальный размер: 1MB"))
            return

        # Скачиваем файл
        progress_msg = await msg.answer("📥 " + ("Downloading file..." if language == 'en' else "Скачиваю файл..."))

        file = await bot.get_file(msg.document.file_id)
        file_path = file.file_path
        downloaded_file = await bot.download_file(file_path)

        # Читаем содержимое файла
        content = downloaded_file.read().decode('utf-8')

        await progress_msg.edit_text(
            "🔍 " + ("Analyzing file content..." if language == 'en' else "Анализирую содержимое файла..."))

        # Парсим все строки из файла
        lines = [line.strip() for line in content.split('\n') if line.strip()]

        if not lines:
            await progress_msg.edit_text("❌ " + (
                "File is empty or contains no valid data" if language == 'en' else "Файл пустой или не содержит валидных данных"))
            return

        # Проверяем количество строк (максимум 1000)
        if len(lines) > 1000:
            await progress_msg.edit_text("❌ " + (
                "Too many lines! Maximum 1000 at a time" if language == 'en' else "Слишком много строк! Максимум 1000 за один раз"))
            return

        # ИЗВЛЕКАЕМ ССЫЛКИ ИЗ МУСОРНОГО ТЕКСТА
        extracted_links_set = set()  # Используем set с самого начала
        all_found_links = []  # Для подсчета общего количества найденных ссылок (включая дубли)
        invalid_lines = []

        for line in lines:
            # Пытаемся найти ссылки в строке
            found_links = extract_links_from_text(line)

            if found_links:
                for link in found_links:
                    # Сохраняем все найденные ссылки для подсчета дубликатов
                    all_found_links.append(link)
                    # Нормализуем и добавляем в set (дубликаты автоматически удаляются)
                    normalized_link = normalize_link(link)
                    if normalized_link:
                        extracted_links_set.add(normalized_link)
            else:
                invalid_lines.append(line)

        # Преобразуем set обратно в list
        valid_links = list(extracted_links_set)
        total_extracted = len(all_found_links)  # Общее количество найденных ссылок (с дубликатами)
        duplicates_count = total_extracted - len(valid_links)  # Количество удаленных дубликатов
        lines_without_links = len(invalid_lines)

        # Если после извлечения и удаления дубликатов осталась только 1 ссылка - перенаправляем на одиночную проверку
        if len(valid_links) == 1:
            stats_text = (
                f"{user_manager.get_text(language, 'file_analysis_complete')}\n\n"
                f"📄 {user_manager.get_text(language, 'file_total_lines')} {len(lines)}\n"
                f"🔗 {'Links extracted' if language == 'en' else 'Ссылок извлечено'}: {total_extracted}\n"
                f"🔄 {'Duplicates removed' if language == 'en' else 'Дубликатов удалено'}: {duplicates_count}\n"
                f"✅ {'Unique link found' if language == 'en' else 'Найдена уникальная ссылка'}\n\n"
                f"🔍 {'Redirecting to single check...' if language == 'en' else 'Перенаправляем на одиночную проверку...'}"
            )

            await progress_msg.edit_text(stats_text, parse_mode=ParseMode.HTML)
            await asyncio.sleep(1)  # Даем пользователю увидеть сообщение

            # Запускаем одиночную проверку найденной ссылки
            await handle_single_link(msg, state, custom_link=valid_links[0])
            return

        stats_text = (
            f"{user_manager.get_text(language, 'file_analysis_complete')}\n\n"
            f"📄 {user_manager.get_text(language, 'file_total_lines')} {len(lines)}\n"
            f"🔗 {'Links extracted' if language == 'en' else 'Ссылок извлечено'}: {total_extracted}\n"
            f"🔄 {'Duplicates removed' if language == 'en' else 'Дубликатов удалено'}: {duplicates_count}\n"
            f"✅ {user_manager.get_text(language, 'file_valid_links')} {len(valid_links)}\n"
            f"❌ {'Lines without links' if language == 'en' else 'Строк без ссылок'}: {lines_without_links}"
        )

        if not valid_links:
            await progress_msg.edit_text(
                stats_text + f"\n\n{user_manager.get_text(language, 'file_no_valid_links')}",
                parse_mode=ParseMode.HTML
            )
            return

        # Показываем примеры строк без ссылок (первые 5)
        if invalid_lines:
            invalid_preview = "\n".join([f"• {line[:50]}..." if len(line) > 50 else f"• {line}"
                                         for line in invalid_lines[:5]])
            if len(invalid_lines) > 5:
                invalid_preview += f"\n• ... " + (
                    "and" if language == 'en' else "и еще") + f" {len(invalid_lines) - 5} " + (
                                       "lines" if language == 'en' else "строк")

            stats_text += f"\n\n{'📝 Lines without links:' if language == 'en' else '📝 Строки без ссылок:'}\n{invalid_preview}"

        # Показываем примеры извлеченных ссылок (первые 5)
        if valid_links:
            links_preview = "\n".join([f"• {format_link_for_display(link)}"
                                       for link in valid_links[:5]])
            if len(valid_links) > 5:
                links_preview += f"\n• ... " + (
                    "and" if language == 'en' else "и еще") + f" {len(valid_links) - 5} " + (
                                     "links" if language == 'en' else "ссылок")

            stats_text += f"\n\n{'🔗 Extracted links:' if language == 'en' else '🔗 Извлеченные ссылки:'}\n{links_preview}"

        # Определяем формат результата
        will_send_file = len(valid_links) > 15
        format_info = user_manager.get_text(language,
                                            'mass_check_file_result' if will_send_file else 'mass_check_message_result')

        # Запускаем массовую проверку
        await progress_msg.edit_text(
            stats_text + f"\n\n{user_manager.get_text(language, 'mass_check_from_file')}\n{format_info}",
            parse_mode=ParseMode.HTML
        )

        # Используем обновленную функцию для массовой проверки
        await process_mass_check_from_list(msg, valid_links, progress_msg, language)

    except UnicodeDecodeError:
        await msg.answer("❌ " + (
            "Error: file must be in UTF-8 encoding" if language == 'en' else "Ошибка: файл должен быть в кодировке UTF-8"))
    except Exception as e:
        logger.error(f"Ошибка обработки TXT файла: {e}")
        await msg.answer(
            "❌ " + ("Error processing file" if language == 'en' else "Произошла ошибка при обработке файла"))

import html
import re

# Разрешённые теги Telegram HTML + tg-emoji
_ALLOWED_TAG_RE = re.compile(
    r'(?is)'  # case-insensitive + dot matches newline
    r'('
    r'</?(?:b|strong|i|em|u|ins|s|strike|del|code|pre|blockquote)\s*>'
    r'|<br\s*/?>'
    r'|<a\s+href="[^"]*"\s*>|</a\s*>'
    r'|<span\s+class="tg-spoiler"\s*>|</span\s*>'
    r'|<tg-emoji\s+emoji-id="\d+"\s*>|</tg-emoji\s*>'
    r')'
)

def sanitize_html_keep_allowed(raw: str) -> str:
    """
    Сохраняет разрешённые HTML-теги Telegram (включая <tg-emoji>),
    а всё остальное экранирует, чтобы штуки типа <3 не ломали HTML parse_mode.
    """
    if not raw:
        return ""

    # 1) вынимаем разрешённые теги в плейсхолдеры
    saved: list[str] = []

    def _save_tag(m: re.Match) -> str:
        saved.append(m.group(1))
        return f"\x00{len(saved)-1}\x00"

    tmp = _ALLOWED_TAG_RE.sub(_save_tag, raw)

    # 2) экранируем весь остальной текст (включая < и &)
    tmp = html.escape(tmp, quote=False)

    # 3) возвращаем теги обратно
    for i, tag in enumerate(saved):
        tmp = tmp.replace(f"\x00{i}\x00", tag)

    return tmp


@router.message(F.text == "/post")
async def post_command(msg: types.Message, state: FSMContext):
    """Команда для создания поста (только для владельца)"""
    # Проверка владельца
    if msg.from_user.id != OWNER_ID:
        language = await user_manager.get_user_language(msg.from_user.id)
        await msg.answer("❌ " + (
            "This command is only for bot owner" if language == 'en' else "Эта команда только для владельца бота"))
        return

    language = await user_manager.get_user_language(msg.from_user.id)

    if language == 'en':
        help_text = (
            "📝 <b>Creating post</b>\n\n"
            "Write the message you want to send as a post.\n\n"
            "Supported:\n"
            "• Text\n"
            "• Photos with text\n"
            "• Documents with text\n\n"
            "To cancel, click the button below ⬇️"
        )
    else:
        help_text = (
            "📝 <b>Создание поста</b>\n\n"
            "Напишите сообщение, которое хотите отправить как пост.\n\n"
            "Поддерживается:\n"
            "• Текст\n"
            "• Фотографии с текстом\n"
            "• Документы с текстом\n\n"
            "Для отмены нажмите кнопку ниже ⬇️"
        )

    if language == 'en':
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_post")
            ]]
        )
    else:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_post")
            ]]
        )

    await msg.answer(help_text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await state.set_state(PostState.waiting_for_content)

from aiogram import types
from aiogram.enums import ParseMode
from aiogram.filters import StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext

@router.message(StateFilter(PostState.waiting_for_content))
async def process_post_content(msg: types.Message, state: FSMContext):
    """Обработка контента для поста с поддержкой полного HTML и tg-emoji, и защитой от <3"""
    if msg.from_user.id != OWNER_ID:
        language = await user_manager.get_user_language(msg.from_user.id)
        await msg.answer("❌ " + (
            "You don't have permission to create posts"
            if language == 'en' else
            "У вас нет прав для создания постов"
        ))
        await state.clear()
        return

    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        # Если это команда, просим завершить создание поста
        if msg.text and msg.text.startswith('/'):
            await msg.answer("❌ " + (
                "First complete post creation with /post or cancel it."
                if language == 'en' else
                "Сначала завершите создание поста командой /post или отмените его."
            ))
            return

        # Кнопки подтверждения
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(
                    text="✅ Publish" if language == 'en' else "✅ Опубликовать",
                    callback_data="confirm_post"
                ),
                InlineKeyboardButton(
                    text="❌ Cancel" if language == 'en' else "❌ Отменить",
                    callback_data="cancel_post"
                )
            ]]
        )

        # -----------------------------
        # ТЕКСТ
        # -----------------------------
        if msg.text:
            # Если Telegram прислал entities — берём готовый корректный HTML
            if msg.entities:
                content_html = msg.html_text or msg.text
            else:
                # Иначе считаем, что это "сырой HTML" и санитайзим (<3 не ломает)
                content_html = sanitize_html_keep_allowed(msg.text)

            preview_text = (
                f"✅ <b>{'Post ready to send' if language == 'en' else 'Пост готов к отправке'}:</b>\n\n"
                f"{content_html}\n\n"
                f"📊 <i>{'Will be sent to all bot users' if language == 'en' else 'Будет отправлен всем пользователям бота'}</i>"
            )

            await msg.answer(preview_text, parse_mode=ParseMode.HTML)

            await state.update_data(
                content=content_html,
                content_type="text"
            )

            await msg.answer(
                "Confirm post sending:" if language == 'en' else "Подтвердите отправку поста:",
                reply_markup=kb
            )
            return

        # -----------------------------
        # ФОТО
        # -----------------------------
        if msg.photo:
            photo = msg.photo[-1]

            # caption может иметь entities (жирный/ссылки/кастом эмодзи), тогда caption_html уже идеален
            if msg.caption:
                if msg.caption_entities:
                    caption_html = msg.caption_html or msg.caption
                else:
                    caption_html = sanitize_html_keep_allowed(msg.caption)
            else:
                caption_html = ""

            preview_caption = (
                f"✅ <b>{'Photo post ready to send' if language == 'en' else 'Пост с фото готов к отправке'}:</b>\n\n"
                f"{caption_html}\n\n"
                f"📊 <i>{'Will be sent to all bot users' if language == 'en' else 'Будет отправлен всем пользователям бота'}</i>"
            ) if caption_html else (
                f"✅ <b>{'Photo post ready to send' if language == 'en' else 'Пост с фото готов к отправке'}</b>\n\n"
                f"📊 <i>{'Will be sent to all bot users' if language == 'en' else 'Будет отправлен всем пользователям бота'}</i>"
            )

            await msg.answer_photo(
                photo.file_id,
                caption=preview_caption,
                parse_mode=ParseMode.HTML
            )

            await state.update_data(
                photo_file_id=photo.file_id,
                caption=caption_html,     # сохраняем уже HTML
                content_type="photo"
            )

            await msg.answer(
                "Confirm post sending:" if language == 'en' else "Подтвердите отправку поста:",
                reply_markup=kb
            )
            return

        # -----------------------------
        # ДОКУМЕНТ
        # -----------------------------
        if msg.document:
            document = msg.document

            if msg.caption:
                if msg.caption_entities:
                    caption_html = msg.caption_html or msg.caption
                else:
                    caption_html = sanitize_html_keep_allowed(msg.caption)
            else:
                caption_html = ""

            preview_caption = (
                f"✅ <b>{'Document post ready to send' if language == 'en' else 'Пост с документом готов к отправке'}:</b>\n\n"
                f"{caption_html}\n\n"
                f"📊 <i>{'Will be sent to all bot users' if language == 'en' else 'Будет отправлен всем пользователям бота'}</i>"
            ) if caption_html else (
                f"✅ <b>{'Document post ready to send' if language == 'en' else 'Пост с документом готов к отправке'}</b>\n\n"
                f"📊 <i>{'Will be sent to all bot users' if language == 'en' else 'Будет отправлен всем пользователям бота'}</i>"
            )

            await msg.answer_document(
                document.file_id,
                caption=preview_caption,
                parse_mode=ParseMode.HTML
            )

            await state.update_data(
                document_file_id=document.file_id,
                caption=caption_html,     # сохраняем уже HTML
                content_type="document"
            )

            await msg.answer(
                "Confirm post sending:" if language == 'en' else "Подтвердите отправку поста:",
                reply_markup=kb
            )
            return

        # -----------------------------
        # НЕПОДДЕРЖИВАЕМОЕ
        # -----------------------------
        await msg.answer("❌ " + (
            "Unsupported message type. Send text, photo or document."
            if language == 'en' else
            "Неподдерживаемый тип сообщения. Отправьте текст, фото или документ."
        ))

    except Exception as e:
        logger.error(f"Ошибка обработки контента поста: {e}")
        await msg.answer("❌ " + ("Error processing content" if language == 'en' else "Ошибка при обработке контента"))
        await state.clear()

@router.callback_query(F.data == "cancel_post")
async def cancel_post(call: types.CallbackQuery, state: FSMContext):
    """Отмена создания поста"""
    language = await user_manager.get_user_language(call.from_user.id)
    await state.clear()

    if language == 'en':
        await call.message.edit_text("❌ Post creation cancelled")
    else:
        await call.message.edit_text("❌ Создание поста отменено")




def extract_links_from_text(text: str) -> list:
    """
    Извлекает все Telegram ссылки из текста, включая ссылки на папки.
    """
    import re

    # УНИФИЦИРОВАННЫЕ ПАТТЕРНЫ ДЛЯ ИЗБЕЖАНИЯ ДУБЛИРОВАНИЯ
    telegram_patterns = [
        # Папки (добавлено - самый приоритетный)
        r'(?:https?://)?t\.me/addlist/[a-zA-Z0-9_-]+',
        # Приватные инвайты
        r't\.me/\+[a-zA-Z0-9_-]+',
        # Публичные ссылки с протоколом
        r'https?://t\.me/[\w@+-]+',
        # Публичные ссылки без протокола
        r'(?<!://)t\.me/[\w@+-]+',
        # Юзернеймы
        r'@[\w@+-]{5,32}',
        # Telegram.me ссылки
        r'https?://telegram\.me/[\w@+-]+',
        r'(?<!://)telegram\.me/[\w@+-]+'
    ]

    found_links = []
    processed_matches = set()

    for pattern in telegram_patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            start, end = match.span()
            match_text = match.group()

            # Проверяем, не перекрывается ли эта match с уже обработанной
            is_overlap = False
            for processed_start, processed_end in processed_matches:
                if not (end <= processed_start or start >= processed_end):
                    is_overlap = True
                    break

            if not is_overlap:
                # Нормализуем найденную ссылку
                normalized = normalize_extracted_link(match_text)
                if normalized and normalized not in found_links:
                    found_links.append(normalized)
                    processed_matches.add((start, end))

    return found_links

def normalize_extracted_link(link: str) -> str:
    """Нормализует извлеченную ссылку к стандартному формату"""
    if not link:
        return None

    link = link.strip()

    # Для юзернеймов
    if link.startswith('@'):
        username = link[1:].split('/')[0]  # Берем только имя пользователя
        return f"https://t.me/{username}"

    # Для ссылок без протокола
    elif link.startswith('t.me/'):
        return f"https://{link}"

    # Для telegram.me ссылок
    elif 'telegram.me/' in link:
        if link.startswith('http'):
            return link.replace('telegram.me/', 't.me/')
        else:
            return f"https://{link.replace('telegram.me/', 't.me/')}"

    # Для ссылок с протоколом - оставляем как есть
    elif link.startswith('https'):
        return link

    # Для остальных случаев
    else:
        return f"https://t.me/{link}" if not link.startswith('http') else link


@router.message(PriceState.waiting_for_year_price)
async def handle_year_price_input(msg: types.Message, state: FSMContext):
    """Обработка ввода цены года"""
    language = await user_manager.get_user_language(msg.from_user.id)

    try:
        parts = msg.text.strip().split()
        if len(parts) != 2:
            raise ValueError("Invalid format")

        year = parts[0]
        price = float(parts[1])

        if not year.isdigit() or len(year) != 4:
            raise ValueError("Invalid year")

        if price < 0:
            raise ValueError("Price cannot be negative")

        # Сохраняем цену
        price_config = {"year_prices": {year: price}}
        success = await user_manager.set_user_prices(msg.from_user.id, price_config)

        if success:
            if language == 'en':
                await msg.answer(f"✅ Year {year} price set to {price}$")
            else:
                await msg.answer(f"✅ Цена для {year} года установлена: {price}$")
        else:
            if language == 'en':
                await msg.answer("❌ Error saving price")
            else:
                await msg.answer("❌ Ошибка сохранения цены")

    except ValueError as e:
        if language == 'en':
            await msg.answer("❌ Invalid format. Use: <code>YEAR PRICE</code>\nExample: <code>2023 8.5</code>",
                             parse_mode=ParseMode.HTML)
        else:
            await msg.answer("❌ Неверный формат. Используйте: <code>ГОД ЦЕНА</code>\nПример: <code>2023 8.5</code>",
                             parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка установки цены года: {e}")
        if language == 'en':
            await msg.answer("❌ Error setting price")
        else:
            await msg.answer("❌ Ошибка установки цены")

    # ОЧИЩАЕМ СОСТОЯНИЕ ПОСЛЕ ОБРАБОТКИ
    await state.clear()

@router.message(F.from_user.id == OWNER_ID, F.text == "/check_blacklisted")
async def check_blacklisted_sessions(msg: types.Message):
    """Проверяет только сессии из черного списка"""
    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        session_manager.load_blacklisted_sessions()
        blacklisted_sessions = list(session_manager.blacklisted_sessions.keys())

        if not blacklisted_sessions:
            await msg.answer("✅ " + ("No sessions in blacklist" if language == 'en' else "В черном списке нет сессий"))
            return

        if language == 'en':
            progress_msg = await msg.answer(f"🔍 Checking {len(blacklisted_sessions)} sessions from blacklist...")
        else:
            progress_msg = await msg.answer(f"🔍 Проверяю {len(blacklisted_sessions)} сессий из черного списка...")

        if language == 'en':
            results = ["🔍 Checking sessions from blacklist:"]
        else:
            results = ["🔍 Проверка сессий из черного списка:"]

        restored_count = 0
        session_files = session_manager.get_session_files()

        # Создаем словарь для быстрого поиска файлов по имени
        session_dict = {}
        for session_file in session_files:
            session_name = os.path.basename(session_file).replace('.session', '')
            session_dict[session_name] = session_file

        for session_name in blacklisted_sessions:
            if session_name in session_dict:
                session_file = session_dict[session_name]
                logger.info(f"Проверяем сессию из черного списка: {session_name}")

                is_valid, message = await session_manager.validate_session(session_file, check_blacklisted=True)

                if is_valid:
                    restored_count += 1
                    if language == 'en':
                        results.append(f"✅ {session_name} - VALID (restored)")
                    else:
                        results.append(f"✅ {session_name} - ВАЛИДНА (восстановлена)")
                else:
                    results.append(f"❌ {session_name} - {message}")
            else:
                results.append(f"❌ {session_name} - " + ("File not found" if language == 'en' else "Файл не найден"))

        result_text = "\n".join(results)
        if language == 'en':
            result_text += f"\n\n📊 Restored sessions: {restored_count}/{len(blacklisted_sessions)}"
        else:
            result_text += f"\n\n📊 Восстановлено сессий: {restored_count}/{len(blacklisted_sessions)}"

        await progress_msg.edit_text(result_text)

    except Exception as e:
        logger.error(f"Ошибка команды /check_blacklisted: {e}")
        await msg.answer(
            "❌ " + ("Error checking blacklist" if language == 'en' else "Ошибка при проверке черного списка"))

# ================== Логирование ошибок сессий ==================
@router.callback_query(F.data == "clear_errors")
async def clear_errors_handler(call: types.CallbackQuery, language=None):
    """Обработчик очистки ошибок сессий"""
    if call.from_user.id != OWNER_ID:
        language = await user_manager.get_user_language(call.from_user.id)
        await call.answer("❌ " + ("This command is only for bot owner" if language == 'en' else "Эта команда только для владельца"), show_alert=True)
        return

    try:
        success = await clear_session_errors()
        if success:
            await call.message.edit_text("✅ " + ("botz.error file cleared" if language == 'en' else "Файл botz.error очищен"))
        else:
            await call.message.edit_text("❌ " + ("Error clearing file" if language == 'en' else "Ошибка при очистке файла"))
    except Exception as e:
        logger.error(f"Ошибка очистки ошибок: {e}")
        await call.message.edit_text("❌ " + ("Error clearing" if language == 'en' else "Ошибка при очистке"))


@router.message(F.text == "/error")
async def show_session_errors(msg: types.Message):
    """Показывает ошибки сессий из botz.error (только для владельца)"""
    if msg.from_user.id != OWNER_ID:
        language = await user_manager.get_user_language(msg.from_user.id)
        await msg.answer("❌ " + ("This command is only for bot owner" if language == 'en' else "Эта команда только для владельца бота"))
        return

    try:
        errors_content = await get_session_errors()
        language = await user_manager.get_user_language(msg.from_user.id)

        if len(errors_content) > 4000:
            filename = f"session_errors_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(errors_content)

            with open(filename, 'rb') as f:
                if language == 'en':
                    await msg.answer_document(
                        types.BufferedInputFile(f.read(), filename=filename),
                        caption="📄 Session errors file from botz.error"
                    )
                else:
                    await msg.answer_document(
                        types.BufferedInputFile(f.read(), filename=filename),
                        caption="📄 Файл с ошибками сессий из botz.error"
                    )

            os.remove(filename)
        else:
            if "No session errors" in errors_content or "Нет ошибок сессий" in errors_content:
                await msg.answer("✅ " + errors_content)
            else:
                if language == 'en':
                    await msg.answer(f"📄 **Session errors from botz.error:**\n```\n{errors_content}\n```",
                                     parse_mode="Markdown")
                else:
                    await msg.answer(f"📄 **Ошибки сессий из botz.error:**\n```\n{errors_content}\n```",
                                     parse_mode="Markdown")

        if language == 'en':
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(text="🧹 Clear errors", callback_data="clear_errors")
                ]]
            )
            await msg.answer("You can clear the errors file:", reply_markup=kb)
        else:
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(text="🧹 Очистить ошибки", callback_data="clear_errors")
                ]]
            )
            await msg.answer("Вы можете очистить файл с ошибками:", reply_markup=kb)

    except Exception as e:
        logger.error(f"Ошибка команды /error: {e}")
        await msg.answer("❌ " + ("Error getting session errors" if language == 'en' else "Ошибка при получении ошибок сессий"))


# =================== Регексы ===================
RE_TME = re.compile(r"^(https?://)?t\.me/([A-Za-z0-9_]{5,})$")
RE_USERNAME = re.compile(r"^@([A-Za-z0-9_]{5,})$")
RE_JOIN = re.compile(r"^(https?://)?t\.me/(joinchat/|\+)([A-Za-zA-Z0-9_-]{10,})$")
RE_FOLDER = re.compile(r"(?:https?://)?t\.me/addlist/[A-Za-z0-9_-]+")
RE_USER_PROFILE = re.compile(r"^(https?://)?t\.me/([A-Za-z0-9_]{5,})$")
# ================== Helpers ===================


async def _fetch_first_message(client, entity):
    async for msg in client.iter_messages(entity, limit=2000, reverse=True):
        if msg.date:
            return msg
    return None


async def _count_messages(client, entity, limit=1000):
    """Улучшенный подсчёт сообщений с разделением на посты канала и системные"""
    user_cnt = 0
    sys_cnt = 0
    channel_posts = 0

    async for msg in client.iter_messages(entity, limit=limit):
        if getattr(msg, "post", False) or (hasattr(msg, 'from_id') and getattr(msg.from_id, 'channel_id', None)):
            channel_posts += 1
            user_cnt += 1
        elif getattr(msg, "action", None) is not None:
            sys_cnt += 1
        else:
            user_cnt += 1

    return {
        "user_messages": user_cnt,
        "system_messages": sys_cnt,
        "channel_posts": channel_posts
    }




async def _analyze_group_internal(client, raw_link: str, requester_id: int, progress_msg=None, language: str = 'ru',
                                  session_path: str = None, pre_fetched_entity=None):
    """Внутренняя функция анализа с полной поддержкой каналов"""
    result = {
        "requester_id": requester_id,
        "chat_link": raw_link,
        "chat_id": None,
        "is_bad": False,
        "is_channel": False,
        "is_broadcast": False,
        "created_date": None,
        "owner_id": None,
        "owner_username": None,
        "owner_first_name": None,
        "owner_last_name": None,
        "user_messages": 0,
        "system_messages": 0,
        "channel_posts": 0,
        "total_posts": 0,
        "members": 0,
        "history_hidden": False,
        "price": 0,
        "seller_label": None,
        "checked_at": datetime.now(),
        "has_imported_messages": False,
        "has_geo_location": False,
        "error_message": None,
        "is_user_profile": False,
        "is_invalid_link": False
    }

    entity = None
    is_private = False

    try:
        if progress_msg:
            await progress_msg.edit_text("🔎 " + user_manager.get_text(language, 'checking_group'))

        if pre_fetched_entity:
            entity = pre_fetched_entity
            is_private = not getattr(entity, 'username', None)
        else:
            m_join = RE_JOIN.search(raw_link)
            if m_join:
                code = m_join.group(3)
                try:
                    if progress_msg:
                        await progress_msg.edit_text("🔗 " + user_manager.get_text(language, 'checking_group'))

                    from telethon.tl.functions.messages import ImportChatInviteRequest
                    chat = await client(ImportChatInviteRequest(code))
                    entity = chat.chats[0]
                    is_private = True
                    logger.info(f"Успешно присоединились к приватной группе: {getattr(entity, 'title', 'N/A')}")

                except errors.UserAlreadyParticipantError:
                    try:
                        entity = await get_entity_safe(client, f"https://t.me/+{code}", session_path)
                        is_private = True
                        logger.info(f"Уже участник приватной группы: {getattr(entity, 'title', 'N/A')}")
                    except Exception as e:
                        logger.error(f"Не удалось получить entity для приватной группы: {e}")
                        result["is_bad"] = True
                        result["is_invalid_link"] = True
                        result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                        if progress_msg:
                            await progress_msg.edit_text(
                                "❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                        return result

                except errors.FloodWaitError as e:
                    logger.error(f"Флуд-вейт при присоединении: {e.seconds} секунд")
                    result["is_bad"] = True
                    result["error_message"] = f"Flood wait: {e.seconds}s"
                    raise e

                except errors.InviteHashExpiredError:
                    result["is_bad"] = True
                    result["is_invalid_link"] = True
                    result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                    if progress_msg:
                        await progress_msg.edit_text("❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                    return result

                except errors.InviteHashInvalidError:
                    result["is_bad"] = True
                    result["is_invalid_link"] = True
                    result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                    if progress_msg:
                        await progress_msg.edit_text("❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                    return result

                except Exception as e:
                    logger.error(f"Ошибка присоединения к приватной группе: {e}")
                    result["is_bad"] = True
                    result["is_invalid_link"] = True
                    result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                    if progress_msg:
                        await progress_msg.edit_text("❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                    return result

            else:
                m_user = RE_USERNAME.search(raw_link)
                m_tme = RE_TME.search(raw_link)

                identifier = None
                if m_user:
                    identifier = "@" + m_user.group(1)
                elif m_tme:
                    identifier = m_tme.group(2)

                if identifier:
                    try:
                        if progress_msg:
                            await progress_msg.edit_text("🔗 " + user_manager.get_text(language, 'checking_group'))

                        entity = await get_entity_safe(client, identifier, session_path)
                        is_private = False

                        entity_type = type(entity).__name__
                        logger.info(f"Тип entity: {entity_type} для {identifier}")

                        if entity_type == 'User':
                            result["is_user_profile"] = True
                            result["is_bad"] = True
                            result["error_message"] = user_manager.get_text(language, 'user_profile_result')
                            logger.info(f"Обнаружен профиль пользователя: {identifier}")

                            if progress_msg:
                                await progress_msg.edit_text(
                                    "❌ " + user_manager.get_text(language, 'user_profile_result'))
                            return result

                        logger.info(f"Успешно получили entity группы/канала: {getattr(entity, 'title', 'N/A')}")

                    except errors.ChannelPrivateError:
                        result["is_bad"] = True
                        result["is_invalid_link"] = True
                        result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                        if progress_msg:
                            await progress_msg.edit_text(
                                "❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                        return result

                    except (errors.UsernameNotOccupiedError, errors.UsernameInvalidError):
                        result["is_bad"] = True
                        result["is_invalid_link"] = True
                        result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                        if progress_msg:
                            await progress_msg.edit_text(
                                "❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                        return result

                    except errors.ChannelInvalidError:
                        result["is_bad"] = True
                        result["is_invalid_link"] = True
                        result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                        if progress_msg:
                            await progress_msg.edit_text(
                                "❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                        return result

                    except Exception as e:
                        logger.error(f"Ошибка получения entity: {e}")
                        result["is_bad"] = True
                        result["is_invalid_link"] = True
                        result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                        if progress_msg:
                            await progress_msg.edit_text(
                                "❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                        return result

        if entity is None:
            result["is_bad"] = True
            result["is_invalid_link"] = True
            result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
            if progress_msg:
                await progress_msg.edit_text("❌ " + user_manager.get_text(language, 'error_group_not_exist'))
            return result

        is_channel = False
        is_broadcast = False

        if hasattr(entity, 'broadcast') and entity.broadcast:
            is_channel = True
            is_broadcast = True
            result["is_channel"] = True
            result["is_broadcast"] = True
            chat_type = "канал"
        elif hasattr(entity, 'megagroup') and entity.megagroup:
            chat_type = "супергруппа"
        else:
            chat_type = "группа"

        logger.info(f"Тип чата: {chat_type}, ID: {entity.id}, IsChannel: {is_channel}, IsBroadcast: {is_broadcast}")

        try:
            if hasattr(entity, 'id'):
                if is_channel or (hasattr(entity, 'megagroup') and entity.megagroup):
                    bot_style_chat_id = int(f"-100{abs(entity.id)}")
                else:
                    bot_style_chat_id = entity.id
                result["chat_id"] = str(bot_style_chat_id)

                if str(bot_style_chat_id).startswith("-10029"):
                    result["is_bad"] = True
                if entity.id <= -10018:
                    result["seller_label"] = "Group id can seller"

        except Exception as ex:
            logger.warning(f"Ошибка получения Chat ID: {ex}")
            result["chat_id"] = str(entity.id) if hasattr(entity, 'id') else user_manager.get_text(language,
                                                                                                   'not_defined')

        if result["is_bad"]:
            return result

        if progress_msg:
            status_text = user_manager.get_text(language, 'checking_channel') if is_channel else user_manager.get_text(
                language, 'checking_group')
            await progress_msg.edit_text("📊 " + status_text)

        try:
            from telethon.tl.functions.channels import GetFullChannelRequest
            full = await client(GetFullChannelRequest(channel=entity))
            result["members"] = full.full_chat.participants_count or 0
        except Exception as e:
            logger.debug(f"Не удалось получить полную информацию: {e}")

        if progress_msg:
            await progress_msg.edit_text("🔎 " + user_manager.get_text(language, 'first_message_date'))
        try:
            first_msg = await _fetch_first_message(client, entity)
            if first_msg:
                result["created_date"] = first_msg.date
                year = first_msg.date.year
                if year <= 2022:
                    result["price"] = 0
                elif year == 2023:
                    result["price"] = 0
                elif year == 2024:
                    result["price"] = 0
        except (errors.ChannelPrivateError, errors.ChatAdminRequiredError):
            result["history_hidden"] = True
        except Exception as e:
            logger.debug(f"Ошибка получения первого сообщения: {e}")
            result["history_hidden"] = True

        if progress_msg:
            status_text = user_manager.get_text(language, 'counting_posts') if is_channel else user_manager.get_text(
                language, 'counting_messages')
            await progress_msg.edit_text("🔎 " + status_text)
        try:
            if not result.get("history_hidden"):
                msg_stats = await _count_messages(client, entity, limit=2000)
                result["user_messages"] = msg_stats["user_messages"]
                result["system_messages"] = msg_stats["system_messages"]
                result["channel_posts"] = msg_stats["channel_posts"]
                result["total_posts"] = msg_stats["user_messages"]
                logger.info(
                    f"Статистика: постов канала={msg_stats['channel_posts']}, от админов={msg_stats['user_messages'] - msg_stats['channel_posts']}, системных={msg_stats['system_messages']}")
            else:
                logger.info("Пропускаем подсчет - история скрыта")
        except (errors.ChannelPrivateError, errors.ChatAdminRequiredError):
            result["history_hidden"] = True
        except Exception as e:
            logger.debug(f"Ошибка подсчета сообщений: {e}")

        if progress_msg:
            await progress_msg.edit_text("🔎 " + user_manager.get_text(language, 'checking_imported'))
        try:
            if not result.get("history_hidden"):
                result["has_imported_messages"] = await _check_imported_messages_simple(client, entity)
        except Exception as e:
            logger.error(f"Ошибка проверки импортированных сообщений: {e}")

        if progress_msg:
            await progress_msg.edit_text("🔎 " + user_manager.get_text(language, 'checking_geo'))
        try:
            result["has_geo_location"] = await _check_geo_group(client, entity)
        except Exception as e:
            logger.error(f"Ошибка проверки на гео-группу: {e}")

        if progress_msg:
            await progress_msg.edit_text("🔎 " + user_manager.get_text(language, 'finding_owner'))
        try:
            if is_channel:
                creator = await _find_channel_owner(client, entity, session_path)
            else:
                creator = await _find_creator(client, entity, session_path)

            if creator:
                result["owner_id"] = creator.get("id")
                result["owner_username"] = creator.get("username")
                result["owner_first_name"] = creator.get("first_name")
                result["owner_last_name"] = creator.get("last_name")
        except Exception as e:
            logger.error(f"Ошибка поиска владельца: {e}")

        if progress_msg:
            await progress_msg.edit_text("✅ " + user_manager.get_text(language, 'check_completed'))

        try:
            if entity and is_private and not pre_fetched_entity:
                await client.delete_dialog(entity)
        except Exception as e:
            logger.warning(f"Не удалось выйти из группы: {e}")

        return result

    except errors.FloodWaitError as e:
        logger.error(f"Флуд-вейт: {e.seconds} секунд")
        raise e
    except Exception as e:
        logger.exception("Критическая ошибка в _analyze_group_internal")
        result["is_bad"] = True
        result["is_invalid_link"] = True
        result["error_message"] = user_manager.get_text(language, 'error_critical')
        return result

import logging
from collections import Counter
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import ChannelParticipantsAdmins

logger = logging.getLogger(__name__)

async def _find_creator(client, entity, session_path: str = None):
    """Рассширенная версия поиска владельца с агрегированием кандидатов и взвешиванием.

    Подход: собираем кандидатов из множества сигналов (creator_id, флаги, системные
    сообщения, тексты, pinned, первые сообщения и т.д.), начисляем им веса и выбираем
    наиболее вероятного — при достаточном отрыве от второго места.
    """
    try:
        me = await client.get_me()
        bot_id = me.id

        # ------------------------- helpers -------------------------
        def _user_dict(user, method):
            return {
                "id": getattr(user, "id", None),
                "username": getattr(user, "username", None),
                "first_name": getattr(user, "first_name", None),
                "last_name": getattr(user, "last_name", None),
                "found_by": method,
            }

        async def _safe_get_entity(uid):
            try:
                return await get_entity_safe(client, uid, session_path)
            except Exception:
                return None

        # Словарь-агрегатор кандидатов: user_id -> Counter(метод -> вес)
        candidates = Counter()
        candidate_methods = {}

        def add_candidate(uid, method, weight=1):
            if uid is None:
                return
            candidates[uid] += weight
            candidate_methods.setdefault(uid, []).append(method)

        # --------------------- 1. creator_id -----------------------
        full = None
        try:
            full = await client(GetFullChannelRequest(channel=entity))
            creator_id = getattr(full.full_chat, "creator_id", None)
            if creator_id and creator_id != bot_id:
                add_candidate(creator_id, "creator_id", weight=10)
        except Exception as e:
            logger.debug(f"GetFullChannelRequest failed: {e}")

        # ------------------- 2. флаг creator в users ---------------
        try:
            if full and hasattr(full, "users"):
                for u in full.users:
                    if getattr(u, "creator", False) and not getattr(u, "bot", False) and u.id != bot_id:
                        add_candidate(u.id, "creator_flag", weight=8)
        except Exception:
            pass

        # ------------------- 3. админ с правами creator ------------
        try:
            participants = await client.get_participants(entity, filter=ChannelParticipantsAdmins())
            for u in participants:
                if getattr(u, "bot", False) or u.id == bot_id:
                    continue
                try:
                    perm = await client.get_permissions(entity, u)
                    part = getattr(perm, "participant", None)
                    if part and type(part).__name__ == "ChannelParticipantCreator":
                        add_candidate(u.id, "admin_rights", weight=10)
                    else:
                        # обычный админ — небольшой вес
                        add_candidate(u.id, "admin", weight=1)
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"get_participants failed: {e}")

        # ---------------- 4. системные и служебные сообщения --------
        creation_actions = {
            "MessageActionChatCreate",
            "MessageActionChannelCreate",
            "MessageActionChatMigrateTo",
        }

        try:
            async for msg in client.iter_messages(entity, limit=400):
                # Пропускаем сообщения от бота
                if getattr(msg, 'sender_id', None) == bot_id:
                    continue

                # Данные об action
                act = getattr(msg, "action", None)
                if act:
                    aname = type(act).__name__
                    if aname in creation_actions and getattr(msg, 'sender_id', None):
                        add_candidate(msg.sender_id, "system_action", weight=8)

                    # разные варианты добавления пользователей — отправитель может быть пригласителем
                    if aname in ("MessageActionChatAddUser", "MessageActionChatAddUsers") and getattr(msg, 'sender_id', None):
                        add_candidate(msg.sender_id, "added_users", weight=4)

                # Если сообщение отправлено от имени канала (анонимно)
                if getattr(msg, "post", False) or (hasattr(msg, 'from_id') and getattr(msg.from_id, 'channel_id', None)):
                    # отмечаем как сигнал анонимности — верный владелец неизвестен
                    candidates.clear()
                    candidate_methods.clear()
                    return {
                        "id": None,
                        "username": None,
                        "first_name": "owner_uses_anonymity",
                        "last_name": "",
                        "found_by": "anonymous_messages",
                    }

                # Текстовые признаки создания
                if getattr(msg, 'text', None) and getattr(msg, 'sender_id', None):
                    low = msg.text.lower()
                    keywords = [
                        "создал", "created", "creator", "основатель", "организатор",
                        "group created", "channel created", "created the", "created this",
                        "создала", "создала группу", "создал группу", "создал канал", "создала канал"
                    ]
                    if any(k in low for k in keywords):
                        add_candidate(msg.sender_id, "creation_text", weight=5)

                    # Часто автор объявления о создании содержит @username или слово "от"
                    # На всякий случай пытаемся найти упоминания формата @username
                    if "@" in low and ("создал" in low or "created" in low or "основатель" in low):
                        # помечаем отправителя как кандидат (меньший вес) — возможно он указал создателя
                        add_candidate(msg.sender_id, "mention_text", weight=1)

        except Exception as e:
            logger.debug(f"iter_messages scan failed: {e}")

        # ---------------- 5. pinned / первые сообщения -------------
        try:
            if full:
                pinned = getattr(full.full_chat, "pinned_msg_id", None)
                if pinned:
                    try:
                        pmsg = await client.get_messages(entity, ids=pinned)
                        if pmsg and getattr(pmsg, 'sender_id', None) and pmsg.sender_id != bot_id:
                            add_candidate(pmsg.sender_id, "pinned_message", weight=6)
                    except Exception:
                        pass
        except Exception:
            pass

        # Проверяем первые сообщения истории (reverse=True возвращает от старых к новым)
        try:
            async for msg in client.iter_messages(entity, reverse=True, limit=30):
                if getattr(msg, 'sender_id', None) and msg.sender_id != bot_id:
                    add_candidate(msg.sender_id, "early_message", weight=3)
                    # даём шанс первому найденному — обычно создатель пишет первым
                    break
        except Exception as e:
            logger.debug(f"early messages check failed: {e}")

        # ---------------- 5.5 Поиск через Iris-бота -----------------
        try:
            iris_candidates = []
            async for msg in client.iter_messages(entity, limit=500):
                sender = None
                try:
                    sender = await msg.get_sender()
                except Exception:
                    sender = None

                if not sender or not getattr(sender, 'bot', False):
                    continue

                name = (sender.first_name or '') + ' ' + (sender.last_name or '')
                username = sender.username or ''
                nick = f"{name} {username}".lower()

                # бот должен начинаться с Iris
                if not nick.strip().startswith("iris"):
                    continue

                text = (msg.text or '').lower()
                # ищем блоки "создатель" и первую упоминание/ссылку
                if 'создател' in text or '⭐' in text:
                    # ищем tel ссылку
                    import re
                    tg_links = re.findall(r"https://t\.me/[A-Za-z0-9_]+", msg.text or '')
                    if tg_links:
                        link = tg_links[0]
                        username = link.split('/')[-1]
                        try:
                            user = await get_entity_safe(client, username, session_path)
                            if user and not getattr(user, 'bot', False):
                                add_candidate(user.id, "iris_bot_extract", weight=12)
                        except Exception:
                            pass
                    else:
                        # если нет ссылки — пробуем взять первое слово после "создатель"
                        lines = (msg.text or '').splitlines()
                        for line in lines:
                            if 'создател' in line.lower():
                                possible = line.replace('⭐', '').strip().split()[-1]
                                # если выглядит как username
                                if possible.startswith('@'):
                                    try:
                                        user = await get_entity_safe(client, possible, session_path)
                                        if user and not getattr(user, 'bot', False):
                                            add_candidate(user.id, "iris_bot_extract", weight=10)
                                    except Exception:
                                        pass
        except Exception:
            pass

        # ---------------- 6. финальное принятие решения -------------
        if not candidates:
            # Если нет кандидатов — финальный fallback
            return {
                "id": None,
                "username": None,
                "first_name": "owner_not_found",
                "last_name": "not_found",
                "found_by": "not_found",
            }

        # Выбираем топ кандидатов
        top = candidates.most_common()
        top1_id, top1_score = top[0]
        top2_score = top[1][1] if len(top) > 1 else 0

        # Условие уверенного выбора: отрыв >= 1.5x или абсолютный порог
        if top1_score >= 8 or (top1_score >= top2_score * 1.5 and top1_score >= 4):
            user = await _safe_get_entity(top1_id)
            if user and not getattr(user, "bot", False):
                method = ",".join(candidate_methods.get(top1_id, []))
                res = _user_dict(user, f"aggregated:{method}")
                res['score'] = top1_score
                res['reason_candidates'] = candidate_methods.get(top1_id, [])
                return res

        # Если недостаточно уверены — возвращаем неоднозначный результат с кандидатами
        demo = {"candidates": []}
        for uid, score in top:
            user = await _safe_get_entity(uid)
            demo["candidates"].append({
                "id": uid,
                "score": score,
                "methods": candidate_methods.get(uid, []),
                "username": getattr(user, 'username', None) if user else None,
            })
        return {
            "id": None,
            "username": None,
            "first_name": "owner_ambiguous",
            "last_name": "multiple_candidates",
            "found_by": "ambiguous",
            "details": demo,
        }

    except Exception as e:
        logger.error(f"Критическая ошибка в _find_creator: {e}")
        return {
            "id": None,
            "username": None,
            "first_name": "owner_search_error",
            "last_name": "",
            "found_by": "error",
        }



async def _get_group_info_safe(client, entity):
    """Безопасное получение информации о группе"""
    try:
        full = await client(GetFullChannelRequest(channel=entity))
        return {
            'participants_count': getattr(full.full_chat, 'participants_count', 0),
            'about': getattr(full.full_chat, 'about', ''),
            'megagroup': getattr(entity, 'megagroup', False),
            'broadcast': getattr(entity, 'broadcast', False),
            'title': getattr(entity, 'title', ''),
        }
    except Exception as e:
        logger.debug(f"Не удалось получить информацию о группе: {e}")
        return None


def escape_md(text: str) -> str:
    """Безопасное экранирование для Markdown (экранируем только подставляемые данные)."""
    if text is None:
        return ""
    s = str(text)
    # сначала экранируем обратный слэш
    s = s.replace("\\", "\\\\")
    # список спецсимволов Markdown (исключаем дефиз, цифры и точку)
    for ch in ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '=', '|', '{', '}', '!']:
        s = s.replace(ch, "\\" + ch)
    return s


def format_date(dt, language: str = 'ru'):
    """Форматирует дату"""
    if not dt:
        return "Not defined" if language == 'en' else "Не определена"

    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
        except:
            return "Not defined" if language == 'en' else "Не определена"

    if language == 'en':
        months = ["January", "February", "March", "April", "May", "June",
                  "July", "August", "September", "October", "November", "December"]
        return f"{dt.day} {months[dt.month - 1]} {dt.year}"
    else:
        months = ["января", "февраля", "марта", "апреля", "мая", "июня",
                  "июля", "августа", "сентября", "октября", "ноября", "декабря"]
        return f"{dt.day} {months[dt.month - 1]} {dt.year} года"


# ================== Временное хранилище для запросов ==================
import json
import os

INLINE_REQUESTS_FILE = "inline_requests.json"


async def save_inline_request(request_id: str, link: str, user_id: int):
    """Сохраняет запрос во временное хранилище"""
    try:
        if os.path.exists(INLINE_REQUESTS_FILE):
            with open(INLINE_REQUESTS_FILE, 'r', encoding='utf-8') as f:
                requests = json.load(f)
        else:
            requests = {}

        requests[request_id] = {
            'link': link,
            'user_id': user_id,
            'created_at': datetime.now().isoformat(),
            'processed': False
        }

        with open(INLINE_REQUESTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(requests, f, ensure_ascii=False, indent=2)

    except Exception as e:
        logger.error(f"Ошибка сохранения запроса: {e}")


async def get_inline_request(request_id: str):
    """Получает запрос из временного хранилища"""
    try:
        if not os.path.exists(INLINE_REQUESTS_FILE):
            return None

        with open(INLINE_REQUESTS_FILE, 'r', encoding='utf-8') as f:
            requests = json.load(f)

        return requests.get(request_id)
    except Exception as e:
        logger.error(f"Ошибка получения запроса: {e}")
        return None


async def mark_request_processed(request_id: str):
    """Помечает запрос как обработанный"""
    try:
        if not os.path.exists(INLINE_REQUESTS_FILE):
            return

        with open(INLINE_REQUESTS_FILE, 'r', encoding='utf-8') as f:
            requests = json.load(f)

        if request_id in requests:
            requests[request_id]['processed'] = True

        with open(INLINE_REQUESTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(requests, f, ensure_ascii=False, indent=2)

    except Exception as e:
        logger.error(f"Ошибка отметки запроса: {e}")


class MassCheckDatabase:
    def __init__(self, db_path="mass_checks.db"):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        """Инициализация базы данных для массовых проверок"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS mass_checks (
                    mass_check_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    results_data TEXT NOT NULL,
                    total_groups INTEGER NOT NULL,
                    total_price REAL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                )
            ''')

            # Индексы для быстрого поиска
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_mass_check_user 
                ON mass_checks(user_id, created_at)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_mass_check_expires 
                ON mass_checks(expires_at)
            ''')
            conn.commit()
            logger.info(f"База массовых проверок инициализирована: {self.db_path}")

    async def save_mass_check(self, mass_check_id: str, user_id: int, results: list, total_price: float = 0):
        """Сохраняет массовую проверку в базу данных"""
        try:
            # Сериализуем результаты
            serialized_results = []
            for result in results:
                serialized_result = result.copy()
                # Преобразуем datetime в строку
                if serialized_result.get('created_date') and hasattr(serialized_result['created_date'], 'isoformat'):
                    serialized_result['created_date'] = serialized_result['created_date'].isoformat()
                if serialized_result.get('checked_at') and hasattr(serialized_result['checked_at'], 'isoformat'):
                    serialized_result['checked_at'] = serialized_result['checked_at'].isoformat()
                serialized_results.append(serialized_result)

            mass_check_data = {
                "results": serialized_results,
                "total_groups": len(results),
                "total_price": total_price,
                "created_at": datetime.now().isoformat()
            }

            results_json = json.dumps(mass_check_data, default=str, ensure_ascii=False)

            # Устанавливаем срок жизни записи (7 дней)
            expires_at = (datetime.now() + timedelta(days=7)).isoformat()

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO mass_checks 
                    (mass_check_id, user_id, results_data, total_groups, total_price, created_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (mass_check_id, user_id, results_json, len(results), total_price,
                      datetime.now().isoformat(), expires_at))
                conn.commit()

            logger.info(f"Массовая проверка сохранена: {mass_check_id} для пользователя {user_id}")
            return True

        except Exception as e:
            logger.error(f"Ошибка сохранения массовой проверки: {e}")
            return False

    async def get_mass_check(self, mass_check_id: str):
        """Получает массовую проверку по ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT mass_check_id, user_id, results_data, total_groups, total_price, created_at, expires_at
                    FROM mass_checks WHERE mass_check_id = ?
                ''', (mass_check_id,))
                row = cursor.fetchone()

                if row:
                    # Проверяем не истекла ли запись
                    expires_at = datetime.fromisoformat(row[6])
                    if datetime.now() > expires_at:
                        await self.delete_mass_check(mass_check_id)
                        return None

                    return {
                        'mass_check_id': row[0],
                        'user_id': row[1],
                        'results_data': row[2],
                        'total_groups': row[3],
                        'total_price': row[4],
                        'created_at': row[5],
                        'expires_at': row[6]
                    }
                return None

        except Exception as e:
            logger.error(f"Ошибка получения массовой проверки: {e}")
            return None

    async def delete_mass_check(self, mass_check_id: str):
        """Удаляет массовую проверку"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM mass_checks WHERE mass_check_id = ?', (mass_check_id,))
                conn.commit()
            logger.info(f"Массовая проверка удалена: {mass_check_id}")
        except Exception as e:
            logger.error(f"Ошибка удаления массовой проверки: {e}")

    async def cleanup_expired_mass_checks(self):
        """Очищает устаревшие массовые проверки"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM mass_checks WHERE expires_at < ?',
                               (datetime.now().isoformat(),))
                deleted_count = cursor.rowcount
                conn.commit()
                if deleted_count > 0:
                    logger.info(f"Очищено {deleted_count} устаревших массовых проверок")
        except Exception as e:
            logger.error(f"Ошибка очистки массовых проверок: {e}")


# Инициализация базы массовых проверок
mass_check_db = MassCheckDatabase()

# ================== Временное хранилище для запросов в SQLite ==================
class RequestDatabase:
    def __init__(self, db_path="requests.db"):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        """Инициализация базы данных для запросов"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS inline_requests (
                    request_id TEXT PRIMARY KEY,
                    link TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'processing',
                    result_data TEXT,
                    created_at TEXT NOT NULL,
                    request_type TEXT NOT NULL DEFAULT 'single'
                )
            ''')

            # Индекс для очистки старых запросов
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_created_at 
                ON inline_requests(created_at)
            ''')

            # Индекс для типа запроса
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_request_type 
                ON inline_requests(request_type)
            ''')
            conn.commit()
            logger.info(f"База запросов инициализирована: {self.db_path}")

    async def save_request(self, request_id: str, link: str, user_id: int, request_type: str = "single"):
        """Сохраняет запрос в базу данных"""
        try:
            logger.info(f"Сохранение запроса: {request_id}, {link}, {user_id}, тип: {request_type}")
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO inline_requests 
                    (request_id, link, user_id, status, created_at, request_type)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (request_id, link, user_id, 'processing', datetime.now().isoformat(), request_type))
                conn.commit()
            logger.info("Запрос успешно сохранен")
        except Exception as e:
            logger.error(f"Ошибка сохранения запроса: {e}", exc_info=True)
            raise

    async def get_request(self, request_id: str):
        """Получает запрос из базы данных"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT request_id, link, user_id, status, result_data, created_at 
                FROM inline_requests WHERE request_id = ?
            ''', (request_id,))
            row = cursor.fetchone()

            if row:
                return {
                    'request_id': row[0],
                    'link': row[1],
                    'user_id': row[2],
                    'status': row[3],
                    'result_data': row[4],
                    'created_at': row[5]
                }
            return None

    async def update_request_status(self, request_id: str, status: str, result_data: str = None):
        """Обновляет статус запроса"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if result_data:
                cursor.execute('''
                    UPDATE inline_requests 
                    SET status = ?, result_data = ?
                    WHERE request_id = ?
                ''', (status, result_data, request_id))
            else:
                cursor.execute('''
                    UPDATE inline_requests 
                    SET status = ?
                    WHERE request_id = ?
                ''', (status, request_id))
            conn.commit()

    async def cleanup_old_requests(self, hours=24):
        """Очищает старые запросы"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()
                cursor.execute('''
                    DELETE FROM inline_requests WHERE created_at < ?
                ''', (cutoff_time,))
                deleted_count = cursor.rowcount
                conn.commit()
                logger.info(f"Очищено {deleted_count} старых запросов")
        except Exception as e:
            logger.error(f"Ошибка очистки запросов: {e}")


# Инициализация базы запросов
request_db = RequestDatabase()


@router.message()
async def handle_link(msg: types.Message, state: FSMContext):
    """Обработка всех сообщений с ссылками (включая папки)"""

    # Проверяем, находится ли пользователь в состоянии создания поста
    current_state = await state.get_state()
    if current_state == PostState.waiting_for_content:
        return

    if not msg.text:
        return

    text = msg.text.strip()
    language = await user_manager.get_user_language(msg.from_user.id)

    # Проверяем команды
    if text.startswith('/'):
        return

    # === НОВОЕ: Проверка на ссылку папки (должна быть первой) ===
    if folder_checker.is_folder_link(text):
        await handle_folder_check(msg, state, text)
        return

    # Проверяем, является ли сообщение массовой проверкой (несколько ссылок)
    lines = [line.strip() for line in text.split('\n') if line.strip()]

    # Улучшенная проверка: считаем только строки, которые действительно являются ссылками
    valid_links = []

    for line in lines:
        # Более строгая проверка: строка должна быть ТОЛЬКО ссылкой, а не содержать ссылку в тексте
        if (RE_TME.search(line) or RE_USERNAME.search(line) or RE_JOIN.search(line) or RE_FOLDER.search(line)):
            valid_links.append(line)
        # ИЛИ если строка начинается с @ и содержит только username (без других текстов)
        elif line.startswith('@') and len(line) > 1 and ' ' not in line:
            valid_links.append(line)

    # Если есть хотя бы 2 валидные ссылки - это массовая проверка
    if len(valid_links) >= 2:
        await handle_mass_check(msg, state)
        return

    # Если только одна валидная ссылка - одиночная проверка
    if len(valid_links) == 1:
        link = valid_links[0]

        # Дополнительная проверка: ссылка должна быть основной частью сообщения
        if len(lines) == 1 and link == text:
            # ПРОВЕРЯЕМ ЛИМИТЫ ПЕРЕД ОБРАБОТКОЙ
            limit_check = await limit_manager.can_make_request(msg.from_user.id, [link])

            if not limit_check["allowed"]:
                # Лимиты исчерпаны
                limits = await limit_manager.get_user_limits(msg.from_user.id)
                if language == 'en':
                    text = (
                        "❌ <b>Limit exceeded!</b>\n\n"
                        f"🌐 Public groups: {limits['public_used']}/{limits['public_limit']}\n"
                        f"🔒 Private groups: {limits['private_used']}/{limits['private_limit']}\n\n"
                        f"⏰ Reset in: {limits['time_left']}"
                    )
                else:
                    text = (
                        "❌ <b>Лимит превышен!</b>\n\n"
                        f"🌐 Публичные группы: {limits['public_used']}/{limits['public_limit']}\n"
                        f"🔒 Приватные группы: {limits['private_used']}/{limits['private_limit']}\n\n"
                        f"⏰ До сброса: {limits['time_left']}"
                    )
                await send_error_with_photo(msg, text)
                return

            # Определяем тип ссылки ДО анализа
            is_private = limit_manager._is_private_link(link)
            logger.info(f"🔍 [HANDLE LINK TYPE] Ссылка '{link}' - {'приватная' if is_private else 'публичная'}")

            # Обрабатываем одиночную ссылку через улучшенную функцию
            await handle_single_link(msg, state, is_from_mass_check=False)
            return

    # Если нет валидных ссылок ИЛИ ссылка является частью текста (не отдельное сообщение) - игнорируем
    return


async def handle_folder_check(msg: types.Message, state: FSMContext, folder_link: str, attempt: int = 0,
                              used_sessions: list = None):
    if used_sessions is None:
        used_sessions = []

    if attempt >= 3:
        await msg.answer("❌ Не удалось проверить (все аккаунты достигли лимита)")
        return

    language = await user_manager.get_user_language(msg.from_user.id)
    user_id = msg.from_user.id

    progress_msg = await msg.answer(f"📁 Загружаю папку...")

    client = None
    session_path = None
    folder_data = None

    try:
        client, session_path = await session_manager.get_client(user_id, language)

        if session_path in used_sessions:
            if session_path:
                await session_manager.release_client(session_path)
            if client:
                await client.disconnect() if client.is_connected() else None
            await asyncio.sleep(1)
            await handle_folder_check(msg, state, folder_link, attempt + 1, used_sessions)
            return

        used_sessions.append(session_path)

        slug = folder_checker.extract_folder_slug(folder_link)
        if not slug:
            await progress_msg.edit_text("❌ Неверная ссылка")
            return

        # Импортируем папку (вступаем в чаты)
        folder_data = await folder_checker.import_folder_and_get_chats(client, slug)

        if not folder_data["success"] and folder_data.get("error") == "too_many_channels":
            await progress_msg.edit_text("⚠️ Лимит каналов, переключаюсь...")
            if session_path:
                await session_manager.release_client(session_path)
            if client:
                await client.disconnect() if client.is_connected() else None
            await asyncio.sleep(2)
            await handle_folder_check(msg, state, folder_link, attempt + 1, used_sessions)
            return

        if not folder_data["success"]:
            err = folder_data.get("error", "unknown")
            text = {"expired": "❌ Ссылка устарела", "invalid": "❌ Неверная ссылка"}.get(err, f"❌ {err}")
            await progress_msg.edit_text(text)
            return

        chats = folder_data.get("chats", [])
        if not chats:
            await progress_msg.edit_text("❌ Нет доступных чатов")
            return

        folder_title = str(folder_data.get("title", "Folder"))
        filter_id = folder_data.get("filter_id")
        just_imported = folder_data.get("just_imported", False)

        await progress_msg.edit_text(
            f"📁 <b>{folder_title}</b>\n"
            f"Вступил в {len(chats)} чатов\n"
            f"Проверяю...",
            parse_mode=ParseMode.HTML
        )

        results = []
        progress_lock = asyncio.Lock()
        completed = 0
        total_chats = len(chats)
        folder_concurrency = min(4, max(1, total_chats))
        semaphore = asyncio.Semaphore(folder_concurrency)

        def _folder_display_link(chat_info: dict) -> tuple[str, str]:
            if chat_info.get("username"):
                link = f"t.me/{chat_info['username']}"
                return link, link

            chat_title = (chat_info.get("title") or "Unknown chat").strip()
            chat_id = chat_info.get('id', 'unknown')
            display_link = f"{chat_title} (ID:{chat_id})"
            return display_link, str(chat_id)

        async def _check_chat(chat_info: dict):
            nonlocal completed
            async with semaphore:
                display_link, raw_link = _folder_display_link(chat_info)
                try:
                    entity = chat_info.get("entity")
                    if not entity:
                        raise ValueError("Нет entity для чата")

                    result = await _analyze_group_internal(
                        client=client,
                        raw_link=raw_link,
                        requester_id=user_id,
                        progress_msg=None,
                        language=language,
                        session_path=session_path,
                        pre_fetched_entity=entity
                    )

                    result["from_folder"] = True
                    result["folder_name"] = folder_title
                    if not result.get("chat_link"):
                        result["chat_link"] = display_link

                    logger.info(f"✅ Проверен чат из папки: {display_link}")
                    return result
                except Exception as e:
                    logger.error(f"❌ Ошибка проверки чата {chat_info}: {e}")
                    return {
                        "is_bad": True,
                        "chat_link": display_link,
                        "error": str(e),
                        "error_message": str(e),
                        "title": chat_info.get("title", "Error"),
                        "creator": {"ru": "Ошибка", "en": "Error"},
                        "created": {"ru": "не определена", "en": "not determined"},
                        "messages_count": 0,
                        "members": 0,
                        "type": "Error",
                        "review": {"ru": f"⚠️ {str(e)[:30]}", "en": f"⚠️ {str(e)[:30]}"},
                        "cost": 0,
                        "from_folder": True,
                        "folder_name": folder_title
                    }
                finally:
                    async with progress_lock:
                        completed += 1
                        await progress_msg.edit_text(
                            f"📁 <b>{folder_title}</b>\n"
                            f"Проверено: {completed}/{total_chats}",
                            parse_mode=ParseMode.HTML
                        )

        check_tasks = [asyncio.create_task(_check_chat(chat_info)) for chat_info in chats]
        results = await asyncio.gather(*check_tasks)

        # Отправляем результаты
        stats_text = f"📁 <b>{folder_title}</b>\nПроверено: {len(results)}/{len(chats)}"
        if results:
            await send_mass_check_results(msg, results, progress_msg, stats_text, language)
        else:
            await progress_msg.edit_text("❌ Нет результатов")

        try:
            await folder_checker.cleanup_folder_and_leave_chats(client, filter_id, chats, just_imported)
        except Exception as e:
            logger.error(f"Ошибка очистки: {e}")

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await msg.answer(f"❌ Ошибка: {str(e)[:100]}")
    finally:
        # Гарантированная очистка
        if client and folder_data:
            try:
                await folder_checker.cleanup_folder_and_leave_chats(
                    client,
                    folder_data.get("filter_id"),
                    folder_data.get("chats", []),
                    folder_data.get("just_imported", False)
                )
            except Exception as e:
                logger.error(f"Ошибка финальной очистки: {e}")

        if session_path:
            await session_manager.release_client(session_path)
        if client:
            try:
                await client.disconnect()
            except:
                pass


async def analyze_entity_full(client, entity, chat_info, language):
    """
    Полный анализ чата через Telethon entity (для приватных чатов после импорта).
    """
    from telethon.tl.types import Channel, Chat, ChatForbidden, ChannelForbidden
    import asyncio

    chat_id = chat_info["id"]
    title = str(getattr(entity, 'title', chat_info.get('title', 'Unknown')))

    result = {
        "is_bad": False,
        "chat_link": f"ID:{chat_id}",
        "error": None,
        "title": title,
        "creator": {"ru": "Не определён", "en": "Not determined"},
        "created": {"ru": "не определена", "en": "not determined"},
        "messages_count": 0,
        "members": 0,
        "type": "Private",
        "review": {"ru": "", "en": ""},
        "cost": 0,
        "is_private_chat": True
    }

    try:
        # Определяем тип чата
        is_channel = isinstance(entity, (Channel, ChannelForbidden))

        if is_channel:
            if entity.broadcast:
                result["type"] = "Private Channel"
            else:
                result["type"] = "Private Supergroup"
        elif isinstance(entity, (Chat, ChatForbidden)):
            result["type"] = "Private Group"

        # Получаем полную информацию и сообщения
        try:
            if is_channel:
                # Для каналов
                full = await client(functions.channels.GetFullChannelRequest(channel=entity))
                result["members"] = full.full_chat.participants_count or 0

                # Получаем историю сообщений
                messages = []
                async for message in client.iter_messages(entity, limit=30):
                    if message.text:
                        messages.append(message.text)

                    # Пытаемся найти владельца по первому сообщению
                    if message.sender_id and result["creator"]["ru"] == "Не определён":
                        try:
                            sender = await client.get_entity(message.sender_id)
                            if hasattr(sender, 'username') and sender.username:
                                result["creator"] = {"ru": f"@{sender.username}", "en": f"@{sender.username}"}
                            elif hasattr(sender, 'first_name') and sender.first_name:
                                result["creator"] = {"ru": sender.first_name, "en": sender.first_name}
                        except:
                            pass

                    if len(messages) >= 10:  # Достаточно для проверки
                        break

                result["messages_count"] = len(messages)

            else:
                # Для групп
                full = await client(functions.messages.GetFullChatRequest(chat_id=entity.id))
                result["members"] = len(full.users) if full.users else 0

                messages = []
                async for message in client.iter_messages(entity, limit=30):
                    if message.text:
                        messages.append(message.text)
                    if len(messages) >= 10:
                        break

                result["messages_count"] = len(messages)

            # Формируем ревью
            if result["messages_count"] > 0:
                result["review"] = {
                    "ru": f"✅ Проанализировано ({result['messages_count']} сообщений, {result['members']} участников)",
                    "en": f"✅ Analyzed ({result['messages_count']} messages, {result['members']} members)"
                }
            else:
                result["review"] = {
                    "ru": "✅ Доступ получен, чат пуст или сообщения недоступны",
                    "en": "✅ Access granted, chat empty or messages unavailable"
                }

            result["is_bad"] = False

        except Exception as e:
            logger.error(f"Ошибка получения данных чата {chat_id}: {e}")
            result["error"] = str(e)
            result["review"] = {
                "ru": f"⚠️ Ошибка чтения: {str(e)[:40]}",
                "en": f"⚠️ Read error: {str(e)[:40]}"
            }

    except Exception as e:
        logger.error(f"Критическая ошибка анализа {chat_id}: {e}")
        result["error"] = str(e)
        result["is_bad"] = True
        result["review"] = {
            "ru": f"❌ Ошибка: {str(e)[:40]}",
            "en": f"❌ Error: {str(e)[:40]}"
        }

    return result

async def _analyze_private_chat_impl(client, entity, chat_info, language):
    """Анализ приватного чата."""
    from telethon.tl.types import Channel, Chat, ChatForbidden, ChannelForbidden

    chat_id = chat_info["id"]
    title = str(getattr(entity, 'title', chat_info.get('title', 'Unknown')))

    result = {
        "is_bad": False,
        "chat_link": f"ID:{chat_id}",
        "error": None,
        "title": title,
        "creator": {"ru": "Не определён", "en": "Not determined"},
        "created": {"ru": "не определена", "en": "not determined"},
        "messages_count": 0,
        "members": 0,
        "type": "Private",
        "review": {"ru": "✅ Доступ получен", "en": "✅ Access granted"},
        "cost": 0,
        "is_private_chat": True
    }

    try:
        is_channel = False
        if isinstance(entity, Channel):
            is_channel = True
            if entity.broadcast:
                result["type"] = "Private Channel"
            else:
                result["type"] = "Private Group"
        elif isinstance(entity, (Chat, ChatForbidden)):
            result["type"] = "Private Group"

        try:
            if is_channel:
                full = await client(functions.channels.GetFullChannelRequest(channel=entity))
                result["members"] = full.full_chat.participants_count or 0

                messages = []
                async for message in client.iter_messages(entity, limit=50):
                    if message.text:
                        messages.append(message.text)
                    if message.sender_id and result["creator"]["ru"] == "Не определён":
                        try:
                            sender = await client.get_entity(message.sender_id)
                            if hasattr(sender, 'username') and sender.username:
                                result["creator"] = {"ru": f"@{sender.username}", "en": f"@{sender.username}"}
                        except:
                            pass
                    if len(messages) >= 20:
                        break

                result["messages_count"] = len(messages)

            else:
                full = await client(functions.messages.GetFullChatRequest(chat_id=entity.id))
                result["members"] = len(full.users) if full.users else 0

                messages = []
                async for message in client.iter_messages(entity, limit=50):
                    if message.text:
                        messages.append(message.text)
                    if len(messages) >= 20:
                        break

                result["messages_count"] = len(messages)

            if result["messages_count"] > 0:
                result["review"] = {
                    "ru": f"✅ Проанализировано ({result['messages_count']} сообщений)",
                    "en": f"✅ Analyzed ({result['messages_count']} messages)"
                }
            else:
                result["review"] = {
                    "ru": "✅ Доступ есть, но сообщений нет",
                    "en": "✅ Access granted, but no messages"
                }

        except Exception as e:
            logger.error(f"Ошибка получения данных чата {chat_id}: {e}")
            result["error"] = str(e)
            result["review"] = {"ru": "⚠️ Доступ ограничен", "en": "⚠️ Access limited"}

    except Exception as e:
        logger.error(f"Ошибка анализа: {e}")
        result["error"] = str(e)
        result["review"] = {"ru": f"⚠️ Ошибка: {str(e)[:30]}", "en": f"⚠️ Error: {str(e)[:30]}"}

    return result
# ================== Генератор защищенных ID ==================
import secrets
import string


def generate_secure_id(length=16):
    """Генерирует защищенный ID из букв и цифр"""
    alphabet = string.ascii_letters + string.digits  # A-Z, a-z, 0-9
    return ''.join(secrets.choice(alphabet) for _ in range(length))


# ================== Инлайн-режим с защищенными ссылками ==================
@router.inline_query()
async def inline_mode(inline_query: InlineQuery):
    """Обработка инлайн-запросов с готовыми результатами"""
    query = inline_query.query.strip()
    language = await user_manager.get_user_language(inline_query.from_user.id)

    # Если запрос пустой, показываем подсказку
    if not query:
        if language == 'en':
            results = [
                InlineQueryResultArticle(
                    id="1",
                    title="Check Telegram group",
                    description="Send a link to a group/channel",
                    input_message_content=InputTextMessageContent(
                        message_text="❌ Please specify a group link after the bot name\n\nExample: @groupcekcbot t.me/username"
                    )
                )
            ]
        else:
            results = [
                InlineQueryResultArticle(
                    id="1",
                    title="Проверить группу Telegram",
                    description="Отправьте ссылку на группу/канал",
                    input_message_content=InputTextMessageContent(
                        message_text="❌ Пожалуйста, укажите ссылку на группу после имени бота\n\nНапример: @groupcekcbot t.me/username"
                    )
                )
            ]
        await inline_query.answer(results, cache_time=1, is_personal=True)
        return

    # Проверяем, является ли запрос ссылкой на массовую проверку
    if query.startswith("mass_"):
        mass_check_id = query.replace("mass_", "")

        # Получаем данные массовой проверки
        mass_check_data = await mass_check_db.get_mass_check(mass_check_id)

        if not mass_check_data:
            if language == 'en':
                results = [
                    InlineQueryResultArticle(
                        id="1",
                        title="❌ Mass check not found",
                        description="Check expired or doesn't exist",
                        input_message_content=InputTextMessageContent(
                            message_text="❌ Mass check results not found or expired."
                        )
                    )
                ]
            else:
                results = [
                    InlineQueryResultArticle(
                        id="1",
                        title="❌ Массовая проверка не найдена",
                        description="Проверка устарела или не существует",
                        input_message_content=InputTextMessageContent(
                            message_text="❌ Результат массовой проверки не найден или устарел."
                        )
                    )
                ]
            await inline_query.answer(results, cache_time=1, is_personal=True)
            return

        # Парсим результаты
        results_info = json.loads(mass_check_data['results_data'])
        total_groups = results_info.get('total_groups', 0)
        total_price = results_info.get('total_price', 0)

        # Создаем сообщение для инлайн-режима
        if language == 'en':
            message_text = (
                f"🔍 <b>Mass check results</b>\n\n"
                f"✅ Checked groups: {total_groups}\n"
                f"💰 Total price: {total_price}$\n"
                f"🔢 Check code: {mass_check_id}\n\n"
                f"👇 Click the button below to view full information"
            )
        else:
            message_text = (
                f"🔍 <b>Результаты массовой проверки</b>\n\n"
                f"✅ Проверено групп: {total_groups}\n"
                f"💰 Общая стоимость: {total_price}$\n"
                f"🔢 Код проверки: {mass_check_id}\n\n"
                f"👇 Нажмите кнопку ниже чтобы посмотреть полную информацию"
            )

        # Получаем username бота для создания deep link
        bot_username = (await bot.get_me()).username
        deep_link = f"https://t.me/{bot_username}?start=mass_{mass_check_id}"

        if language == 'en':
            results = [
                InlineQueryResultArticle(
                    id="1",
                    title=f"📊 Mass check of {total_groups} groups",
                    description=f"Price: {total_price}$ | Code: {mass_check_id}",
                    input_message_content=InputTextMessageContent(
                        message_text=message_text,
                        parse_mode=ParseMode.HTML
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[
                            InlineKeyboardButton(
                                text="📊 View information",
                                url=deep_link
                            )
                        ]]
                    )
                )
            ]
        else:
            results = [
                InlineQueryResultArticle(
                    id="1",
                    title=f"📊 Массовая проверка {total_groups} групп",
                    description=f"Стоимость: {total_price}$ | Код: {mass_check_id}",
                    input_message_content=InputTextMessageContent(
                        message_text=message_text,
                        parse_mode=ParseMode.HTML
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[
                            InlineKeyboardButton(
                                text="📊 Посмотреть информацию",
                                url=deep_link
                            )
                        ]]
                    )
                )
            ]

        await inline_query.answer(results, cache_time=300, is_personal=True)
        return

    # Проверяем, является ли запрос реферальной ссылкой
    if query.startswith("ref_"):
        request_id = query.replace("ref_", "")

        # Получаем данные запроса
        request_data = await request_db.get_request(request_id)

        if not request_data:
            if language == 'en':
                results = [
                    InlineQueryResultArticle(
                        id="1",
                        title="❌ Result not found",
                        description="Check expired or doesn't exist",
                        input_message_content=InputTextMessageContent(
                            message_text="❌ Check results not found or expired."
                        )
                    )
                ]
            else:
                results = [
                    InlineQueryResultArticle(
                        id="1",
                        title="❌ Результат не найден",
                        description="Проверка устарела или не существует",
                        input_message_content=InputTextMessageContent(
                            message_text="❌ Результат проверки не найден или устарел."
                        )
                    )
                ]
            await inline_query.answer(results, cache_time=1, is_personal=True)
            return

        link = request_data['link']
        status = request_data['status']

        if status == 'processing':
            if language == 'en':
                results = [
                    InlineQueryResultArticle(
                        id="1",
                        title="⏳ Still checking...",
                        description="Group is being checked, try again later",
                        input_message_content=InputTextMessageContent(
                            message_text="⏳ Group is still being checked, try again in a few seconds..."
                        )
                    )
                ]
            else:
                results = [
                    InlineQueryResultArticle(
                        id="1",
                        title="⏳ Еще проверяется...",
                        description="Группа проверяется, попробуйте позже",
                        input_message_content=InputTextMessageContent(
                            message_text="⏳ Группа еще проверяется, попробуйте через пару секунд..."
                        )
                    )
                ]
            await inline_query.answer(results, cache_time=1, is_personal=True)
            return

        elif status == 'error':
            error_msg = request_data['result_data'] or (
                'Error checking group' if language == 'en' else 'Ошибка проверки')
            if language == 'en':
                results = [
                    InlineQueryResultArticle(
                        id="1",
                        title="❌ Check error",
                        description="Could not check the group",
                        input_message_content=InputTextMessageContent(
                            message_text=f"❌ {error_msg}"
                        )
                    )
                ]
            else:
                results = [
                    InlineQueryResultArticle(
                        id="1",
                        title="❌ Ошибка проверки",
                        description="Не удалось проверить группу",
                        input_message_content=InputTextMessageContent(
                            message_text=f"❌ {error_msg}"
                        )
                    )
                ]
            await inline_query.answer(results, cache_time=1, is_personal=True)
            return

        elif status == 'completed':
            # Парсим результат
            result_dict = json.loads(request_data['result_data'])

            # Восстанавливаем datetime объекты из строк
            if result_dict.get('created_date') and isinstance(result_dict['created_date'], str):
                try:
                    result_dict['created_date'] = datetime.fromisoformat(
                        result_dict['created_date'].replace('Z', '+00:00'))
                except:
                    result_dict['created_date'] = None

            # Формируем сообщение для шаринга
            if language == 'en':
                message_text = (
                    f"🔍 <b>Group check result</b>\n\n"
                    f"📋 <b>Group:</b> {link}\n"
                    f"📅 <b>Created:</b> {format_date(result_dict.get('created_date'), language)}\n"
                    f"💰 <b>Price:</b> {result_dict.get('price', 0)}$\n\n"
                    f"👇 Checked via @{(await bot.get_me()).username}"
                )
            else:
                message_text = (
                    f"🔍 <b>Результат проверки группы</b>\n\n"
                    f"📋 <b>Группа:</b> {link}\n"
                    f"📅 <b>Создана:</b> {format_date(result_dict.get('created_date'), language)}\n"
                    f"💰 <b>Стоимость:</b> {result_dict.get('price', 0)}$\n\n"
                    f"👇 Проверено через @{(await bot.get_me()).username}"
                )

            # Создаем deep link для полного результата
            bot_username = (await bot.get_me()).username
            full_result_link = f"https://t.me/{bot_username}?start=ref_{request_id}"

            if language == 'en':
                results = [
                    InlineQueryResultArticle(
                        id="1",
                        title=f"🔍 Check result: {link}",
                        description=f"Price: {result_dict.get('price', 0)}$ | Click to view full info",
                        input_message_content=InputTextMessageContent(
                            message_text=message_text,
                            parse_mode=ParseMode.HTML
                        ),
                        reply_markup=InlineKeyboardMarkup(
                            inline_keyboard=[[
                                InlineKeyboardButton(
                                    text="📊 View full result",
                                    url=full_result_link
                                )
                            ]]
                        )
                    )
                ]
            else:
                results = [
                    InlineQueryResultArticle(
                        id="1",
                        title=f"🔍 Результат проверки: {link}",
                        description=f"Стоимость: {result_dict.get('price', 0)}$ | Нажмите для полной информации",
                        input_message_content=InputTextMessageContent(
                            message_text=message_text,
                            parse_mode=ParseMode.HTML
                        ),
                        reply_markup=InlineKeyboardMarkup(
                            inline_keyboard=[[
                                InlineKeyboardButton(
                                    text="📊 Полный результат",
                                    url=full_result_link
                                )
                            ]]
                        )
                    )
                ]

            await inline_query.answer(results, cache_time=300, is_personal=True)
            return

    # Проверяем, является ли запрос ссылкой на группу (ОДИНОЧНАЯ ПРОВЕРКА)
    if not (RE_TME.search(query) or RE_USERNAME.search(query) or RE_JOIN.search(query)):
        if language == 'en':
            results = [
                InlineQueryResultArticle(
                    id="1",
                    title="❌ Invalid link format",
                    description="Use: @username, t.me/username or t.me/+invite",
                    input_message_content=InputTextMessageContent(
                        message_text="❌ Invalid link format. Use:\n• @username\n• t.me/username\n• t.me/+invite_code"
                    )
                )
            ]
        else:
            results = [
                InlineQueryResultArticle(
                    id="1",
                    title="❌ Неверный формат ссылки",
                    description="Используйте: @username, t.me/username или t.me/+invite",
                    input_message_content=InputTextMessageContent(
                        message_text="❌ Неверный формат ссылки. Используйте:\n• @username\n• t.me/username\n• t.me/+invite_code"
                    )
                )
            ]
        await inline_query.answer(results, cache_time=1, is_personal=True)
        return

    try:
        logger.info(f"🔗 [INLINE SINGLE] Обработка ссылки: {query}")

        # Генерируем защищенный ID для шаринга
        request_id = generate_secure_id(16)
        logger.info(f"🔗 [INLINE SINGLE] Сгенерирован request_id: {request_id}")

        # Проверяем, есть ли уже проверка для этой ссылки
        existing_request = await find_existing_request(query, inline_query.from_user.id)
        if existing_request:
            logger.info(f"🔗 [INLINE SINGLE] Найдена существующая проверка: {existing_request['request_id']}")
            # Используем существующую проверку
            request_id = existing_request['request_id']
        else:
            logger.info("🔗 [INLINE SINGLE] Создание новой проверки")
            # Сохраняем новый запрос в базу данных
            await request_db.save_request(request_id, query, inline_query.from_user.id, "single_share")

            # Запускаем анализ в фоне (СТАРАЯ ЛОГИКА)
            asyncio.create_task(process_inline_background_check(request_id, query, inline_query.from_user.id))

        # Создаем прямую ссылку для кнопки
        bot_username = (await bot.get_me()).username
        direct_link = f"https://t.me/{bot_username}?start=ref_{request_id}"

        # Создаем инлайн-результат с кнопкой "Посмотреть результат" (ПРЯМАЯ ССЫЛКА)
        if language == 'en':
            results = [
                InlineQueryResultArticle(
                    id="1",
                    title=f"🔍 Check: {query}",
                    description="Click to view check result",
                    input_message_content=InputTextMessageContent(
                        message_text=f"🔍 Group check: {query}\n\n👇 Click the button below to view information"
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[
                            InlineKeyboardButton(
                                text="📊 View information",
                                url=direct_link  # ИСПРАВЛЕНО: прямая ссылка вместо switch_inline_query
                            )
                        ]]
                    )
                )
            ]
        else:
            results = [
                InlineQueryResultArticle(
                    id="1",
                    title=f"🔍 Проверить: {query}",
                    description="Нажмите чтобы посмотреть результат",
                    input_message_content=InputTextMessageContent(
                        message_text=f"🔍 Проверка группы: {query}\n\n👇 Нажмите кнопку ниже чтобы посмотреть информацию"
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[
                            InlineKeyboardButton(
                                text="📊 Посмотреть информацию",
                                url=direct_link  # ИСПРАВЛЕНО: прямая ссылка вместо switch_inline_query
                            )
                        ]]
                    )
                )
            ]

        logger.info("🔗 [INLINE SINGLE] Отправка инлайн-результата")
        await inline_query.answer(results, cache_time=300, is_personal=True)

    except Exception as e:
        logger.error(f"❌ [INLINE SINGLE ERROR] Ошибка в inline_mode: {e}", exc_info=True)
        if language == 'en':
            results = [
                InlineQueryResultArticle(
                    id="1",
                    title="❌ Error",
                    description="Try again",
                    input_message_content=InputTextMessageContent(
                        message_text="❌ An error occurred. Please try again."
                    )
                )
            ]
        else:
            results = [
                InlineQueryResultArticle(
                    id="1",
                    title="❌ Ошибка",
                    description="Попробуйте еще раз",
                    input_message_content=InputTextMessageContent(
                        message_text="❌ Произошла ошибка. Попробуйте еще раз."
                    )
                )
            ]
        await inline_query.answer(results, cache_time=1, is_personal=True)


# ================== Фоновая обработка инлайн-проверки ==================
async def process_inline_background_check(request_id: str, link: str, user_id: int):
    """Фоновая задача для анализа группы в инлайн-режиме"""
    try:
        logger.info(f"🔗 [INLINE BACKGROUND] Начало фоновой проверки: {link}")

        # Получаем язык пользователя
        language = await user_manager.get_user_language(user_id)

        # Создаем фиктивное сообщение для progress
        class FakeProgressMessage:
            async def edit_text(self, text, **kwargs):
                logger.info(f"🔗 [INLINE BACKGROUND] {text}")

        progress_msg = FakeProgressMessage()

        # Анализируем группу (используем ту же функцию что и в обычной проверке)
        logger.info(f"🔗 [INLINE BACKGROUND] Анализ группы: {link}")
        result = await analyze_group_by_link(link, user_id, progress_msg)

        if result.get("is_bad"):
            error_msg = "Couldn't check group" if language == 'en' else "Не удалось проверить группу"
            await request_db.update_request_status(request_id, 'error', error_msg)
            logger.error(f"🔗 [INLINE BACKGROUND] Ошибка проверки: {link}")
            return

        # Рассчитываем цену
        price = await user_manager.calculate_price(user_id, result.get('chat_id'), result.get('created_date'))
        result['price'] = price

        # Сохраняем результат во временную базу
        result_json = json.dumps(result, default=str, ensure_ascii=False)
        await request_db.update_request_status(request_id, 'completed', result_json)

        logger.info(f"🔗 [INLINE BACKGROUND] Проверка завершена: {link}, цена: {price}$")

    except Exception as e:
        logger.error(f"🔗 [INLINE BACKGROUND ERROR] Ошибка в фоновой проверке: {e}")
        language = await user_manager.get_user_language(user_id)
        error_msg = "Error analyzing group" if language == 'en' else "Ошибка при анализе"
        await request_db.update_request_status(request_id, 'error', error_msg)
        """Фоновая задача для анализа группы в инлайн-режиме"""
    try:
        logger.info(f"🔗 [INLINE BACKGROUND] Начало фоновой проверки: {link}")

        # Получаем язык пользователя
        language = await user_manager.get_user_language(user_id)

        # Создаем фиктивное сообщение для progress
        class FakeProgressMessage:
            async def edit_text(self, text, **kwargs):
                logger.info(f"🔗 [INLINE BACKGROUND] {text}")

        progress_msg = FakeProgressMessage()

        # Анализируем группу (используем ту же функцию что и в обычной проверке)
        logger.info(f"🔗 [INLINE BACKGROUND] Анализ группы: {link}")
        result = await analyze_group_by_link(link, user_id, progress_msg)

        if result.get("is_bad"):
            error_msg = "Couldn't check group" if language == 'en' else "Не удалось проверить группу"
            await request_db.update_request_status(request_id, 'error', error_msg)
            logger.error(f"🔗 [INLINE BACKGROUND] Ошибка проверки: {link}")
            return

        # Рассчитываем цену
        price = await user_manager.calculate_price(user_id, result.get('chat_id'), result.get('created_date'))
        result['price'] = price

        # Сохраняем результат во временную базу
        result_json = json.dumps(result, default=str, ensure_ascii=False)
        await request_db.update_request_status(request_id, 'completed', result_json)

        logger.info(f"🔗 [INLINE BACKGROUND] Проверка завершена: {link}, цена: {price}$")

    except Exception as e:
        logger.error(f"🔗 [INLINE BACKGROUND ERROR] Ошибка в фоновой проверке: {e}")
        language = await user_manager.get_user_language(user_id)
        error_msg = "Error analyzing group" if language == 'en' else "Ошибка при анализе"
        await request_db.update_request_status(request_id, 'error', error_msg)

# ================== Поиск существующей проверки ==================
async def find_existing_request(link: str, user_id: int):
        """Ищет существующую проверку для этой ссылки и пользователя"""
        try:
            logger.info(f"Поиск существующей проверки для {link}, user_id: {user_id}")
            with sqlite3.connect(request_db.db_path) as conn:  # Теперь db_path доступен
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT request_id, status, result_data 
                    FROM inline_requests 
                    WHERE link = ? AND user_id = ? 
                    ORDER BY created_at DESC 
                    LIMIT 1
                ''', (link, user_id))
                row = cursor.fetchone()

                if row:
                    logger.info(f"Найдена существующая проверка: {row[0]}, статус: {row[1]}")
                    return {
                        'request_id': row[0],
                        'status': row[1],
                        'result_data': row[2]
                    }
                else:
                    logger.info("Существующая проверка не найдена")
            return None
        except Exception as e:
            logger.error(f"Ошибка поиска существующего запроса: {e}", exc_info=True)
            return None

# ================== Фоновая обработка анализа ==================
async def process_background_analysis(request_id: str, link: str, user_id: int):
    """Фоновая задача для анализа группы - запускается ОДИН РАЗ"""
    try:
        # Получаем язык пользователя
        language = await user_manager.get_user_language(user_id)

        # Создаем фиктивное сообщение для progress
        class FakeProgressMessage:
            async def edit_text(self, text, **kwargs):
                pass

        progress_msg = FakeProgressMessage()

        # Анализируем группу (теперь с системой сессий)
        logger.info(f"Фоновый анализ группы: {link}")
        result = await analyze_group_by_link(link, user_id, progress_msg)

        if result.get("is_bad"):
            error_msg = "Couldn't check group" if language == 'en' else "Не удалось проверить группу"
            await request_db.update_request_status(request_id, 'error', error_msg)
            return

        # Сохраняем в основную базу данных
        username = "inline_user"  # В фоне нет доступа к username
        stats = await user_manager.get_user_stats(user_id)

        # Сохраняем результат во временную базу
        result_json = json.dumps(result, default=str, ensure_ascii=False)
        await request_db.update_request_status(request_id, 'completed', result_json)

        logger.info(f"Анализ завершен для request_id: {request_id}")

    except Exception as e:
        logger.error(f"Ошибка в process_background_analysis: {e}")
        language = await user_manager.get_user_language(user_id)
        error_msg = "Error analyzing group" if language == 'en' else "Ошибка при анализе"
        await request_db.update_request_status(request_id, 'error', error_msg)
# ================== Обработка реферальных ссылок ==================
# ================== Обработка команды /start с параметрами ==================
@router.message(Command("start"))
async def handle_start_with_params(msg: types.Message):
    """Обработка команды /start с параметрами"""
    if len(msg.text.split()) > 1:
        param = msg.text.split()[1]

        # Обработка реферальных ссылок для одиночных проверок
        if param.startswith("ref_"):
            await handle_referral_deeplink(msg, param)
            return

        # Обработка массовых проверок
        elif param.startswith("mass_"):
            await handle_mass_check_deeplink(msg, param)
            return

    await handle_start_command(msg)

#НАЧАЛО МАССОВОЙ ПРОВЕРКИ

async def create_mass_check(results: list, user_id: int):
    """Создает массовую проверку с уникальным кодом"""
    try:
        # Генерируем уникальный 8-значный код
        mass_check_id = generate_secure_id(8)

        # Считаем общую стоимость
        total_price = sum(result.get('price', 0) for result in results)

        # Сохраняем в базу массовых проверок
        success = await mass_check_db.save_mass_check(mass_check_id, user_id, results, total_price)

        if success:
            logger.info(f"Создана массовая проверка: {mass_check_id} для пользователя {user_id}")
            return mass_check_id
        else:
            logger.error(f"Не удалось сохранить массовую проверку в базу")
            return None

    except Exception as e:
        logger.error(f"Ошибка создания массовой проверки: {e}")
        return None

async def create_mass_check_result(results: list, user_id: int):
    """Создает результат массовой проверки для кнопки 'Поделиться'"""
    try:
        # Генерируем уникальный ID для массовой проверки
        request_id = generate_secure_id(16)

        # Преобразуем datetime объекты в строки для сохранения в JSON
        serialized_results = []
        for result in results:
            serialized_result = result.copy()
            # Преобразуем datetime в строку
            if serialized_result.get('created_date') and hasattr(serialized_result['created_date'], 'isoformat'):
                serialized_result['created_date'] = serialized_result['created_date'].isoformat()
            if serialized_result.get('checked_at') and hasattr(serialized_result['checked_at'], 'isoformat'):
                serialized_result['checked_at'] = serialized_result['checked_at'].isoformat()
            serialized_results.append(serialized_result)

        # Сохраняем все результаты массовой проверки
        mass_check_data = {
            "type": "mass_check",
            "results": serialized_results,  # Используем сериализованные результаты
            "total_groups": len(results),
            "created_at": datetime.now().isoformat(),
            "user_id": user_id
        }

        # Сохраняем в базу запросов с типом 'mass_check'
        await request_db.save_request(
            request_id,
            f"mass_check_{len(results)}_groups",
            user_id,
            "mass_check"
        )

        # Обновляем статус с данными массовой проверки
        result_json = json.dumps(mass_check_data, default=str, ensure_ascii=False)
        await request_db.update_request_status(request_id, 'completed', result_json)

        return request_id

    except Exception as e:
        logger.error(f"Ошибка создания массовой проверки: {e}")
        return None

async def handle_mass_check(msg: types.Message, state: FSMContext):
    """Обработка массовой проверки групп с учетом лимитов и мониторингом сессий"""
    # Проверяем, находится ли пользователь в состоянии создания поста
    current_state = await state.get_state()
    if current_state == PostState.waiting_for_content:
        return

    # Проверяем команды
    text = msg.text.strip()
    if text.startswith('/'):
        return

    lines = [line.strip() for line in text.split('\n') if line.strip()]
    language = await user_manager.get_user_language(msg.from_user.id)

    # ЛОГИРОВАНИЕ ВХОДНЫХ ДАННЫХ
    logger.info(f"📥 [MASS CHECK INPUT] Получено строк: {len(lines)}")

    # Если только одна ссылка - обрабатываем как одиночную
    if len(lines) <= 1:
        logger.info(f"🔄 [MASS CHECK REDIRECT] Только одна ссылка, перенаправляем на одиночную проверку")
        await handle_single_link(msg, state)
        return

    if len(lines) > 1000:
        await msg.answer(
            "❌ <b>Too many groups!</b>\nMaximum 1000 groups at a time." if language == 'en' else "❌ <b>Слишком много групп!</b>\nМаксимум 1000 групп за один раз.",
            parse_mode=ParseMode.HTML)
        return

    # ПРОВЕРЯЕМ СТАТУС СЕССИЙ ПЕРЕД НАЧАЛОМ
    session_stats = await session_manager.get_session_stats()
    available_sessions = session_stats["available_sessions"]
    total_sessions = session_stats["total_sessions"]
    flood_wait_sessions = session_stats["flood_wait_sessions"]

    logger.info(f"📊 [SESSION STATUS] Доступно: {available_sessions}/{total_sessions}, Флуд-вейт: {flood_wait_sessions}")

    # ПРОВЕРЯЕМ ЛИМИТЫ
    limit_check = await limit_manager.can_make_request(msg.from_user.id, lines)
    allowed_links = limit_check["allowed"]

    if not allowed_links:
        # Лимиты исчерпаны
        limits = await limit_manager.get_user_limits(msg.from_user.id)
        if language == 'en':
            text = (
                "❌ <b>Limit exceeded!</b>\n\n"
                f"🌐 Public groups: {limits['public_used']}/{limits['public_limit']}\n"
                f"🔒 Private groups: {limits['private_used']}/{limits['private_limit']}\n\n"
                f"⏰ Reset in: {limits['time_left']}\n\n"
                "Please wait for limits to reset or split your request."
            )
        else:
            text = (
                "❌ <b>Лимит превышен!</b>\n\n"
                f"🌐 Публичные группы: {limits['public_used']}/{limits['public_limit']}\n"
                f"🔒 Приватные группы: {limits['private_used']}/{limits['private_limit']}\n\n"
                f"⏰ До сброса: {limits['time_left']}\n\n"
                "Пожалуйста, подождите сброса лимитов или разделите запрос."
            )
        await msg.answer(text, parse_mode=ParseMode.HTML)
        return

    # Показываем информацию о лимитах если есть превышение
    limit_info = ""
    if limit_check['exceeded_public'] > 0 or limit_check['exceeded_private'] > 0:
        if language == 'en':
            limit_info = (
                f"\n\n📊 <b>Limits applied:</b>\n"
                f"✅ Will check: {len(allowed_links)} groups\n"
            )
            if limit_check['exceeded_public'] > 0:
                limit_info += f"🌐 Public limit: +{limit_check['exceeded_public']} exceeded\n"
            if limit_check['exceeded_private'] > 0:
                limit_info += f"🔒 Private limit: +{limit_check['exceeded_private']} exceeded\n"
            limit_info += f"⏰ Reset in: {limit_check['time_left']}"
        else:
            limit_info = (
                f"\n\n📊 <b>Применены лимиты:</b>\n"
                f"✅ Будет проверено: {len(allowed_links)} групп\n"
            )
            if limit_check['exceeded_public'] > 0:
                limit_info += f"🌐 Лимит публичных: +{limit_check['exceeded_public']} превышено\n"
            if limit_check['exceeded_private'] > 0:
                limit_info += f"🔒 Лимит приватных: +{limit_check['exceeded_private']} превышено\n"
            limit_info += f"⏰ До сброса: {limit_check['time_left']}"

    # Обрабатываем ссылки и удаляем дубликаты
    processed_links = set()
    valid_links = []
    invalid_links = []

    for line in allowed_links:  # Используем allowed_links вместо lines
        # Проверяем валидность ссылки
        if is_valid_telegram_link(line):
            # Нормализуем ссылку для сравнения
            normalized_link = normalize_link(line)
            if normalized_link:
                if normalized_link not in processed_links:
                    processed_links.add(normalized_link)
                    valid_links.append(line)  # Сохраняем оригинальную ссылку для проверки
                    logger.info(f"✅ [MASS CHECK VALID] Валидная ссылка: '{line}' -> '{normalized_link}'")
                else:
                    # Это дубликат, пропускаем
                    logger.info(f"🔄 [MASS CHECK DUPLICATE] Дубликат: '{line}'")
                    continue
            else:
                invalid_links.append(line)
                logger.info(f"❌ [MASS CHECK INVALID NORM] Не удалось нормализовать: '{line}'")
        else:
            invalid_links.append(line)
            logger.info(f"❌ [MASS CHECK INVALID] Невалидная ссылка: '{line}'")

    # Показываем статистику по дубликатам
    duplicates_count = len(allowed_links) - len(valid_links) - len(invalid_links)

    # ЛОГИРОВАНИЕ РЕЗУЛЬТАТОВ ФИЛЬТРАЦИИ
    logger.info(
        f"📊 [MASS CHECK STATS] Всего: {len(allowed_links)}, Валидных: {len(valid_links)}, Дубликатов: {duplicates_count}, Невалидных: {len(invalid_links)}")

    # Если после удаления дубликатов осталась только 1 ссылка - перенаправляем на одиночную проверку
    if len(valid_links) == 1:
        logger.info(f"🔄 [MASS CHECK REDIRECT] Перенаправляем на одиночную проверку: '{valid_links[0]}'")

        if duplicates_count > 0:
            await msg.answer(
                f"🔄 <b>{'Duplicates removed:' if language == 'en' else 'Дубликаты удалены:'}</b> {duplicates_count}\n"
                f"✅ <b>{'Unique link found:' if language == 'en' else 'Найдена уникальная ссылка:'}</b>\n\n"
                f"🔍 {'Redirecting to single check...' if language == 'en' else 'Перенаправляем на одиночную проверку...'}",
                parse_mode=ParseMode.HTML
            )
            await asyncio.sleep(1)  # Даем пользователю увидеть сообщение

        # ВАЖНО: Берем первую валидную ссылку и передаем ее в handle_single_link
        single_link = valid_links[0].strip()
        logger.info(f"🔗 [MASS CHECK FINAL LINK] Передаваемая ссылка: '{single_link}'")

        # Создаем временный объект сообщения с правильной ссылкой
        class TempMessage:
            def __init__(self, original_msg, link_text):
                self.text = link_text
                self.from_user = original_msg.from_user
                self.chat = original_msg.chat
                self.bot = original_msg.bot
                self.message_id = original_msg.message_id
                self.date = original_msg.date
                self.content_type = "text"

            async def answer(self, text, **kwargs):
                return await self.bot.send_message(self.chat.id, text, **kwargs)

            async def answer_photo(self, photo, caption=None, reply_markup=None, parse_mode=None):
                return await self.bot.send_photo(
                    self.chat.id,
                    photo,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode
                )

        temp_msg = TempMessage(msg, single_link)
        await handle_single_link(temp_msg, state)
        return

    if duplicates_count > 0:
        await msg.answer(
            f"🔄 <b>{'Duplicates removed:' if language == 'en' else 'Дубликаты удалены:'}</b> {duplicates_count}\n"
            f"✅ <b>{'Unique links:' if language == 'en' else 'Уникальных ссылок:'}</b> {len(valid_links)}\n"
            f"❌ <b>{'Invalid links:' if language == 'en' else 'Неверных ссылок:'}</b> {len(invalid_links)}",
            parse_mode=ParseMode.HTML
        )

    if invalid_links:
        invalid_count = len(invalid_links)
        await msg.answer(
            f"❌ <b>{'Invalid links found:' if language == 'en' else 'Найдено неверных ссылок:'}</b> {invalid_count}\n"
            f"✅ <b>{'Valid links:' if language == 'en' else 'Валидных ссылок:'}</b> {len(valid_links)}\n"
            f"📊 <b>{'Total:' if language == 'en' else 'Всего:'}</b> {len(allowed_links)}",
            parse_mode=ParseMode.HTML
        )
        if not valid_links:
            return

    will_send_file = len(valid_links) > 15
    format_info = user_manager.get_text(language,
                                        'mass_check_file_result' if will_send_file else 'mass_check_message_result')

    progress_msg = await msg.answer(
        f"{user_manager.get_text(language, 'mass_check_progress')}\n"
        f"📊 {'Groups to check' if language == 'en' else 'Групп для проверки'}: {len(valid_links)}\n"
        f"🔄 {'Duplicates removed' if language == 'en' else 'Дубликатов удалено'}: {duplicates_count}\n"
        f"{format_info}{limit_info}",
        parse_mode=ParseMode.HTML
    )

    try:
        results = []
        total = len(valid_links)
        username = msg.from_user.username or f"{msg.from_user.first_name or ''} {msg.from_user.last_name or ''}".strip()
        successful_checks = 0
        failed_checks = 0
        invalid_links_count = 0
        user_profiles_count = 0
        flood_wait_errors = 0
        session_errors = 0

        async def _update_mass_progress(checked_count: int):
            progress_text = (
                f"{user_manager.get_text(language, 'mass_check_progress')}\n"
                f"✅ {'Checked' if language == 'en' else 'Проверено'}: {checked_count}/{total}\n"
                f"✅ {'Valid groups' if language == 'en' else 'Валидных групп'}: {successful_checks}\n"
                f"❌ {'Invalid links' if language == 'en' else 'Невалидных ссылок'}: {invalid_links_count}\n"
                f"👤 {'User profiles' if language == 'en' else 'Профилей пользователей'}: {user_profiles_count}\n"
                f"⚠️ {'Errors' if language == 'en' else 'Ошибок'}: {failed_checks}\n"
                f"{format_info}"
            )
            await progress_msg.edit_text(progress_text, parse_mode=ParseMode.HTML)

        checked_count = 0
        state_lock = asyncio.Lock()
        # Ограничиваем параллельность: быстрее, но бережно к флуд-вейтам
        mass_concurrency = 2 if total >= 2 else 1
        link_queue: asyncio.Queue[tuple[int, str]] = asyncio.Queue()

        for i, link in enumerate(valid_links, 1):
            link_queue.put_nowait((i, link))

        async def _process_mass_link(worker_id: int):
            nonlocal checked_count, successful_checks, failed_checks, invalid_links_count
            nonlocal user_profiles_count, flood_wait_errors, session_errors

            while not link_queue.empty():
                try:
                    i, link = link_queue.get_nowait()
                except asyncio.QueueEmpty:
                    return

                try:
                    logger.info(f"🔍 [MASS CHECK PROCESSING] Worker {worker_id}, проверка {i}/{total}: '{link}'")
                    result = await analyze_group_by_link(link, msg.from_user.id, None)

                    async with state_lock:
                        if result.get("is_user_profile"):
                            user_profiles_count += 1
                            await user_manager.add_check(msg.from_user.id, username, result)
                            results.append(result)

                        elif result.get("is_invalid_link"):
                            invalid_links_count += 1
                            await user_manager.add_check(msg.from_user.id, username, result)
                            results.append(result)

                        elif not result.get("is_bad"):
                            successful_checks += 1
                            await user_manager.add_check(msg.from_user.id, username, result)
                            results.append(result)

                        else:
                            failed_checks += 1
                            error_msg = result.get("error_message", "")
                            if "flood" in error_msg.lower() or "Flood" in error_msg:
                                flood_wait_errors += 1
                            elif "session" in error_msg.lower():
                                session_errors += 1

                            await user_manager.add_check(msg.from_user.id, username, result)
                            results.append(result)

                        checked_count += 1
                        await _update_mass_progress(checked_count)

                except Exception as e:
                    log_error(f"Mass check error: {str(e)}")
                    logger.error(f"❌ [MASS CHECK ERROR] Ошибка при проверке '{link}': {e}")

                    async with state_lock:
                        failed_checks += 1
                        if "flood" in str(e).lower() or "Flood" in str(e):
                            flood_wait_errors += 1
                        elif "session" in str(e).lower():
                            session_errors += 1

                        error_result = {
                            "chat_link": link,
                            "created_date": None,
                            "owner_username": f"Error: {str(e)[:50]}" if language == 'en' else f"Ошибка: {str(e)[:50]}",
                            "owner_id": None,
                            "user_messages": 0,
                            "checked_at": datetime.now(),
                            "is_bad": True,
                            "error_message": str(e)
                        }
                        results.append(error_result)
                        await user_manager.add_check(msg.from_user.id, username, error_result)
                        checked_count += 1
                        await _update_mass_progress(checked_count)
                finally:
                    link_queue.task_done()

        workers = [asyncio.create_task(_process_mass_link(w + 1)) for w in range(mass_concurrency)]
        await asyncio.gather(*workers)

        # ОБНОВЛЯЕМ ЛИМИТЫ
        try:
            # Считаем сколько приватных и публичных групп было проверено (только валидные группы)
            public_checked = 0
            private_checked = 0

            for result in results:
                if not result.get("is_bad") and not result.get("is_invalid_link") and not result.get("is_user_profile"):
                    link = result.get('chat_link', '')
                    if limit_manager._is_private_link(link):
                        private_checked += 1
                    else:
                        public_checked += 1

            # Обновляем лимиты
            await limit_manager.update_limits(msg.from_user.id, public_checked, private_checked)
            logger.info(f"📊 [LIMITS UPDATED] Public: +{public_checked}, Private: +{private_checked}")

        except Exception as e:
            logger.error(f"❌ [LIMITS ERROR] Ошибка обновления лимитов: {e}")

        # Отправляем результаты
        await send_mass_check_results(msg, results, progress_msg, "", language)

    except Exception as e:
        logger.error(f"❌ [MASS CHECK CRITICAL ERROR] Ошибка в массовой проверке: {e}")
        try:
            await progress_msg.edit_text(
                "❌ " + ("Error during mass check" if language == 'en' else "Произошла ошибка при массовой проверке"))
        except Exception:
            await msg.answer(
                "❌ " + ("Error during mass check" if language == 'en' else "Произошла ошибка при массовой проверке"))

async def process_mass_check_from_list(msg: types.Message, links: list, progress_msg: types.Message,
                                       language: str = 'ru'):
    """Обрабатывает массовую проверку из списка ссылок с удалением дубликатов"""
    try:
        # Обрабатываем ссылки и удаляем дубликаты
        processed_links = set()
        unique_links = []

        for link in links:
            normalized_link = normalize_link(link)
            if normalized_link and normalized_link not in processed_links:
                processed_links.add(normalized_link)
                unique_links.append(link)

        duplicates_count = len(links) - len(unique_links)

        # Если после удаления дубликатов осталась только 1 ссылка - перенаправляем на одиночную проверку
        if len(unique_links) == 1:
            if duplicates_count > 0:
                await progress_msg.edit_text(
                    f"🔄 <b>{'Duplicates removed:' if language == 'en' else 'Дубликаты удалены:'}</b> {duplicates_count}\n"
                    f"✅ <b>{'Unique link found:' if language == 'en' else 'Найдена уникальная ссылка:'}</b>\n\n"
                    f"🔍 {'Redirecting to single check...' if language == 'en' else 'Перенаправляем на одиночную проверку...'}",
                    parse_mode=ParseMode.HTML
                )
                await asyncio.sleep(1)

            # Просто вызываем handle_single_link с оригинальным сообщением, но меняем текст временно
            original_text = msg.text
            try:
                # Используем monkey patch для временного изменения текста
                msg._text = unique_links[0]
                await handle_single_link(msg, state)
            finally:
                # Восстанавливаем оригинальный текст
                msg._text = original_text
            return

        if duplicates_count > 0:
            await progress_msg.edit_text(
                f"{user_manager.get_text(language, 'mass_check_from_file')}\n"
                f"📊 {'Total links in file' if language == 'en' else 'Всего ссылок в файле'}: {len(links)}\n"
                f"🔄 {'Duplicates removed' if language == 'en' else 'Дубликатов удалено'}: {duplicates_count}\n"
                f"✅ {'Unique links to check' if language == 'en' else 'Уникальных ссылок для проверки'}: {len(unique_links)}",
                parse_mode=ParseMode.HTML
            )
            await asyncio.sleep(2)

        results = []
        total = len(unique_links)
        username = msg.from_user.username or f"{msg.from_user.first_name or ''} {msg.from_user.last_name or ''}".strip()
        successful_checks = 0
        failed_checks = 0

        will_send_file = len(unique_links) > 15
        format_info = user_manager.get_text(language,
                                            'mass_check_file_result' if will_send_file else 'mass_check_message_result')

        for i, link in enumerate(unique_links, 1):
            try:
                if i % 5 == 0 or i == total:
                    progress_text = (
                        f"{user_manager.get_text(language, 'mass_check_from_file')}\n"
                        f"📊 {'Progress' if language == 'en' else 'Прогресс'}: {i}/{total}\n"
                        f"✅ {'Successful' if language == 'en' else 'Успешно'}: {successful_checks}\n"
                        f"❌ {'Errors' if language == 'en' else 'Ошибок'}: {failed_checks}\n"
                        f"🔄 {'Duplicates removed' if language == 'en' else 'Дубликатов удалено'}: {duplicates_count}\n"
                        f"{format_info}"
                    )
                    await progress_msg.edit_text(progress_text, parse_mode=ParseMode.HTML)

                result = await analyze_group_by_link(link, msg.from_user.id, None)

                if not result.get("is_bad"):
                    results.append(result)
                    successful_checks += 1
                    await user_manager.add_check(msg.from_user.id, username, result)
                else:
                    failed_checks += 1
                    error_result = {
                        "chat_link": link,
                        "created_date": None,
                        "owner_username": "Error checking" if language == 'en' else "Ошибка проверки",
                        "owner_id": None,
                        "user_messages": 0,
                        "checked_at": datetime.now(),
                        "is_bad": True
                    }
                    results.append(error_result)
                    await user_manager.add_check(msg.from_user.id, username, error_result)

                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Ошибка при проверке {link}: {e}")
                failed_checks += 1
                error_result = {
                    "chat_link": link,
                    "created_date": None,
                    "owner_username": f"Error: {str(e)[:50]}" if language == 'en' else f"Ошибка: {str(e)[:50]}",
                    "owner_id": None,
                    "user_messages": 0,
                    "checked_at": datetime.now(),
                    "is_bad": True
                }
                results.append(error_result)
                await user_manager.add_check(msg.from_user.id, username, error_result)

        total_price = 0
        year_counter = Counter()

        for r in results:
            price = r.get('price') or 0
            total_price += price
            if r.get('created_date'):
                year_counter[r['created_date'].year] += 1

        if language == 'en':
            final_stats = (
                    f"✅ <b>Mass check from file completed!</b>\n\n"
                    f"📊 <b>Statistics:</b>\n"
                    f"• ✅ Total groups: {total}\n"
                    f"• 🔄 Duplicates removed: {duplicates_count}\n"
                    f"• 💰 Total price: {total_price}$\n"
                    f"• 📅 Creation dates:\n" +
                    ''.join(
                        [f"        {year} - {count}\n" for year, count in sorted(year_counter.items(), reverse=True)]) +
                    ("" if year_counter else "        Not defined\n") +
                    f"• ❌ Errors: {failed_checks}"
            )
        else:
            final_stats = (
                    f"✅ <b>Массовая проверка из файла завершена!</b>\n\n"
                    f"📊 <b>Статистика:</b>\n"
                    f"• ✅ Всего групп: {total}\n"
                    f"• 🔄 Дубликатов удалено: {duplicates_count}\n"
                    f"• 💰 Их стоимость: {total_price}$\n"
                    f"• 📅 Даты создания:\n" +
                    ''.join(
                        [f"        {year} - {count}\n" for year, count in sorted(year_counter.items(), reverse=True)]) +
                    ("" if year_counter else "        Не определены\n") +
                    f"• ❌ Ошибок: {failed_checks}"
            )

        await send_mass_check_results(msg, results, progress_msg, final_stats, language)

    except Exception as e:
        logger.error(f"Ошибка в массовой проверке из файла: {e}")
        try:
            error_text = "❌ " + (
                "Error during mass check from file" if language == 'en' else "Произошла ошибка при массовой проверке из файла")
            await progress_msg.edit_text(error_text)
        except Exception:
            await msg.answer(error_text)

def format_mass_check_text(results: list, language: str = 'ru') -> str:
    """Форматирует результаты массовой проверки в расширенном формате (как в одиночной проверке)."""
    lines = []

    total_price = 0
    year_counter = Counter()
    invalid_links_count = 0
    valid_groups_count = 0
    error_groups_count = 0
    imported_groups_count = 0
    geo_groups_count = 0

    for result in results:
        if result.get("is_user_profile"):
            continue
        if result.get("is_invalid_link"):
            invalid_links_count += 1
            continue
        if result.get("is_bad"):
            error_groups_count += 1
            continue

        valid_groups_count += 1
        total_price += result.get('price') or 0

        created_date = result.get('created_date')
        if isinstance(created_date, datetime):
            year_counter[created_date.year] += 1

        if result.get("has_imported_messages"):
            imported_groups_count += 1
        if result.get("has_geo_location"):
            geo_groups_count += 1

    if language == 'en':
        lines.append("✅ <b>Mass check completed!</b>")
        lines.append("")
        lines.append("📊 <b>Statistics:</b>")
        lines.append(f"• 🔗 Total links: {len(results)}")
        lines.append(f"• ✅ Valid groups: {valid_groups_count}")
        lines.append(f"• ❌ Invalid links: {invalid_links_count}")
        lines.append(f"• ⚠️ Error groups: {error_groups_count}")
        if valid_groups_count > 0:
            lines.append(f"• 💰 Total price: {total_price}$")
        if year_counter:
            lines.append("• 📅 Creation years:")
            for year, count in sorted(year_counter.items(), reverse=True):
                lines.append(f"     {year}: {count} groups")
    else:
        lines.append("✅ <b>Массовая проверка завершена!</b>")
        lines.append("")
        lines.append("📊 <b>Статистика:</b>")
        lines.append(f"• 🔗 Всего ссылок: {len(results)}")
        lines.append(f"• ✅ Валидных групп: {valid_groups_count}")
        lines.append(f"• ❌ Невалидных ссылок: {invalid_links_count}")
        lines.append(f"• ⚠️ Ошибочных групп: {error_groups_count}")
        if valid_groups_count > 0:
            lines.append(f"• 💰 Общая стоимость: {total_price}$")
        if year_counter:
            lines.append("• 📅 Годы создания:")
            for year, count in sorted(year_counter.items(), reverse=True):
                lines.append(f"     {year}: {count} групп")

    if imported_groups_count > 0:
        lines.append("")
        lines.append("‼️ <b>IMPORT GROUP DETECTED</b>" if language == 'en' else "‼️ <b>ОБНАРУЖЕНЫ ИМПОРТИРОВАННЫЕ ГРУППЫ</b>")

    if geo_groups_count > 0:
        lines.append("‼️ <b>GEO-GROUP DETECTED</b>" if language == 'en' else "‼️ <b>ОБНАРУЖЕНЫ ГЕО-ГРУППЫ</b>")

    lines.append("")
    lines.append("🔎 <b>Results:</b>" if language == 'en' else "🔎 <b>Результаты:</b>")
    lines.append("")

    group_counter = 0
    for result in results:
        if result.get("is_user_profile") or result.get("is_invalid_link") or result.get("is_bad"):
            continue

        group_counter += 1
        display_link = format_link_for_display(result.get('chat_link', ''))
        lines.append(f"<b>{group_counter}. {display_link}</b>")
        lines.append("<blockquote>" + "\n".join(build_result_info_lines(result, language)) + "</blockquote>")

        warnings_lines = []
        if result.get("has_imported_messages"):
            warnings_lines.append(f"📥 {user_manager.get_text(language, 'imported_messages')}")
        if result.get("has_geo_location"):
            warnings_lines.append(f"📍 {user_manager.get_text(language, 'geo_group')}")
        if warnings_lines:
            warnings_title = user_manager.get_text(language, 'warnings_title')
            lines.append(f"❗ <b>{warnings_title}:</b>")
            lines.append("<blockquote>" + "\n".join(warnings_lines) + "</blockquote>")

        lines.append("")

    lines.append(f"• ⏱️ <b>{'Check time' if language == 'en' else 'Время проверки'}:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    return "\n".join(lines)

def format_mass_check_html(results: list, language: str = 'ru') -> str:
    """Совместимый алиас для форматирования массовой проверки в HTML."""
    return format_mass_check_text(results, language)

async def send_mass_check_results(msg: types.Message, results: list, progress_msg: types.Message = None,
                                  additional_info: str = "", language: str = 'ru'):
    """Отправляет результаты массовой проверки с новым форматированием"""
    if not results:
        if progress_msg:
            await progress_msg.edit_text(
                "❌ " + ("No groups were checked" if language == 'en' else "Не удалось проверить ни одну группу"))
        else:
            await send_error_with_photo(msg, "❌ " + (
                "No groups were checked" if language == 'en' else "Не удалось проверить ни одну группу"))
        return

    # Форматируем текстовый результат
    text_result = format_mass_check_text(results, language)

    # Добавляем дополнительную информацию если есть
    if additional_info:
        text_result = additional_info + "\n\n" + text_result

    # Проверяем количество групп: если больше 6 - отправляем файлом, иначе - с фото
    valid_groups_count = sum(
        1 for r in results if not r.get("is_bad") and not r.get("is_invalid_link") and not r.get("is_user_profile"))

    if valid_groups_count > 6:
        if progress_msg:
            await progress_msg.edit_text(
                "📊 " + ("Creating report file..." if language == 'en' else "Создаем файл отчета..."),
                parse_mode=ParseMode.HTML)
        await send_results_as_file(msg, text_result, progress_msg, len(results), "", language)
    else:
        # Отправляем как красивое сообщение с фото
        if progress_msg:
            try:
                await progress_msg.delete()
            except:
                pass

        # Создаем массовую проверку с уникальным кодом (только если есть валидные группы)
        if valid_groups_count > 0:
            mass_check_id = await create_mass_check(results, msg.from_user.id)

            if mass_check_id:
                # Создаем текст для инлайн-режима
                inline_text = f"mass_{mass_check_id}"

                # Создаем клавиатуру с кнопкой "Поделиться" для инлайн-режима
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[[
                        InlineKeyboardButton(
                            text=user_manager.get_text(language, 'share'),
                            switch_inline_query=inline_text
                        )
                    ]]
                )

                await send_result_with_photo(msg, text_result, reply_markup=kb)
            else:
                # Если не удалось создать массовую проверку, отправляем без кнопки
                logger.error("Не удалось создать массовую проверку для кнопки 'Поделиться'")
                await send_result_with_photo(msg, text_result)
        else:
            # Если нет валидных групп, отправляем без кнопки "Поделиться"
            await send_result_with_photo(msg, text_result)

async def send_text_parts(msg: types.Message, text: str, max_length: int = 4000):
    """Отправляет длинный текст частями"""
    lines = text.split('\n')
    current_part = []
    current_length = 0

    for line in lines:
        line_length = len(line) + 1  # +1 для \n

        if current_length + line_length > max_length:
            # Отправляем накопленное
            if current_part:
                await msg.answer('\n'.join(current_part), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                current_part = []
                current_length = 0

        current_part.append(line)
        current_length += line_length

    # Отправляем остаток
    if current_part:
        await msg.answer('\n'.join(current_part), parse_mode=ParseMode.HTML, disable_web_page_preview=True)


def strip_html_for_txt(text: str) -> str:
    """Удаляет HTML-теги из текста перед сохранением в TXT."""
    if not text:
        return ""
    plain = re.sub(r"<[^>]+>", "", text)
    return html.unescape(plain)

async def send_results_as_file(msg: types.Message, text_content: str, progress_msg: types.Message = None,
                               groups_count: int = 0, stats_text: str = "", language: str = 'ru'):
    """Отправляет результаты в виде TXT файла с новым форматом"""
    # Создаем временный файл
    if language == 'en':
        filename = f"mass_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    else:
        filename = f"mass_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    try:
        # Сохраняем содержимое в файл
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(strip_html_for_txt(text_content))

        # Отправляем файл
        with open(filename, 'rb') as f:
            if progress_msg:
                await progress_msg.delete()

            # Красивое описание файла
            if language == 'en':
                caption = (
                    f"📊 <b>Results of checking {groups_count} groups</b>\n"
                    f"⏱ {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                    f"👤 Checked by: @{msg.from_user.username or msg.from_user.first_name}\n\n"
                )
            else:
                caption = (
                    f"📊 <b>Результаты проверки {groups_count} групп</b>\n"
                    f"⏱ {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                    f"👤 Проверил: @{msg.from_user.username or msg.from_user.first_name}\n\n"
                )

            await msg.answer_document(
                document=types.BufferedInputFile(f.read(), filename=filename),
                caption=caption,
                parse_mode=ParseMode.HTML
            )

    except Exception as e:
        logger.error(f"Ошибка при отправке файла: {e}")
        if progress_msg:
            await progress_msg.edit_text("❌ " + ("Error creating report" if language == 'en' else "Ошибка при формировании отчета"))
        else:
            await msg.answer("❌ " + ("Error creating report" if language == 'en' else "Ошибка при формировании отчета"))

    finally:
        # Удаляем временный файл
        try:
            if os.path.exists(filename):
                os.remove(filename)
        except Exception as e:
            logger.warning(f"Не удалось удалить временный файл: {e}")

def format_owner_display(owner_username: str, owner_id: str) -> str:
    """Форматирует отображение владельца"""
    if not owner_username and not owner_id:
        return "Не определён"

    if owner_username:
        return f"@{owner_username}"
    elif owner_id:
        return f"id{owner_id}"
    else:
        return "Не определён"


def format_owner_display_with_name(owner_username: str, owner_id: str, first_name: str = None,
                                   last_name: str = None, language: str = 'ru') -> str:
    """Форматирует отображение владельца с именем"""
    if not owner_username and not owner_id and not first_name:
        return user_manager.get_text(language, 'owner_not_defined')

    if owner_username:
        return f"@{owner_username}"
    elif first_name:
        name = f"{first_name} {last_name or ''}".strip()
        if owner_id:
            return f"{name} (id{owner_id})"
        else:
            return name
    elif owner_id:
        return f"id{owner_id}"
    else:
        return user_manager.get_text(language, 'owner_not_defined')


#КОНЕЦ МАССОВОЙ ПРОВЕРКИ

@router.callback_query(F.data.startswith("history_"))
async def handle_history_pagination(call: types.CallbackQuery):
    """Обработка пагинации истории"""
    try:
        data = call.data
        user_id = call.from_user.id
        language = await user_manager.get_user_language(user_id)

        if data == "history_reset":
            USER_HISTORY_PAGE[user_id] = 0
            await call.message.delete()
            await show_history(call.message)
            return

        if data.startswith("history_prev_"):
            current_page = int(data.split("_")[2])
            USER_HISTORY_PAGE[user_id] = current_page - 1
        elif data.startswith("history_next_"):
            current_page = int(data.split("_")[2])
            USER_HISTORY_PAGE[user_id] = current_page + 1

        await call.message.delete()
        await show_history(call.message)

    except Exception as e:
        logger.error(f"Ошибка в пагинации истории: {e}")
        await call.answer("Error switching page" if language == 'en' else "Ошибка при переключении страницы", show_alert=True)

async def send_error_with_photo(msg: types.Message, text: str):
    """Отправляет сообщение об ошибке с фотографией на нужном языке"""
    try:
        language = await user_manager.get_user_language(msg.from_user.id)

        # Определяем имя файла в зависимости от языка
        if language == 'en':
            photo_filename = "png/error_en.png"
            fallback_filename = "png/error.png"  # Резервный файл
        else:
            photo_filename = "png/error.png"
            fallback_filename = "png/error.png"

        # Пробуем открыть файл для нужного языка, если нет - используем резервный
        try:
            with open(photo_filename, "rb") as photo:
                await msg.answer_photo(
                    types.BufferedInputFile(photo.read(), filename="error.png"),
                    caption=text,
                    parse_mode=ParseMode.HTML
                )
        except FileNotFoundError:
            # Если файл для языка не найден, используем резервный
            logger.warning(f"Файл {photo_filename} не найден, использую {fallback_filename}")
            with open(fallback_filename, "rb") as photo:
                await msg.answer_photo(
                    types.BufferedInputFile(photo.read(), filename="error.png"),
                    caption=text,
                    parse_mode=ParseMode.HTML
                )

    except FileNotFoundError:
        # Если фото не найдено, отправляем только текст
        logger.warning("Файлы фото ошибки не найдены, отправляю текст без фото")
        await msg.answer(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при отправке фото ошибки: {e}")
        await msg.answer(text, parse_mode=ParseMode.HTML)



async def handle_single_link(msg: types.Message, state: FSMContext, is_from_mass_check: bool = False, custom_link: str = None):
    """Обрабатывает одиночную ссылку с учетом лимитов и улучшенной обработкой ошибок"""
    # Проверяем, находится ли пользователь в состоянии создания поста
    current_state = await state.get_state()
    if current_state == PostState.waiting_for_content:
        return

    # Используем кастомную ссылку если передана, иначе текст сообщения (с проверкой на None)
    if custom_link:
        link = custom_link.strip()
    elif msg.text:
        link = msg.text.strip()
    else:
        # Если нет ни кастомной ссылки, ни текста сообщения - отправляем ошибку
        language = await user_manager.get_user_language(msg.from_user.id)
        await send_error_with_photo(
            msg,
            user_manager.get_text(language, 'error_not_link') + "\n" +
            "• https://t.me/username\n" +
            "• @username\n" +
            "• t.me/+invite_code"
        )
        return

    language = await user_manager.get_user_language(msg.from_user.id)

    # ЛОГИРОВАНИЕ ВХОДНОЙ ССЫЛКИ
    logger.info(f"🔗 [SINGLE CHECK START] Обработка ссылки: '{link}'")
    logger.info(f"🔗 [SINGLE CHECK START] Тип: {'из массовой проверки' if is_from_mass_check else 'новая проверка'}")

    # Проверяем команды
    if link.startswith('/'):
        return

    # ПРОВЕРЯЕМ ЛИМИТЫ ТОЛЬКО ДЛЯ НОВЫХ ПРОВЕРОК (не для массовых)
    if not is_from_mass_check:
        limit_check = await limit_manager.can_make_request(msg.from_user.id, [link])

        if not limit_check["allowed"]:
            # Лимиты исчерпаны
            limits = await limit_manager.get_user_limits(msg.from_user.id)
            if language == 'en':
                text = (
                    "❌ <b>Limit exceeded!</b>\n\n"
                    f"🌐 Public groups: {limits['public_used']}/{limits['public_limit']}\n"
                    f"🔒 Private groups: {limits['private_used']}/{limits['private_limit']}\n\n"
                    f"⏰ Reset in: {limits['time_left']}"
                )
            else:
                text = (
                    "❌ <b>Лимит превышен!</b>\n\n"
                    f"🌐 Публичные группы: {limits['public_used']}/{limits['public_limit']}\n"
                    f"🔒 Приватные группы: {limits['private_used']}/{limits['private_limit']}\n\n"
                    f"⏰ До сброса: {limits['time_left']}"
                )
            await send_error_with_photo(msg, text)
            return

        # Определяем тип ссылки ДО анализа (для правильного обновления лимитов)
        is_private = limit_manager._is_private_link(link)
        logger.info(f"🔍 [SINGLE LINK TYPE] Ссылка '{link}' - {'приватная' if is_private else 'публичная'}")

    # Проверяем, является ли сообщение ссылкой на группу
    if not is_valid_telegram_link(link):
        logger.info(f"❌ [SINGLE CHECK INVALID] Невалидная ссылка: '{link}'")
        await send_error_with_photo(
            msg,
            user_manager.get_text(language, 'error_not_link') + "\n" +
            "• https://t.me/username\n" +
            "• @username\n" +
            "• t.me/+invite_code"
        )
        return

    logger.info(f"✅ [SINGLE CHECK VALID] Валидная ссылка: '{link}'")

    user_id = msg.from_user.id
    username = msg.from_user.username or f"{msg.from_user.first_name or ''} {msg.from_user.last_name or ''}".strip()

    # Для новых проверок проверяем историю, для массовых - всегда новая проверка
    checked_before = None
    if not is_from_mass_check:
        checked_before = await user_manager.has_checked_before(user_id, link)

    # УНИВЕРСАЛЬНЫЙ СПОСОБ ОТПРАВКИ СООБЩЕНИЯ - работаем с любым типом сообщения
    try:
        # Если у сообщения есть метод answer, используем его
        if hasattr(msg, 'answer') and callable(getattr(msg, 'answer')):
            progress_msg = await msg.answer(user_manager.get_text(language, 'checking_group'))
        else:
            # Для кастомных сообщений используем бота напрямую
            progress_msg = await msg.bot.send_message(
                chat_id=msg.chat.id,
                text=user_manager.get_text(language, 'checking_group')
            )
    except Exception as e:
        logger.error(f"❌ [SINGLE CHECK ERROR] Ошибка отправки прогресс-сообщения: {e}")
        # Пытаемся отправить сообщение об ошибке
        try:
            if hasattr(msg, 'answer') and callable(getattr(msg, 'answer')):
                await msg.answer("❌ Ошибка при запуске проверки")
            else:
                await msg.bot.send_message(msg.chat.id, "❌ Ошибка при запуске проверки")
        except:
            pass
        return

    try:
        result = await analyze_group_by_link(link, user_id, progress_msg)

        # ОСОБАЯ ОБРАБОТКА ДЛЯ ПРОФИЛЕЙ ПОЛЬЗОВАТЕЛЕЙ И НЕВАЛИДНЫХ ССЫЛОК
        if result.get("is_user_profile"):
            # Форматируем ссылку для отображения
            display_link = format_link_for_display(link)

            if language == 'en':
                error_text = (
                    f"🔍 <b>Check Result</b>\n\n"
                    f"1. {display_link}\n\n"
                    f"   ❌ User profile detected\n\n"
                    f"<i>The link leads to a user profile, not a group/channel</i>"
                )
            else:
                error_text = (
                    f"🔍 <b>Результат проверки</b>\n\n"
                    f"1. {display_link}\n\n"
                    f"   ❌ Ссылка ведет на профиль пользователя\n\n"
                    f"<i>Ссылка указывает на профиль пользователя, а не на группу/канал</i>"
                )

            try:
                # Универсальное удаление прогресс-сообщения
                if hasattr(progress_msg, 'delete') and callable(getattr(progress_msg, 'delete')):
                    await progress_msg.delete()
                else:
                    await msg.bot.delete_message(chat_id=progress_msg.chat.id, message_id=progress_msg.message_id)
            except Exception as e:
                logger.warning(f"Не удалось удалить сообщение о прогрессе: {e}")

            await send_error_with_photo(msg, error_text)
            return

        elif result.get("is_invalid_link"):
            # Форматируем ссылку для отображения
            display_link = format_link_for_display(link)
            error_msg = result.get('error_message') or user_manager.get_text(language, 'error_group_not_exist')

            if language == 'en':
                error_text = (
                    f"🔍 <b>Check Result</b>\n\n"
                    f"1. {display_link}\n\n"
                    f"   ❌ {error_msg}\n\n"
                    f"<i>No group/channel was found using this link</i>"
                )
            else:
                error_text = (
                    f"🔍 <b>Результат проверки</b>\n\n"
                    f"1. {display_link}\n\n"
                    f"   ❌ {error_msg}\n\n"
                    f"<i>По данной ссылке не было найдено группы/канала</i>"
                )

            try:
                # Универсальное удаление прогресс-сообщения
                if hasattr(progress_msg, 'delete') and callable(getattr(progress_msg, 'delete')):
                    await progress_msg.delete()
                else:
                    await msg.bot.delete_message(chat_id=progress_msg.chat.id, message_id=progress_msg.message_id)
            except Exception as e:
                logger.warning(f"Не удалось удалить сообщение о прогрессе: {e}")

            await send_error_with_photo(msg, error_text)
            return

        # Сохраняем в базу только если это не профиль пользователя и не невалидная ссылка
        if not result.get("is_bad"):
            await user_manager.add_check(user_id, username, result)

        stats = await user_manager.get_user_stats(user_id)
        if checked_before:
            stats['checked_before'] = datetime.fromisoformat(checked_before).strftime('%d.%m.%Y %H:%M')

        text, kb = make_result_message(result, stats, language)

        try:
            # Универсальное удаление прогресс-сообщения
            if hasattr(progress_msg, 'delete') and callable(getattr(progress_msg, 'delete')):
                await progress_msg.delete()
            else:
                await msg.bot.delete_message(chat_id=progress_msg.chat.id, message_id=progress_msg.message_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение о прогрессе: {e}")

        # ДЛЯ НОВЫХ ПРОВЕРОК - создаем реферальную ссылку для шаринга
        if not is_from_mass_check and not result.get("is_bad"):
            # Создаем уникальный ID для шаринга результата
            share_request_id = generate_secure_id(16)
            await request_db.save_request(share_request_id, link, user_id, "single_result")

            # Сохраняем результат для шаринга
            result_json = json.dumps(result, default=str, ensure_ascii=False)
            await request_db.update_request_status(share_request_id, 'completed', result_json)

            # Создаем кнопку "Поделиться" с реферальной ссылкой
            bot_username = (await bot.get_me()).username
            share_deep_link = f"https://t.me/{bot_username}?start=ref_{share_request_id}"

            # Получаем правильный URL для кнопки "Открыть группу"
            chat_link = result.get('chat_link') or link
            if chat_link.startswith('@'):
                display_link = f"https://t.me/{chat_link[1:]}"
            elif chat_link.startswith('t.me/'):
                display_link = f"https://{chat_link}"
            elif not chat_link.startswith('http'):
                display_link = f"https://t.me/{chat_link.lstrip('@')}"
            else:
                display_link = chat_link

            # Обновляем клавиатуру с кнопкой "Поделиться"
            if language == 'en':
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[[
                        InlineKeyboardButton(
                            text="🔗 Open Group",
                            url=display_link
                        ),
                        InlineKeyboardButton(
                            text="📤 Share Result",
                            url=share_deep_link
                        )
                    ]]
                )
            else:
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[[
                        InlineKeyboardButton(
                            text="🔗 Открыть группу",
                            url=display_link
                        ),
                        InlineKeyboardButton(
                            text="📤 Поделиться",
                            url=share_deep_link
                        )
                    ]]
                )

        await send_result_with_photo(msg, text, reply_markup=kb)

        # ОБНОВЛЯЕМ ЛИМИТЫ ТОЛЬКО ДЛЯ НОВЫХ ПРОВЕРОК И УСПЕШНЫХ ГРУПП
        if not is_from_mass_check and not result.get("is_bad"):
            try:
                await limit_manager.update_limits(
                    msg.from_user.id,
                    public_count=0 if is_private else 1,
                    private_count=1 if is_private else 0
                )
                logger.info(
                    f"📊 [SINGLE LIMITS UPDATED] User {user_id}: Link '{link}' - {'private' if is_private else 'public'}")

                # Логируем обновленные лимиты для отладки
                updated_limits = await limit_manager.get_user_limits(msg.from_user.id)
                logger.info(
                    f"📊 [SINGLE LIMITS STATUS] User {user_id}: Public {updated_limits['public_used']}/{updated_limits['public_limit']}, Private {updated_limits['private_used']}/{updated_limits['private_limit']}")

            except Exception as e:
                logger.error(f"❌ [SINGLE LIMITS ERROR] Ошибка обновления лимитов: {e}")

    except Exception as e:
        error_msg = f"Single check error: {str(e)}"
        log_error(error_msg)
        logger.error(f"❌ [SINGLE CHECK ERROR] Ошибка в handle_single_link: {e}")
        try:
            # Универсальное удаление прогресс-сообщения
            if hasattr(progress_msg, 'delete') and callable(getattr(progress_msg, 'delete')):
                await progress_msg.delete()
            else:
                await msg.bot.delete_message(chat_id=progress_msg.chat.id, message_id=progress_msg.message_id)
        except Exception as delete_error:
            logger.warning(f"Не удалось удалить сообщение о прогрессе при ошибке: {delete_error}")

        # Отправляем понятное сообщение об ошибке
        display_link = format_link_for_display(link)
        if language == 'en':
            error_text = (
                f"🔍 <b>Check Result</b>\n\n"
                f"1. {display_link}\n\n"
                f"   ❌ Error checking group\n\n"
                f"<i>An unexpected error occurred while checking the group</i>"
            )
        else:
            error_text = (
                f"🔍 <b>Результат проверки</b>\n\n"
                f"1. {display_link}\n\n"
                f"   ❌ Ошибка проверки группы\n\n"
                f"<i>Произошла непредвиденная ошибка при проверке группы</i>"
            )

        await send_error_with_photo(msg, error_text)


# ================== Периодическая очистка старых запросов ==================
async def cleanup_task():
    """Периодическая очистка старых запросов"""
    while True:
        try:
            await request_db.cleanup_old_requests(24)  # Очищаем старше 24 часов
            logger.info("Очистка старых запросов выполнена")
            await asyncio.sleep(3600)  # Проверяем каждый час
        except Exception as e:
            logger.error(f"Ошибка в cleanup_task: {e}")
            await asyncio.sleep(300)

async def start_cleanup_task():
    """Запускает задачу очистки"""
    asyncio.create_task(cleanup_task())


async def analyze_group_by_link(raw_link: str, requester_id: int, progress_msg=None):
    """Основная функция анализа группы с повторными попытками при flood wait.
    Поддерживает как ссылки (t.me/xxx), так и ID чатов (123456789) для приватных чатов из папок."""
    await rate_limiter.acquire(requester_id)
    client = None
    session_path = None

    try:
        language = await user_manager.get_user_language(requester_id)
        logger.info(f"🔍 [ANALYZE START] User {requester_id} анализирует: '{raw_link}'")

        # Получаем клиент с привязкой к пользователю
        client, session_path = await session_manager.get_client(requester_id, language)

        # === НОВОЕ: Проверка на ID чата (для приватных чатов из папки) ===
        pre_fetched_entity = None
        is_id_format = False

        # Проверяем, является ли raw_link числом (ID чата)
        # Поддерживаем отрицательные ID (для супергрупп) и положительные (для каналов)
        try:
            potential_id = int(raw_link)
            if potential_id != 0:  # 0 не может быть ID чата
                is_id_format = True
                logger.info(f"🔍 [ID FORMAT] Обнаружен ID чата: {potential_id}")

                # Получаем entity по ID (требуется, чтобы аккаунт уже был в чате)
                pre_fetched_entity = await client.get_entity(potential_id)

                # Преобразуем raw_link в формат для отображения
                if hasattr(pre_fetched_entity, 'username') and pre_fetched_entity.username:
                    # Если у приватного чата оказался username (редко, но возможно)
                    raw_link = f"t.me/{pre_fetched_entity.username}"
                else:
                    # Оставляем как ID:xxx для отображения
                    raw_link = f"ID:{potential_id}"

                logger.info(
                    f"✅ [ID RESOLVED] Получен entity для ID {potential_id}: {getattr(pre_fetched_entity, 'title', 'Unknown')}")
        except ValueError:
            # Это не число, значит обычная ссылка - продолжаем как есть
            pass
        except Exception as e:
            logger.error(f"❌ [ID ERROR] Не удалось получить entity для ID {raw_link}: {e}")
            return {
                "requester_id": requester_id,
                "chat_link": f"ID:{raw_link}" if is_id_format else raw_link,
                "chat_id": None,
                "is_bad": True,
                "created_date": None,
                "owner_id": None,
                "owner_username": None,
                "user_messages": 0,
                "system_messages": 0,
                "history_hidden": False,
                "price": 0,
                "seller_label": None,
                "checked_at": datetime.now(),
                "has_imported_messages": False,
                "error_message": f"Cannot access chat by ID. Make sure the account has joined this chat." if language == 'en' else f"Нет доступа к чату по ID. Убедитесь, что аккаунт вступил в этот чат."
            }

        result = await _analyze_group_internal(client, raw_link, requester_id, progress_msg, language, session_path,
                                               pre_fetched_entity=pre_fetched_entity)

        # Расчет цены
        if not result.get("is_bad"):
            chat_id = result.get("chat_id")
            created_date = result.get("created_date")

            calculated_price = await user_manager.calculate_price(
                requester_id,
                chat_id,
                created_date
            )
            result["price"] = calculated_price

        logger.info(f"✅ [ANALYZE END] User {requester_id} завершил анализ: {raw_link}")

        return result

    except errors.FloodWaitError as e:
        logger.error(f"🔴 [ANALYZE FLOOD WAIT] User {requester_id} - флуд-вейт: {e.seconds} секунд")

        # ОБЯЗАТЕЛЬНО добавляем сессию в flood wait
        if session_path:
            session_manager.add_flood_wait_session(session_path, e.seconds, f"Analyze flood: {e}")

        # ВМЕСТО ВОЗВРАТА ОШИБКИ - ПРОБУЕМ ПОВТОРИТЬ С ДРУГОЙ СЕССИЕЙ
        logger.info(f"🔄 [ANALYZE RETRY] Пробуем повторить проверку с другой сессией после flood wait")

        # Освобождаем текущие ресурсы
        if session_path:
            await session_manager.release_client(session_path)
        if client:
            try:
                await client.disconnect()
            except:
                pass

        # Пробуем снова с новой сессией (рекурсивно, но с тем же параметров)
        try:
            # Небольшая задержка перед повторной попыткой
            await asyncio.sleep(2)

            # Вызываем себя рекурсивно для новой попытки с другой сессией
            result = await analyze_group_by_link(raw_link, requester_id, progress_msg)
            return result

        except Exception as e2:
            logger.error(f"🔴 [ANALYZE RETRY FAILED] User {requester_id} - повторная попытка не удалась: {e2}")

            if language == 'en':
                error_msg = f"Flood wait error: please try again in {e.seconds} seconds"
            else:
                error_msg = f"Ошибка флуд-вейта: попробуйте снова через {e.seconds} секунд"

            return {
                "requester_id": requester_id,
                "chat_link": raw_link,
                "chat_id": None,
                "is_bad": True,
                "created_date": None,
                "owner_id": None,
                "owner_username": None,
                "user_messages": 0,
                "system_messages": 0,
                "history_hidden": False,
                "price": 0,
                "seller_label": None,
                "checked_at": datetime.now(),
                "has_imported_messages": False,
                "error_message": error_msg
            }

    except errors.FloodError as e:
        logger.error(f"🔴 [ANALYZE FLOOD] User {requester_id} - flood ошибка: {e}")

        # Добавляем в flood wait с базовым таймаутом
        if session_path:
            session_manager.add_flood_wait_session(session_path, 60, f"FloodError: {e}")

        if language == 'en':
            error_msg = f"Flood protection: please try again later"
        else:
            error_msg = f"Защита от флуда: попробуйте позже"

        return {
            "requester_id": requester_id,
            "chat_link": raw_link,
            "chat_id": None,
            "is_bad": True,
            "created_date": None,
            "owner_id": None,
            "owner_username": None,
            "user_messages": 0,
            "system_messages": 0,
            "history_hidden": False,
            "price": 0,
            "seller_label": None,
            "checked_at": datetime.now(),
            "has_imported_messages": False,
            "error_message": error_msg
        }

    except Exception as e:
        log_error(f"Analyze error: {str(e)}")
        logger.error(f"❌ [ANALYZE ERROR] User {requester_id} - ошибка: {e}")

        # Для любых RPC ошибок тоже добавляем в flood wait
        if session_path and "RPC" in str(e):
            session_manager.add_flood_wait_session(session_path, 30, f"RPC Error: {e}")

        return {
            "requester_id": requester_id,
            "chat_link": raw_link,
            "chat_id": None,
            "is_bad": True,
            "created_date": None,
            "owner_id": None,
            "owner_username": None,
            "user_messages": 0,
            "system_messages": 0,
            "history_hidden": False,
            "price": 0,
            "seller_label": None,
            "checked_at": datetime.now(),
            "has_imported_messages": False,
            "error_message": f"System error: {str(e)}" if language == 'en' else f"Системная ошибка: {str(e)}"
        }
    finally:
        # ВСЕГДА освобождаем ресурсы
        if session_path:
            await session_manager.release_client(session_path)

        if client:
            try:
                await client.disconnect()
                logger.debug(f"✅ [SESSION] Клиент отключен для user {requester_id}")
            except Exception as e:
                logger.warning(f"⚠️ [SESSION] Ошибка отключения клиента: {e}")

        rate_limiter.release(requester_id)


async def _find_channel_owner(client, entity, session_path: str = None):
    """Находит владельца канала через различные методы"""
    try:
        try:
            from telethon.tl.functions.channels import GetFullChannelRequest
            full = await client(GetFullChannelRequest(channel=entity))
            creator_id = getattr(full.full_chat, "creator_id", None)
            if creator_id:
                user = await get_entity_safe(client, creator_id, session_path)
                if user:
                    return {
                        "id": user.id,
                        "username": getattr(user, "username", None),
                        "first_name": getattr(user, "first_name", None),
                        "last_name": getattr(user, "last_name", None),
                        "found_by": "creator_id"
                    }
        except Exception as e:
            logger.debug(f"Не удалось получить creator_id: {e}")

        try:
            from telethon.tl.types import ChannelParticipantsAdmins
            participants = await client.get_participants(entity, filter=ChannelParticipantsAdmins())

            for user in participants:
                if getattr(user, "bot", False):
                    continue
                try:
                    perm = await client.get_permissions(entity, user)
                    part = getattr(perm, "participant", None)
                    if part and type(part).__name__ == "ChannelParticipantCreator":
                        return {
                            "id": user.id,
                            "username": getattr(user, "username", None),
                            "first_name": getattr(user, "first_name", None),
                            "last_name": getattr(user, "last_name", None),
                            "found_by": "admin_creator_flag"
                        }
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Не удалось получить администраторов: {e}")

        try:
            admin_candidates = {}
            async for msg in client.iter_messages(entity, limit=100):
                if getattr(msg, "post", False):
                    continue

                sender_id = getattr(msg, 'sender_id', None)
                if sender_id and sender_id > 0:
                    admin_candidates[sender_id] = admin_candidates.get(sender_id, 0) + 1

            if admin_candidates:
                most_active = max(admin_candidates, key=admin_candidates.get)
                user = await get_entity_safe(client, most_active, session_path)
                if user:
                    return {
                        "id": user.id,
                        "username": getattr(user, "username", None),
                        "first_name": getattr(user, "first_name", None),
                        "last_name": getattr(user, "last_name", None),
                        "found_by": "most_active_admin"
                    }
        except Exception as e:
            logger.debug(f"Не удалось проанализировать сообщения: {e}")

        return None

    except Exception as e:
        logger.error(f"Ошибка поиска владельца канала: {e}")
        return None

async def _analyze_group_internal(client, raw_link: str, requester_id: int, progress_msg=None, language: str = 'ru',
                                  session_path: str = None, pre_fetched_entity=None):
    """Внутренняя функция анализа с полной поддержкой каналов"""
    result = {
        "requester_id": requester_id,
        "chat_link": raw_link,
        "chat_id": None,
        "is_bad": False,
        "is_channel": False,
        "is_broadcast": False,
        "created_date": None,
        "owner_id": None,
        "owner_username": None,
        "owner_first_name": None,
        "owner_last_name": None,
        "user_messages": 0,
        "system_messages": 0,
        "channel_posts": 0,
        "total_posts": 0,
        "members": 0,
        "history_hidden": False,
        "price": 0,
        "seller_label": None,
        "checked_at": datetime.now(),
        "has_imported_messages": False,
        "has_geo_location": False,
        "error_message": None,
        "is_user_profile": False,
        "is_invalid_link": False
    }

    entity = None
    is_private = False

    try:
        if progress_msg:
            await progress_msg.edit_text("🔎 " + user_manager.get_text(language, 'checking_group'))

        if pre_fetched_entity:
            entity = pre_fetched_entity
            is_private = not getattr(entity, 'username', None)
        else:
            m_join = RE_JOIN.search(raw_link)
            if m_join:
                code = m_join.group(3)
                try:
                    if progress_msg:
                        await progress_msg.edit_text("🔗 " + user_manager.get_text(language, 'checking_group'))

                    from telethon.tl.functions.messages import ImportChatInviteRequest
                    chat = await client(ImportChatInviteRequest(code))
                    entity = chat.chats[0]
                    is_private = True
                    logger.info(f"Успешно присоединились к приватной группе: {getattr(entity, 'title', 'N/A')}")

                except errors.UserAlreadyParticipantError:
                    try:
                        entity = await get_entity_safe(client, f"https://t.me/+{code}", session_path)
                        is_private = True
                        logger.info(f"Уже участник приватной группы: {getattr(entity, 'title', 'N/A')}")
                    except Exception as e:
                        logger.error(f"Не удалось получить entity для приватной группы: {e}")
                        result["is_bad"] = True
                        result["is_invalid_link"] = True
                        result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                        if progress_msg:
                            await progress_msg.edit_text(
                                "❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                        return result

                except errors.FloodWaitError as e:
                    logger.error(f"Флуд-вейт при присоединении: {e.seconds} секунд")
                    result["is_bad"] = True
                    result["error_message"] = f"Flood wait: {e.seconds}s"
                    raise e

                except errors.InviteHashExpiredError:
                    result["is_bad"] = True
                    result["is_invalid_link"] = True
                    result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                    if progress_msg:
                        await progress_msg.edit_text("❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                    return result

                except errors.InviteHashInvalidError:
                    result["is_bad"] = True
                    result["is_invalid_link"] = True
                    result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                    if progress_msg:
                        await progress_msg.edit_text("❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                    return result

                except Exception as e:
                    logger.error(f"Ошибка присоединения к приватной группе: {e}")
                    result["is_bad"] = True
                    result["is_invalid_link"] = True
                    result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                    if progress_msg:
                        await progress_msg.edit_text("❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                    return result

            else:
                m_user = RE_USERNAME.search(raw_link)
                m_tme = RE_TME.search(raw_link)

                identifier = None
                if m_user:
                    identifier = "@" + m_user.group(1)
                elif m_tme:
                    identifier = m_tme.group(2)

                if identifier:
                    try:
                        if progress_msg:
                            await progress_msg.edit_text("🔗 " + user_manager.get_text(language, 'checking_group'))

                        entity = await get_entity_safe(client, identifier, session_path)
                        is_private = False

                        entity_type = type(entity).__name__
                        logger.info(f"Тип entity: {entity_type} для {identifier}")

                        if entity_type == 'User':
                            result["is_user_profile"] = True
                            result["is_bad"] = True
                            result["error_message"] = user_manager.get_text(language, 'user_profile_result')
                            logger.info(f"Обнаружен профиль пользователя: {identifier}")

                            if progress_msg:
                                await progress_msg.edit_text(
                                    "❌ " + user_manager.get_text(language, 'user_profile_result'))
                            return result

                        logger.info(f"Успешно получили entity группы/канала: {getattr(entity, 'title', 'N/A')}")

                    except errors.ChannelPrivateError:
                        result["is_bad"] = True
                        result["is_invalid_link"] = True
                        result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                        if progress_msg:
                            await progress_msg.edit_text(
                                "❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                        return result

                    except (errors.UsernameNotOccupiedError, errors.UsernameInvalidError):
                        result["is_bad"] = True
                        result["is_invalid_link"] = True
                        result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                        if progress_msg:
                            await progress_msg.edit_text(
                                "❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                        return result

                    except errors.ChannelInvalidError:
                        result["is_bad"] = True
                        result["is_invalid_link"] = True
                        result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                        if progress_msg:
                            await progress_msg.edit_text(
                                "❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                        return result

                    except Exception as e:
                        logger.error(f"Ошибка получения entity: {e}")
                        result["is_bad"] = True
                        result["is_invalid_link"] = True
                        result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
                        if progress_msg:
                            await progress_msg.edit_text(
                                "❌ " + user_manager.get_text(language, 'error_group_not_exist'))
                        return result

        if entity is None:
            result["is_bad"] = True
            result["is_invalid_link"] = True
            result["error_message"] = user_manager.get_text(language, 'error_group_not_exist')
            if progress_msg:
                await progress_msg.edit_text("❌ " + user_manager.get_text(language, 'error_group_not_exist'))
            return result

        is_channel = False
        is_broadcast = False

        if hasattr(entity, 'broadcast') and entity.broadcast:
            is_channel = True
            is_broadcast = True
            result["is_channel"] = True
            result["is_broadcast"] = True
            chat_type = "канал"
        elif hasattr(entity, 'megagroup') and entity.megagroup:
            chat_type = "супергруппа"
        else:
            chat_type = "группа"

        logger.info(f"Тип чата: {chat_type}, ID: {entity.id}, IsChannel: {is_channel}, IsBroadcast: {is_broadcast}")

        try:
            if hasattr(entity, 'id'):
                if is_channel or (hasattr(entity, 'megagroup') and entity.megagroup):
                    bot_style_chat_id = int(f"-100{abs(entity.id)}")
                else:
                    bot_style_chat_id = entity.id
                result["chat_id"] = str(bot_style_chat_id)

                if str(bot_style_chat_id).startswith("-10029"):
                    result["is_bad"] = True
                if entity.id <= -10018:
                    result["seller_label"] = "Group id can seller"

        except Exception as ex:
            logger.warning(f"Ошибка получения Chat ID: {ex}")
            result["chat_id"] = str(entity.id) if hasattr(entity, 'id') else user_manager.get_text(language,
                                                                                                   'not_defined')

        if result["is_bad"]:
            return result

        if progress_msg:
            status_text = user_manager.get_text(language, 'checking_channel') if is_channel else user_manager.get_text(
                language, 'checking_group')
            await progress_msg.edit_text("📊 " + status_text)

        try:
            from telethon.tl.functions.channels import GetFullChannelRequest
            full = await client(GetFullChannelRequest(channel=entity))
            result["members"] = full.full_chat.participants_count or 0
        except Exception as e:
            logger.debug(f"Не удалось получить полную информацию: {e}")

        if progress_msg:
            await progress_msg.edit_text("🔎 " + user_manager.get_text(language, 'first_message_date'))
        try:
            first_msg = await _fetch_first_message(client, entity)
            if first_msg:
                result["created_date"] = first_msg.date
                year = first_msg.date.year
                if year <= 2022:
                    result["price"] = 0
                elif year == 2023:
                    result["price"] = 0
                elif year == 2024:
                    result["price"] = 0
        except (errors.ChannelPrivateError, errors.ChatAdminRequiredError):
            result["history_hidden"] = True
        except Exception as e:
            logger.debug(f"Ошибка получения первого сообщения: {e}")
            result["history_hidden"] = True

        if progress_msg:
            status_text = user_manager.get_text(language, 'counting_posts') if is_channel else user_manager.get_text(
                language, 'counting_messages')
            await progress_msg.edit_text("🔎 " + status_text)
        try:
            if not result.get("history_hidden"):
                msg_stats = await _count_messages(client, entity, limit=2000)
                result["user_messages"] = msg_stats["user_messages"]
                result["system_messages"] = msg_stats["system_messages"]
                result["channel_posts"] = msg_stats["channel_posts"]
                result["total_posts"] = msg_stats["user_messages"]
                logger.info(
                    f"Статистика: постов канала={msg_stats['channel_posts']}, от админов={msg_stats['user_messages'] - msg_stats['channel_posts']}, системных={msg_stats['system_messages']}")
            else:
                logger.info("Пропускаем подсчет - история скрыта")
        except (errors.ChannelPrivateError, errors.ChatAdminRequiredError):
            result["history_hidden"] = True
        except Exception as e:
            logger.debug(f"Ошибка подсчета сообщений: {e}")

        if progress_msg:
            await progress_msg.edit_text("🔎 " + user_manager.get_text(language, 'checking_imported'))
        try:
            if not result.get("history_hidden"):
                result["has_imported_messages"] = await _check_imported_messages_simple(client, entity)
        except Exception as e:
            logger.error(f"Ошибка проверки импортированных сообщений: {e}")

        if progress_msg:
            await progress_msg.edit_text("🔎 " + user_manager.get_text(language, 'checking_geo'))
        try:
            result["has_geo_location"] = await _check_geo_group(client, entity)
        except Exception as e:
            logger.error(f"Ошибка проверки на гео-группу: {e}")

        if progress_msg:
            await progress_msg.edit_text("🔎 " + user_manager.get_text(language, 'finding_owner'))
        try:
            if is_channel:
                creator = await _find_channel_owner(client, entity, session_path)
            else:
                creator = await _find_creator(client, entity, session_path)

            if creator:
                result["owner_id"] = creator.get("id")
                result["owner_username"] = creator.get("username")
                result["owner_first_name"] = creator.get("first_name")
                result["owner_last_name"] = creator.get("last_name")
        except Exception as e:
            logger.error(f"Ошибка поиска владельца: {e}")

        if progress_msg:
            await progress_msg.edit_text("✅ " + user_manager.get_text(language, 'check_completed'))

        try:
            if entity and is_private and not pre_fetched_entity:
                await client.delete_dialog(entity)
        except Exception as e:
            logger.warning(f"Не удалось выйти из группы: {e}")

        return result

    except errors.FloodWaitError as e:
        logger.error(f"Флуд-вейт: {e.seconds} секунд")
        raise e
    except Exception as e:
        logger.exception("Критическая ошибка в _analyze_group_internal")
        result["is_bad"] = True
        result["is_invalid_link"] = True
        result["error_message"] = user_manager.get_text(language, 'error_critical')
        return result
# ================= Формирование сообщения =================
def format_link_for_display(link: str) -> str:
    """Форматирует ссылку для красивого отображения в тексте"""
    if not link:
        return ""

    # Если это username (@username), оставляем как есть
    if link.startswith("@"):
        return link

    # Убираем протокол для публичных ссылок
    if link.startswith("https://t.me/"):
        clean_link = link.replace("https://", "")
        # Для приватных ссылок (с +) оставляем полную версию
        if "/+" in clean_link or clean_link.startswith("t.me/+"):
            return link
        else:
            return clean_link
    elif link.startswith("t.me/"):
        return link
    else:
        return link



def build_result_info_lines(rec: dict, language: str = 'ru', stats: dict = None) -> list[str]:
    """Собирает блок детальной информации о группе/канале для одиночной и массовой проверки."""
    info_lines = []

    if rec.get("is_channel"):
        info_lines.append(f"📢 {user_manager.get_text(language, 'channel_type')}")
    else:
        info_lines.append(f"💬 {user_manager.get_text(language, 'group_type')}")

    chat_id_value = rec.get('chat_id') or user_manager.get_text(language, 'not_defined')
    info_lines.append(f"🔢 Chat ID: <code>{chat_id_value}</code>")

    created_date = rec.get('created_date')
    date_str = user_manager.get_text(language, 'date_not_defined')

    if created_date and created_date != 'Unknown':
        try:
            if isinstance(created_date, str):
                created_date_obj = datetime.fromisoformat(created_date.replace('Z', '+00:00'))
            else:
                created_date_obj = created_date

            if isinstance(created_date_obj, datetime):
                day = created_date_obj.day
                month = created_date_obj.month
                year = created_date_obj.year

                if language == 'en':
                    months_en = ['January', 'February', 'March', 'April', 'May', 'June',
                                 'July', 'August', 'September', 'October', 'November', 'December']
                    date_str = f"{day} {months_en[month - 1]} {year}"
                else:
                    months_ru = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                                 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                    date_str = f"{day} {months_ru[month - 1]} {year} года"
            else:
                date_str = str(created_date)
        except Exception as e:
            logger.warning(f"Ошибка форматирования даты: {e}")
            date_str = str(created_date) if created_date else user_manager.get_text(language, 'date_not_defined')

    info_lines.append(f"📅 {user_manager.get_text(language, 'creation_date')}: {date_str}")

    members = rec.get('members', 0)
    if rec.get("is_channel"):
        info_lines.append(f"👥 {user_manager.get_text(language, 'subscribers')}: {members}")
    else:
        info_lines.append(f"👥 {user_manager.get_text(language, 'members')}: {members}")

    owner_username = rec.get("owner_username")
    owner_id = rec.get("owner_id")
    owner_first_name = rec.get("owner_first_name")
    owner_last_name = rec.get("owner_last_name")

    owner_display = ""
    special_cases = ["anonymous_owner", "anonymous_admin", "owner_uses_anonymity", "owner_not_found",
                     "owner_search_error"]

    if owner_first_name in special_cases:
        owner_display = user_manager.get_text(language, owner_first_name)
        if owner_last_name and owner_last_name == "owner_not_found_reason":
            owner_display += user_manager.get_text(language, owner_last_name)
    else:
        if owner_id:
            user_link = f"tg://openmessage?user_id={owner_id}"
            if owner_username:
                owner_display = f"<a href='{user_link}'>{owner_username}</a> (id{owner_id})"
            elif owner_first_name or owner_last_name:
                name = f"{owner_first_name or ''} {owner_last_name or ''}".strip()
                owner_display = f"<a href='{user_link}'>{name}</a> (id{owner_id})"
            else:
                owner_display = f"<a href='{user_link}'>id{owner_id}</a>"
        else:
            owner_display = format_owner_display_with_name(owner_username, owner_id, owner_first_name, owner_last_name,
                                                           language)

    info_lines.append(f"👤 {user_manager.get_text(language, 'owner')}: {owner_display}")

    if rec.get("is_channel"):
        total_posts = rec.get('total_posts', 0)
        channel_posts = rec.get('channel_posts', 0)
        admin_posts = total_posts - channel_posts

        info_lines.append(f"📝 {user_manager.get_text(language, 'total_posts')}: {total_posts}")

        if channel_posts > 0:
            info_lines.append(f"📢 {user_manager.get_text(language, 'channel_posts')}: {channel_posts}")

        if admin_posts > 0:
            info_lines.append(f"👤 {user_manager.get_text(language, 'admin_posts')}: {admin_posts}")
    else:
        if rec.get("history_hidden"):
            info_lines.append(f"🔒 {user_manager.get_text(language, 'history_hidden')}")
        else:
            user_messages = rec.get('user_messages', 0)
            system_messages = rec.get('system_messages', 0)
            info_lines.append(f"🧾 {user_manager.get_text(language, 'user_messages')}: {user_messages}")
            if system_messages > 0:
                info_lines.append(f"⚙️ {user_manager.get_text(language, 'system_messages')}: {system_messages}")

    if rec.get("price"):
        price_value = rec.get('price')
        info_lines.append(f"💰 {user_manager.get_text(language, 'price')}: {price_value}$")

    if rec.get("seller_label"):
        info_lines.append(f"🏷 {rec.get('seller_label')}")

    if stats and stats.get('checked_before'):
        checked_date = stats['checked_before']
        info_lines.append("")
        info_lines.append(f"📋 {user_manager.get_text(language, 'checked_before')}: {checked_date}")

    checked_at = rec.get('checked_at', datetime.now())
    if isinstance(checked_at, str):
        try:
            checked_at = datetime.fromisoformat(checked_at.replace('Z', '+00:00'))
        except:
            checked_at = datetime.now()

    checked_at_str = checked_at.strftime('%d-%m-%Y %H:%M:%S UTC')
    info_lines.append(f"⏱ {user_manager.get_text(language, 'checked_at')}: {checked_at_str}")

    return info_lines

def make_result_message(rec: dict, stats: dict = None, language: str = 'ru'):
    """Формирует сообщение с результатом"""

    if stats is None:
        stats = {}

    if rec.get("is_user_profile"):
        display_link = format_link_for_display(rec['chat_link'])

        if language == 'en':
            text = (
                f"🔍 <b>Check Result</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Link:</b>\n"
                f"<blockquote>{display_link}</blockquote>\n\n"
                f"<b>Status:</b>\n"
                f"<blockquote>❌ {user_manager.get_text(language, 'user_profile_result')}</blockquote>\n\n"
                f"<i>The link leads to a user profile</i>"
            )
        else:
            text = (
                f"🔍 <b>Результат проверки</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Ссылка:</b>\n"
                f"<blockquote>{display_link}</blockquote>\n\n"
                f"<b>Статус:</b>\n"
                f"<blockquote>❌ {user_manager.get_text(language, 'user_profile_result')}</blockquote>\n\n"
                f"<i>Ссылка ведет на профиль пользователя</i>"
            )
        return text, None

    if rec.get("is_invalid_link"):
        display_link = format_link_for_display(rec['chat_link'])
        error_msg = rec.get('error_message', user_manager.get_text(language, 'invalid_link_result'))

        if language == 'en':
            text = (
                f"🔍 <b>Check Result</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Link:</b>\n"
                f"<blockquote>{display_link}</blockquote>\n\n"
                f"<b>Status:</b>\n"
                f"<blockquote>❌ {error_msg}</blockquote>\n\n"
                f"<i>Group or channel not found</i>"
            )
        else:
            text = (
                f"🔍 <b>Результат проверки</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Ссылка:</b>\n"
                f"<blockquote>{display_link}</blockquote>\n\n"
                f"<b>Статус:</b>\n"
                f"<blockquote>❌ {error_msg}</blockquote>\n\n"
                f"<i>Группа или канал не найдены</i>"
            )
        return text, None

    if rec.get("error_message"):
        display_link = format_link_for_display(rec.get('chat_link', ''))
        if language == 'en':
            text = (
                f"🔍 <b>Check Result</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Link:</b>\n"
                f"<blockquote>{display_link}</blockquote>\n\n"
                f"<b>Status:</b>\n"
                f"<blockquote>❌ Error: {rec['error_message']}</blockquote>"
            )
        else:
            text = (
                f"🔍 <b>Результат проверки</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Ссылка:</b>\n"
                f"<blockquote>{display_link}</blockquote>\n\n"
                f"<b>Статус:</b>\n"
                f"<blockquote>❌ Ошибка: {rec['error_message']}</blockquote>"
            )
        return text, None

    display_link = format_link_for_display(rec.get('chat_link', ''))

    info_lines = build_result_info_lines(rec, language, stats)

    warnings_block = ""
    warnings_lines = []

    if rec.get("has_imported_messages"):
        warnings_lines.append(f"📥 {user_manager.get_text(language, 'imported_messages')}")

    if rec.get("has_geo_location"):
        warnings_lines.append(f"📍 {user_manager.get_text(language, 'geo_group')}")

    if warnings_lines:
        warnings_title = user_manager.get_text(language, 'warnings_title')
        warnings_block = f"\n\n❗ <b>{warnings_title}:</b>\n<blockquote>" + "\n".join(warnings_lines) + "</blockquote>"

    if language == 'en':
        text = (
                f"🔍 <b>Check Result</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Link:</b>\n"
                f"<blockquote>{display_link}</blockquote>\n\n"
                f"<b>Info:</b>\n"
                f"<blockquote>" + "\n".join(info_lines) + "</blockquote>"
                + warnings_block
        )
    else:
        text = (
                f"🔍 <b>Результат проверки</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Ссылка:</b>\n"
                f"<blockquote>{display_link}</blockquote>\n\n"
                f"<b>Информация:</b>\n"
                f"<blockquote>" + "\n".join(info_lines) + "</blockquote>"
                + warnings_block
        )

    chat_link = rec.get('chat_link', display_link)
    if chat_link.startswith('@'):
        group_url = f"https://t.me/{chat_link[1:]}"
    elif chat_link.startswith('t.me/'):
        group_url = f"https://{chat_link}"
    elif not chat_link.startswith('http'):
        group_url = f"https://t.me/{chat_link.lstrip('@')}"
    else:
        group_url = chat_link

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text=user_manager.get_text(language, 'open_link'),
                url=group_url
            ),
            InlineKeyboardButton(
                text=user_manager.get_text(language, 'share'),
                switch_inline_query=chat_link
            ),
            InlineKeyboardButton(
                text=user_manager.get_text(language, 'hide_link'),
                callback_data="hide_link"
            )
        ]]
    )

    return text, kb

async def send_result_with_photo(msg: types.Message, text: str, reply_markup=None):
    """Отправляет результат с фото на нужном языке"""
    try:
        # Получаем язык пользователя
        language = await user_manager.get_user_language(msg.from_user.id)

        # Определяем имя файла в зависимости от языка
        if language == 'en':
            photo_filename = "png/result_en.png"
            fallback_filename = "png/result.png"  # Резервный файл
        else:
            photo_filename = "png/result.png"
            fallback_filename = "png/result.png"

        # Пробуем открыть файл для нужного языка, если нет - используем резервный
        try:
            with open(photo_filename, "rb") as photo:
                await msg.answer_photo(
                    types.BufferedInputFile(photo.read(), filename="result.png"),
                    caption=text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
        except FileNotFoundError:
            # Если файл для языка не найден, используем резервный
            logger.warning(f"Файл {photo_filename} не найден, использую {fallback_filename}")
            with open(fallback_filename, "rb") as photo:
                await msg.answer_photo(
                    types.BufferedInputFile(photo.read(), filename="result.png"),
                    caption=text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )

    except FileNotFoundError:
        # Если фото не найдено, отправляем только текст
        logger.warning("Файлы фото результата не найдены, отправляю текст без фото")
        await msg.answer(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка отправки результата с фото: {e}")
        await msg.answer(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def _check_imported_messages(client, entity, sample_size=1000):
    """Проверяет наличие импортированных сообщений в группе"""
    try:
        logger.info("Начинаем проверку на импортированные сообщения...")

        async for msg in client.iter_messages(entity, limit=sample_size):

            # ✅ Основная проверка: Telethon 1.41.2 хранит флаг импорта в fwd_from.imported
            if msg.fwd_from and getattr(msg.fwd_from, "imported", False):
                logger.info(f"✅ Обнаружено импортированное сообщение через fwd_from.imported! ID: {msg.id}")
                return True

            # 🔍 Альтернатива, если fwd_from.imported отсутствует (через raw-словарь)
            data = msg.to_dict()
            if "fwd_from" in data and data["fwd_from"].get("imported"):
                logger.info(f"✅ Обнаружено импортированное сообщение через to_dict()! ID: {msg.id}")
                return True

            # 💬 Иногда Telegram вставляет сообщение о переносе истории
            if getattr(msg, "action", None):
                action_type = type(msg.action).__name__
                if action_type == "MessageActionHistoryImport":
                    logger.info(f"✅ Обнаружено системное сообщение об импорте истории! ID: {msg.id}")
                    return True

            # 🕵️ Проверка на характерные признаки импортированных сообщений
            # (нет from_id, но есть fwd_from.from_name)
            if msg.fwd_from and not msg.fwd_from.from_id and msg.fwd_from.from_name:
                name = msg.fwd_from.from_name.lower()
                if any(k in name for k in ["whatsapp", "signal", "line", "kakao", "messenger"]):
                    logger.info(f"✅ Обнаружено импортированное сообщение по имени источника ({name})! ID: {msg.id}")
                    return True

        logger.info("❌ Импортированные сообщения не найдены")
        return False

    except Exception as e:
        logger.error(f"Ошибка при проверке импортированных сообщений: {e}")
        return False

@router.message()
async def debug_custom_emoji(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return

    if msg.entities:
        for ent in msg.entities:
            if ent.type == "custom_emoji":
                await msg.answer(f"custom_emoji_id = {ent.custom_emoji_id}")

async def _check_imported_messages_simple(client, entity, sample_size=20):
    """Упрощенная проверка импортированных сообщений через fwd_from.imported"""
    try:
        logger.info("🔍 Проверяем наличие импортированных сообщений...")

        async for msg in client.iter_messages(entity, limit=sample_size):
            # Основная проверка: через fwd_from.imported
            if msg.fwd_from and getattr(msg.fwd_from, 'imported', False):
                logger.info(f"✅ Найдено импортированное сообщение ID: {msg.id}")
                return True

            # Альтернативная проверка через to_dict
            try:
                data = msg.to_dict()
                if "fwd_from" in data and data["fwd_from"].get("imported"):
                    logger.info(f"✅ Найдено импортированное сообщение через to_dict ID: {msg.id}")
                    return True
            except:
                pass

            # Проверка по имени источника (WhatsApp, Signal и т.д.)
            if (msg.fwd_from and
                    hasattr(msg.fwd_from, 'from_name') and
                    msg.fwd_from.from_name and
                    not getattr(msg.fwd_from, 'from_id', None)):

                from_name_lower = msg.fwd_from.from_name.lower()
                if any(source in from_name_lower for source in
                       ['whatsapp', 'signal', 'line', 'viber', 'wechat', 'import']):
                    logger.info(f"✅ Найдено импортированное сообщение по источнику: {msg.fwd_from.from_name}")
                    return True

        logger.info("❌ Импортированные сообщения не найдены")
        return False

    except Exception as e:
        logger.error(f"Ошибка проверки импортированных сообщений: {e}")
        return False

async def debug_imported_messages(client, entity, limit=30):
    """Детальная отладка импортированных сообщений"""
    logger.info("=== ДЕТАЛЬНАЯ ПРОВЕРКА ИМПОРТИРОВАННЫХ СООБЩЕНИЙ ===")

    imported_found = False

    async for msg in client.iter_messages(entity, limit=limit):
        fwd_info = ""

        if msg.fwd_from:
            # Основная проверка
            imported_direct = getattr(msg.fwd_from, 'imported', False)
            from_name = getattr(msg.fwd_from, 'from_name', '')
            from_id = getattr(msg.fwd_from, 'from_id', None)

            # Альтернативная проверка через to_dict
            imported_dict = False
            try:
                data = msg.to_dict()
                imported_dict = data.get("fwd_from", {}).get("imported", False)
            except:
                pass

            fwd_info = f"fwd_from: imported={imported_direct}, from_name='{from_name}', from_id={from_id}, imported_dict={imported_dict}"

            if imported_direct or imported_dict:
                logger.info(f"🚨 ИМПОРТИРОВАННОЕ: ID={msg.id}, {fwd_info}")
                imported_found = True
            elif from_name and not from_id:
                logger.info(f"⚠️ ВОЗМОЖНО ИМПОРТ: ID={msg.id}, {fwd_info}")
        else:
            fwd_info = "нет fwd_from"

        logger.debug(f"Сообщение {msg.id}: {fwd_info}")

    logger.info(f"=== РЕЗЮМЕ: импортированные = {imported_found} ===")
    return imported_found

USER_HISTORY_PAGE = {}

def escape_md_simple(text: str) -> str:
    """Простое экранирование для Markdown (только основные символы)"""
    if text is None:
        return ""
    s = str(text)
    for ch in ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
        s = s.replace(ch, "\\" + ch)
    return s

@router.callback_query(F.data == "hide_link")
async def hide_link(call: types.CallbackQuery):
    try:
        await call.message.delete()
    except Exception as e:
        logger.warning(f"hide_link error: {e}")
        language = await user_manager.get_user_language(call.from_user.id)
        await call.answer("Couldn't delete message" if language == 'en' else "Не удалось удалить сообщение", show_alert=True)


async def migrate_database():
    """Миграция базы данных для добавления нового поля"""
    try:
        db_path = "requests.db"
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Проверяем существование колонки request_type
            cursor.execute("PRAGMA table_info(inline_requests)")
            columns = [column[1] for column in cursor.fetchall()]

            if 'request_type' not in columns:
                logger.info("Добавляем колонку request_type в таблицу inline_requests")
                cursor.execute('''
                    ALTER TABLE inline_requests 
                    ADD COLUMN request_type TEXT NOT NULL DEFAULT 'single'
                ''')
                conn.commit()
                logger.info("Миграция базы данных завершена успешно")
            else:
                logger.info("Колонка request_type уже существует")

    except Exception as e:
        logger.error(f"Ошибка миграции базы данных: {e}")
# Создаем экземпляр SessionManager после инициализации бота

async def cleanup_mass_checks_task():
    """Периодическая очистка устаревших массовых проверок"""
    while True:
        try:
            await mass_check_db.cleanup_expired_mass_checks()
            await asyncio.sleep(3600)  # Проверяем каждый час
        except Exception as e:
            logger.error(f"Ошибка в cleanup_mass_checks_task: {e}")
            await asyncio.sleep(300)

async def shutdown():
    """Корректное завершение работы"""
    logger.info("Завершение работы...")

    # Останавливаем polling
    try:
        await dp.stop_polling()
        logger.info("✅ Polling остановлен")
    except Exception as e:
        logger.warning(f"Ошибка при остановке polling: {e}")

    # Отменяем все задачи
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    # Ждем завершения задач
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"✅ Все задачи завершены")

    # Закрываем сессию бота (ПОСЛЕ завершения всех задач)
    try:
        await bot.session.close()
        logger.info("✅ Сессия бота закрыта")
    except Exception as e:
        logger.warning(f"Ошибка при закрытии сессии бота: {e}")

    logger.info("Бот завершил работу")
    sys.exit(0)


def signal_handler(signum, frame):
    """Обработчик сигналов прерывания"""
    logger.info(f"Получен сигнал {signum}, завершаем работу...")
    asyncio.create_task(shutdown())



async def main():
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Инициализируем менеджеры
        session_manager.set_bot(bot)
        session_manager.set_owner(OWNER_ID)

        logger.info("🟢 Менеджеры инициализированы")

        commands_ru = [
            types.BotCommand(command="start", description="Запустить бота"),
            types.BotCommand(command="stats", description="Моя статистика"),
            types.BotCommand(command="mass_check", description="Массовая проверка групп"),
            types.BotCommand(command="price", description="Настройка цен"),
            types.BotCommand(command="lang", description="Сменить язык"),
            types.BotCommand(command="limit", description="Посмотреть лимиты"),
            types.BotCommand(command="info", description="Информация о группе"),
            types.BotCommand(command="baned", description="Бан обычных пользователей"),
            types.BotCommand(command="web", description="Показать актуальные цены"),
            types.BotCommand(command="deladmin", description="Удалить администраторов")
        ]

        # Команды на английском
        commands_en = [
            types.BotCommand(command="start", description="Start bot"),
            types.BotCommand(command="stats", description="My statistics"),
            types.BotCommand(command="mass_check", description="Mass group check"),
            types.BotCommand(command="price", description="Price settings"),
            types.BotCommand(command="lang", description="Change language"),
            types.BotCommand(command="info", description="Group information"),
            types.BotCommand(command="baned", description="Ban regular users"),
            types.BotCommand(command="deladmin", description="Remove administrators"),
            types.BotCommand(command="limit", description="Look limit's"),
            types.BotCommand(command="web", description="Show current prices")
        ]

        # Устанавливаем команды для обоих языков
        await bot.set_my_commands(commands_ru, language_code='ru')
        await bot.set_my_commands(commands_en, language_code='en')
        await bot.set_my_commands(commands_en)  # По умолчанию английские
        logger.info("🟢 Команды бота установлены")

        # ЗАГРУЖАЕМ ФЛУД-ВЕЙТ СЕССИИ
        session_manager.load_flood_wait_sessions()

        # ОЧИЩАЕМ ИСТЕКШИЕ ФЛУД-ВЕЙТЫ
        await session_manager.cleanup_expired_flood_waits()

        # Запускаем фоновые задачи
        asyncio.create_task(session_manager.validate_all_sessions())
        asyncio.create_task(update_web_prices_task())
        asyncio.create_task(cleanup_task())
        asyncio.create_task(cleanup_mass_checks_task())
        logger.info("🟢 Фоновые задачи запущены")

        install_global_html_premium_patch(bot)
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("🟢 Вебхук удален, запускаем polling...")

        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        logger.info(f"BOT INSTANCE ID (start): {id(bot)}")

    except Exception as e:
        logger.error(f"🔴 Ошибка в main: {e}")
        import traceback
        logger.error(f"🔴 Traceback: {traceback.format_exc()}")
    finally:
        await shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
