import asyncio
import logging
import os
import zipfile
import shutil
import json
import aiosqlite
import subprocess
import signal
import sys
import pickle
import re
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import aiofiles

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Токен бота
BOT_TOKEN = "5002126853:AAFK0H5Z8sQbmgDfi7hGlWvHrc8DktOydjQ/test"  # Замените на реальный токен

# Максимальное количество одновременно запущенных ботов
MAX_CONCURRENT_BOTS = 8

# Список админов (ID пользователей, которые имеют доступ к админ-панели)
ADMIN_IDS = [5000282571, 123456789]  # Добавьте сюда ID админов

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Пути к директориям
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, 'data')
PROJECTS_DIR = os.path.join(BASE_DIR, 'projects')
TEMP_DIR = os.path.join(BASE_DIR, 'temp')
STATE_FILE = os.path.join(DB_DIR, 'bot_state.pkl')
DB_PATH = os.path.join(DB_DIR, 'bot_database.db')

# Определяем состояния FSM
class ProjectStates(StatesGroup):
    waiting_for_project_name = State()
    waiting_for_file = State()
    waiting_for_lib_name = State()

class BroadcastStates(StatesGroup):
    waiting_for_content = State()

class AdminStates(StatesGroup):
    waiting_for_bot_source = State()

# Глобальные переменные
running_count = 0
active_processes = {}  # Stores subprocess objects

# Функция для проверки прав админа
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# Функция для создания необходимых директорий
def create_necessary_directories():
    """Создает все необходимые директории для работы бота"""
    directories = [
        DB_DIR,
        PROJECTS_DIR,
        TEMP_DIR
    ]
    
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"✅ Директория создана/проверена: {directory}")
        except Exception as e:
            logger.error(f"❌ Ошибка создания директории {directory}: {e}")
            raise

# Функции для сохранения и восстановления состояния
def save_bot_state():
    """Сохраняет состояние бота в файл"""
    try:
        state_data = {
            'running_count': running_count,
            'active_processes_info': {
                project_id: {
                    'user_id': process_info.get('user_id'),
                    'project_name': process_info.get('project_name'),
                    'start_time': process_info.get('start_time')
                }
                for project_id, process_info in active_processes.items()
            }
        }
        
        with open(STATE_FILE, 'wb') as f:
            pickle.dump(state_data, f)
        
        logger.info("✅ Состояние бота сохранено")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения состояния бота: {e}")

def load_bot_state():
    """Загружает состояние бота из файла"""
    global running_count
    
    if not os.path.exists(STATE_FILE):
        logger.info("Файл состояния не найден, начинаем с чистого состояния")
        return
    
    try:
        with open(STATE_FILE, 'rb') as f:
            state_data = pickle.load(f)
        
        running_count = state_data.get('running_count', 0)
        logger.info(f"✅ Состояние бота загружено. running_count: {running_count}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки состояния бота: {e}")
        running_count = 0

def cleanup_state_file():
    """Очищает файл состояния при корректном завершении"""
    if os.path.exists(STATE_FILE):
        try:
            os.remove(STATE_FILE)
            logger.info("✅ Файл состояния очищен")
        except Exception as e:
            logger.error(f"❌ Ошибка очистки файла состояния: {e}")

# Инициализация базы данных
async def init_db():
    logger.info(f"Инициализация базы данных: {DB_PATH}")
    try:
        # Создаем директорию для базы данных
        create_necessary_directories()
        
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("PRAGMA foreign_keys = ON")
            await db.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    name TEXT NOT NULL,
                    safe_name TEXT NOT NULL,
                    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    file_path TEXT,
                    process_id INTEGER,
                    requirements TEXT DEFAULT '[]',
                    is_running BOOLEAN DEFAULT FALSE,
                    logs TEXT DEFAULT '',
                    auto_restart BOOLEAN DEFAULT FALSE,
                    bot_username TEXT DEFAULT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
                    UNIQUE(user_id, name)
                )
            ''')
            await db.commit()
            logger.info("✅ База данных инициализирована успешно")
        
        # Устанавливаем правильные права доступа (только для Unix-систем)
        if os.name != 'nt':  # Не Windows
            os.chmod(DB_PATH, 0o600)
            logger.info(f"Установлены права доступа для {DB_PATH}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации базы данных: {e}")
        raise

async def check_and_create_tables():
    """Проверяет и создает таблицы с улучшенной обработкой ошибок"""
    logger.info("Проверка существования таблиц")
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            # Создаем директории если нужно
            create_necessary_directories()
            
            async with aiosqlite.connect(DB_PATH, timeout=30) as db:
                # Проверяем целостность базы данных
                cursor = await db.execute("PRAGMA integrity_check")
                result = await cursor.fetchone()
                if result and result[0] != "ok":
                    logger.warning(f"База данных повреждена на попытке {attempt}, пересоздаём")
                    await db.close()
                    if os.path.exists(DB_PATH):
                        os.remove(DB_PATH)
                    await init_db()
                    return
                
                # Проверяем существование таблиц
                cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('users', 'projects')")
                tables = {row[0] async for row in cursor}
                
                if 'users' not in tables or 'projects' not in tables:
                    logger.warning(f"Не все таблицы найдены на попытке {attempt}, инициализируем")
                    await init_db()
                else:
                    logger.info("✅ Все таблицы существуют")
                    return
                    
        except aiosqlite.OperationalError as e:
            logger.error(f"Ошибка базы данных на попытке {attempt}: {e}")
            if "unable to open database file" in str(e):
                logger.info("Попытка пересоздать базу данных...")
                if os.path.exists(DB_PATH):
                    try:
                        os.remove(DB_PATH)
                    except Exception as remove_error:
                        logger.error(f"Ошибка удаления старой БД: {remove_error}")
                await init_db()
                return
                
            if attempt == max_retries:
                logger.critical("❌ Не удалось подключиться к базе данных после всех попыток")
                raise
            await asyncio.sleep(2)
            
        except Exception as e:
            logger.error(f"Неизвестная ошибка на попытке {attempt}: {e}")
            if attempt == max_retries:
                raise
            await asyncio.sleep(2)

# Функции для работы с пользователями
async def add_user(user_id: int, username: str = None):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                'INSERT OR REPLACE INTO users (user_id, username, last_active) VALUES (?, ?, ?)',
                (user_id, username, datetime.now())
            )
            await db.commit()
            logger.info(f"Пользователь {user_id} добавлен или обновлён")
    except Exception as e:
        logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
        raise

async def update_user_activity(user_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                'UPDATE users SET last_active = ? WHERE user_id = ?',
                (datetime.now(), user_id)
            )
            await db.commit()
            logger.info(f"Активность пользователя {user_id} обновлена")
    except Exception as e:
        logger.error(f"Ошибка обновления активности пользователя {user_id}: {e}")
        raise

async def get_all_users():
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT user_id FROM users')
            rows = await cursor.fetchall()
            return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Ошибка получения списка пользователей: {e}")
        return []

async def get_users_with_projects():
    """Получает всех пользователей, у которых есть проекты"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('''
                SELECT DISTINCT u.user_id, u.username 
                FROM users u 
                JOIN projects p ON u.user_id = p.user_id
            ''')
            rows = await cursor.fetchall()
            return [{'user_id': row[0], 'username': row[1]} for row in rows]
    except Exception as e:
        logger.error(f"Ошибка получения пользователей с проектами: {e}")
        return []

async def get_users_with_running_bots():
    """Получает всех пользователей, у которых есть запущенные боты"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('''
                SELECT DISTINCT u.user_id, u.username 
                FROM users u 
                JOIN projects p ON u.user_id = p.user_id 
                WHERE p.is_running = 1
            ''')
            rows = await cursor.fetchall()
            return [{'user_id': row[0], 'username': row[1]} for row in rows]
    except Exception as e:
        logger.error(f"Ошибка получения пользователей с запущенными ботами: {e}")
        return []

async def get_user_projects_files(user_id: int):
    """Получает все файлы проектов пользователя"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                'SELECT name, file_path, bot_username FROM projects WHERE user_id = ? AND file_path IS NOT NULL',
                (user_id,)
            )
            rows = await cursor.fetchall()
            return [{'name': row[0], 'file_path': row[1], 'bot_username': row[2]} for row in rows]
    except Exception as e:
        logger.error(f"Ошибка получения файлов проектов пользователя {user_id}: {e}")
        return []

# Функции для работы с проектами
async def add_project(user_id: int, project_name: str):
    safe_name = create_safe_directory_name(project_name)
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                'INSERT INTO projects (user_id, name, safe_name, created) VALUES (?, ?, ?, ?)',
                (user_id, project_name, safe_name, datetime.now())
            )
            await db.commit()
            logger.info(f"Проект '{project_name}' для пользователя {user_id} создан")
    except Exception as e:
        logger.error(f"Ошибка создания проекта '{project_name}': {e}")
        raise

async def get_user_projects(user_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                'SELECT id, name, safe_name, created, file_path, process_id, requirements, is_running, logs, auto_restart, bot_username FROM projects WHERE user_id = ?',
                (user_id,)
            )
            rows = await cursor.fetchall()
            projects = []
            for row in rows:
                project_data = {
                    'id': row[0],
                    'name': row[1],
                    'safe_name': row[2],
                    'created': datetime.fromisoformat(row[3]) if isinstance(row[3], str) else row[3],
                    'file_path': row[4],
                    'process_id': row[5],
                    'requirements': json.loads(row[6]) if row[6] else [],
                    'is_running': bool(row[7]),
                    'logs': row[8] or '',
                    'auto_restart': bool(row[9]),
                    'bot_username': row[10],
                    'process': active_processes.get(row[0])
                }
                projects.append(project_data)
            return projects
    except Exception as e:
        logger.error(f"Ошибка получения проектов пользователя {user_id}: {e}")
        return []

async def update_project(project_id: int, **kwargs):
    if not kwargs:
        return
    set_clause = ', '.join([f"{key} = ?" for key in kwargs.keys()])
    values = list(kwargs.values())
    values.append(project_id)
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(f'UPDATE projects SET {set_clause} WHERE id = ?', values)
            await db.commit()
            logger.info(f"Проект {project_id} обновлён")
    except Exception as e:
        logger.error(f"Ошибка обновления проекта {project_id}: {e}")
        raise

async def delete_project(project_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('DELETE FROM projects WHERE id = ?', (project_id,))
            await db.commit()
            logger.info(f"Проект {project_id} удалён")
        if project_id in active_processes:
            try:
                process_info = active_processes[project_id]
                if process_info and process_info['process'] and process_info['process'].poll() is None:
                    if os.name == 'nt':  # Windows
                        process_info['process'].terminate()
                    else:  # Unix-like
                        os.kill(process_info['process'].pid, signal.SIGTERM)
                    process_info['process'].wait(timeout=5)
            except Exception as e:
                logger.error(f"Ошибка остановки процесса для проекта {project_id}: {e}")
            del active_processes[project_id]
        
        # Сохраняем состояние после удаления
        save_bot_state()
        
    except Exception as e:
        logger.error(f"Ошибка удаления проекта {project_id}: {e}")
        raise

async def get_project_by_name(user_id: int, project_name: str):
    projects = await get_user_projects(user_id)
    return next((p for p in projects if p['name'] == project_name), None)

async def get_project_by_id(project_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                'SELECT id, user_id, name, safe_name, created, file_path, process_id, requirements, is_running, logs, auto_restart, bot_username FROM projects WHERE id = ?',
                (project_id,)
            )
            row = await cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'user_id': row[1],
                    'name': row[2],
                    'safe_name': row[3],
                    'created': datetime.fromisoformat(row[4]) if isinstance(row[4], str) else row[4],
                    'file_path': row[5],
                    'process_id': row[6],
                    'requirements': json.loads(row[7]) if row[7] else [],
                    'is_running': bool(row[8]),
                    'logs': row[9] or '',
                    'auto_restart': bool(row[10]),
                    'bot_username': row[11],
                    'process': active_processes.get(row[0])
                }
            return None
    except Exception as e:
        logger.error(f"Ошибка получения проекта {project_id}: {e}")
        return None

# Функция для извлечения токена бота из кода
def extract_bot_token_from_code(file_path: str) -> str:
    """Извлекает токен бота из Python файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Паттерны для поиска токена
        patterns = [
            r"BOT_TOKEN\s*=\s*['\"]([^'\"]+)['\"]",
            r"token\s*=\s*['\"]([^'\"]+)['\"]",
            r"TOKEN\s*=\s*['\"]([^'\"]+)['\"]",
            r"bot_token\s*=\s*['\"]([^'\"]+)['\"]",
            r"getenv\s*\(\s*['\"]BOT_TOKEN['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
            r"environ\.get\s*\(\s*['\"]BOT_TOKEN['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                token = match.group(1)
                if token and len(token) > 10:  # Базовая проверка на валидность токена
                    return token
        
        return None
    except Exception as e:
        logger.error(f"Ошибка извлечения токена из файла {file_path}: {e}")
        return None

# Функция для получения информации о боте по токену
async def get_bot_info_by_token(token: str) -> dict:
    """Получает информацию о боте по токену"""
    try:
        temp_bot = Bot(token=token)
        bot_info = await temp_bot.get_me()
        await temp_bot.session.close()
        return {
            'username': bot_info.username,
            'first_name': bot_info.first_name,
            'id': bot_info.id
        }
    except Exception as e:
        logger.error(f"Ошибка получения информации о боте: {e}")
        return None

# Функция для обновления информации о боте в проекте
async def update_bot_info_for_project(project_id: int, file_path: str):
    """Обновляет информацию о боте для проекта"""
    try:
        token = extract_bot_token_from_code(file_path)
        if not token:
            await update_project(project_id, bot_username=None)
            return
        
        bot_info = await get_bot_info_by_token(token)
        if bot_info:
            await update_project(project_id, bot_username=bot_info['username'])
            logger.info(f"✅ Обновлена информация о боте для проекта {project_id}: @{bot_info['username']}")
        else:
            await update_project(project_id, bot_username=None)
            logger.warning(f"❌ Не удалось получить информацию о боте для проекта {project_id}")
    except Exception as e:
        logger.error(f"Ошибка обновления информации о боте для проекта {project_id}: {e}")
        await update_project(project_id, bot_username=None)

# Функция для создания безопасного имени директории
def create_safe_directory_name(project_name: str) -> str:
    safe_name = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in project_name)
    safe_name = '_'.join(filter(None, safe_name.split('_')))
    return safe_name

# Функция для получения пути к проекту
def get_project_path(user_id: int, safe_name: str) -> str:
    return os.path.abspath(os.path.join(PROJECTS_DIR, str(user_id), safe_name))

# Функция для создания главного меню с проектами
async def get_main_menu(user_id: int) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    
    # Показываем админ-панель только админам
    if is_admin(user_id):
        keyboard.inline_keyboard.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_panel")])
        keyboard.inline_keyboard.append([InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast")])
        keyboard.inline_keyboard.append([InlineKeyboardButton(text="📊 Статистика", callback_data="stats")])
    
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="➕ Создать проект", callback_data="create_project")])
    user_projects = await get_user_projects(user_id)
    if user_projects:
        for proj in user_projects:
            status = " 🟢" if proj['is_running'] else " 🔴"
            bot_info = f" (@{proj['bot_username']})" if proj['bot_username'] else ""
            keyboard.inline_keyboard.append([InlineKeyboardButton(text=f"{proj['name']}{status}{bot_info}", callback_data=f"project_{proj['name']}")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh")])
    return keyboard

# Функция для создания клавиатуры меню проекта
async def get_project_menu(project_name: str, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    project = await get_project_by_name(user_id, project_name)
    if not project:
        return "❌ Проект не найден.", InlineKeyboardMarkup(inline_keyboard=[])
    
    created_str = project['created'].strftime('%Y-%m-%d %H:%M')
    status = "🟢 запущен" if project['is_running'] else "🔴 остановлен"
    auto_restart_status = "✅ ВКЛ" if project['auto_restart'] else "❌ ВЫКЛ"
    bot_info = f"\n🤖 Бот: @{project['bot_username']}" if project['bot_username'] else "\n🤖 Бот: не указан"
    
    text = f"📁 Информация о проекте:\n\n" \
           f"📛 Название: {project['name']}\n" \
           f"📅 Дата создания: {created_str}\n" \
           f"📊 Статус: {status}" \
           f"{bot_info}\n" \
           f"🔄 Авто-рестарт: {auto_restart_status}\n" \
           f"🤖 Запущено ботов всего: {running_count}/{MAX_CONCURRENT_BOTS}"
    
    inline_keyboard = []
    if not project['file_path']:
        inline_keyboard.append([InlineKeyboardButton(text="📤 Установить файл", callback_data=f"install_file_{project_name}")])
    else:
        inline_keyboard.append([InlineKeyboardButton(text="🔄 Сменить файл", callback_data=f"change_file_{project_name}")])
        if project['is_running']:
            inline_keyboard.append([InlineKeyboardButton(text="⏹️ Остановить", callback_data=f"stop_{project_name}")])
        else:
            inline_keyboard.append([InlineKeyboardButton(text="▶️ Запуск", callback_data=f"run_{project_name}")])
        
        # Кнопка авто-рестарта
        if project['auto_restart']:
            inline_keyboard.append([InlineKeyboardButton(text="🔴 Выключить авто-рестарт", callback_data=f"toggle_restart_{project_name}")])
        else:
            inline_keyboard.append([InlineKeyboardButton(text="🟢 Включить авто-рестарт", callback_data=f"toggle_restart_{project_name}")])
        
        inline_keyboard.append([InlineKeyboardButton(text="📚 Установить библиотеку", callback_data=f"install_lib_{project_name}")])
        if project['is_running']:
            inline_keyboard.append([InlineKeyboardButton(text="📋 Логи", callback_data=f"logs_{project_name}")])
    
    inline_keyboard.append([InlineKeyboardButton(text="🗑️ Удалить проект", callback_data=f"delete_{project_name}")])
    inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)
    return text, keyboard

# Функция для создания админ-панели
async def get_admin_panel() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 Боты в хосте", callback_data="admin_bots_in_host")],
        [InlineKeyboardButton(text="📁 Исходники ботов", callback_data="admin_bot_sources")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")]
    ])
    return keyboard

# Хэндлер для команды /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    await add_user(user_id, message.from_user.username)
    keyboard = await get_main_menu(user_id)
    await message.answer(
        "👋 Привет! Добро пожаловать в бота для хостинга Python скриптов.\n\n"
        "Нажмите кнопку ниже, чтобы создать новый проект.\n\n"
        "📂 Ваши проекты:",
        reply_markup=keyboard
    )

# Хэндлер для админ-панели
@dp.callback_query(lambda c: c.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    await update_user_activity(user_id)
    keyboard = await get_admin_panel()
    await callback.message.edit_text(
        "👑 Админ-панель\n\n"
        "Выберите действие:",
        reply_markup=keyboard
    )
    await callback.answer()

# Хэндлер для "Боты в хосте"
@dp.callback_query(lambda c: c.data == "admin_bots_in_host")
async def admin_bots_in_host(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    await update_user_activity(user_id)
    
    # Получаем пользователей с запущенными ботами
    users_with_bots = await get_users_with_running_bots()
    
    if not users_with_bots:
        text = "🤖 В настоящее время нет запущенных ботов в хосте."
    else:
        text = "🤖 Боты в хосте:\n\n"
        for user in users_with_bots:
            username = user['username'] or "Без username"
            
            # Получаем проекты пользователя с информацией о ботах
            user_projects = await get_user_projects(user['user_id'])
            running_projects = [p for p in user_projects if p['is_running'] and p['bot_username']]
            
            text += f"👤 Пользователь: {username} (ID: {user['user_id']})\n"
            
            if running_projects:
                for project in running_projects:
                    text += f"   📁 {project['name']} → 🤖 @{project['bot_username']}\n"
            else:
                text += f"   📁 Без запущенных ботов\n"
            text += "\n"
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в админ-панель", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard)
    await callback.answer()

# Хэндлер для "Исходники ботов"
@dp.callback_query(lambda c: c.data == "admin_bot_sources")
async def admin_bot_sources(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    await update_user_activity(user_id)
    
    # Получаем всех пользователей с проектами
    users_with_projects = await get_users_with_projects()
    
    if not users_with_projects:
        text = "📁 Нет пользователей с ботами."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад в админ-панель", callback_data="admin_panel")]
        ])
    else:
        text = "📁 Выберите пользователя для просмотра исходников:"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[])
        
        for user in users_with_projects:
            username = user['username'] or f"User_{user['user_id']}"
            
            # Получаем проекты пользователя для отображения информации о ботах
            user_projects = await get_user_projects_files(user['user_id'])
            bot_count = sum(1 for p in user_projects if p['bot_username'])
            
            button_text = f"👤 {username}"
            if bot_count > 0:
                button_text += f" ({bot_count} ботов)"
            
            keyboard.inline_keyboard.append([
                InlineKeyboardButton(text=button_text, callback_data=f"admin_user_sources_{user['user_id']}")
            ])
        
        keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад в админ-панель", callback_data="admin_panel")])
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# Хэндлер для выбора пользователя
@dp.callback_query(lambda c: c.data.startswith("admin_user_sources_"))
async def admin_user_sources(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    await update_user_activity(user_id)
    
    target_user_id = int(callback.data.replace("admin_user_sources_", ""))
    
    # Получаем проекты пользователя
    projects = await get_user_projects_files(target_user_id)
    
    if not projects:
        text = f"❌ У пользователя {target_user_id} нет проектов с файлами."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к списку пользователей", callback_data="admin_bot_sources")]
        ])
    else:
        text = f"📁 Проекты пользователя {target_user_id}:\n\n"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[])
        
        for project in projects:
            bot_info = f" → 🤖 @{project['bot_username']}" if project['bot_username'] else " → 🤖 нет информации"
            keyboard.inline_keyboard.append([
                InlineKeyboardButton(text=f"📄 {project['name']}{bot_info}", callback_data=f"admin_view_source_{target_user_id}_{project['name']}")
            ])
        
        keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад к списку пользователей", callback_data="admin_bot_sources")])
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# Хэндлер для просмотра исходного кода
@dp.callback_query(lambda c: c.data.startswith("admin_view_source_"))
async def admin_view_source(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    await update_user_activity(user_id)
    
    # Парсим данные из callback
    data_parts = callback.data.replace("admin_view_source_", "").split("_")
    target_user_id = int(data_parts[0])
    project_name = "_".join(data_parts[1:])
    
    # Получаем проект
    project = await get_project_by_name(target_user_id, project_name)
    
    if not project or not project['file_path'] or not os.path.exists(project['file_path']):
        await callback.message.answer("❌ Файл проекта не найден.")
        await callback.answer()
        return
    
    try:
        # Отправляем файл
        caption = f"📄 Файл проекта '{project['name']}'\n👤 Пользователь: {target_user_id}"
        if project['bot_username']:
            caption += f"\n🤖 Бот: @{project['bot_username']}"
        else:
            caption += "\n🤖 Бот: не указан"
        
        await bot.send_document(
            chat_id=callback.from_user.id,
            document=FSInputFile(project['file_path']),
            caption=caption
        )
        
        bot_info = f"\n🤖 Привязан к боту: @{project['bot_username']}" if project['bot_username'] else "\n🤖 Бот: не указан"
        
        text = f"📄 Исходный код проекта '{project_name}'\n👤 Пользователь: {target_user_id}{bot_info}\n\n✅ Файл отправлен!"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить инфо о боте", callback_data=f"admin_refresh_bot_{target_user_id}_{project['name']}")],
            [InlineKeyboardButton(text="⬅️ Назад к проектам", callback_data=f"admin_user_sources_{target_user_id}")]
        ])
        
        await callback.message.edit_text(text, reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка отправки файла: {str(e)}")
    
    await callback.answer()

# Хэндлер для обновления информации о боте
@dp.callback_query(lambda c: c.data.startswith("admin_refresh_bot_"))
async def admin_refresh_bot_info(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    await update_user_activity(user_id)
    
    # Парсим данные из callback
    data_parts = callback.data.replace("admin_refresh_bot_", "").split("_")
    target_user_id = int(data_parts[0])
    project_name = "_".join(data_parts[1:])
    
    # Получаем проект
    project = await get_project_by_name(target_user_id, project_name)
    
    if not project or not project['file_path']:
        await callback.answer("❌ Проект или файл не найден.")
        return
    
    try:
        await callback.answer("🔄 Обновляем информацию о боте...")
        await update_bot_info_for_project(project['id'], project['file_path'])
        
        # Обновляем проект после обновления информации
        project = await get_project_by_name(target_user_id, project_name)
        bot_info = f"🤖 @{project['bot_username']}" if project['bot_username'] else "❌ не удалось определить"
        
        await callback.message.answer(f"✅ Информация о боте обновлена: {bot_info}")
        
    except Exception as e:
        await callback.answer(f"❌ Ошибка обновления: {str(e)}")

# Хэндлер для скачивания файла
@dp.callback_query(lambda c: c.data.startswith("admin_download_"))
async def admin_download_file(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    await update_user_activity(user_id)
    
    # Парсим данные из callback
    data_parts = callback.data.replace("admin_download_", "").split("_")
    target_user_id = int(data_parts[0])
    project_name = "_".join(data_parts[1:])
    
    # Получаем проект
    project = await get_project_by_name(target_user_id, project_name)
    
    if not project or not project['file_path'] or not os.path.exists(project['file_path']):
        await callback.answer("❌ Файл проекта не найден.")
        return
    
    try:
        # Отправляем файл
        with open(project['file_path'], 'rb') as file:
            await bot.send_document(
                chat_id=callback.from_user.id,
                document=types.BufferedInputFile(
                    file.read(),
                    filename=f"{project_name}_{target_user_id}.py"
                ),
                caption=f"📄 Файл проекта '{project_name}'\n👤 Пользователь: {target_user_id}\n🤖 Бот: @{project['bot_username']}" if project['bot_username'] else f"📄 Файл проекта '{project_name}'\n👤 Пользователь: {target_user_id}\n🤖 Бот: не указан"
            )
        await callback.answer("✅ Файл отправлен")
    except Exception as e:
        await callback.answer(f"❌ Ошибка отправки файла: {str(e)}")

# Хэндлер для обновления меню
@dp.callback_query(lambda c: c.data == "refresh")
async def refresh_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    await update_user_activity(user_id)
    keyboard = await get_main_menu(user_id)
    await callback.message.edit_reply_markup(reply_markup=keyboard)
    await callback.answer("✅ Меню обновлено")

# Хэндлер для нажатия кнопки "Создать проект"
@dp.callback_query(lambda c: c.data == "create_project")
async def process_create_project(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("✏️ Введите название проекта:")
    await state.set_state(ProjectStates.waiting_for_project_name)
    await callback.answer()

# Хэндлер для получения названия проекта
@dp.message(ProjectStates.waiting_for_project_name)
async def process_project_name(message: types.Message, state: FSMContext):
    project_name = message.text.strip()
    user_id = message.from_user.id
    existing_projects = await get_user_projects(user_id)
    if any(p['name'] == project_name for p in existing_projects):
        await message.answer("❌ Проект с таким названием уже существует. Введите другое название.")
        return
    try:
        await add_project(user_id, project_name)
        await message.answer(f"✅ Проект '{project_name}' создан!")
    except Exception as e:
        await message.answer(f"❌ Ошибка при создании проекта: {str(e)}")
        return
    keyboard = await get_main_menu(user_id)
    await message.answer(
        "👋 Привет! Добро пожаловать в бота для хостинга Python скриптов.\n\n"
        "Нажмите кнопку ниже, чтобы создать новый проект.\n\n"
        "📂 Ваши проекты:",
        reply_markup=keyboard
    )
    await state.clear()

# Хэндлер для нажатия на кнопку проекта
@dp.callback_query(lambda c: c.data.startswith("project_"))
async def process_project_button(callback: CallbackQuery):
    project_name = callback.data.replace("project_", "")
    user_id = callback.from_user.id
    await update_user_activity(user_id)
    text, keyboard = await get_project_menu(project_name, user_id)
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# Хэндлер для кнопки "Назад в меню"
@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    await update_user_activity(user_id)
    keyboard = await get_main_menu(user_id)
    await callback.message.edit_text(
        "👋 Привет! Добро пожаловать в бота для хостинга Python скриптов.\n\n"
        "Нажмите кнопку ниже, чтобы создать новый проект.\n\n"
        "📂 Ваши проекты:",
        reply_markup=keyboard
    )
    await callback.answer()

# Хэндлер для переключения авто-рестарта
@dp.callback_query(lambda c: c.data.startswith("toggle_restart_"))
async def toggle_auto_restart(callback: CallbackQuery):
    project_name = callback.data.replace("toggle_restart_", "")
    user_id = callback.from_user.id
    await update_user_activity(user_id)
    
    project = await get_project_by_name(user_id, project_name)
    if not project:
        await callback.answer("❌ Проект не найден")
        return
    
    new_auto_restart = not project['auto_restart']
    await update_project(project['id'], auto_restart=new_auto_restart)
    
    text, keyboard = await get_project_menu(project_name, user_id)
    await callback.message.edit_text(text, reply_markup=keyboard)
    
    status = "включен" if new_auto_restart else "выключен"
    await callback.answer(f"✅ Авто-рестарт {status}")

# Хэндлер для статистики (только админ)
@dp.callback_query(lambda c: c.data == "stats")
async def show_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    all_users = await get_all_users()
    total_projects = 0
    running_projects = 0
    auto_restart_projects = 0
    bots_with_username = 0
    
    for user_id in all_users:
        projects = await get_user_projects(user_id)
        total_projects += len(projects)
        running_projects += sum(1 for p in projects if p['is_running'])
        auto_restart_projects += sum(1 for p in projects if p['auto_restart'])
        bots_with_username += sum(1 for p in projects if p['bot_username'])
    
    stats_text = (
        f"📊 Статистика бота:\n\n"
        f"👥 Всего пользователей: {len(all_users)}\n"
        f"📁 Всего проектов: {total_projects}\n"
        f"🟢 Запущено проектов: {running_projects}\n"
        f"🔴 Остановлено проектов: {total_projects - running_projects}\n"
        f"🤖 Ботов с username: {bots_with_username}\n"
        f"🔄 Авто-рестарт: {auto_restart_projects}\n"
        f"🚀 Лимит ботов: {running_count}/{MAX_CONCURRENT_BOTS}\n"
        f"🐍 Используется: Python subprocess"
    )
    await callback.message.edit_text(stats_text)
    await callback.answer()

# Хэндлер для кнопки "Рассылка" (только для админа)
@dp.callback_query(lambda c: c.data == "broadcast")
async def start_broadcast(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    all_users = await get_all_users()
    await callback.message.edit_text(
        f"📢 Отправьте сообщение для рассылки (текст или фото с подписью).\n"
        f"Получателей: {len(all_users)} пользователей"
    )
    await state.set_state(BroadcastStates.waiting_for_content)
    await callback.answer()

# Хэндлер для получения контента рассылки
@dp.message(BroadcastStates.waiting_for_content)
async def receive_broadcast_content(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    content = {}
    if message.text:
        content['type'] = 'text'
        content['text'] = message.text
    elif message.photo:
        content['type'] = 'photo'
        content['photo'] = message.photo[-1].file_id
        if message.caption:
            content['caption'] = message.caption
    else:
        await message.answer("❌ Поддерживается только текст или фото.")
        return
    await state.update_data(broadcast_content=content)
    if content['type'] == 'text':
        preview_text = f"📋 Предпросмотр:\n\n{content['text']}"
    else:
        preview_text = "📋 Предпросмотр: Фото будет отправлено"
        if content.get('caption'):
            preview_text += f"\n\nПодпись:\n\n{content['caption']}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="confirm_broadcast")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_broadcast")]
    ])
    await message.answer(preview_text, reply_markup=keyboard)

# Хэндлер для подтверждения рассылки
@dp.callback_query(lambda c: c.data == "confirm_broadcast")
async def confirm_broadcast(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    data = await state.get_data()
    content = data.get('broadcast_content')
    if not content:
        await callback.answer("❌ Нет контента для рассылки.")
        return
    all_users = await get_all_users()
    sent_count = 0
    failed_count = 0
    progress_msg = await callback.message.edit_text(f"📤 Начинаем рассылку... 0/{len(all_users)}")
    for i, user_id in enumerate(all_users):
        try:
            if content['type'] == 'text':
                await bot.send_message(user_id, content['text'])
            else:
                await bot.send_photo(user_id, content['photo'], caption=content.get('caption'))
            sent_count += 1
            if i % 10 == 0:
                await progress_msg.edit_text(f"📤 Рассылка... {i+1}/{len(all_users)}")
        except Exception as e:
            failed_count += 1
            logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")]
    ])
    await progress_msg.edit_text(
        f"✅ Рассылка завершена!\n"
        f"📤 Отправлено: {sent_count}\n"
        f"❌ Ошибок: {failed_count}\n"
        f"👥 Всего получателей: {len(all_users)}\n\n"
        f"Нажмите 'Назад в меню' для возврата.",
        reply_markup=back_keyboard
    )
    await state.clear()
    await callback.answer()

# Хэндлер для отмены рассылки
@dp.callback_query(lambda c: c.data == "cancel_broadcast")
async def cancel_broadcast(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён.")
        return
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")]
    ])
    await callback.message.edit_text(
        "❌ Рассылка отменена.\n\nНажмите 'Назад в меню' для возврата.",
        reply_markup=back_keyboard
    )
    await state.clear()
    await callback.answer()

# Хэндлер для "Установить файл"
@dp.callback_query(lambda c: c.data.startswith("install_file_"))
async def install_file_start(callback: CallbackQuery, state: FSMContext):
    project_name = callback.data.replace("install_file_", "")
    await state.update_data(project_name=project_name)
    await callback.message.edit_text("📤 Отправьте .py файл или архив (.zip) для проекта.")
    await state.set_state(ProjectStates.waiting_for_file)
    await callback.answer()

# Хэндлер для "Сменить файл"
@dp.callback_query(lambda c: c.data.startswith("change_file_"))
async def change_file_start(callback: CallbackQuery, state: FSMContext):
    project_name = callback.data.replace("change_file_", "")
    await state.update_data(project_name=project_name, is_change=True)
    await callback.message.edit_text("📤 Отправьте новый .py файл или архив (.zip) для замены.")
    await state.set_state(ProjectStates.waiting_for_file)
    await callback.answer()

# Хэндлер для получения файла
@dp.message(ProjectStates.waiting_for_file)
async def process_file(message: types.Message, state: FSMContext):
    data = await state.get_data()
    project_name = data['project_name']
    is_change = data.get('is_change', False)
    user_id = message.from_user.id
    await update_user_activity(user_id)
    if not message.document:
        await message.answer("❌ Пожалуйста, отправьте файл.")
        return
    file_name = message.document.file_name or ""
    if not (file_name.endswith('.py') or file_name.endswith('.zip')):
        await message.answer("❌ Файл должен быть .py или .zip архивом.")
        return
    project = await get_project_by_name(user_id, project_name)
    if not project:
        await message.answer("❌ Проект не найден.")
        await state.clear()
        return
    if is_change and project['is_running']:
        global running_count
        if project.get('process'):
            try:
                process_info = project['process']
                if process_info and process_info['process'] and process_info['process'].poll() is None:
                    if os.name == 'nt':
                        process_info['process'].terminate()
                    else:
                        os.kill(process_info['process'].pid, signal.SIGTERM)
                    process_info['process'].wait(timeout=5)
            except Exception as e:
                logger.error(f"Ошибка остановки процесса: {e}")
            active_processes.pop(project['id'], None)
            running_count = max(0, running_count - 1)
        project['is_running'] = False
        project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Процесс остановлен для смены файла.\n"
        await update_project(project['id'], is_running=False, process_id=None, logs=project['logs'])
        
        # Сохраняем состояние
        save_bot_state()
        
    project_dir = get_project_path(user_id, project['safe_name'])
    if is_change and os.path.exists(project_dir):
        shutil.rmtree(project_dir)
    os.makedirs(project_dir, exist_ok=True)
    try:
        file = await bot.get_file(message.document.file_id)
        downloaded_path = os.path.join(project_dir, file_name)
        await bot.download_file(file.file_path, downloaded_path)
        if file_name.endswith('.py'):
            project['file_path'] = downloaded_path
            
            # Обновляем информацию о боте после загрузки файла
            asyncio.create_task(update_bot_info_for_project(project['id'], downloaded_path))
            
            try:
                async with aiofiles.open(downloaded_path, 'r', encoding='utf-8') as f:
                    content = await f.read()
                preview = content[:1000] + "..." if len(content) > 1000 else content
                await message.answer(f"✅ Файл '{file_name}' {'заменён' if is_change else 'установлен'}!\n\nСодержимое файла:\n\n```{preview}```\n\nГлавный файл: {os.path.basename(project['file_path'])}")
            except Exception as e:
                await message.answer(f"✅ Файл '{file_name}' {'заменён' if is_change else 'установлен'}, но не удалось прочитать содержимое: {str(e)}")
        elif file_name.endswith('.zip'):
            with zipfile.ZipFile(downloaded_path, 'r') as zip_ref:
                zip_ref.extractall(project_dir)
            os.remove(downloaded_path)
            possible_files = ['main.py', 'app.py', 'bot.py']
            project['file_path'] = None
            for pf in possible_files:
                if os.path.exists(os.path.join(project_dir, pf)):
                    project['file_path'] = os.path.join(project_dir, pf)
                    break
            if not project['file_path']:
                for root, _, files in os.walk(project_dir):
                    for f in files:
                        if f.endswith('.py'):
                            project['file_path'] = os.path.join(root, f)
                            break
                    if project['file_path']:
                        break
            if not project['file_path']:
                await message.answer("❌ В архиве не найден .py файл для запуска.")
                await state.clear()
                return
            
            # Обновляем информацию о боте после распаковки архива
            asyncio.create_task(update_bot_info_for_project(project['id'], project['file_path']))
            
            await message.answer(f"✅ Архив '{file_name}' {'заменён' if is_change else 'распакован'}! Главный файл: {os.path.basename(project['file_path'])}")
        await update_project(project['id'], file_path=project['file_path'], logs=project['logs'])
    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке файла: {str(e)}")
        await state.clear()
        return
    text, keyboard = await get_project_menu(project_name, user_id)
    await message.answer(text, reply_markup=keyboard)
    await state.clear()

# Хэндлер для "Установить библиотеку"
@dp.callback_query(lambda c: c.data.startswith("install_lib_"))
async def install_lib_start(callback: CallbackQuery, state: FSMContext):
    project_name = callback.data.replace("install_lib_", "")
    await state.update_data(project_name=project_name)
    try:
        await check_and_create_tables()
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка базы данных: {str(e)}")
        await callback.answer()
        return
    await callback.message.edit_text("📚 Введите название библиотеки Python для установки (например, aiogram):")
    await state.set_state(ProjectStates.waiting_for_lib_name)
    await callback.answer()

# Хэндлер для получения названия библиотеки
@dp.message(ProjectStates.waiting_for_lib_name)
async def process_lib_name(message: types.Message, state: FSMContext):
    data = await state.get_data()
    project_name = data['project_name']
    lib_name = message.text.strip()
    user_id = message.from_user.id
    await update_user_activity(user_id)
    try:
        await check_and_create_tables()
    except Exception as e:
        await message.answer(f"❌ Ошибка базы данных: {str(e)}")
        await state.clear()
        return
    project = await get_project_by_name(user_id, project_name)
    if not project:
        await message.answer("❌ Проект не найден.")
        await state.clear()
        return
    project_dir = get_project_path(user_id, project['safe_name'])
    os.makedirs(project_dir, exist_ok=True)
    try:
        install_msg = await message.reply(f"⏳ Устанавливаем '{lib_name}'...")
        
        # Установка библиотеки через pip
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "pip", "install", lib_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=project_dir
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            requirements = project['requirements']
            if lib_name not in requirements:
                requirements.append(lib_name)
                await update_project(project['id'], requirements=json.dumps(requirements))
            
            output = stdout.decode('utf-8', errors='ignore') if stdout else ""
            project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Установлена библиотека: {lib_name}\n{output}\n"
            await update_project(project['id'], logs=project['logs'])
            
            await install_msg.edit_text(f"✅ Библиотека '{lib_name}' установлена!\n{output[-500:]}")
        else:
            error_output = stderr.decode('utf-8', errors='ignore') if stderr else "Неизвестная ошибка"
            project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка установки {lib_name}:\n{error_output}\n"
            await update_project(project['id'], logs=project['logs'])
            await install_msg.edit_text(f"❌ Ошибка установки '{lib_name}':\n{error_output[-500:]}")
    except Exception as e:
        await message.answer(f"❌ Ошибка при установке библиотеки: {str(e)}")
        await state.clear()
        return
    text, keyboard = await get_project_menu(project_name, user_id)
    await message.reply(text, reply_markup=keyboard)
    await state.clear()

# Хэндлер для "Запуск"
@dp.callback_query(lambda c: c.data.startswith("run_"))
async def run_project(callback: CallbackQuery):
    global running_count
    project_name = callback.data.replace("run_", "")
    user_id = callback.from_user.id
    await update_user_activity(user_id)
    project = await get_project_by_name(user_id, project_name)
    if not project:
        await callback.message.answer("❌ Проект не найден.")
        await callback.answer()
        return
    if project['is_running']:
        await callback.message.answer("⚠️ Проект уже запущен. Используйте 'Остановить' для перезапуска.")
        await callback.answer()
        return
    if not project['file_path']:
        await callback.message.answer("❌ Сначала установите файл.")
        await callback.answer()
        return
    if running_count >= MAX_CONCURRENT_BOTS:
        await callback.message.answer(f"❌ Достигнут лимит запущенных ботов: {MAX_CONCURRENT_BOTS}.")
        await callback.answer()
        return
    project_dir = os.path.dirname(project['file_path'])
    if not os.path.exists(project_dir):
        project_dir = get_project_path(user_id, project['safe_name'])
        os.makedirs(project_dir, exist_ok=True)
    if not os.path.exists(project['file_path']):
        await callback.message.answer(f"❌ Файл проекта не найден: {project['file_path']}")
        await callback.answer()
        return
    try:
        script_name = os.path.basename(project['file_path'])
        
        # Установка зависимостей если есть
        if project['requirements']:
            install_msg = await callback.message.answer("⏳ Устанавливаем зависимости...")
            reqs_path = os.path.join(project_dir, 'requirements.txt')
            with open(reqs_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(project['requirements']))
            
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "pip", "install", "-r", "requirements.txt",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_dir
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                await install_msg.edit_text("✅ Зависимости установлены!")
                project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Зависимости установлены.\n"
            else:
                error_output = stderr.decode('utf-8', errors='ignore') if stderr else "Неизвестная ошибка"
                project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка установки зависимостей:\n{error_output}\n"
                await update_project(project['id'], logs=project['logs'])
                await install_msg.edit_text(f"❌ Ошибка установки зависимостей:\n{error_output[-500:]}")
                await callback.answer()
                return
        
        # Запуск основного скрипта
        process = await asyncio.create_subprocess_exec(
            sys.executable, script_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=project_dir
        )
        
        # Сохраняем информацию о процессе
        process_info = {
            'process': process,
            'user_id': user_id,
            'project_name': project_name,
            'start_time': datetime.now()
        }
        
        active_processes[project['id']] = process_info
        project['process'] = process_info
        project['is_running'] = True
        running_count += 1
        project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Процесс запущен: PID {process.pid}\n"
        await update_project(project['id'], is_running=True, logs=project['logs'], process_id=process.pid)
        
        # Сохраняем состояние
        save_bot_state()
        
        # Мониторинг вывода процесса
        asyncio.create_task(monitor_process_output(process, project['id']))
        asyncio.create_task(wait_for_process(process, project['id'], user_id, project_name))
        
        text, keyboard = await get_project_menu(project_name, user_id)
        await callback.message.answer(text, reply_markup=keyboard)
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка при запуске проекта: {str(e)}")
        logger.error(f"Ошибка запуска проекта: {e}")
    await callback.answer()

# Функция для мониторинга вывода процесса
async def monitor_process_output(process, project_id):
    try:
        while process.returncode is None:
            if process.stdout:
                line = await process.stdout.readline()
                if line:
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if decoded:
                        log_entry = f"[{datetime.now().strftime('%H:%M:%S')}] {decoded}\n"
                        project = await get_project_by_id(project_id)
                        if project:
                            current_logs = project['logs'] + log_entry
                            if len(current_logs) > 10000:
                                current_logs = current_logs[-10000:]
                            await update_project(project_id, logs=current_logs)
            await asyncio.sleep(0.1)
    except Exception as e:
        logger.error(f"Ошибка чтения вывода процесса {process.pid}: {e}")

# Функция для ожидания завершения процесса
async def wait_for_process(process, project_id, user_id, project_name):
    global running_count
    try:
        await process.wait()
        returncode = process.returncode
        
        project = await get_project_by_id(project_id)
        if project:
            project['is_running'] = False
            project['process'] = None
            running_count = max(0, running_count - 1)
            log_entry = f"[{datetime.now().strftime('%H:%M:%S')}] Процесс завершён с кодом: {returncode}\n"
            await update_project(project_id, is_running=False, logs=project['logs'] + log_entry, process_id=None)
            active_processes.pop(project_id, None)
            
            # Авто-рестарт если включен
            if project['auto_restart'] and returncode != 0:
                logger.info(f"🔄 Авто-рестарт проекта {project_name}")
                await asyncio.sleep(5)  # Ждем 5 секунд перед рестартом
                await restart_project(project_id, user_id, project_name)
            else:
                try:
                    status_text = "успешно завершён" if returncode == 0 else f"завершён с ошибкой (код: {returncode})"
                    await bot.send_message(user_id, f"📋 Проект '{project_name}' {status_text}.")
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
            
            # Сохраняем состояние
            save_bot_state()
            
    except Exception as e:
        logger.error(f"Ошибка ожидания процесса: {e}")
        project = await get_project_by_id(project_id)
        if project:
            await update_project(project_id, is_running=False, logs=f"{project['logs']}\n[{datetime.now().strftime('%H:%M:%S')}] Ошибка ожидания процесса: {str(e)}", process_id=None)
        active_processes.pop(project_id, None)
        running_count = max(0, running_count - 1)
        
        # Сохраняем состояние
        save_bot_state()

# Функция для авто-рестарта проекта
async def restart_project(project_id, user_id, project_name):
    global running_count
    
    if running_count >= MAX_CONCURRENT_BOTS:
        logger.warning(f"❌ Не могу перезапустить {project_name} - достигнут лимит ботов")
        try:
            await bot.send_message(user_id, f"❌ Не удалось перезапустить '{project_name}' - достигнут лимит ботов.")
        except:
            pass
        return
    
    project = await get_project_by_id(project_id)
    if not project or not project['file_path'] or not os.path.exists(project['file_path']):
        return
    
    try:
        project_dir = os.path.dirname(project['file_path'])
        script_name = os.path.basename(project['file_path'])
        
        process = await asyncio.create_subprocess_exec(
            sys.executable, script_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=project_dir
        )
        
        process_info = {
            'process': process,
            'user_id': user_id,
            'project_name': project_name,
            'start_time': datetime.now()
        }
        
        active_processes[project_id] = process_info
        project['process'] = process_info
        project['is_running'] = True
        running_count += 1
        project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 Процесс перезапущен: PID {process.pid}\n"
        await update_project(project_id, is_running=True, logs=project['logs'], process_id=process.pid)
        
        # Сохраняем состояние
        save_bot_state()
        
        asyncio.create_task(monitor_process_output(process, project_id))
        asyncio.create_task(wait_for_process(process, project_id, user_id, project_name))
        
        logger.info(f"✅ Проект {project_name} перезапущен")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при перезапуске проекта {project_name}: {e}")
        try:
            await bot.send_message(user_id, f"❌ Ошибка при перезапуске '{project_name}': {str(e)}")
        except:
            pass

# Хэндлер для "Остановить"
@dp.callback_query(lambda c: c.data.startswith("stop_"))
async def stop_project(callback: CallbackQuery):
    global running_count
    project_name = callback.data.replace("stop_", "")
    user_id = callback.from_user.id
    await update_user_activity(user_id)
    project = await get_project_by_name(user_id, project_name)
    if not project:
        await callback.message.answer("❌ Проект не найден.")
        await callback.answer()
        return
    if not project['is_running']:
        await callback.message.answer("⚠️ Проект уже остановлен.")
        await callback.answer()
        return
    if project.get('process'):
        try:
            process_info = project['process']
            if process_info and process_info['process'] and process_info['process'].poll() is None:
                if os.name == 'nt':  # Windows
                    process_info['process'].terminate()
                else:  # Unix-like
                    os.kill(process_info['process'].pid, signal.SIGTERM)
                try:
                    await asyncio.wait_for(process_info['process'].wait(), timeout=5)
                except asyncio.TimeoutError:
                    process_info['process'].kill()
                    await process_info['process'].wait()
        except Exception as e:
            logger.error(f"Ошибка остановки процесса: {e}")
        active_processes.pop(project['id'], None)
        project['is_running'] = False
        project['process'] = None
        project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Процесс остановлен пользователем.\n"
        running_count = max(0, running_count - 1)
        await update_project(project['id'], is_running=False, logs=project['logs'], process_id=None)
        
        # Сохраняем состояние
        save_bot_state()
        
    text, keyboard = await get_project_menu(project_name, user_id)
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# Хэндлер для "Логи"
@dp.callback_query(lambda c: c.data.startswith("logs_"))
async def show_logs(callback: CallbackQuery):
    project_name = callback.data.replace("logs_", "")
    user_id = callback.from_user.id
    await update_user_activity(user_id)
    project = await get_project_by_name(user_id, project_name)
    if not project:
        await callback.message.answer("❌ Проект не найден.")
        await callback.answer()
        return
    logs = project['logs'] or "Логи отсутствуют."
    if len(logs) > 4000:
        logs = logs[-4000:]
        logs = "...\n" + logs
    await callback.message.answer(f"📋 Логи проекта '{project_name}':\n\n```{logs}```")
    await callback.answer()

# Хэндлер для "Удалить проект"
@dp.callback_query(lambda c: c.data.startswith("delete_"))
async def delete_project_handler(callback: CallbackQuery):
    project_name = callback.data.replace("delete_", "")
    user_id = callback.from_user.id
    await update_user_activity(user_id)
    project = await get_project_by_name(user_id, project_name)
    if not project:
        await callback.message.answer("❌ Проект не найден.")
        await callback.answer()
        return
    if project['is_running']:
        global running_count
        if project.get('process'):
            try:
                process_info = project['process']
                if process_info and process_info['process'] and process_info['process'].poll() is None:
                    if os.name == 'nt':
                        process_info['process'].terminate()
                    else:
                        os.kill(process_info['process'].pid, signal.SIGTERM)
                    process_info['process'].wait(timeout=5)
            except Exception as e:
                logger.error(f"Ошибка остановки процесса: {e}")
            active_processes.pop(project['id'], None)
            running_count = max(0, running_count - 1)
    project_dir = get_project_path(user_id, project['safe_name'])
    if os.path.exists(project_dir):
        shutil.rmtree(project_dir)
    await delete_project(project['id'])
    await callback.message.answer(f"🗑️ Проект '{project_name}' удалён.")
    keyboard = await get_main_menu(user_id)
    await callback.message.answer(
        "👋 Привет! Добро пожаловать в бота для хостинга Python скриптов.\n\n"
        "Нажмите кнопку ниже, чтобы создать новый проект.\n\n"
        "📂 Ваши проекты:",
        reply_markup=keyboard
    )
    await callback.answer()

# Функция для восстановления состояния запущенных проектов
async def restore_running_projects():
    global running_count
    logger.info("🔍 Восстанавливаем состояние запущенных проектов...")
    
    # Загружаем состояние из файла
    load_bot_state()
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            'SELECT id, user_id, name, safe_name, file_path, process_id, logs, auto_restart FROM projects WHERE is_running = 1'
        )
        running_projects = await cursor.fetchall()
    
    restored_count = 0
    for project_row in running_projects:
        project_id, user_id, project_name, safe_name, file_path, process_id, logs, auto_restart = project_row
        
        if not file_path or not os.path.exists(file_path):
            logger.warning(f"❌ Файл проекта {project_name} не найден, помечаем как остановленный")
            await update_project(project_id, is_running=False, logs=f"{logs}\n[{datetime.now().strftime('%H:%M:%S')}] Файл не найден при восстановлении", process_id=None)
            continue
        
        if running_count >= MAX_CONCURRENT_BOTS:
            logger.warning(f"❌ Достигнут лимит ботов при восстановлении {project_name}")
            await update_project(project_id, is_running=False, logs=f"{logs}\n[{datetime.now().strftime('%H:%M:%S')}] Не восстановлен - достигнут лимит ботов", process_id=None)
            continue
        
        # Автоматически перезапускаем проекты с включенным авто-рестартом
        if auto_restart:
            logger.info(f"🔄 Авто-восстановление проекта: {project_name}")
            await restart_project(project_id, user_id, project_name)
            restored_count += 1
        else:
            logger.info(f"❌ Проект {project_name} помечен как остановленный (авто-рестарт выключен)")
            await update_project(project_id, is_running=False, logs=f"{logs}\n[{datetime.now().strftime('%H:%M:%S')}] Процесс остановлен при перезапуске бота", process_id=None)
    
    logger.info(f"✅ Восстановлено проектов: {restored_count}")

# Функция для периодической очистки неактивных пользователей
async def cleanup_inactive_users():
    while True:
        try:
            cutoff_date = datetime.now().timestamp() - (30 * 24 * 60 * 60)
            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute(
                    'SELECT user_id FROM users WHERE last_active < ?',
                    (datetime.fromtimestamp(cutoff_date),)
                )
                inactive_users = await cursor.fetchall()
                for user_row in inactive_users:
                    user_id = user_row[0]
                    projects = await get_user_projects(user_id)
                    for project in projects:
                        if project['is_running'] and project.get('process'):
                            try:
                                process_info = project['process']
                                if process_info and process_info['process'] and process_info['process'].poll() is None:
                                    if os.name == 'nt':
                                        process_info['process'].terminate()
                                    else:
                                        os.kill(process_info['process'].pid, signal.SIGTERM)
                                    process_info['process'].wait(timeout=5)
                            except Exception as e:
                                logger.error(f"Ошибка остановки процесса для пользователя {user_id}: {e}")
                            active_processes.pop(project['id'], None)
                            global running_count
                            running_count = max(0, running_count - 1)
                        project_dir = get_project_path(user_id, project['safe_name'])
                        if os.path.exists(project_dir):
                            shutil.rmtree(project_dir)
                    await db.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
                    await db.execute('DELETE FROM projects WHERE user_id = ?', (user_id,))
                    logger.info(f"🗑️ Удалён неактивный пользователь: {user_id}")
                await db.commit()
                
                # Сохраняем состояние после очистки
                save_bot_state()
                
        except Exception as e:
            logger.error(f"Ошибка при очистке неактивных пользователей: {e}")
        await asyncio.sleep(24 * 60 * 60)

# Функция для graceful shutdown
async def on_shutdown():
    global running_count
    logger.info("🔌 Выполняем graceful shutdown...")
    
    # Сохраняем состояние перед завершением
    save_bot_state()
    
    for project_id, process_info in list(active_processes.items()):
        try:
            if process_info and process_info['process'] and process_info['process'].poll() is None:
                if os.name == 'nt':
                    process_info['process'].terminate()
                else:
                    os.kill(process_info['process'].pid, signal.SIGTERM)
                try:
                    await asyncio.wait_for(process_info['process'].wait(), timeout=5)
                except asyncio.TimeoutError:
                    process_info['process'].kill()
                    await process_info['process'].wait()
                await update_project(project_id, is_running=False, process_id=None)
                logger.info(f"Остановлен процесс для проекта {project_id}")
        except Exception as e:
            logger.error(f"Ошибка остановки процесса {process_info['process'].pid if process_info and process_info['process'] else 'N/A'}: {e}")
    
    running_count = 0
    active_processes.clear()
    
    # Очищаем файл состояния при корректном завершении
    cleanup_state_file()

# Основная функция
async def main():
    try:
        # Создаем необходимые директории в первую очередь
        create_necessary_directories()
        
        # Инициализируем базу данных
        await check_and_create_tables()
        
        # Восстанавливаем запущенные проекты
        await restore_running_projects()
        
        # Запускаем фоновые задачи
        asyncio.create_task(cleanup_inactive_users())
        
        logger.info("🤖 Бот запущен! (без Docker)")
        logger.info("💾 Система сохранения состояния активна")
        logger.info(f"👑 Админы: {ADMIN_IDS}")
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}")
        # Сохраняем состояние даже при ошибке
        save_bot_state()
        # Попробуем отправить сообщение об ошибке если бот частично работает
        try:
            await bot.send_message(ADMIN_IDS[0], f"❌ Бот упал с ошибкой: {e}")
        except:
            pass
    finally:
        await on_shutdown()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
        # Сохраняем состояние при принудительной остановке
        save_bot_state()
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")
        # Сохраняем состояние при критической ошибке
        save_bot_state()