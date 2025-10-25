import asyncio
import logging
import os
import subprocess
import zipfile
import shutil
import json
import aiosqlite
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
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
BOT_TOKEN = "8319655667:AAHmk8XQ8rlrD-H6L51SCePfjWB-dHCjUa0"  # Замените на реальный токен

# Максимальное количество одновременно запущенных ботов
MAX_CONCURRENT_BOTS = 8

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Путь к базе данных
DB_PATH = './data/bot_database.db'

# Определяем состояния FSM
class ProjectStates(StatesGroup):
    waiting_for_project_name = State()
    waiting_for_file = State()
    waiting_for_lib_name = State()

class BroadcastStates(StatesGroup):
    waiting_for_content = State()

# Глобальные переменные
running_count = 0
active_processes = {}

# Инициализация базы данных
async def init_db():
    """Инициализация базы данных с обработкой ошибок"""
    logger.info(f"Инициализация базы данных: {DB_PATH}")
    try:
        # Создаём директорию для базы данных
        db_dir = os.path.dirname(DB_PATH)
        if db_dir:
            logger.info(f"Создание директории для базы данных: {db_dir}")
            os.makedirs(db_dir, exist_ok=True)

        async with aiosqlite.connect(DB_PATH) as db:
            # Включаем внешние ключи
            await db.execute("PRAGMA foreign_keys = ON")
            logger.info("Внешние ключи включены")

            # Таблица пользователей
            await db.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            logger.info("Таблица users создана или существует")

            # Таблица проектов
            await db.execute('''
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    name TEXT NOT NULL,
                    safe_name TEXT NOT NULL,
                    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    file_path TEXT,
                    requirements TEXT DEFAULT '[]',
                    is_running BOOLEAN DEFAULT FALSE,
                    logs TEXT DEFAULT '',
                    FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
                    UNIQUE(user_id, name)
                )
            ''')
            logger.info("Таблица projects создана или существует")

            await db.commit()
            logger.info("✅ База данных инициализирована успешно")

        # Устанавливаем права доступа к файлу базы данных
        os.chmod(DB_PATH, 0o600)
        logger.info(f"Установлены права доступа для {DB_PATH}")

    except Exception as e:
        logger.error(f"❌ Ошибка инициализации базы данных: {e}")
        raise

async def check_and_create_tables():
    """Проверяет существование таблиц и создаёт их при необходимости"""
    logger.info("Проверка существования таблиц")
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                # Проверяем целостность базы данных
                cursor = await db.execute("PRAGMA integrity_check")
                result = await cursor.fetchone()
                if result[0] != "ok":
                    logger.warning(f"База данных повреждена на попытке {attempt}, удаляем и создаём заново")
                    await db.close()
                    if os.path.exists(DB_PATH):
                        os.remove(DB_PATH)
                    await init_db()
                    return

                # Проверяем наличие таблиц
                cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('users', 'projects')")
                tables = {row[0] async for row in cursor}
                if 'users' not in tables or 'projects' not in tables:
                    logger.warning(f"Не все таблицы найдены на попытке {attempt}, инициализируем базу")
                    await init_db()
                else:
                    logger.info("✅ Все таблицы существуют")
                    return

        except aiosqlite.OperationalError as e:
            logger.error(f"Ошибка базы данных на попытке {attempt}: {e}")
            if attempt == max_retries:
                logger.error("Достигнуто максимальное количество попыток")
                raise
            await asyncio.sleep(1)  # Пауза перед повторной попыткой
        except Exception as e:
            logger.error(f"Неизвестная ошибка на попытке {attempt}: {e}")
            if attempt == max_retries:
                raise
            await asyncio.sleep(1)

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
                'SELECT id, name, safe_name, created, file_path, requirements, is_running, logs FROM projects WHERE user_id = ?',
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
                    'requirements': json.loads(row[5]) if row[5] else [],
                    'is_running': bool(row[6]),
                    'logs': row[7] or '',
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
            await db.execute(
                f'UPDATE projects SET {set_clause} WHERE id = ?',
                values
            )
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
            del active_processes[project_id]
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
                'SELECT id, user_id, name, safe_name, created, file_path, requirements, is_running, logs FROM projects WHERE id = ?',
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
                    'requirements': json.loads(row[6]) if row[6] else [],
                    'is_running': bool(row[7]),
                    'logs': row[8] or '',
                    'process': active_processes.get(row[0])
                }
            return None
    except Exception as e:
        logger.error(f"Ошибка получения проекта {project_id}: {e}")
        return None

# Функция для создания безопасного имени директории
def create_safe_directory_name(project_name: str) -> str:
    safe_name = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in project_name)
    safe_name = '_'.join(filter(None, safe_name.split('_')))
    return safe_name

# Функция для получения пути к проекту
def get_project_path(user_id: int, safe_name: str) -> str:
    return os.path.abspath(os.path.join("./projects", str(user_id), safe_name))

# Функция для создания главного меню с проектами
async def get_main_menu(user_id: int) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    if user_id == 5000282571:  # ID админа
        keyboard.inline_keyboard.append([InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast")])
        keyboard.inline_keyboard.append([InlineKeyboardButton(text="📊 Статистика", callback_data="stats")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="➕ Создать проект", callback_data="create_project")])
    user_projects = await get_user_projects(user_id)
    if user_projects:
        for proj in user_projects:
            status = " 🟢" if proj['is_running'] else " 🔴"
            keyboard.inline_keyboard.append([InlineKeyboardButton(text=f"{proj['name']}{status}", callback_data=f"project_{proj['name']}")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh")])
    return keyboard

# Функция для создания клавиатуры меню проекта
async def get_project_menu(project_name: str, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    project = await get_project_by_name(user_id, project_name)
    if not project:
        return "❌ Проект не найден.", InlineKeyboardMarkup(inline_keyboard=[])
    created_str = project['created'].strftime('%Y-%m-%d %H:%M')
    status = "🟢 запущен" if project['is_running'] else "🔴 остановлен"
    text = f"📁 Информация о проекте:\n\n" \
           f"📛 Название: {project['name']}\n" \
           f"📅 Дата создания: {created_str}\n" \
           f"📊 Статус: {status}\n" \
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
    inline_keyboard.append([InlineKeyboardButton(text="📚 Установить библиотеку", callback_data=f"install_lib_{project_name}")])
    if project['is_running']:
        inline_keyboard.append([InlineKeyboardButton(text="📋 Логи", callback_data=f"logs_{project_name}")])
    inline_keyboard.append([InlineKeyboardButton(text="🗑️ Удалить проект", callback_data=f"delete_{project_name}")])
    inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)
    return text, keyboard

# Хэндлер для команды /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    await add_user(user_id, message.from_user.username)
    keyboard = await get_main_menu(user_id)
    await message.answer(
        "👋 Привет! Добро пожаловать в бота для хостинга кода.\n\n"
        "Нажмите кнопку ниже, чтобы создать новый проект.\n\n"
        "📂 Ваши проекты:",
        reply_markup=keyboard
    )

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
        "👋 Привет! Добро пожаловать в бота для хостинга кода.\n\n"
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
        "👋 Привет! Добро пожаловать в бота для хостинга кода.\n\n"
        "Нажмите кнопку ниже, чтобы создать новый проект.\n\n"
        "📂 Ваши проекты:",
        reply_markup=keyboard
    )
    await callback.answer()

# Хэндлер для статистики (только админ)
@dp.callback_query(lambda c: c.data == "stats")
async def show_stats(callback: CallbackQuery):
    if callback.from_user.id != 5000282571:
        await callback.answer("❌ Доступ запрещён.")
        return
    all_users = await get_all_users()
    total_projects = 0
    running_projects = 0
    for user_id in all_users:
        projects = await get_user_projects(user_id)
        total_projects += len(projects)
        running_projects += sum(1 for p in projects if p['is_running'])
    stats_text = (
        f"📊 Статистика бота:\n\n"
        f"👥 Всего пользователей: {len(all_users)}\n"
        f"📁 Всего проектов: {total_projects}\n"
        f"🟢 Запущено проектов: {running_projects}\n"
        f"🔴 Остановлено проектов: {total_projects - running_projects}\n"
        f"🤖 Лимит ботов: {running_count}/{MAX_CONCURRENT_BOTS}"
    )
    await callback.message.edit_text(stats_text)
    await callback.answer()

# Хэндлер для кнопки "Рассылка" (только для админа)
@dp.callback_query(lambda c: c.data == "broadcast")
async def start_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != 5000282571:
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
    if message.from_user.id != 5000282571:
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
    if callback.from_user.id != 5000282571:
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
    if callback.from_user.id != 5000282571:
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
            project['process'].terminate()
            try:
                await asyncio.wait_for(project['process'].wait(), timeout=5)
            except asyncio.TimeoutError:
                project['process'].kill()
            active_processes.pop(project['id'], None)
        project['is_running'] = False
        project['process'] = None
        project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Процесс остановлен для смены файла.\n"
        running_count = max(0, running_count - 1)
        await update_project(project['id'], is_running=False, logs=project['logs'])
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
    prev_dir = os.getcwd()
    os.chdir(project_dir)
    install_msg = await message.reply(f"⏳ Скачиваем и устанавливаем '{lib_name}'...\n")
    output_log = []
    async def read_and_update(stream, install_msg):
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                decoded = line.decode('utf-8', errors='ignore').rstrip()
                if decoded:
                    output_log.append(f"[{datetime.now().strftime('%H:%M:%S')}] {decoded}")
                    display_lines = '\n'.join([log.split('] ', 1)[1] if '] ' in log else log for log in output_log[-5:]])
                    new_text = f"⏳ Скачиваем и устанавливаем '{lib_name}'...\n{display_lines}"
                    try:
                        await install_msg.edit_text(new_text)
                    except Exception as e:
                        logger.error(f"Ошибка редактирования сообщения: {e}")
        except Exception as e:
            logger.error(f"Ошибка чтения потока: {e}")
    try:
        process = await asyncio.create_subprocess_exec(
            'python', '-m', 'pip', 'install', '--user', lib_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout_task = asyncio.create_task(read_and_update(process.stdout, install_msg))
        stderr_task = asyncio.create_task(read_and_update(process.stderr, install_msg))
        try:
            await asyncio.wait_for(process.wait(), timeout=120)
        except asyncio.TimeoutError:
            process.terminate()
            await process.wait()
            output_log.append(f"[{datetime.now().strftime('%H:%M:%S')}] Таймаут установки.")
            await install_msg.edit_text(f"❌ Таймаут установки '{lib_name}'.\n" + '\n'.join([log.split('] ', 1)[1] if '] ' in log else log for log in output_log[-5:]]))
        else:
            stdout_task.cancel()
            stderr_task.cancel()
            display_lines = '\n'.join([log.split('] ', 1)[1] if '] ' in log else log for log in output_log[-5:]])
            if process.returncode == 0:
                requirements = project['requirements']
                if lib_name not in requirements:
                    requirements.append(lib_name)
                    await update_project(project['id'], requirements=json.dumps(requirements))
                project['logs'] += '\n'.join(output_log) + '\n'
                await update_project(project['id'], logs=project['logs'])
                await install_msg.edit_text(f"✅ Библиотека '{lib_name}' успешно установлена!\n{display_lines}")
            else:
                project['logs'] += '\n'.join(output_log) + '\n'
                await update_project(project['id'], logs=project['logs'])
                error_display = f"❌ Ошибка установки '{lib_name}':\n{display_lines}"
                await install_msg.edit_text(error_display)
    except Exception as e:
        await install_msg.edit_text(f"❌ Ошибка при установке библиотеки: {str(e)}")
    os.chdir(prev_dir)
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
        await callback.message.answer(f"❌ Достигнут лимит запущенных ботов: {MAX_CONCURRENT_BOTS}. Попробуйте позже.")
        await callback.answer()
        return
    project_dir = os.path.dirname(project['file_path'])
    if not os.path.exists(project_dir):
        project_dir = get_project_path(user_id, project['safe_name'])
        os.makedirs(project_dir, exist_ok=True)
        if not os.path.exists(project_dir):
            await callback.message.answer(f"❌ Директория проекта не найдена и не может быть создана: {project_dir}")
            await callback.answer()
            return
    if not os.path.exists(project['file_path']):
        await callback.message.answer(f"❌ Файл проекта не найден: {project['file_path']}")
        await callback.answer()
        return
    prev_dir = os.getcwd()
    try:
        os.chdir(project_dir)
        if project['requirements']:
            reqs_path = 'requirements.txt'
            with open(reqs_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(project['requirements']))
            install_msg = await callback.message.answer("⏳ Устанавливаем зависимости...")
            install_process = await asyncio.create_subprocess_exec(
                'python', '-m', 'pip', 'install', '--user', '-r', reqs_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await install_process.communicate()
            if install_process.returncode == 0:
                await install_msg.edit_text("✅ Зависимости установлены успешно!")
                project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Requirements установлены успешно.\n"
            else:
                error_msg = stderr.decode('utf-8', errors='ignore') if stderr else "Неизвестная ошибка"
                await install_msg.edit_text(f"❌ Ошибка установки зависимостей: {error_msg[:1000]}")
                project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка установки requirements: {error_msg}\n"
                os.chdir(prev_dir)
                return
        script_name = os.path.basename(project['file_path'])
        await callback.message.answer(f"🚀 Запускаем проект: {script_name}")
        process = await asyncio.create_subprocess_exec(
            'python', script_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=project_dir
        )
        active_processes[project['id']] = process
        project['process'] = process
        project['is_running'] = True
        running_count += 1
        project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Проект запущен: {script_name}\n"
        await update_project(project['id'], is_running=True, logs=project['logs'])
        async def read_output(stream, prefix, project_id):
            try:
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if decoded:
                        log_entry = f"[{datetime.now().strftime('%H:%M:%S')}] {prefix}: {decoded}\n"
                        current_project = await get_project_by_id(project_id)
                        if current_project:
                            current_logs = current_project['logs'] + log_entry
                            if len(current_logs) > 10000:
                                current_logs = current_logs[-10000:]
                            await update_project(project_id, logs=current_logs)
            except Exception as e:
                logger.error(f"Ошибка чтения {prefix}: {e}")
        asyncio.create_task(read_output(process.stdout, "STDOUT", project['id']))
        asyncio.create_task(read_output(process.stderr, "STDERR", project['id']))
        asyncio.create_task(wait_for_process(process, project['id'], user_id, project_name))
        text, keyboard = await get_project_menu(project_name, user_id)
        await callback.message.answer(text, reply_markup=keyboard)
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка при запуске проекта: {str(e)}")
        logger.error(f"Ошибка запуска проекта: {e}")
    finally:
        os.chdir(prev_dir)
    await callback.answer()

# Функция для ожидания завершения процесса
async def wait_for_process(process, project_id, user_id, project_name):
    global running_count
    try:
        returncode = await process.wait()
        project = await get_project_by_id(project_id)
        if project:
            project['is_running'] = False
            project['process'] = None
            running_count = max(0, running_count - 1)
            log_entry = f"[{datetime.now().strftime('%H:%M:%S')}] Процесс завершён с кодом: {returncode}\n"
            new_logs = project['logs'] + log_entry
            await update_project(project_id, is_running=False, logs=new_logs)
            active_processes.pop(project_id, None)
            try:
                status_text = "успешно завершён" if returncode == 0 else f"завершён с ошибкой (код: {returncode})"
                await bot.send_message(user_id, f"📋 Проект '{project_name}' {status_text}.")
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка ожидания процесса: {e}")

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
        project['process'].terminate()
        try:
            await asyncio.wait_for(project['process'].wait(), timeout=5)
        except asyncio.TimeoutError:
            project['process'].kill()
        active_processes.pop(project['id'], None)
        project['is_running'] = False
        project['process'] = None
        project['logs'] += f"[{datetime.now().strftime('%H:%M:%S')}] Процесс остановлен пользователем.\n"
        running_count = max(0, running_count - 1)
        await update_project(project['id'], is_running=False, logs=project['logs'])
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
    logs = project['logs']
    if not logs:
        logs = "Логи отсутствуют."
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
            project['process'].terminate()
            try:
                await asyncio.wait_for(project['process'].wait(), timeout=5)
            except asyncio.TimeoutError:
                project['process'].kill()
            active_processes.pop(project['id'], None)
        running_count = max(0, running_count - 1)
    project_dir = get_project_path(user_id, project['safe_name'])
    if os.path.exists(project_dir):
        shutil.rmtree(project_dir)
    await delete_project(project['id'])
    await callback.message.answer(f"🗑️ Проект '{project_name}' удалён.")
    keyboard = await get_main_menu(user_id)
    await callback.message.answer(
        "👋 Привет! Добро пожаловать в бота для хостинга кода.\n\n"
        "Нажмите кнопку ниже, чтобы создать новый проект.\n\n"
        "📂 Ваши проекты:",
        reply_markup=keyboard
    )
    await callback.answer()

# Функция для восстановления состояния запущенных проектов
async def restore_running_projects():
    global running_count
    logger.info("🔍 Восстанавливаем состояние запущенных проектов...")
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            'SELECT id, user_id, name, safe_name, file_path, logs FROM projects WHERE is_running = 1'
        )
        running_projects = await cursor.fetchall()
    restored_count = 0
    for project_row in running_projects:
        project_id, user_id, project_name, safe_name, file_path, logs = project_row
        if not file_path or not os.path.exists(file_path):
            logger.warning(f"❌ Файл проекта {project_name} не найден, помечаем как остановленный")
            await update_project(project_id, is_running=False, logs=f"{logs}\n[{datetime.now().strftime('%H:%M:%S')}] Файл не найден при восстановлении")
            continue
        if running_count >= MAX_CONCURRENT_BOTS:
            logger.warning(f"❌ Достигнут лимит ботов при восстановлении {project_name}")
            await update_project(project_id, is_running=False, logs=f"{logs}\n[{datetime.now().strftime('%H:%M:%S')}] Не восстановлен - достигнут лимит ботов")
            continue
        try:
            project_dir = os.path.dirname(file_path)
            script_name = os.path.basename(file_path)
            process = await asyncio.create_subprocess_exec(
                'python', script_name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_dir
            )
            active_processes[project_id] = process
            running_count += 1
            restored_count += 1
            new_logs = f"{logs}\n[{datetime.now().strftime('%H:%M:%S')}] Проект восстановлен после перезапуска бота\n"
            await update_project(project_id, logs=new_logs)
            async def read_output_for_restored(stream, prefix, project_id):
                try:
                    while True:
                        line = await stream.readline()
                        if not line:
                            break
                        decoded = line.decode('utf-8', errors='ignore').strip()
                        if decoded:
                            log_entry = f"[{datetime.now().strftime('%H:%M:%S')}] {prefix}: {decoded}\n"
                            current_project = await get_project_by_id(project_id)
                            if current_project:
                                current_logs = current_project['logs'] + log_entry
                                if len(current_logs) > 10000:
                                    current_logs = current_logs[-10000:]
                                await update_project(project_id, logs=current_logs)
                except Exception as e:
                    logger.error(f"Ошибка чтения {prefix} для восстановленного проекта: {e}")
            asyncio.create_task(read_output_for_restored(process.stdout, "STDOUT", project_id))
            asyncio.create_task(read_output_for_restored(process.stderr, "STDERR", project_id))
            asyncio.create_task(wait_for_restored_process(process, project_id, user_id, project_name))
            logger.info(f"✅ Восстановлен проект: {project_name}")
        except Exception as e:
            logger.error(f"❌ Ошибка восстановления проекта {project_name}: {e}")
            await update_project(project_id, is_running=False, logs=f"{logs}\n[{datetime.now().strftime('%H:%M:%S')}] Ошибка восстановления: {str(e)}")
    logger.info(f"✅ Восстановлено проектов: {restored_count}")

async def wait_for_restored_process(process, project_id, user_id, project_name):
    global running_count
    try:
        returncode = await process.wait()
    except Exception as e:
        logger.error(f"Ошибка ожидания восстановленного процесса: {e}")
        returncode = -1
    running_count = max(0, running_count - 1)
    project = await get_project_by_id(project_id)
    if project:
        log_entry = f"[{datetime.now().strftime('%H:%M:%S')}] Процесс завершён с кодом: {returncode}\n"
        new_logs = project['logs'] + log_entry
        await update_project(project_id, is_running=False, logs=new_logs)
    active_processes.pop(project_id, None)
    try:
        status_text = "успешно завершён" if returncode == 0 else f"завершён с ошибкой (код: {returncode})"
        await bot.send_message(user_id, f"📋 Проект '{project_name}' {status_text}.")
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")

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
                        if project['is_running']:
                            global running_count
                            if project.get('process'):
                                project['process'].terminate()
                                try:
                                    await asyncio.wait_for(project['process'].wait(), timeout=5)
                                except asyncio.TimeoutError:
                                    project['process'].kill()
                                active_processes.pop(project['id'], None)
                            running_count = max(0, running_count - 1)
                        project_dir = get_project_path(user_id, project['safe_name'])
                        if os.path.exists(project_dir):
                            shutil.rmtree(project_dir)
                    await db.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
                    await db.execute('DELETE FROM projects WHERE user_id = ?', (user_id,))
                    logger.info(f"🗑️ Удалён неактивный пользователь: {user_id}")
                await db.commit()
        except Exception as e:
            logger.error(f"Ошибка при очистке неактивных пользователей: {e}")
        await asyncio.sleep(24 * 60 * 60)

# Основная функция
async def main():
    try:
        await check_and_create_tables()
        await restore_running_projects()
        asyncio.create_task(cleanup_inactive_users())
        logger.info("🤖 Бот запущен!")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    os.makedirs("./projects", exist_ok=True)
    asyncio.run(main())