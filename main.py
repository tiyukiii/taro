import asyncio
import json
import random
import logging
import aiosqlite
from datetime import datetime, timedelta, date
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import LabeledPrice, PreCheckoutQuery, InlineKeyboardMarkup, InlineKeyboardButton, ChatJoinRequest
from aiogram.utils.keyboard import InlineKeyboardBuilder

# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = "8445697844:AAEP5-qB3p4CtE84Ect5Gis1jAqOSJ7DKTA"

# !!! ВАЖНО: Впишите сюда свой цифровой ID
ADMIN_IDS = [544039604, 1540889862]

DB_NAME = "tarot_v5.db" 
JSON_FILE = "cards.json"
FORTUNE_TELLER_IMG = "https://cdn.displate.com/artwork/857x1200/2023-01-11/4e9987ecc0d5f4e35a4eae87d35049b7_44ffaee58be41a20f85dda51064b05e9.jpg"

DAILY_FREE_LIMIT = 2  # Количество бесплатных гаданий в сутки

# Фразы для анимации ожидания
LOADING_PHRASES = [
    "🔮 Заглядываю в будущее...",
    "🃏 Перемешиваю колоду...",
    "✨ Настраиваюсь на твою энергию...",
    "🌙 Карты шепчут ответ...",
    "💫 Считываю знаки судьбы...",
    "👁 Открываю завесу тайны...",
    "🧘‍♀️ Соединяюсь с потоком..."
]

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- СОСТОЯНИЯ (FSM) ---
class AdminStates(StatesGroup):
    waiting_for_broadcast = State()
    waiting_for_channel_id = State()
    waiting_for_channel_url = State()
    waiting_for_channel_name = State()
    waiting_for_channel_type = State()
    waiting_for_unlimited_user_id = State()
    waiting_for_campaign_name = State()

class CompatStates(StatesGroup):
    waiting_for_partner_name = State()

# --- БАЗА ДАННЫХ ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Таблица пользователей
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                last_free_time TEXT,
                daily_usage INTEGER DEFAULT 0,
                last_card_id TEXT,
                bonus_requests INTEGER DEFAULT 0,
                referrer_id TEXT, 
                joined_at TEXT,
                is_unlimited INTEGER DEFAULT 0,
                last_daily_card_time TEXT,
                streak_days INTEGER DEFAULT 0,
                last_active_date TEXT,
                unlimited_until TEXT,
                last_notification_time TEXT
            )
        """)
        
        # Миграции (добавление новых колонок)
        columns = [
            ("daily_usage", "INTEGER DEFAULT 0"),
            ("last_daily_card_time", "TEXT"),
            ("streak_days", "INTEGER DEFAULT 0"),
            ("last_active_date", "TEXT"),
            ("unlimited_until", "TEXT"),
            ("last_notification_time", "TEXT") # Новая колонка для уведомлений
        ]
        for col, type_def in columns:
            try: await db.execute(f"ALTER TABLE users ADD COLUMN {col} {type_def}")
            except Exception: pass 

        # Остальные таблицы...
        await db.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                campaign_code TEXT PRIMARY KEY,
                clicks INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                card_id TEXT,
                card_name TEXT,
                category TEXT,
                date TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT,
                title TEXT,
                url TEXT,
                type TEXT DEFAULT 'channel',
                position INTEGER DEFAULT 0
            )
        """)
        try: await db.execute("ALTER TABLE channels ADD COLUMN type TEXT DEFAULT 'channel'")
        except Exception: pass
        try: await db.execute("ALTER TABLE channels ADD COLUMN position INTEGER DEFAULT 0")
        except Exception: pass

        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_rewards (
                user_id INTEGER,
                channel_id TEXT,
                PRIMARY KEY (user_id, channel_id)
            )
        """)
        await db.commit()

# --- ФОНОВАЯ ЗАДАЧА: УМНЫЕ УВЕДОМЛЕНИЯ ---
async def retention_worker(bot: Bot):
    """
    Проверяет пользователей, которые не брали Карту Дня более 24 часов
    и отправляет им напоминание.
    """
    while True:
        try:
            # Ждем час перед следующей проверкой (можно менять)
            await asyncio.sleep(3600) 
            
            now = datetime.now()
            notify_list = []

            async with aiosqlite.connect(DB_NAME) as db:
                # Выбираем тех, у кого есть last_daily_card_time
                async with db.execute("SELECT user_id, last_daily_card_time, last_notification_time FROM users WHERE last_daily_card_time IS NOT NULL") as cursor:
                    users = await cursor.fetchall()

                for row in users:
                    user_id, last_daily_str, last_notif_str = row
                    
                    if not last_daily_str: continue
                    
                    last_daily = datetime.fromisoformat(last_daily_str)
                    
                    # Если прошло больше 24 часов с последней карты дня
                    if (now - last_daily) > timedelta(hours=24):
                        
                        should_send = False
                        # Если уведомление еще ни разу не отправляли
                        if not last_notif_str:
                            should_send = True
                        else:
                            # Или если с последнего уведомления прошло больше 24 часов (чтобы не спамить)
                            last_notif = datetime.fromisoformat(last_notif_str)
                            if (now - last_notif) > timedelta(hours=24):
                                should_send = True
                        
                        if should_send:
                            notify_list.append(user_id)

                # Отправляем уведомления
                for uid in notify_list:
                    try:
                        builder = InlineKeyboardBuilder()
                        builder.button(text="🃏 Вытянуть Карту Дня", callback_data="daily_card")
                        
                        await bot.send_message(
                            uid, 
                            "👋 **Твоя Карта Дня уже заждалась...**\n\nПрошел день, энергии изменились. Заглянем в будущее? 🔮",
                            reply_markup=builder.as_markup(),
                            parse_mode="Markdown"
                        )
                        # Обновляем время уведомления
                        await db.execute("UPDATE users SET last_notification_time = ? WHERE user_id = ?", (now.isoformat(), uid))
                        await asyncio.sleep(0.1) # Пауза чтобы не ловить лимиты телеграма
                    except Exception as e:
                        # Если бот заблокирован пользователем
                        pass
                
                if notify_list:
                    await db.commit()
                    logging.info(f"Retention: Sent notifications to {len(notify_list)} users.")

        except Exception as e:
            logging.error(f"Retention Worker Error: {e}")
            await asyncio.sleep(60) # Если ошибка, ждем минуту и пробуем снова

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def format_time_remaining(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours} ч. {minutes} мин."

async def update_streak(user_id):
    today_str = date.today().isoformat()
    yesterday_str = (date.today() - timedelta(days=1)).isoformat()
    
    async with aiosqlite.connect(DB_NAME) as db:
        row = await (await db.execute("SELECT streak_days, last_active_date FROM users WHERE user_id = ?", (user_id,))).fetchone()
        if not row: return
        
        streak, last_date = row[0] or 0, row[1]
        
        if last_date == today_str:
            return 
        
        if last_date == yesterday_str:
            new_streak = streak + 1
        else:
            new_streak = 1 
            
        await db.execute("UPDATE users SET streak_days = ?, last_active_date = ? WHERE user_id = ?", (new_streak, today_str, user_id))
        await db.commit()

# --- КЛАВИАТУРЫ ---

def get_main_keyboard(bot_username):
    builder = InlineKeyboardBuilder()
    builder.button(text="🃏 Карта дня (раз в 24ч)", callback_data="daily_card")
    builder.button(text="💼 Работа", callback_data="category_work")
    builder.button(text="❤️ Любовь", callback_data="category_love")
    builder.button(text="🔮 Будущее", callback_data="category_future")
    builder.button(text="💞 Совместимость", callback_data="compat_start")
    builder.button(text="👤 Мой профиль", callback_data="show_profile")
    builder.adjust(1, 2, 2, 1)
    return builder.as_markup()

def get_profile_keyboard(bot_username):
    builder = InlineKeyboardBuilder()
    builder.button(text="📖 Моя история", callback_data="show_history")
    builder.button(text="♾ Безлимит (24ч) - 100 ⭐️", callback_data="buy_unlimited_24h")
    builder.button(text="👥 Пригласить друга", callback_data="referral_info")
    builder.button(text="➕ Добавить в чат", url=f"https://t.me/{bot_username}?startgroup=true")
    builder.button(text="🔙 В главное меню", callback_data="back_to_menu")
    builder.adjust(1, 1, 2, 1)
    return builder.as_markup()

def get_post_prediction_keyboard(category, show_extra_btn=True):
    builder = InlineKeyboardBuilder()
    if show_extra_btn:
        builder.button(text="🃏 Доп. карта (подробнее)", callback_data=f"extra_card_{category}")
    builder.button(text="✨ Другой вопрос", callback_data="back_to_menu")
    builder.button(text="👤 Профиль", callback_data="show_profile")
    
    if show_extra_btn:
        builder.adjust(1, 2)
    else:
        builder.adjust(2)
    return builder.as_markup()

def get_back_button():
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 В главное меню", callback_data="back_to_menu")
    return builder.as_markup()

async def get_pay_menu(user_id):
    builder = InlineKeyboardBuilder()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT channel_id, title, url, type FROM channels ORDER BY position ASC") as cursor:
            all_channels = await cursor.fetchall()
        async with db.execute("SELECT channel_id FROM user_rewards WHERE user_id = ?", (user_id,)) as cursor:
            claimed_rows = await cursor.fetchall()
            claimed_ids = [row[0] for row in claimed_rows]

    channels_available = False
    for ch_id, title, url, ch_type in all_channels:
        if ch_id not in claimed_ids:
            btn_text = f"🤖 Запустить: {title} (+1)" if ch_type == 'bot' else f"📢 Подписаться: {title} (+1)"
            builder.button(text=btn_text, url=url)
            channels_available = True
    
    if channels_available:
        builder.button(text="✅ Проверить выполнение", callback_data="check_all_subs")
    
    builder.button(text="⭐ 1 Расклад (5 Stars)", callback_data="buy_stars")
    builder.button(text="♾ Безлимит 24ч (100 Stars)", callback_data="buy_unlimited_24h")
    builder.button(text="👥 Позвать друга (+1)", callback_data="referral_info")
    builder.button(text="🔙 В главное меню", callback_data="back_to_menu")
    builder.adjust(1)
    return builder.as_markup()

# --- КЛАВИАТУРЫ АДМИНА ---
def get_admin_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="🔗 Создать ссылку", callback_data="admin_create_link")
    builder.button(text="♾ Безлимит (Вечный)", callback_data="admin_unlimited")
    builder.button(text="📢 Рассылка", callback_data="admin_broadcast")
    builder.button(text="📺 Каналы спонсоров", callback_data="admin_channels")
    builder.button(text="❌ Закрыть", callback_data="admin_close")
    builder.adjust(1, 2, 2, 1)
    return builder.as_markup()

def get_channels_manage_keyboard(channels):
    builder = InlineKeyboardBuilder()
    for ch in channels:
        ch_db_id = ch[0]
        title = ch[2]
        builder.button(text=f"🗑 {title}", callback_data=f"admin_del_ch_{ch_db_id}")
        builder.button(text="⬆️", callback_data=f"admin_mov_up_{ch_db_id}")
        builder.button(text="⬇️", callback_data=f"admin_mov_dw_{ch_db_id}")
    builder.button(text="➕ Добавить спонсора", callback_data="admin_add_channel_start")
    builder.button(text="🔙 В админ-меню", callback_data="admin_menu")
    row_sizes = [3] * len(channels) + [1, 1]
    builder.adjust(*row_sizes)
    return builder.as_markup()

# --- ЛОГИКА ДОСТУПА ---

async def check_access(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        row = await (await db.execute("SELECT last_free_time, daily_usage, bonus_requests, is_unlimited, unlimited_until FROM users WHERE user_id = ?", (user_id,))).fetchone()
        
        if not row: return True, "free" 
        
        last_time_str, daily_usage, bonus_req, is_unlimited, unlimited_until = row
        
        if is_unlimited == 1:
            return True, "unlimited"
        
        now = datetime.now()

        if unlimited_until:
            until_dt = datetime.fromisoformat(unlimited_until)
            if now < until_dt:
                return True, "unlimited"

        if last_time_str:
            last_time = datetime.fromisoformat(last_time_str)
            time_diff = now - last_time
            if time_diff > timedelta(hours=24):
                await db.execute("UPDATE users SET daily_usage = 0, last_free_time = NULL WHERE user_id = ?", (user_id,))
                await db.commit()
                daily_usage = 0
                last_time_str = None
        
        if daily_usage < DAILY_FREE_LIMIT:
            return True, "free"
        
        if bonus_req > 0:
            return True, "bonus"
            
        if last_time_str:
            last_time = datetime.fromisoformat(last_time_str)
            reset_time = last_time + timedelta(hours=24)
            remaining = (reset_time - now).total_seconds()
            return False, remaining if remaining > 0 else 0
            
        return False, 0

async def use_access(user_id, mode):
    if mode == "unlimited": return 

    now = datetime.now()
    async with aiosqlite.connect(DB_NAME) as db:
        if mode == "bonus":
            await db.execute("UPDATE users SET bonus_requests = bonus_requests - 1 WHERE user_id = ?", (user_id,))
        elif mode == "free":
            row = await (await db.execute("SELECT last_free_time FROM users WHERE user_id = ?", (user_id,))).fetchone()
            if row and row[0]:
                await db.execute("UPDATE users SET daily_usage = daily_usage + 1 WHERE user_id = ?", (user_id,))
            else:
                await db.execute("UPDATE users SET daily_usage = daily_usage + 1, last_free_time = ? WHERE user_id = ?", (now.isoformat(), user_id))
        await db.commit()

# --- ХЕНДЛЕРЫ ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    bot_info = await bot.get_me()
    now_str = datetime.now().isoformat()
    args = command.args
    
    async with aiosqlite.connect(DB_NAME) as db:
        row = await (await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))).fetchone()
        
        if not row:
            referrer = None
            if args:
                if args.isdigit() and int(args) != user_id:
                    referrer = args
                    await db.execute("UPDATE users SET bonus_requests = bonus_requests + 1 WHERE user_id = ?", (int(referrer),))
                    try: await bot.send_message(int(referrer), "🎁 Друг зашел по ссылке! Вам начислен +1 запрос.")
                    except: pass
                else:
                    referrer = args
                    await db.execute("INSERT OR IGNORE INTO campaigns (campaign_code, clicks) VALUES (?, 0)", (referrer,))
                    await db.execute("UPDATE campaigns SET clicks = clicks + 1 WHERE campaign_code = ?", (referrer,))

            await db.execute("INSERT INTO users (user_id, referrer_id, joined_at, is_unlimited, daily_usage) VALUES (?, ?, ?, 0, 0)", 
                             (user_id, referrer, now_str))
            await db.commit()
    
    await update_streak(user_id)

    try:
        await message.answer_photo(
            photo=FORTUNE_TELLER_IMG, 
            caption=f"Приветствую... ✨ Твоя судьба в твоих руках.\n\nВыбери сферу для гадания:", 
            reply_markup=get_main_keyboard(bot_info.username),
            parse_mode="Markdown"
        )
    except Exception as e:
        await message.answer(f"Приветствую... ✨\nУ тебя есть **{DAILY_FREE_LIMIT} бесплатных расклада** каждые 24 часа.\nВыбери сферу:", reply_markup=get_main_keyboard(bot_info.username))

# --- ЗАЯВКИ ---
@dp.chat_join_request()
async def on_join_request(request: ChatJoinRequest):
    user_id = request.from_user.id
    channel_id = str(request.chat.id)
    
    async with aiosqlite.connect(DB_NAME) as db:
        row = await (await db.execute("SELECT title FROM channels WHERE channel_id = ?", (channel_id,))).fetchone()
        
        if row:
            try: await request.approve()
            except: pass
            
            reward_check = await (await db.execute("SELECT channel_id FROM user_rewards WHERE user_id = ? AND channel_id = ?", (user_id, channel_id))).fetchone()
            
            if not reward_check:
                await db.execute("INSERT INTO user_rewards (user_id, channel_id) VALUES (?, ?)", (user_id, channel_id))
                await db.execute("UPDATE users SET bonus_requests = bonus_requests + 1 WHERE user_id = ?", (user_id,))
                await db.commit()
                try: await bot.send_message(user_id, f"✅ Вы приняты в канал **{row[0]}**!\n🎁 Вам начислен **+1 расклад**.")
                except: pass

@dp.message(F.new_chat_members)
async def on_user_join(message: types.Message):
    bot_obj = await bot.get_me()
    for member in message.new_chat_members:
        if member.id == bot_obj.id:
            try:
                await message.answer_photo(
                    photo=FORTUNE_TELLER_IMG,
                    caption="🔮 **Приветствую путников!**\nДобавьте меня в контакты или нажмите кнопку ниже.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔮 Погадать лично", url=f"https://t.me/{bot_obj.username}?start=group")]])
                )
            except: pass

@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    bot_info = await bot.get_me()
    try: await callback.message.delete()
    except: pass
    
    await callback.message.answer_photo(
        photo=FORTUNE_TELLER_IMG,
        caption="Главное меню. Выберите сферу:", 
        reply_markup=get_main_keyboard(bot_info.username)
    )
    await callback.answer()

# --- ПРОФИЛЬ ---

@dp.callback_query(F.data == "show_profile")
async def show_profile(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    bot_info = await bot.get_me()
    
    async with aiosqlite.connect(DB_NAME) as db:
        row = await (await db.execute("SELECT is_unlimited, unlimited_until, bonus_requests, streak_days, daily_usage FROM users WHERE user_id = ?", (user_id,))).fetchone()
        
    if not row:
        await callback.answer("Ошибка профиля")
        return
        
    is_unlim, unlim_until, bonus, streak, usage = row
    
    status = "Обычный"
    if is_unlim == 1:
        status = "♾ Вечный Безлимит"
    elif unlim_until:
        until_dt = datetime.fromisoformat(unlim_until)
        if datetime.now() < until_dt:
            status = f"♾ До {until_dt.strftime('%d.%m %H:%M')}"
    
    free_left = max(0, DAILY_FREE_LIMIT - usage)
    
    text = (
        f"👤 **Мой профиль**\n\n"
        f"🆔 ID: `{user_id}`\n"
        f"🏅 Статус: **{status}**\n"
        f"🔥 Ударный режим: **{streak or 0} дн.**\n\n"
        f"🎫 Лимиты:\n"
        f"— Бесплатных на сегодня: **{free_left}**\n"
        f"— Дополнительных: **{bonus}**"
    )
    
    try: await callback.message.delete()
    except: pass
    
    await callback.message.answer(text, reply_markup=get_profile_keyboard(bot_info.username), parse_mode="Markdown")
    await callback.answer()

# --- КАРТА ДНЯ ---

@dp.callback_query(F.data == "daily_card")
async def process_daily_card(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    now = datetime.now()
    
    async with aiosqlite.connect(DB_NAME) as db:
        row = await (await db.execute("SELECT last_daily_card_time FROM users WHERE user_id = ?", (user_id,))).fetchone()
        last_daily = row[0] if row else None
        
        if last_daily:
            last_dt = datetime.fromisoformat(last_daily)
            diff = now - last_dt
            if diff < timedelta(hours=24):
                wait_sec = (timedelta(hours=24) - diff).total_seconds()
                await callback.answer(f"⏳ Карта дня будет доступна через {format_time_remaining(wait_sec)}", show_alert=True)
                return

        await db.execute("UPDATE users SET last_daily_card_time = ? WHERE user_id = ?", (now.isoformat(), user_id))
        await db.commit()
    
    try: await callback.message.delete()
    except: pass
    
    # СЛУЧАЙНАЯ ФРАЗА ЗАГРУЗКИ
    loading_text = random.choice(LOADING_PHRASES)
    msg = await callback.message.answer(loading_text)
    await asyncio.sleep(2)
    try: await msg.delete()
    except: pass

    await update_streak(user_id)
    await send_prediction(callback.message, user_id, "general", None, is_extra=False, is_daily=True)
    await callback.answer()

# --- ОБЫЧНОЕ ГАДАНИЕ ---

@dp.callback_query(F.data.startswith("category_") | F.data.startswith("extra_card_"))
async def process_prediction_request(callback: types.CallbackQuery):
    if callback.data.startswith("category_"):
        category = callback.data.split("_")[1]
        is_extra = False
    else:
        category = callback.data.split("extra_card_")[1]
        is_extra = True

    user_id = callback.from_user.id
    check_result, data = await check_access(user_id)
    
    if check_result:
        await use_access(user_id, data)
        await update_streak(user_id)
        
        async with aiosqlite.connect(DB_NAME) as db:
            row = await (await db.execute("SELECT last_card_id FROM users WHERE user_id = ?", (user_id,))).fetchone()
            last_card_id = row[0] if row else None
        
        try: await callback.message.delete()
        except: pass
        
        # СЛУЧАЙНАЯ ФРАЗА ЗАГРУЗКИ
        loading_text = random.choice(LOADING_PHRASES)
        msg = await callback.message.answer(loading_text)
        await asyncio.sleep(1.5)
        try: await msg.delete()
        except: pass
        
        await send_prediction(callback.message, user_id, category, last_card_id, is_extra=is_extra)
        await callback.answer()
    else:
        remaining_seconds = data
        time_text = format_time_remaining(remaining_seconds)
        pay_kb = await get_pay_menu(user_id)
        
        try: await callback.message.delete()
        except: pass

        await callback.message.answer(
            f"⌛ **Энергия карт исчерпана.**\n\nДо следующего бесплатного гадания: **{time_text}**\n\nЧтобы погадать прямо сейчас, выполните задание или купите безлимит:",
            reply_markup=pay_kb,
            parse_mode="Markdown"
        )
        await callback.answer()

# --- СОВМЕСТИМОСТЬ ---

@dp.callback_query(F.data == "compat_start")
async def compat_start_handler(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    check_result, data = await check_access(user_id)
    
    try: await callback.message.delete()
    except: pass

    if not check_result:
        time_text = format_time_remaining(data)
        pay_kb = await get_pay_menu(user_id)
        await callback.message.answer(f"⌛ **Лимит исчерпан.**\nДоступно через: {time_text}", reply_markup=pay_kb)
        await callback.answer()
        return

    await callback.message.answer("💞 **Анализ совместимости**\n\nВведите имя вашего партнера:", reply_markup=get_back_button())
    await state.set_state(CompatStates.waiting_for_partner_name)
    await callback.answer()

@dp.message(CompatStates.waiting_for_partner_name)
async def compat_process_name(message: types.Message, state: FSMContext):
    partner_name = message.text
    user_id = message.from_user.id
    
    check_result, data = await check_access(user_id)
    if not check_result:
        await message.answer("У вас закончились попытки.", reply_markup=await get_pay_menu(user_id))
        await state.clear()
        return
        
    await use_access(user_id, data)
    await update_streak(user_id)
    
    # СЛУЧАЙНАЯ ФРАЗА ЗАГРУЗКИ
    loading_text = random.choice(LOADING_PHRASES)
    msg = await message.answer(loading_text)
    await asyncio.sleep(2)
    try: await msg.delete()
    except: pass
    
    random.seed(user_id + len(partner_name)) 
    compat_percent = random.randint(40, 99)
    random.seed() 
    
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        cards = json.load(f)
    
    card = random.choice(cards)
    desc = card["predictions"].get("love", card["predictions"]["general"])
    
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO history (user_id, card_id, card_name, category, date) VALUES (?, ?, ?, ?, ?)", 
            (user_id, card['id'], card['name'], "Совместимость", now)
        )
        await db.commit()

    text = f"💞 **Совместимость с {partner_name}: {compat_percent}%**\n\n🃏 **Карта отношений: {card['name']}**\n\n{desc}"
    
    try:
        await message.answer_photo(
            photo=card["image_url"],
            caption=text,
            reply_markup=get_post_prediction_keyboard("love", show_extra_btn=False) 
        )
    except Exception as e:
        await message.answer_photo(
            photo=FORTUNE_TELLER_IMG,
            caption=text + "\n\n⚠️ _(Образ карты скрыт)_",
            parse_mode="Markdown",
            reply_markup=get_post_prediction_keyboard("love", show_extra_btn=False)
        )
    
    await state.clear()

# --- ИСТОРИЯ ---
@dp.callback_query(F.data == "show_history")
async def show_history_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, card_name, date, card_id FROM history WHERE user_id = ? ORDER BY id DESC LIMIT 8", (user_id,)) as cursor:
            rows = await cursor.fetchall()
            
    if not rows:
        await callback.answer("История пуста...", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    for row in rows:
        dt = datetime.fromisoformat(row[2]).strftime('%d.%m')
        card_id = row[3] if row[3] else "unknown"
        builder.button(text=f"{row[1]} ({dt})", callback_data=f"hist_view_{card_id}")
    builder.button(text="🔙 В профиль", callback_data="show_profile")
    builder.adjust(1) 
    
    try: await callback.message.delete()
    except: pass
    await callback.message.answer("📖 **Ваша история:**", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("hist_view_"))
async def history_view_card(callback: types.CallbackQuery):
    card_id = callback.data.split("hist_view_")[1]
    if card_id == "unknown":
        await callback.answer("Запись устарела.", show_alert=True)
        return
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        cards = json.load(f)
    found_card = next((c for c in cards if c["id"] == card_id), None)
    if not found_card:
        await callback.answer("Карта не найдена.", show_alert=True)
        return
    
    desc = found_card["predictions"]["general"]
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 К списку", callback_data="show_history")
    builder.button(text="🏠 В меню", callback_data="back_to_menu")
    builder.adjust(1)
    
    try: await callback.message.delete()
    except: pass
    
    try:
        await callback.message.answer_photo(photo=found_card["image_url"], caption=f"🕰 **Из истории:**\n\n🔮 **{found_card['name']}**\n\n{desc}", reply_markup=builder.as_markup())
    except:
        await callback.message.answer(f"🕰 **Из истории:**\n\n🔮 **{found_card['name']}**\n\n{desc}", reply_markup=builder.as_markup())
    await callback.answer()

# --- ОТПРАВКА ПРЕДСКАЗАНИЯ ---

async def send_prediction(message_obj, user_id, category, last_card_id, is_extra=False, is_daily=False):
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            cards = json.load(f)
    except Exception as e:
        await message_obj.answer("Ошибка базы данных карт.")
        return
    
    available = [c for c in cards if c['id'] != last_card_id]
    if not available: available = cards
    
    card = random.choice(available)
    desc = card["predictions"].get(category, card["predictions"]["general"])
    
    now = datetime.now().isoformat()
    cat_name = "Карта дня" if is_daily else category

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET last_card_id = ? WHERE user_id = ?", (card['id'], user_id))
        await db.execute("INSERT INTO history (user_id, card_id, card_name, category, date) VALUES (?, ?, ?, ?, ?)", (user_id, card['id'], card['name'], cat_name, now))
        await db.commit()

    prefix = ""
    if is_extra: prefix = "🃏 **Дополнительная карта:**\n"
    if is_daily: prefix = "🌞 **Карта Дня:**\n"

    caption_text = f"{prefix}🔮 **{card['name']}**\n\n{desc}"
    
    show_extra_btn = (not is_extra) and (not is_daily)

    try:
        await message_obj.answer_photo(
            photo=card["image_url"],
            caption=caption_text,
            parse_mode="Markdown",
            reply_markup=get_post_prediction_keyboard(category, show_extra_btn=show_extra_btn)
        )
    except Exception:
        await message_obj.answer_photo(
            photo=FORTUNE_TELLER_IMG,
            caption=caption_text + "\n\n⚠️ _(Образ карты скрыт)_",
            parse_mode="Markdown",
            reply_markup=get_post_prediction_keyboard(category, show_extra_btn=show_extra_btn)
        )

# --- ПРОВЕРКА ПОДПИСОК ---

@dp.callback_query(F.data == "check_all_subs")
async def check_all_subs(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    rewards_count = 0
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT channel_id, title, type FROM channels") as cursor:
            all_channels = await cursor.fetchall()
        async with db.execute("SELECT channel_id FROM user_rewards WHERE user_id = ?", (user_id,)) as cursor:
            claimed_rows = await cursor.fetchall()
            claimed_ids = [row[0] for row in claimed_rows]
            
        for ch_id, title, ch_type in all_channels:
            if ch_id in claimed_ids: continue 
            is_completed = False
            if ch_type == 'bot':
                is_completed = True
            else:
                try:
                    member = await bot.get_chat_member(chat_id=ch_id, user_id=user_id)
                    if member.status in ["member", "administrator", "creator"]:
                        is_completed = True
                except Exception: pass

            if is_completed:
                await db.execute("INSERT INTO user_rewards (user_id, channel_id) VALUES (?, ?)", (user_id, ch_id))
                rewards_count += 1

        if rewards_count > 0:
            await db.execute("UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?", (rewards_count, user_id))
            await db.commit()
            new_kb = await get_pay_menu(user_id)
            try: await callback.message.delete()
            except: pass
            await callback.message.answer(f"✅ Успешно! Начислено **+{rewards_count}** раскладов.", reply_markup=new_kb)
        else:
            await callback.answer("❌ Подписки не найдены.", show_alert=True)

# --- РЕФЕРАЛКА ---

@dp.callback_query(F.data == "referral_info")
async def ref_info(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={user_id}"
    
    async with aiosqlite.connect(DB_NAME) as db:
        row = await (await db.execute("SELECT COUNT(*) FROM users WHERE referrer_id = ?", (str(user_id),))).fetchone()
        count = row[0] if row else 0

    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 В профиль", callback_data="show_profile")
    
    try: await callback.message.delete()
    except: pass

    await callback.message.answer(
        f"👥 **Партнерская программа**\n\nВы пригласили друзей: **{count}**\nЗа каждого друга вы получаете **+1 запрос**.\n\nВаша ссылка:\n`{ref_link}`",
        parse_mode="Markdown",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

# --- ОПЛАТА ---
@dp.callback_query(F.data == "buy_stars")
async def buy_stars(callback: types.CallbackQuery):
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="1 Предсказание Таро",
        description="Купить один запрос вне очереди",
        payload="pay_1_req",
        provider_token="", 
        currency="XTR",
        prices=[LabeledPrice(label="1 Расклад", amount=5)]
    )
    await callback.answer()

@dp.callback_query(F.data == "buy_unlimited_24h")
async def buy_unlimited_24h(callback: types.CallbackQuery):
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="♾ Безлимит на 24 часа",
        description="Полный доступ ко всем гаданиям на сутки",
        payload="pay_unlim_24h",
        provider_token="", 
        currency="XTR",
        prices=[LabeledPrice(label="Безлимит 24ч", amount=100)]
    )
    await callback.answer()

@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp.message(F.successful_payment)
async def success_payment_handler(message: types.Message):
    payload = message.successful_payment.invoice_payload
    user_id = message.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        if payload == "pay_1_req":
            await db.execute("UPDATE users SET bonus_requests = bonus_requests + 1 WHERE user_id = ?", (user_id,))
            msg_text = "✨ Оплата прошла! Вам начислен +1 запрос."
        elif payload == "pay_unlim_24h":
            new_until = datetime.now() + timedelta(hours=24)
            await db.execute("UPDATE users SET unlimited_until = ? WHERE user_id = ?", (new_until.isoformat(), user_id))
            msg_text = "✨ Оплата прошла! Безлимит активирован на 24 часа. ♾"
            
        await db.commit()
    await message.answer(msg_text, reply_markup=get_back_button())

# --- АДМИН ПАНЕЛЬ ---
# (Код админ панели остался без изменений, но для корректности включен)

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return 
    await message.answer("🛠 **Панель Администратора**", reply_markup=get_admin_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_menu")
async def admin_menu_back(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("🛠 **Панель Администратора**", reply_markup=get_admin_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_close")
async def admin_close(callback: types.CallbackQuery):
    await callback.message.delete()

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    today = date.today().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        total_users = await (await db.execute("SELECT COUNT(*) FROM users")).fetchone()
        new_users_today = await (await db.execute("SELECT COUNT(*) FROM users WHERE joined_at LIKE ?", (f"{today}%",))).fetchone()
        top_refs_cursor = await db.execute("SELECT referrer_id, COUNT(*) as c FROM users WHERE referrer_id GLOB '[0-9]*' GROUP BY referrer_id ORDER BY c DESC LIMIT 3")
        top_refs = await top_refs_cursor.fetchall()
        campaign_cursor = await db.execute("SELECT campaign_code, clicks FROM campaigns ORDER BY clicks DESC")
        campaigns = await campaign_cursor.fetchall()

    stats_text = (f"📊 **Статистика:**\n👥 Всего: {total_users[0]}\n🆕 Сегодня: {new_users_today[0]}\n\n🥇 **Топ рефоводов:**\n")
    for ref_id, count in top_refs: stats_text += f"ID {ref_id}: {count} чел.\n"
    stats_text += "\n📢 **Реклама:**\n"
    if campaigns:
        for code, clicks in campaigns: stats_text += f"🏷 {code}: {clicks}\n"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Назад", callback_data="admin_menu")
    await callback.message.edit_text(stats_text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_create_link")
async def admin_create_link_start(callback: types.CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Отмена", callback_data="admin_menu")
    await callback.message.edit_text("🔗 Введите название кампании:", reply_markup=builder.as_markup())
    await state.set_state(AdminStates.waiting_for_campaign_name)

@dp.message(AdminStates.waiting_for_campaign_name)
async def admin_create_link_finish(message: types.Message, state: FSMContext):
    code = message.text.strip()
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={code}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO campaigns (campaign_code, clicks) VALUES (?, 0)", (code,))
        await db.commit()
    await message.answer(f"✅ Ссылка: `{link}`", parse_mode="Markdown")
    await state.clear()
    await message.answer("🛠 Админ-панель", reply_markup=get_admin_keyboard())

@dp.callback_query(F.data == "admin_channels")
async def admin_channels(callback: types.CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, channel_id, title, url, position FROM channels ORDER BY position ASC") as cursor:
            channels = await cursor.fetchall()
    await callback.message.edit_text("📺 **Каналы спонсоров:**", reply_markup=get_channels_manage_keyboard(channels))

@dp.callback_query(F.data.startswith("admin_del_ch_"))
async def admin_del_channel(callback: types.CallbackQuery):
    ch_db_id = int(callback.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM channels WHERE id = ?", (ch_db_id,))
        await db.commit()
    await admin_channels(callback)

@dp.callback_query(F.data.startswith("admin_mov_"))
async def admin_move_channel(callback: types.CallbackQuery):
    action = callback.data.split("_")[2]
    ch_db_id = int(callback.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        rows = await (await db.execute("SELECT id, position FROM channels ORDER BY position ASC")).fetchall()
        current_idx = -1
        for i, row in enumerate(rows):
            if row[0] == ch_db_id:
                current_idx = i
                break
        if current_idx == -1: return
        swap_idx = -1
        if action == 'up' and current_idx > 0: swap_idx = current_idx - 1
        elif action == 'dw' and current_idx < len(rows) - 1: swap_idx = current_idx + 1
        if swap_idx != -1:
            id1, pos1 = rows[current_idx]
            id2, pos2 = rows[swap_idx]
            await db.execute("UPDATE channels SET position = ? WHERE id = ?", (pos2, id1))
            await db.execute("UPDATE channels SET position = ? WHERE id = ?", (pos1, id2))
            await db.commit()
    await admin_channels(callback)

@dp.callback_query(F.data == "admin_add_channel_start")
async def admin_add_channel_start(callback: types.CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="📢 Канал (с проверкой)", callback_data="type_channel")
    builder.button(text="🤖 Бот/Сайт (клик = подписка)", callback_data="type_bot")
    builder.button(text="🔙 Отмена", callback_data="admin_menu")
    builder.adjust(1)
    await callback.message.edit_text("Тип спонсора:", reply_markup=builder.as_markup())
    await state.set_state(AdminStates.waiting_for_channel_type)

@dp.callback_query(AdminStates.waiting_for_channel_type)
async def admin_set_channel_type(callback: types.CallbackQuery, state: FSMContext):
    ch_type = callback.data.split("_")[1]
    await state.update_data(channel_type=ch_type)
    msg = "ID канала:" if ch_type == 'channel' else "ID бота (число или 0):"
    await callback.message.edit_text(msg, reply_markup=None)
    await state.set_state(AdminStates.waiting_for_channel_id)

@dp.message(AdminStates.waiting_for_channel_id)
async def admin_add_channel_id(message: types.Message, state: FSMContext):
    await state.update_data(channel_id=message.text)
    await message.answer("Ссылка:")
    await state.set_state(AdminStates.waiting_for_channel_url)

@dp.message(AdminStates.waiting_for_channel_url)
async def admin_add_channel_url(message: types.Message, state: FSMContext):
    await state.update_data(channel_url=message.text)
    await message.answer("Название:")
    await state.set_state(AdminStates.waiting_for_channel_name)

@dp.message(AdminStates.waiting_for_channel_name)
async def admin_add_channel_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    async with aiosqlite.connect(DB_NAME) as db:
        max_pos_row = await (await db.execute("SELECT MAX(position) FROM channels")).fetchone()
        new_pos = (max_pos_row[0] or 0) + 1
        try:
            await db.execute("INSERT INTO channels (channel_id, title, url, type, position) VALUES (?, ?, ?, ?, ?)", 
                             (data['channel_id'], message.text, data['channel_url'], data['channel_type'], new_pos))
            await db.commit()
            await message.answer("✅ Добавлено!")
        except Exception as e: await message.answer(f"❌ Ошибка: {e}")
    await state.clear()
    await message.answer("🛠 Админ-панель", reply_markup=get_admin_keyboard())

@dp.callback_query(F.data == "admin_unlimited")
async def admin_unlimited_start(callback: types.CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Отмена", callback_data="admin_menu")
    await callback.message.edit_text("♾ ID пользователя:", reply_markup=builder.as_markup())
    await state.set_state(AdminStates.waiting_for_unlimited_user_id)

@dp.message(AdminStates.waiting_for_unlimited_user_id)
async def admin_unlimited_finish(message: types.Message, state: FSMContext):
    if not message.text.isdigit(): return
    target = int(message.text)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET is_unlimited = 1 WHERE user_id = ?", (target,))
        await db.commit()
    await message.answer("✅ Безлимит выдан.")
    try: await bot.send_message(target, "🎁 **Вам выдан вечный безлимитный доступ!**")
    except: pass
    await state.clear()
    await message.answer("🛠 Админ-панель", reply_markup=get_admin_keyboard())

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Отмена", callback_data="admin_menu")
    await callback.message.edit_text("📢 Текст/фото для рассылки:", reply_markup=builder.as_markup())
    await state.set_state(AdminStates.waiting_for_broadcast)

@dp.message(AdminStates.waiting_for_broadcast)
async def admin_perform_broadcast(message: types.Message, state: FSMContext):
    await message.answer("⏳ Рассылка...")
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            users = await cursor.fetchall()
    count = 0
    for row in users:
        try:
            await message.send_copy(chat_id=row[0])
            count += 1
            await asyncio.sleep(0.05)
        except: pass
    await message.answer(f"✅ Отправлено: {count}")
    await state.clear()
    await message.answer("🛠 Админ-панель", reply_markup=get_admin_keyboard())

async def main():
    await init_db()
    print("Бот запущен v8.0 (Daily Card + Profile + 24h Pass + Loading Effect)...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())