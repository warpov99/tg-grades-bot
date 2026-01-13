import os
from dotenv import load_dotenv
import asyncio
import sqlite3
import logging
import random
from datetime import datetime

load_dotenv()

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ========= НАСТРОЙКИ =========
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise SystemExit("TOKEN не найден. Создай .env и добавь TOKEN=...")

raw_admins = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = {int(x.strip()) for x in raw_admins.split(",") if x.strip().isdigit()}
print("ADMIN_IDS =", ADMIN_IDS)


# Код доступа (авторизация) для регистрации. Если не нужен — оставь пустую строку ""
REQUEST_COOLDOWN_SEC = 600  # 10 минут
DB_NAME = "grades.db"

# ========= ЛОГИ =========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("grades-bot")

# ========= КНОПКИ =========
BTN_ADD = "➕ Добавить оценку"
BTN_CAB = "📊 Личный кабинет"
BTN_TOP = "🏆 Лидерборд"
BTN_HELP = "ℹ️ Помощь"
BTN_CANCEL = "❌ Отмена"
BTN_GET_CODE = "📩 Запросить доступ"

BTN_DEL_ONE = "🗑 Удалить одну оценку"
BTN_DEL_ALL = "🧹 Удалить все оценки"

BTN_ADMIN = "🛠 Админка"
BTN_ADM_DEL = "🗑 Удалить пользователя"
BTN_ADM_LIST = "👥 Список пользователей"
BTN_ADM_BACK = "⬅ Назад"
BTN_ADM_DEMO = "🤖 Добавить демо-пользователей"
BTN_ADM_ADD_GRADE = "➕ Оценка пользователю"
BTN_ADM_DEL_GRADE = "🗑 Удалить оценку пользователю"
BTN_ADM_CLEAR_GRADES = "🧹 Очистить оценки пользователю"

BTN_NEW_SUBJ = "➕ Новый предмет"
BTN_ADD_SAME = "✅ Ещё по этому предмету"
BTN_OTHER_SUBJ = "📚 Другой предмет"
BTN_TO_MENU = "🏠 В меню"

# Кнопки для быстрых оценок
BTN_G2 = "2"
BTN_G3 = "3"
BTN_G4 = "4"
BTN_G5 = "5"
BTN_GOTHER = "✍️ Другая"


def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS


def main_kb(tg_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=BTN_ADD), KeyboardButton(text=BTN_CAB)],
        [KeyboardButton(text=BTN_TOP), KeyboardButton(text=BTN_DEL_ONE)],
        [KeyboardButton(text=BTN_DEL_ALL), KeyboardButton(text=BTN_HELP)],
    ]
    if is_admin(tg_id):
        rows.append([KeyboardButton(text=BTN_ADMIN)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True
    )


def unauth_kb() -> ReplyKeyboardMarkup:
    # Клавиатура для неавторизованного пользователя: запросить код у админа или отмена
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_GET_CODE)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True
    )


def admin_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ADM_LIST)],
            [KeyboardButton(text=BTN_ADM_DEL), KeyboardButton(text=BTN_ADM_DEMO)],
            [KeyboardButton(text=BTN_ADM_ADD_GRADE)],
            [KeyboardButton(text=BTN_ADM_DEL_GRADE)],
            [KeyboardButton(text=BTN_ADM_CLEAR_GRADES)],
            [KeyboardButton(text=BTN_ADM_BACK)],
        ],
        resize_keyboard=True
    )


def subject_kb(subjects: list[str]) -> ReplyKeyboardMarkup:
    keyboard = []
    row = []
    for s in subjects:
        row.append(KeyboardButton(text=s))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([KeyboardButton(text=BTN_NEW_SUBJ), KeyboardButton(text=BTN_CANCEL)])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def grade_pick_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_G2), KeyboardButton(text=BTN_G3)],
            [KeyboardButton(text=BTN_G4), KeyboardButton(text=BTN_G5)],
            [KeyboardButton(text=BTN_GOTHER), KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True
    )


def after_add_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ADD_SAME), KeyboardButton(text=BTN_OTHER_SUBJ)],
            [KeyboardButton(text=BTN_TO_MENU)],
        ],
        resize_keyboard=True
    )


from typing import Optional


def parse_grade(text: str) -> Optional[float]:
    """
    Принимаем:
    - 4
    - 4.35
    - 4,35
    Ограничение: 2.0..5.0
    """
    if not text:
        return None
    t = text.strip().replace(",", ".")
    if t in (BTN_G2, BTN_G3, BTN_G4, BTN_G5):
        return float(t)
    try:
        g = float(t)
    except ValueError:
        return None
    if g < 2.0 or g > 5.0:
        return None
    return g


def fmt_grade(g) -> str:
    if g is None:
        return "—"
    try:
        x = float(g)
    except Exception:
        return str(g)
    # красиво: 4 -> "4", 4.3 -> "4.30" (2 знака)
    if abs(x - round(x)) < 1e-9:
        return str(int(round(x)))
    return f"{x:.2f}"


# ========= DB =========
def db_connect():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def db_init():
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        tg_id INTEGER PRIMARY KEY,
        full_name TEXT NOT NULL,
        is_verified INTEGER NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS subjects (
        name TEXT PRIMARY KEY
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS grades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_id INTEGER NOT NULL,
        subject TEXT NOT NULL,
        grade REAL NOT NULL CHECK(grade >= 2.0 AND grade <= 5.0),
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(tg_id) REFERENCES users(tg_id),
        FOREIGN KEY(subject) REFERENCES subjects(name)
    )
    """)


    # Миграции (если база уже существовала)
    try:
        cur.execute("ALTER TABLE users ADD COLUMN is_verified INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_achievements (
        tg_id INTEGER NOT NULL,
        code TEXT NOT NULL,
        unlocked_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (tg_id, code),
        FOREIGN KEY(tg_id) REFERENCES users(tg_id)
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS access_requests (
        tg_id INTEGER PRIMARY KEY,
        full_name TEXT NOT NULL,
        username TEXT,
        status TEXT NOT NULL,
        requested_at TEXT DEFAULT CURRENT_TIMESTAMP,
        last_request_at TEXT DEFAULT CURRENT_TIMESTAMP,
        handled_by INTEGER,
        handled_at TEXT
    )
    """)

    conn.commit()
    conn.close()

    seed_default_subjects()


def seed_default_subjects():
    defaults = ["Русский", "Математика", "История", "Английский", "Информатика"]
    conn = db_connect()
    cur = conn.cursor()
    for s in defaults:
        cur.execute("INSERT OR IGNORE INTO subjects(name) VALUES(?)", (s,))
    conn.commit()
    conn.close()


def seed_demo_data_force():
    """
    Админ-команда: удаляет старые демо (tg_id < 0) и создаёт 5 демо-пользователей с оценками.
    Демо-пользователи — это просто записи в базе, не телеграм-аккаунты.
    """
    first = ["Артём", "Илья", "Даниил", "Максим", "Кирилл", "Егор", "Никита", "Михаил", "Алексей", "Иван",
             "София", "Анна", "Мария", "Екатерина", "Виктория", "Полина", "Алиса", "Дарья", "Ксения", "Елена"]
    last = ["Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов", "Попов", "Васильев", "Соколов", "Морозов", "Новиков",
            "Фёдоров", "Михайлов", "Алексеев", "Орлов", "Макаров", "Зайцев", "Павлов", "Семёнов", "Волков", "Громов"]
    conn = db_connect()
    cur = conn.cursor()

    # удалить старых демо
    cur.execute("DELETE FROM grades WHERE tg_id < 0")
    cur.execute("DELETE FROM users WHERE tg_id < 0")

    subjects = get_subjects()
    if not subjects:
        subjects = ["Русский", "Математика"]

    used = set()
    demo_users = []
    for i in range(1, 6):
        for _ in range(100):
            name = f"{random.choice(first)} {random.choice(last)}"
            if name not in used:
                used.add(name)
                demo_users.append((-i, name))
                break

    for tg_id, name in demo_users:
        cur.execute("INSERT INTO users(tg_id, full_name) VALUES(?, ?)", (tg_id, name))

    # каждому демо — случайные оценки
    for tg_id, _name in demo_users:
        for _ in range(random.randint(8, 14)):
            subj = random.choice(subjects)
            # случайная оценка с дробной частью
            g = round(random.uniform(2.0, 5.0), 2)
            cur.execute("INSERT OR IGNORE INTO subjects(name) VALUES(?)", (subj,))
            cur.execute("INSERT INTO grades(tg_id, subject, grade) VALUES(?, ?, ?)", (tg_id, subj, g))

    conn.commit()
    conn.close()


def get_user(tg_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return row


def upsert_user(tg_id: int, full_name: str):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO users(tg_id, full_name)
        VALUES(?, ?)
        ON CONFLICT(tg_id) DO UPDATE SET full_name=excluded.full_name
    """, (tg_id, full_name))
    conn.commit()
    conn.close()

def set_user_verified(tg_id: int, verified: int = 1):
    conn = db_connect()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET is_verified=? WHERE tg_id=?", (verified, tg_id))
        conn.commit()
    finally:
        conn.close()


def parse_sqlite_ts(value: str):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None


def get_access_request(tg_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM access_requests WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return row


def upsert_access_request_pending(tg_id: int, full_name: str, username: str | None):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO access_requests(tg_id, full_name, username, status, requested_at, last_request_at)
        VALUES(?, ?, ?, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(tg_id) DO UPDATE SET
            full_name=excluded.full_name,
            username=excluded.username,
            status='pending',
            requested_at=CURRENT_TIMESTAMP,
            last_request_at=CURRENT_TIMESTAMP
    """, (tg_id, full_name, username))
    conn.commit()
    conn.close()


def set_access_request_status(tg_id: int, status: str, admin_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE access_requests
        SET status=?, handled_by=?, handled_at=CURRENT_TIMESTAMP
        WHERE tg_id=?
    """, (status, admin_id, tg_id))
    conn.commit()
    conn.close()

def is_user_verified(tg_id: int) -> bool:
    u = get_user(tg_id)
    if not u:
        return False
    try:
        return int(u["is_verified"]) == 1
    except Exception:
        return True

# ====== Достижения ======
ACHIEVEMENTS = {
    "first_grade": ("🥉 Первый тест", "Добавь первую оценку"),
    "ten_tests": ("🥈 10 тестов", "Добавь 10 оценок"),
    "streak3_5": ("🥇 Серия пятёрок", "Получить 3 пятёрки подряд"),
    "avg_45": ("🏅 Отличник", "Достичь общей средней 4.50+ (минимум 5 оценок)"),
}

def unlock_achievement(tg_id: int, code: str) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO user_achievements(tg_id, code) VALUES(?, ?)",
        (tg_id, code)
    )
    conn.commit()
    changed = cur.rowcount == 1
    conn.close()
    return changed

def get_total_count_and_avg(tg_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS cnt, AVG(grade) AS avg FROM grades WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return int(row["cnt"] or 0), (row["avg"] if row else None)

def get_last_grades(tg_id: int, limit: int = 3):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT grade FROM grades WHERE tg_id=? ORDER BY id DESC LIMIT ?", (tg_id, limit))
    rows = cur.fetchall()
    conn.close()
    return [float(r["grade"]) for r in rows]


def list_users(limit: int = 30):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT u.tg_id, u.full_name,
               COUNT(g.id) AS grades_count
        FROM users u
        LEFT JOIN grades g ON g.tg_id = u.tg_id
        GROUP BY u.tg_id
        ORDER BY u.full_name ASC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def delete_user(tg_id: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM users WHERE tg_id=?", (tg_id,))
    exists = cur.fetchone() is not None
    if not exists:
        conn.close()
        return False
    cur.execute("DELETE FROM grades WHERE tg_id=?", (tg_id,))
    cur.execute("DELETE FROM users WHERE tg_id=?", (tg_id,))
    conn.commit()
    conn.close()
    return True


def get_subjects() -> list[str]:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT name FROM subjects ORDER BY name ASC")
    rows = cur.fetchall()
    conn.close()
    return [r["name"] for r in rows]


def add_subject(name: str) -> bool:
    name = name.strip()
    if not name:
        return False
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO subjects(name) VALUES(?)", (name,))
    conn.commit()
    conn.close()
    return True


def add_grade_db(tg_id: int, subject: str, grade: float):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO subjects(name) VALUES(?)", (subject,))
    cur.execute("INSERT INTO grades(tg_id, subject, grade) VALUES(?, ?, ?)", (tg_id, subject, float(grade)))
    conn.commit()
    conn.close()


def get_cabinet_stats(tg_id: int):
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT ROUND(AVG(grade), 2) AS avg_total, COUNT(*) AS cnt_total
        FROM grades
        WHERE tg_id=?
    """, (tg_id,))
    total = cur.fetchone()

    cur.execute("""
        SELECT
            g.subject AS subject,
            ROUND(AVG(g.grade), 2) AS avg_subj,
            COUNT(*) AS cnt,
            MAX(g.grade) AS best_grade,
            (SELECT grade
             FROM grades g2
             WHERE g2.tg_id = g.tg_id AND g2.subject = g.subject
             ORDER BY g2.id DESC
             LIMIT 1) AS last_grade
        FROM grades g
        WHERE g.tg_id=?
        GROUP BY g.subject
        ORDER BY g.subject ASC
    """, (tg_id,))
    by_subject = cur.fetchall()

    conn.close()
    return total, by_subject


def get_top(limit: int = 10):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT u.full_name,
               ROUND(AVG(g.grade), 2) AS avg,
               COUNT(g.id) AS cnt
        FROM users u
        JOIN grades g ON g.tg_id = u.tg_id
        GROUP BY u.tg_id
        ORDER BY avg DESC, cnt DESC, u.full_name ASC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def list_last_grades(tg_id: int, limit: int = 10):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, subject, grade, created_at
        FROM grades
        WHERE tg_id=?
        ORDER BY id DESC
        LIMIT ?
    """, (tg_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows


def delete_grade_by_id(tg_id: int, grade_id: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM grades WHERE id=? AND tg_id=?", (grade_id, tg_id))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def delete_all_grades(tg_id: int) -> int:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM grades WHERE tg_id=?", (tg_id,))
    cnt = cur.rowcount
    conn.commit()
    conn.close()
    return cnt


def delete_grade_for_user(target_id: int, grade_id: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM grades WHERE id=? AND tg_id=?", (grade_id, target_id))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def delete_all_grades_for_user(target_id: int) -> int:
    return delete_all_grades(target_id)


# ========= FSM =========
class Reg(StatesGroup):
    full_name = State()


class AddGrade(StatesGroup):
    subject_choice = State()
    new_subject = State()
    grade_pick = State()
    grade_input = State()
    after = State()


class Admin(StatesGroup):
    del_wait_id = State()

    add_grade_wait_user_id = State()
    add_grade_subject_choice = State()
    add_grade_new_subject = State()
    add_grade_pick = State()
    add_grade_input = State()

    del_grade_wait_user_id = State()
    del_grade_wait_grade_id = State()

    clear_grades_wait_user_id = State()
    clear_grades_confirm = State()


class UserDelete(StatesGroup):
    del_one_wait_id = State()
    del_all_confirm = State()


# ========= BOT =========
async def main():
    if TOKEN == "PASTE_YOUR_TOKEN_HERE":
        raise SystemExit("Вставь токен в переменную TOKEN в начале файла.")

    db_init()

    bot = Bot(TOKEN)
    dp = Dispatcher()

    me = await bot.get_me()
    log.info("✅ Bot started as @%s (id=%s)", me.username, me.id)
    # сообщение админу о запуске
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"✅ Бот запущен: @{me.username}")
        except Exception as e:
            log.warning("Не смог отправить стартовое сообщение админу %s: %s", admin_id, e)

    # --- Отмена в любом месте
    @dp.message(F.text == BTN_CANCEL)
    async def cancel(m: Message, state: FSMContext):
        await state.clear()
        await m.answer("Ок, отменил. Выбирай действие 👇", reply_markup=main_kb(m.from_user.id))

    # --- HELP
    @dp.message(Command("help"))
    @dp.message(F.text == BTN_HELP)
    async def help_cmd(m: Message):
        txt = (
            "Доступно:\n"
            f"• {BTN_ADD} — добавить оценку\n"
            f"• {BTN_CAB} — личный кабинет (средняя + по предметам)\n"
            f"• {BTN_TOP} — лидерборд (общая средняя)\n"
            f"• {BTN_DEL_ONE} — удалить одну свою оценку\n"
            f"• {BTN_DEL_ALL} — удалить все свои оценки\n\n"
            "Если не зарегистрирован — /start\n"
        )
        if is_admin(m.from_user.id):
            txt += "\nАдмин:\n• 🛠 Админка — управление пользователями/демо/оценки"
        await m.answer(txt, reply_markup=main_kb(m.from_user.id))

    # --- START / регистрация
    @dp.message(Command("start"))
    async def start(m: Message, state: FSMContext):
        await state.clear()
        user = get_user(m.from_user.id)

        # Уже авторизован
        if user and is_user_verified(m.from_user.id):
            log.info("start: verified user tg_id=%s name=%s", m.from_user.id, user["full_name"])
            await m.answer(
                f"Привет, {user['full_name']}! 👇",
                reply_markup=main_kb(m.from_user.id)
            )
            return

        # Есть в базе, но не авторизован -> только заявка
        if user and not is_user_verified(m.from_user.id):
            log.info("start: not verified tg_id=%s name=%s", m.from_user.id, user["full_name"])
            await m.answer(
                "🔒 Доступ к функциям бота пока не выдан.\n"
                "Нажми «📩 Запросить доступ», чтобы отправить заявку админу.",
                reply_markup=unauth_kb()
            )
            return

        # Новый пользователь -> регистрация (Имя Фамилия)
        log.info("start: new user tg_id=%s username=@%s", m.from_user.id, m.from_user.username)
        await m.answer(
            "Привет! Зарегистрируйся.\n"
            "Напиши *Имя Фамилия* (пример: Иван Иванов):",
            parse_mode="Markdown",
            reply_markup=cancel_kb()
        )
        await state.set_state(Reg.full_name)
    @dp.message(Reg.full_name)
    async def reg_full_name(m: Message, state: FSMContext):
        full_name = (m.text or "").strip()
        parts = [p for p in full_name.split() if p]
        if len(parts) < 2:
            await m.answer("Нужно *Имя Фамилия* (2 слова). Пример: Иван Иванов", parse_mode="Markdown")
            return

        upsert_user(m.from_user.id, full_name)
        log.info("registered tg_id=%s name=%s", m.from_user.id, full_name)

        await state.clear()
        await m.answer(
            f"✅ Регистрация сохранена: *{full_name}*.\n"
            "Теперь запроси доступ у администратора кнопкой ниже.",
            parse_mode="Markdown",
            reply_markup=unauth_kb()
        )
    @dp.message(F.text == BTN_GET_CODE)
    async def request_access(m: Message):
        """
        Авторизация по заявкам (без кода).
        Пользователь нажимает "Запросить доступ" -> бот отправляет заявку админу.
        Защита от спама: повторная заявка запрещена, если уже pending; после отказа действует кулдаун.
        """
        user_row = get_user(m.from_user.id)
        if not user_row:
            await m.answer("Сначала зарегистрируйся через /start (Имя Фамилия).", reply_markup=cancel_kb())
            return

        # если уже верифицирован — просто показать меню
        if is_user_verified(m.from_user.id):
            await m.answer("✅ Ты уже авторизован.", reply_markup=main_kb(m.from_user.id))
            return

        # анти-спам и кулдаун
        req = get_access_request(m.from_user.id)
        now = datetime.utcnow()

        if req and req["status"] == "pending":
            await m.answer("⏳ Твоя заявка уже отправлена и ожидает решения администратора.", reply_markup=unauth_kb())
            return

        if req and req["status"] == "denied":
            last = req["last_request_at"] or req["handled_at"] or req["requested_at"]
            last_dt = parse_sqlite_ts(last)
            if last_dt:
                seconds = int((now - last_dt).total_seconds())
                if seconds < REQUEST_COOLDOWN_SEC:
                    wait = REQUEST_COOLDOWN_SEC - seconds
                    mins = (wait + 59) // 60
                    await m.answer(f"⛔ Заявка недавно отклонена. Попробуй снова через ~{mins} мин.", reply_markup=unauth_kb())
                    return

        full_name = user_row["full_name"]
        username = f"@{m.from_user.username}" if m.from_user.username else None

        upsert_access_request_pending(m.from_user.id, full_name, username)

        # сообщение админу с кнопками
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Принять", callback_data=f"auth:accept:{m.from_user.id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"auth:deny:{m.from_user.id}")
        ]])

        admin_text = (
            "📩 Заявка на доступ\n"
            f"ID: {m.from_user.id}\n"
            f"Имя (в боте): {full_name}\n"
            f"Username: {username or '(нет username)'}"
        )

        sent_any = False
        for admin_id in ADMIN_IDS:
            try:
                await m.bot.send_message(admin_id, admin_text, reply_markup=kb)
                sent_any = True
            except Exception:
                pass

        if sent_any:
            await m.answer("✅ Заявка отправлена администратору. Ожидай решения.", reply_markup=unauth_kb())
        else:
            await m.answer("⚠️ Не удалось отправить заявку админу. Попробуй позже.", reply_markup=unauth_kb())

    @dp.callback_query(F.data.startswith("auth:"))
    async def auth_decision(q: CallbackQuery):
        # доступ только админу
        if not is_admin(q.from_user.id):
            await q.answer("Нет доступа.", show_alert=True)
            return

        try:
            _, action, tg_id_str = (q.data or "").split(":")
            target_id = int(tg_id_str)
        except Exception:
            await q.answer("Неверные данные.", show_alert=True)
            return

        target_user = get_user(target_id)
        target_name = target_user["full_name"] if target_user else f"tg_id={target_id}"

        admin_username = f"@{q.from_user.username}" if q.from_user.username else f"ID {q.from_user.id}"

        if action == "accept":
            set_user_verified(target_id, 1)
            set_access_request_status(target_id, "approved", q.from_user.id)

            try:
                await q.bot.send_message(
                    target_id,
                    f"✅ Доступ одобрен администратором {admin_username}.\n"
                    "Нажми /start, чтобы открыть меню."
                )
            except Exception:
                pass

            try:
                await q.message.edit_text(
                    (q.message.text or "") + f"\n\n✅ Принято админом {admin_username}",
                    reply_markup=None
                )
            except Exception:
                pass

            await q.answer("Принято.")

        elif action == "deny":
            set_access_request_status(target_id, "denied", q.from_user.id)

            try:
                await q.bot.send_message(
                    target_id,
                    f"❌ Доступ отклонён администратором {admin_username}.\n"
                    f"По вопросам напиши: {admin_username}"
                )
            except Exception:
                pass

            try:
                await q.message.edit_text(
                    (q.message.text or "") + f"\n\n❌ Отклонено админом {admin_username}",
                    reply_markup=None
                )
            except Exception:
                pass

            await q.answer("Отклонено.")
        else:
            await q.answer("Неизвестное действие.", show_alert=True)

    @dp.message(F.text == BTN_CAB)
    async def cabinet(m: Message):
        user = get_user(m.from_user.id)
        if not user:
            await m.answer("Сначала /start", reply_markup=ReplyKeyboardRemove())
            return
        if not is_user_verified(m.from_user.id):
            await m.answer("🔒 Доступ не выдан. Нажми «📩 Запросить доступ» и дождись решения админа.", reply_markup=unauth_kb())
            return

        total, by_subject = get_cabinet_stats(m.from_user.id)
        avg_total = total["avg_total"]
        cnt_total = total["cnt_total"]

        text = f"📊 Личный кабинет\n👤 {user['full_name']}\n\n"
        text += f"Общая средняя: {fmt_grade(avg_total)}\n"
        text += f"Оценок всего: {cnt_total}\n\n"

        if by_subject:
            text += "По предметам:\n"
            for r in by_subject:
                text += f"• {r['subject']}: средн. {fmt_grade(r['avg_subj'])} (оценок {r['cnt']})\n"
        else:
            text += "Пока нет оценок. Добавь через «Добавить оценку»."

        await m.answer(text, reply_markup=main_kb(m.from_user.id))

    # --- Лидерборд
    @dp.message(F.text == BTN_TOP)
    async def top(m: Message):
        user = get_user(m.from_user.id)
        if not user:
            await m.answer("Сначала /start", reply_markup=ReplyKeyboardRemove())
            return
        if not is_user_verified(m.from_user.id):
            await m.answer("🔒 Доступ не выдан. Нажми «📩 Запросить доступ» и дождись решения админа.", reply_markup=unauth_kb())
            return

        rows = get_top(limit=10)
        if not rows:
            await m.answer("Пока нет оценок ни у кого. Добавь первую 🙂", reply_markup=main_kb(m.from_user.id))
            return

        text = "🏆 Лидерборд (общая средняя):\n\n"
        for i, r in enumerate(rows, start=1):
            text += f"{i}) {r['full_name']} — {fmt_grade(r['avg'])} (оценок {r['cnt']})\n"
        await m.answer(text, reply_markup=main_kb(m.from_user.id))

    # --- Добавить оценку (пользователь)
    @dp.message(Command("add"))
    @dp.message(F.text == BTN_ADD)
    async def add(m: Message, state: FSMContext):
        user = get_user(m.from_user.id)
        if not user:
            await m.answer("Сначала /start", reply_markup=ReplyKeyboardRemove())
            return
        if not is_user_verified(m.from_user.id):
            await m.answer("🔒 Доступ не выдан. Нажми «📩 Запросить доступ» и дождись решения админа.", reply_markup=unauth_kb())
            return

        await state.clear()
        subjects = get_subjects()
        await m.answer("Выбери предмет:", reply_markup=subject_kb(subjects))
        await state.set_state(AddGrade.subject_choice)

    @dp.message(AddGrade.subject_choice)
    async def choose_subject(m: Message, state: FSMContext):
        txt = (m.text or "").strip()

        if txt == BTN_NEW_SUBJ:
            await m.answer("Напиши название нового предмета:", reply_markup=cancel_kb())
            await state.set_state(AddGrade.new_subject)
            return

        subjects = set(get_subjects())
        if txt not in subjects:
            await m.answer("Выбери предмет кнопкой или нажми «➕ Новый предмет».")
            return

        await state.update_data(subject=txt)
        await m.answer(f"Предмет: {txt}\nВыбери оценку (или «Другая»):", reply_markup=grade_pick_kb())
        await state.set_state(AddGrade.grade_pick)

    @dp.message(AddGrade.new_subject)
    async def new_subject(m: Message, state: FSMContext):
        name = (m.text or "").strip()
        if len(name) < 2:
            await m.answer("Слишком коротко. Напиши название предмета нормально:")
            return

        add_subject(name)
        await state.update_data(subject=name)
        await m.answer(f"✅ Добавил предмет: {name}\nВыбери оценку (или «Другая»):", reply_markup=grade_pick_kb())
        await state.set_state(AddGrade.grade_pick)

    @dp.message(AddGrade.grade_pick)
    async def grade_pick(m: Message, state: FSMContext):
        txt = (m.text or "").strip()
        if txt == BTN_GOTHER:
            await m.answer("Введи оценку (пример: 4,35 или 3.2). Диапазон 2–5:", reply_markup=cancel_kb())
            await state.set_state(AddGrade.grade_input)
            return

        g = parse_grade(txt)
        if g is None:
            await m.answer("Нажми 2/3/4/5 или «Другая».", reply_markup=grade_pick_kb())
            return

        data = await state.get_data()
        subject = data["subject"]
        add_grade_db(m.from_user.id, subject, g)
        log.info("grade added user=%s subject=%s grade=%s", m.from_user.id, subject, g)

        await state.update_data(last_subject=subject)
        await m.answer(f"✅ Добавлено: {subject} — {fmt_grade(g)}\nЧто дальше?", reply_markup=after_add_kb())
        await state.set_state(AddGrade.after)

    @dp.message(AddGrade.grade_input)
    async def grade_input(m: Message, state: FSMContext):
        g = parse_grade((m.text or "").strip())
        if g is None:
            await m.answer("Не понял. Введи число 2–5, можно с дробью (4,35).")
            return

        data = await state.get_data()
        subject = data["subject"]
        add_grade_db(m.from_user.id, subject, g)
        log.info("grade added user=%s subject=%s grade=%s", m.from_user.id, subject, g)

        await state.update_data(last_subject=subject)
        await m.answer(f"✅ Добавлено: {subject} — {fmt_grade(g)}\nЧто дальше?", reply_markup=after_add_kb())
        await state.set_state(AddGrade.after)

    @dp.message(AddGrade.after)
    async def after_add(m: Message, state: FSMContext):
        txt = (m.text or "").strip()
        data = await state.get_data()
        last_subject = data.get("last_subject")

        if txt == BTN_ADD_SAME and last_subject:
            await state.update_data(subject=last_subject)
            await m.answer(f"Ок, снова {last_subject}. Выбери оценку:", reply_markup=grade_pick_kb())
            await state.set_state(AddGrade.grade_pick)
            return

        if txt == BTN_OTHER_SUBJ:
            subjects = get_subjects()
            await m.answer("Выбери другой предмет:", reply_markup=subject_kb(subjects))
            await state.set_state(AddGrade.subject_choice)
            return

        if txt == BTN_TO_MENU:
            await state.clear()
            await m.answer("Меню 👇", reply_markup=main_kb(m.from_user.id))
            return

        await m.answer("Выбери действие кнопками ниже 👇", reply_markup=after_add_kb())

    # --- Удалить одну оценку (пользователь)
    @dp.message(F.text == BTN_DEL_ONE)
    async def user_del_one_start(m: Message, state: FSMContext):
        user = get_user(m.from_user.id)
        if not user:
            await m.answer("Сначала /start", reply_markup=ReplyKeyboardRemove())
            return
        if not is_user_verified(m.from_user.id):
            await m.answer("🔒 Доступ не выдан. Нажми «📩 Запросить доступ» и дождись решения админа.", reply_markup=unauth_kb())
            return

        rows = list_last_grades(m.from_user.id, limit=10)
        if not rows:
            await m.answer("У тебя пока нет оценок.", reply_markup=main_kb(m.from_user.id))
            return

        text = "🗑 Удаление одной оценки\n\nПоследние оценки (ID):\n"
        for r in rows:
            text += f"ID {r['id']} — {r['subject']}: {fmt_grade(r['grade'])}\n"
        text += "\nНапиши ID оценки, которую удалить:"
        await state.clear()
        await m.answer(text, reply_markup=cancel_kb())
        await state.set_state(UserDelete.del_one_wait_id)

    @dp.message(UserDelete.del_one_wait_id)
    async def user_del_one_do(m: Message, state: FSMContext):
        txt = (m.text or "").strip()
        try:
            grade_id = int(txt)
        except ValueError:
            await m.answer("Нужно число (ID). Попробуй ещё раз:")
            return

        ok = delete_grade_by_id(m.from_user.id, grade_id)
        log.info("user delete one tg_id=%s grade_id=%s ok=%s", m.from_user.id, grade_id, ok)
        await state.clear()
        if ok:
            await m.answer(f"✅ Оценка ID {grade_id} удалена.", reply_markup=main_kb(m.from_user.id))
        else:
            await m.answer("❌ Не нашёл такую оценку (или она не твоя).", reply_markup=main_kb(m.from_user.id))

    # --- Удалить все оценки (пользователь)
    @dp.message(F.text == BTN_DEL_ALL)
    async def user_del_all_start(m: Message, state: FSMContext):
        user = get_user(m.from_user.id)
        if not user:
            await m.answer("Сначала /start", reply_markup=ReplyKeyboardRemove())
            return
        if not is_user_verified(m.from_user.id):
            await m.answer("🔒 Доступ не выдан. Нажми «📩 Запросить доступ» и дождись решения админа.", reply_markup=unauth_kb())
            return
        await state.clear()
        await m.answer("⚠️ Удалить ВСЕ твои оценки?\nНапиши: ДА (или нажми Отмена)", reply_markup=cancel_kb())
        await state.set_state(UserDelete.del_all_confirm)

    @dp.message(UserDelete.del_all_confirm)
    async def user_del_all_do(m: Message, state: FSMContext):
        txt = (m.text or "").strip().upper()
        if txt != "ДА":
            await m.answer("Не удаляю. Если хочешь удалить — напиши: ДА")
            return
        cnt = delete_all_grades(m.from_user.id)
        log.info("user delete all tg_id=%s count=%s", m.from_user.id, cnt)
        await state.clear()
        await m.answer(f"✅ Удалено оценок: {cnt}", reply_markup=main_kb(m.from_user.id))

    # --- Админка
    @dp.message(F.text == BTN_ADMIN)
    async def admin_menu(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await m.answer("Нет доступа.", reply_markup=main_kb(m.from_user.id))
            return
        await state.clear()
        await m.answer("🛠 Админка:", reply_markup=admin_kb())

    @dp.message(F.text == BTN_ADM_BACK)
    async def admin_back(m: Message, state: FSMContext):
        await state.clear()
        await m.answer("Меню 👇", reply_markup=main_kb(m.from_user.id))

    @dp.message(F.text == BTN_ADM_LIST)
    async def admin_list(m: Message):
        if not is_admin(m.from_user.id):
            return
        rows = list_users(limit=30)
        text = "👥 Пользователи (до 30):\n\n"
        for r in rows:
            text += f"{r['full_name']} | id={r['tg_id']} | оценок={r['grades_count']}\n"
        await m.answer(text, reply_markup=admin_kb())

    @dp.message(F.text == BTN_ADM_DEL)
    async def admin_del_start(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            return
        await state.clear()
        await m.answer("Введи TG ID пользователя для удаления:", reply_markup=cancel_kb())
        await state.set_state(Admin.del_wait_id)

    @dp.message(Admin.del_wait_id)
    async def admin_del_do(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await state.clear()
            return

        txt = (m.text or "").strip()
        try:
            target_id = int(txt)
        except ValueError:
            await m.answer("Нужно число (TG ID). Попробуй ещё раз:")
            return

        if target_id in ADMIN_IDS:
            await m.answer("Админа удалять нельзя.", reply_markup=admin_kb())
            await state.clear()
            return

        ok = delete_user(target_id)
        log.info("admin delete user admin=%s target=%s ok=%s", m.from_user.id, target_id, ok)
        await state.clear()
        if ok:
            await m.answer(f"✅ Пользователь id={target_id} удалён (и его оценки тоже).", reply_markup=admin_kb())
        else:
            await m.answer(f"❌ Пользователь id={target_id} не найден.", reply_markup=admin_kb())

    # --- Админ: добавить демо
    @dp.message(F.text == BTN_ADM_DEMO)
    async def admin_demo(m: Message):
        if not is_admin(m.from_user.id):
            return
        seed_demo_data_force()
        log.info("admin demo seed by admin=%s", m.from_user.id)
        await m.answer("✅ Демо-пользователи добавлены (старые демо заменены). Проверь лидерборд.", reply_markup=admin_kb())

    # --- Админ: добавить оценку пользователю
    @dp.message(F.text == BTN_ADM_ADD_GRADE)
    async def admin_add_grade_start(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            return
        await state.clear()
        await m.answer("Введи TG ID пользователя, кому добавить оценку:", reply_markup=cancel_kb())
        await state.set_state(Admin.add_grade_wait_user_id)

    @dp.message(Admin.add_grade_wait_user_id)
    async def admin_add_grade_userid(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await state.clear()
            return
        txt = (m.text or "").strip()
        try:
            target_id = int(txt)
        except ValueError:
            await m.answer("Нужно число (TG ID). Попробуй ещё раз:")
            return

        u = get_user(target_id)
        if not u:
            await m.answer("Такого пользователя нет в базе. Пусть он нажмёт /start и зарегистрируется.")
            return

        await state.update_data(target_id=target_id)
        subjects = get_subjects()
        await m.answer(f"Кому: {u['full_name']} (id={target_id})\nВыбери предмет:", reply_markup=subject_kb(subjects))
        await state.set_state(Admin.add_grade_subject_choice)

    @dp.message(Admin.add_grade_subject_choice)
    async def admin_add_grade_subject(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await state.clear()
            return
        txt = (m.text or "").strip()

        if txt == BTN_NEW_SUBJ:
            await m.answer("Напиши название нового предмета:", reply_markup=cancel_kb())
            await state.set_state(Admin.add_grade_new_subject)
            return

        subjects = set(get_subjects())
        if txt not in subjects:
            await m.answer("Выбери предмет кнопкой или нажми «➕ Новый предмет».")
            return

        await state.update_data(subject=txt)
        await m.answer("Выбери оценку (или «Другая»):", reply_markup=grade_pick_kb())
        await state.set_state(Admin.add_grade_pick)

    @dp.message(Admin.add_grade_new_subject)
    async def admin_add_grade_new_subject(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await state.clear()
            return
        name = (m.text or "").strip()
        if len(name) < 2:
            await m.answer("Слишком коротко. Напиши название предмета нормально:")
            return
        add_subject(name)
        await state.update_data(subject=name)
        await m.answer("Выбери оценку (или «Другая»):", reply_markup=grade_pick_kb())
        await state.set_state(Admin.add_grade_pick)

    @dp.message(Admin.add_grade_pick)
    async def admin_add_grade_pick(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await state.clear()
            return
        txt = (m.text or "").strip()
        if txt == BTN_GOTHER:
            await m.answer("Введи оценку (пример: 4,35). Диапазон 2–5:", reply_markup=cancel_kb())
            await state.set_state(Admin.add_grade_input)
            return

        g = parse_grade(txt)
        if g is None:
            await m.answer("Нажми 2/3/4/5 или «Другая».", reply_markup=grade_pick_kb())
            return

        data = await state.get_data()
        target_id = data["target_id"]
        subject = data["subject"]
        add_grade_db(target_id, subject, g)

        newly = []
        cnt_total, avg_total2 = get_total_count_and_avg(target_id)
        if cnt_total == 1 and unlock_achievement(target_id, "first_grade"):
            newly.append("first_grade")
        if cnt_total >= 10 and unlock_achievement(target_id, "ten_tests"):
            newly.append("ten_tests")
        last3 = get_last_grades(target_id, 3)
        if len(last3) == 3 and all(x >= 5.0 for x in last3) and unlock_achievement(target_id, "streak3_5"):
            newly.append("streak3_5")
        if cnt_total >= 5 and (avg_total2 is not None) and float(avg_total2) >= 4.5 and unlock_achievement(target_id, "avg_45"):
            newly.append("avg_45")

        u = get_user(target_id)
        log.info("admin add grade admin=%s target=%s subject=%s grade=%s", m.from_user.id, target_id, subject, g)

        await state.clear()
        await m.answer(f"✅ Добавлено пользователю {u['full_name']}: {subject} — {fmt_grade(g)}", reply_markup=admin_kb())

    @dp.message(Admin.add_grade_input)
    async def admin_add_grade_input(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await state.clear()
            return
        g = parse_grade((m.text or "").strip())
        if g is None:
            await m.answer("Не понял. Введи число 2–5, можно с дробью (4,35).")
            return

        data = await state.get_data()
        target_id = data["target_id"]
        subject = data["subject"]
        add_grade_db(target_id, subject, g)
        u = get_user(target_id)
        log.info("admin add grade admin=%s target=%s subject=%s grade=%s", m.from_user.id, target_id, subject, g)

        await state.clear()
        await m.answer(f"✅ Добавлено пользователю {u['full_name']}: {subject} — {fmt_grade(g)}", reply_markup=admin_kb())

    # --- Админ: удалить оценку пользователю
    @dp.message(F.text == BTN_ADM_DEL_GRADE)
    async def admin_del_grade_start(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            return
        await state.clear()
        await m.answer("Введи TG ID пользователя (чтобы удалить одну оценку):", reply_markup=cancel_kb())
        await state.set_state(Admin.del_grade_wait_user_id)

    @dp.message(Admin.del_grade_wait_user_id)
    async def admin_del_grade_userid(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await state.clear()
            return
        txt = (m.text or "").strip()
        try:
            target_id = int(txt)
        except ValueError:
            await m.answer("Нужно число (TG ID). Попробуй ещё раз:")
            return

        u = get_user(target_id)
        if not u:
            await m.answer("Пользователь не найден.")
            return

        rows = list_last_grades(target_id, limit=15)
        if not rows:
            await m.answer("У пользователя нет оценок.", reply_markup=admin_kb())
            await state.clear()
            return

        await state.update_data(target_id=target_id)
        text = f"🗑 Удалить оценку пользователю {u['full_name']} (id={target_id})\n\n"
        text += "Последние оценки (ID):\n"
        for r in rows:
            text += f"ID {r['id']} — {r['subject']}: {fmt_grade(r['grade'])}\n"
        text += "\nНапиши ID оценки, которую удалить:"
        await m.answer(text, reply_markup=cancel_kb())
        await state.set_state(Admin.del_grade_wait_grade_id)

    @dp.message(Admin.del_grade_wait_grade_id)
    async def admin_del_grade_do(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await state.clear()
            return

        txt = (m.text or "").strip()
        try:
            grade_id = int(txt)
        except ValueError:
            await m.answer("Нужно число (ID). Попробуй ещё раз:")
            return

        data = await state.get_data()
        target_id = data["target_id"]
        ok = delete_grade_for_user(target_id, grade_id)
        log.info("admin delete grade admin=%s target=%s grade_id=%s ok=%s", m.from_user.id, target_id, grade_id, ok)
        await state.clear()

        if ok:
            await m.answer(f"✅ Удалено: оценка ID {grade_id} у пользователя id={target_id}", reply_markup=admin_kb())
        else:
            await m.answer("❌ Не нашёл такую оценку (или она не принадлежит этому пользователю).", reply_markup=admin_kb())

    # --- Админ: очистить оценки пользователю
    @dp.message(F.text == BTN_ADM_CLEAR_GRADES)
    async def admin_clear_grades_start(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            return
        await state.clear()
        await m.answer("Введи TG ID пользователя (чтобы удалить ВСЕ его оценки):", reply_markup=cancel_kb())
        await state.set_state(Admin.clear_grades_wait_user_id)

    @dp.message(Admin.clear_grades_wait_user_id)
    async def admin_clear_grades_userid(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await state.clear()
            return
        txt = (m.text or "").strip()
        try:
            target_id = int(txt)
        except ValueError:
            await m.answer("Нужно число (TG ID). Попробуй ещё раз:")
            return

        u = get_user(target_id)
        if not u:
            await m.answer("Пользователь не найден.")
            return

        await state.update_data(target_id=target_id)
        await m.answer(f"⚠️ Удалить ВСЕ оценки пользователя {u['full_name']} (id={target_id})?\nНапиши: ДА", reply_markup=cancel_kb())
        await state.set_state(Admin.clear_grades_confirm)

    @dp.message(Admin.clear_grades_confirm)
    async def admin_clear_grades_confirm(m: Message, state: FSMContext):
        if not is_admin(m.from_user.id):
            await state.clear()
            return
        txt = (m.text or "").strip().upper()
        if txt != "ДА":
            await m.answer("Чтобы подтвердить, напиши: ДА (или нажми Отмена)")
            return
        data = await state.get_data()
        target_id = data["target_id"]
        cnt = delete_all_grades_for_user(target_id)
        log.info("admin clear grades admin=%s target=%s count=%s", m.from_user.id, target_id, cnt)
        await state.clear()
        await m.answer(f"✅ Удалено оценок у пользователя id={target_id}: {cnt}", reply_markup=admin_kb())

    # --- Fallback
    @dp.message()
    async def fallback(m: Message):
        user = get_user(m.from_user.id)
        if not user:
            await m.answer("Нажми /start чтобы зарегистрироваться.")
        else:
            await m.answer("Выбирай действие кнопками 👇", reply_markup=main_kb(m.from_user.id))

    log.info("Start polling...")
    try:
        await dp.start_polling(bot)
    finally:
        log.info("Polling stopped.")


if __name__ == "__main__":
    asyncio.run(main())
