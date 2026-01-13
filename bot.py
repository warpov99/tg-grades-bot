import asyncio
import sqlite3
import logging
import random
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

# ========= НАСТРОЙКИ =========
TOKEN = "PASTE_YOUR_TOKEN_HERE"

# ВСТАВЬ СЮДА СВОЙ TG ID (можно несколько)
# Узнать свой ID можно у бота @userinfobot
ADMIN_IDS = {1234567890}

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
        full_name TEXT NOT NULL
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
        SELECT subject,
               ROUND(AVG(grade), 2) AS avg_subj,
               COUNT(*) AS cnt
        FROM grades
        WHERE tg_id=?
        GROUP BY subject
        ORDER BY subject ASC
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
        if user:
            log.info("start: known user tg_id=%s name=%s", m.from_user.id, user["full_name"])
            await m.answer(f"Привет, {user['full_name']}! 👇", reply_markup=main_kb(m.from_user.id))
            return

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
        await m.answer(f"✅ Готово, {full_name}!", reply_markup=main_kb(m.from_user.id))

    # --- Личный кабинет
    @dp.message(F.text == BTN_CAB)
    async def cabinet(m: Message):
        user = get_user(m.from_user.id)
        if not user:
            await m.answer("Сначала /start", reply_markup=ReplyKeyboardRemove())
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
