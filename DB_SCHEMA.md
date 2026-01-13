\# Схема базы данных (SQLite)



\## Таблица users

\- tg\_id (INTEGER, PK) — Telegram ID пользователя

\- full\_name (TEXT) — Имя Фамилия



\## Таблица subjects

\- name (TEXT, PK) — название предмета



\## Таблица grades

\- id (INTEGER, PK, autoincrement)

\- tg\_id (INTEGER, FK -> users.tg\_id)

\- subject (TEXT, FK -> subjects.name)

\- grade (REAL) — оценка (поддержка 4.35 и т.п.)

\- created\_at (TEXT) — дата/время добавления



\## ER-схема (текст)

users (1) --- (N) grades  

subjects (1) --- (N) grades



