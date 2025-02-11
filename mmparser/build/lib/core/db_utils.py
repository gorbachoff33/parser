import sqlite3
import datetime

FILENAME = "storage.sqlite"


def create_db():
    """Создаёт таблицу products, если её нет"""
    sqlite_connection = sqlite3.connect(FILENAME)
    cursor = sqlite_connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            goods_id            TEXT,
            merchant_id         TEXT,
            url                 TEXT,
            title               TEXT,
            price               INTEGER,
            price_bonus         INTEGER,
            bonus_amount        INTEGER,
            bonus_percent       INTEGER,
            available_quantity  INTEGER,
            delivery_date       TEXT,
            merchant_name       TEXT,
            merchant_rating     FLOAT,
            scraped_at          DATETIME,
            notified            BOOL
        );
    """)

    sqlite_connection.commit()
    cursor.close()


def add_to_db(
    job_id,
    job_name,
    goods_id,
    merchant_id,
    url,
    title,
    price,
    price_bonus,
    bonus_amount,
    bonus_percent,
    available_quantity,
    delivery_date,
    merchant_name,
    merchant_rating,
    notified,
):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sqlite_connection = sqlite3.connect(FILENAME)
    cursor = sqlite_connection.cursor()

    # Проверяем, есть ли уже этот товар в базе
    cursor.execute("""
        SELECT id FROM products 
        WHERE goods_id = ? AND merchant_id = ? AND price = ? AND bonus_amount = ?
    """, (goods_id, merchant_id, price, bonus_amount))

    existing_product = cursor.fetchone()

    if not existing_product:
        cursor.execute("""
            INSERT INTO products 
            (goods_id, merchant_id, url, title, price, price_bonus, bonus_amount, 
            bonus_percent, available_quantity, delivery_date, merchant_name, 
            merchant_rating, scraped_at, notified) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            goods_id, merchant_id, url, title, price, price_bonus, bonus_amount,
            bonus_percent, available_quantity, delivery_date, merchant_name,
            merchant_rating, now, notified
        ))

    sqlite_connection.commit()
    cursor.close()



def get_last_notified(goods_id, merchant_id, price, bonus_amount):
    """Возвращает дату последнего уведомления по этому товару"""
    sqlite_connection = sqlite3.connect(FILENAME)
    cursor = sqlite_connection.cursor()
    cursor.execute("""
        SELECT scraped_at FROM products 
        WHERE goods_id = ? AND merchant_id = ? AND price = ? AND bonus_amount = ?
        ORDER BY scraped_at DESC 
        LIMIT 1
    """, (goods_id, merchant_id, price, bonus_amount))
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else None


def delete_old_entries():
    """Удаляет товары старше 24 часов"""
    sqlite_connection = sqlite3.connect(FILENAME)
    cursor = sqlite_connection.cursor()

    one_day_ago = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("DELETE FROM products WHERE scraped_at < ?", (one_day_ago,))
    sqlite_connection.commit()
    cursor.close()


create_db()
