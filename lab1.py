from time import perf_counter
from threading import Thread
import psycopg2

username = 'postgres'
password = 'postgres'
database = 'DB_sem6_lab1'
host = 'localhost'
port = '5432'

# під'єднання до існуючої БД
conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
print(type(conn))

functions = []

def lost_update():
    inner_conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    inner_cursor = inner_conn.cursor()
    for _ in range(10_000):
        inner_cursor.execute(f"SELECT counter FROM user_counter WHERE user_id = 1")
        counter = inner_cursor.fetchone()[0]
        counter += 1
        inner_cursor.execute(f"UPDATE user_counter SET counter = {counter} WHERE user_id = 1")
        inner_conn.commit()
functions.append(lost_update)

cursor = conn.cursor()
def in_place_update():
    for _ in range(10_000):
        cursor.execute(f"UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
        conn.commit()
functions.append(in_place_update)

def row_level_locking():
    inner_conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    inner_cursor = inner_conn.cursor()
    for _ in range(10_000):
        inner_cursor.execute(f"SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
        counter = inner_cursor.fetchone()[0]
        counter += 1
        inner_cursor.execute(f"UPDATE user_counter SET counter = {counter} WHERE user_id = 1")
        inner_conn.commit()
functions.append(row_level_locking)

def optimistic_concurrency_control():
    inner_cursor = conn.cursor()
    for _ in range(10_000):
        while True:
            inner_cursor.execute(f"SELECT counter, version FROM user_counter WHERE user_id = 1")
            result = inner_cursor.fetchone()
            counter, version = result[0], result[1]
            counter = counter + 1
            inner_cursor.execute(
                f"UPDATE user_counter SET counter = {counter}, version = {version + 1} WHERE user_id = 1 AND version = {version}")
            conn.commit()
            count = inner_cursor.rowcount
            if count > 0:
                break
functions.append(optimistic_concurrency_control)

for func in functions:
    start_time = perf_counter()

    # створюємо і запускаємо 10 потоків
    threads = []
    for _ in range(10):
        t = Thread(target=func)
        threads.append(t)
        t.start()

    # чекаємо, поки виконаються потоки
    for t in threads:
        t.join()

    end_time = perf_counter()

    print(f'\n\nВиконана функція: {str(func).split()[1]}')

    print(f'Виконання зайняло {end_time - start_time: 0.2f} секунд.')

    cursor.execute(f"SELECT counter, version FROM user_counter WHERE user_id = 1")
    result = cursor.fetchone()
    counter, version = result[0], result[1]

    print(f'Остаточне значення у таблиці: counter = {counter}, version = {version}')

    cursor.execute(f'UPDATE user_counter SET counter = 0, version = 0')
    conn.commit()