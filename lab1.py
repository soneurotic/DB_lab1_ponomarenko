from time import perf_counter
from threading import Thread
import psycopg2
import random

username = 'postgres'
password = 'postgres'
database = 'DB_sem6_lab1'
host = 'localhost'
port = '5432'

additional_task_query = '''
DROP TABLE IF EXISTS random_user_counter;

CREATE TABLE random_user_counter (
    user_id SERIAL PRIMARY KEY,
    counter INT,
    version INT
);

INSERT INTO random_user_counter (user_id, counter, version)
SELECT generate_series(1, 100000), 0, 0;

SELECT * FROM random_user_counter;
'''


def main_task(table_name):
    functions = []

    # additional info (моє бачення):
    # коли транзакція читає об'єкт користуючись cursor'ом, об'єкт блокується на зміну інших
    # транзакцій, поки об'єкт не буде відпущено, або поки не відбудеться коміт, тому для
    # усіх методів окрім in-place update бажане створення курсору всередині них для
    # подальшого використання функцій у потоках з метою уникнення непередбачуваної поведінки

    with psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port) as outer_conn:
        if table_name == 'random_user_counter':
            with outer_conn.cursor() as cursor_1:
                cursor_1.execute(additional_task_query)
                outer_conn.commit()

        # кожен з потоків виконує роботу (селектять у запиті каунтер для апдейту на +1),
        # швидше ніж встигає відбутися комміт змін попередніх потоків. Але все ж таки між
        # ця "мікрорізниця" у часі накоплюється у життєвому циклі програми, тож на виході воно
        # виливається у ~800 "зайвих" значень каунтера поверх тих 10-и тисяч
        def lost_update():
            with psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port) as conn:
                with conn.cursor() as cursor:
                    for _ in range(10_000):
                        id_val = get_row() if table_name == 'random_user_counter' else 1

                        cursor.execute(f"SELECT counter FROM {table_name} WHERE user_id = {id_val}")
                        counter = cursor.fetchone()[0]
                        counter += 1
                        cursor.execute(f"UPDATE {table_name} SET counter = {counter} WHERE user_id = {id_val}")
                        conn.commit()

        functions.append(lost_update)

        # апдейт відбувається безпосередньо у БД, СУБД якої по дефолту передбачає read commited для транзакцій
        def in_place_update():
            with psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port) as conn:
                with conn.cursor() as cursor:
                    for _ in range(10_000):
                        id_val = get_row() if table_name == 'random_user_counter' else 1
                        cursor.execute(f"UPDATE {table_name} SET counter = counter + 1 WHERE user_id = {id_val}")
                        conn.commit()

        functions.append(in_place_update)

        # кожен вибір каунтера, що здійснюється з програми до БД, активує локінг рядка бази даних для апдейту
        def row_level_locking():
            with psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port) as conn:
                with conn.cursor() as cursor:
                    for _ in range(10_000):
                        id_val = get_row() if table_name == 'random_user_counter' else 1
                        cursor.execute(f"SELECT counter FROM {table_name} WHERE user_id = {id_val} FOR UPDATE")
                        counter = cursor.fetchone()[0]
                        counter += 1
                        cursor.execute(f"UPDATE {table_name} SET counter = {counter} WHERE user_id = {id_val}")
                        conn.commit()

        functions.append(row_level_locking)

        # optimistic concurrency control використовує додатковим параметром версію
        # для того, щоб трекати чи не пройшов паралельно апдейт значення, що змінюється
        def optimistic_concurrency_control():
            with psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port) as conn:
                with conn.cursor() as cursor:
                    for _ in range(10_000):
                        id_val = get_row() if table_name == 'random_user_counter' else 1

                        # якщо наш апдейт не пройде, то нам треба виконати ретрай, бо нам треба ітерувати
                        # допоки він стовідсотково не пройде. Для цього ми використовуємо нескінченний цикл
                        # що працюватиме до моменту коли не пройде остання (10-тисячна) транзакція потоку
                        while True:
                            cursor.execute(f"SELECT counter, version FROM {table_name} WHERE user_id = {id_val}")
                            result = cursor.fetchone()
                            counter, version = result[0], result[1]
                            counter = counter + 1
                            cursor.execute(
                                f"UPDATE {table_name} SET counter = {counter}, version = {version + 1} WHERE user_id = {id_val} AND version = {version}")
                            conn.commit()
                            count = cursor.rowcount
                            if count > 0:
                                break

        functions.append(optimistic_concurrency_control)

        for func in functions:
            with outer_conn.cursor() as cursor:
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

                if table_name == 'user_counter':
                    cursor.execute(f"SELECT counter, version FROM {table_name} WHERE user_id = 1")
                    result = cursor.fetchone()
                    counter, version = result[0], result[1]

                    print(f'Остаточне значення у таблиці: counter = {counter}, version = {version}')

                    cursor.execute(f'UPDATE {table_name} SET counter = 0, version = 0')

                outer_conn.commit()


def get_row():
    choice = random.randint(0, 1)
    if choice == 0:
        return 1
    else:
        return random.randint(2, 100_000)


main_task(table_name='user_counter')

# main_task(table_name='random_user_counter')