# Вызов библиотеки psycopg2
import psycopg2

# Построение класса, содержащего методы, проверяющие корректность данных 
# относительно клиента (фамилия и имя, email, телефон, наличие клиента в базе данных)
class Check:

    # Построение метода, учитывающего наличие фамилии и имени клиента
    def check_customer_name(self, first_name: str, last_name: str) -> int:
        
        # Критерий наличия корректного имени и фамилии клиента
        bool_value_name = 0

        # Проверка наличия корректного имени и фамилии клиента
        if len(first_name.strip()) != 0 and len(last_name.strip())!= 0:
            bool_value_name += 1

        # Вывод результата проверки фамилии и имени клиента 
        # (1 - наличие фамилии и имени клиента, 0 - наоборот)
        return bool_value_name


    # Построение метода, учитывающего корректность электронной почты
    def check_mail(self, mail: str) -> int:

        # Перечень доменных имен сервера (Яндекс, Mail.ru, Google, Microsoft)
        mail_ends =  ['@yandex.ru', '@narod.ru', '@ya.ru', '@yandex.com', 
                      '@yandex.ua', '@yandex.kz', '@yandex.by', '@mail.ru', 
                      '@list.ru', '@inbox.ru', '@bk.ru', '@gmail.com',
                      '@outlook.com', '@live.com', '@hotmail.com', '@msn.com']
        
        # Критерий наличия корректной почты
        bool_value_mail = 0

        # Подсчет количества знаков "@" (собака, at) в почте
        count_at_symbol = 0
        for symbol in mail.strip():
            if symbol == '@':
                count_at_symbol += 1

        # Проверка наличия: 1) только одного знака "@", 2) корректного домена и имени почтового ящика
        if count_at_symbol == 1:
            at_loc = mail.index('@')
            mail_end = mail[at_loc:].strip()
            mail_begin = mail[:at_loc].strip()
            if mail_end in mail_ends and len(mail_begin) != 0:
                bool_value_mail += 1

        # Вывод результата проверки почты
        # (1 - наличие корректной почты клиента, 0 - наоборот)
        return bool_value_mail


    # Построение метода, учитывающего корректность номера телефона
    def check_phones(self, phones: list = None) -> list:

        # Список для заполнения корректных телефонов клиента
        integer_phones_list = []

        # Условие отсутствия пустых значений в списке телефонов (phones)
        if phones is not None and len(phones) != 0:

            # Начало работы со значениями конкретного телефона
            for phone in phones:

                # Условие принадлежности переменной phone к типу "строка" (str) и отсутствие пустых строк
                if type(phone) is str and len(phone.strip()) != 0:
                    
                    # integer_phone_list - лист, содержащий цифры телефона
                    integer_phone_list = []

                    # Начало работы со значениями внутри строки phone, содержащей цифры номера телефона
                    for value in list(phone.strip()):
                        integers_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

                        # Заполнение листа integer_phone_list цифрами телефона
                        if value in integers_list:
                            integer_phone_list.append(value)
                    
                    # Условие отсутствия пустого листа integer_phone_list
                    if len(integer_phone_list) != 0:

                        # Условие наличия 11 цифр в номере телефона
                        # (работаем только со стандартными номерами РФ/Казахстана)
                        if len(integer_phone_list) == 11:
                            integer_phones_list.append('+7' + ''.join(integer_phone_list)[1:])
                        
                        # Условие наличия 10 цифр в номере телефона
                        # (на случай, если пользователь не ввел +7/8 в начале номера телефона)
                        elif len(integer_phone_list) == 10:
                            integer_phones_list.append('+7' + ''.join(integer_phone_list))

        # Вывод результата проверки телефона
        # (список корректных телефонов в случае их наличия или None в случае их отсутствия)
        return integer_phones_list


    # Построение метода, проверяющего наличие клиента в базе данных client
    def check_client(self, cur, client_id: int):

        # Проверка наличия клиента в базе данных client
        try: 

            # Вывод таблицы client для дальнейшего получения id клиента (client_id)
            cur.execute("""
                        SELECT *
                          FROM client
                         WHERE client_id = %s;
                        """, (client_id,))

            # Вывод id клиента (знак того, что он существует)
            client_id_check = cur.fetchone()[0]
            return client_id_check

        # Фиксация отсутствия клиента в базе данных client (если подобный случай имеет место быть)
        except TypeError:

            # В случае отсутствия клиента выводим информацию о его отсутствии
            print(f'Пользователь c id {client_id} отсутствует в базе данных client')
            return None


# Построение класса, содержащего методы, формирующие базу данных 
class Database(Check):

    # Метод, создающий таблицы в базе данных
    def create_db(self, conn):

        # Создаем запрос на изменение базы данных клиента
        with conn.cursor() as cur:

                # Формирование таблицы client с: 1) именем клиента, 2) фамилией, 3) почтой
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS client(
                        client_id SERIAL PRIMARY KEY,
                        client_name VARCHAR(80) NOT NULL,
                        client_surname VARCHAR(120) NOT NULL,
                        email VARCHAR(120) NOT NULL UNIQUE
                    );
                    """)
                
                # Формирование таблицы telephone
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS telephone(
                        tel_id SERIAL PRIMARY KEY,
                        telephone VARCHAR(80) UNIQUE,
                        client_id INTEGER NOT NULL REFERENCES client(client_id) ON DELETE CASCADE
                    );
                    """)

                # Фиксируем изменения
                conn.commit()

                # Информируем о создании таблиц в базе данных
                print('Статус: таблицы в выбранной базе данных сформированы')


    # Метод, создающий нового клиента в базе данных
    def add_client(self, conn, first_name: str, last_name: str, email: str, phones: list=None):
        
        # Начала работы с базой данных
        try:
            
            # Создание запроса на изменение базы данных
            with conn.cursor() as cur:

                # Критерий наличия фамилии и имени клиента
                # (1 - клиент существует в базе данных, 0 - наоборот <=> ошибка)
                error_bool = 0

                # Учет наличия фамилии и имени клиента
                if self.check_customer_name(first_name, last_name) == 1:                    
                    
                    # Учет наличия корректных почт
                    if self.check_mail(email) == 1:
                        
                        # Попытка создания таблицы с основными данными клиента (client)
                        try:

                            # Формирование таблицы с основными данными клиента (client)
                            cur.execute("""
                                INSERT INTO client (client_name, client_surname, email) 
                                VALUES (%s, %s, %s);
                                """, (first_name, last_name, email))

                            # Фиксация изменений
                            conn.commit()

                            # Информирование о добавлении клиента в базу данных
                            print(f'Клиент добавлен в базу данных. Имя - {first_name}, Фамилия - {last_name}, email - {email}')
                        
                        # Вывод ошибки в случае некорректного добавления клиента в базу данных
                        except (psycopg2.errors.UniqueViolation, psycopg2.errors.InFailedSqlTransaction) as e:
                             
                             # Вывод ошибки: наличие пользователя в базе данных
                            print(f'Пользователь {first_name} {last_name} уже добавлен в базу данных')

                             # Заполнение переменной error_bool
                            error_bool += 1

                            # Фиксация изменений
                            conn.commit()

                    # Вывод ошибки в случае некорректного заполнения электронной почты         
                    else:
                        print(f'Некорректно заполнен раздел "Электронная почта клиента". Введенные данные: email - {email}')
                        raise ValueError

                # Вывод ошибки в случае некорректного заполнения имени и/или фамилии клиента
                else:
                    print(f'Некорректно заполнен раздел "Имя и фамилия клиента". Введенные данные: имя - {first_name}, фамилия - {last_name}')
                    raise ValueError

                # Учет отсутствия ошибки, связанной с добавлением существующего клиента в качестве нового
                if error_bool != 1:

                    # Вывод таблицы client для дальнейшего получения id клиента (client_id)
                    cur.execute("""
                        SELECT * 
                        FROM client
                        WHERE client_name = %s 
                          AND client_surname = %s
                          AND email = %s;
                        """, (first_name, last_name, email))
                    
                    # Получение id клиента (client_id)
                    client_id = cur.fetchone()[0]

                # Выведение списка корректных телефонов
                correct_phones = self.check_phones(phones) 

                # Учет наличия корректных телефонов и отсутствия ошибки, связанной с добавлением клиента
                if len(correct_phones) != 0 and error_bool != 1:

                    # Начало работы с конкретным корректным телефоном из списка
                    for correct_phone in correct_phones:
                            
                            # Заполнение таблицы, содержащей телефоны клиентов (telephone)
                            cur.execute("""
                            INSERT INTO telephone (telephone, client_id)
                            VALUES (%s, %s);
                            """, (correct_phone, client_id))

                            # Фиксация изменений
                            conn.commit()

        # В случае отсутствия нужных компонентов или существования клиента в базе данных 
        # ничего не делаем с самой базой данных. Информируем пользователя об ошибке
        except ValueError:
            pass


    # Метод, добавляющий новый телефон для существующего клиента
    def add_phone(self, conn, client_id: int, phone: str):

         # Создаем запрос на изменение базы данных клиента
        with conn.cursor() as cur:
            
            # Проверка наличия клиента в базе данных
            client_id_check = self.check_client(cur, client_id)

            # Проверка корректности введенного телефона
            correct_phone = self.check_phones([phone])

            # Учет наличия клиента в базе данных
            if client_id_check is not None:

                # Учет наличия корректного телефона клиента
                if len(correct_phone) != 0:
                        
                        # Попытка внесения нового телефона в таблицу telephone
                        try:
                            
                            # Заполнение таблицы, содержащей телефоны клиентов (telephone)
                            cur.execute("""
                            INSERT INTO telephone(telephone, client_id)
                            VALUES (%s, %s); 
                            """, (correct_phone[0], client_id_check))

                            # Фиксация изменений
                            conn.commit()

                            # Вывод информации о добавлении нового телефона в базу данных
                            print(f'Добавлен новый номер телефона для пользователя c id {client_id_check}. Номер телефона - {correct_phone[0]}')
                        
                        # Вывод ошибки в случае некорректного добавления нового телефона в базу данных
                        except (psycopg2.errors.UniqueViolation, psycopg2.errors.InFailedSqlTransaction) as e:
                            
                            # Вывод ошибки, отражающей непринадлежность введенного номера телефона к разделу "Новые телефоны клиентов"
                            print(f'Телефон пользователя c id {client_id_check} уже был ранее добавлен в базу данных')

                            # Фиксация изменений
                            conn.commit()

                # Вывод ошибки в случае наличия некорректного нового телефона клиента
                else:
                    print(f'У существующего пользователя c id {client_id_check} введен некорректный номер телефона')


    # Метод, позволяющий изменить данные о существующем клиенте
    def change_client(self, conn, client_id: int, first_name: str = None, last_name: str = None, 
                      email: str = None, phones: list = None):

        # Создание запроса на изменение базы данных
        with conn.cursor() as cur:

            # Проверка наличия клиента в базе данных
            client_id_check = self.check_client(cur, client_id)
            
            # Учет наличия клиента в базе данных
            if client_id_check is not None:
                
                # Учет отсутствия незаполненных имен, фамилии и/или email в базе данных
                if sum([int(None == n) for n in [first_name, last_name, email]]) < 3:
                    
                    # Заполнение словаря данными, требующими изменений в базе данных
                    dict_data = {}
                    for name, el in zip(["client_name", "client_surname", "email"], [first_name, last_name, email]):
                        if el is not None:
                            dict_data[name] = el
                    
                    # Ввод значений из словаря dict_data в таблицу, содержащую данные клиента (client)
                    for val_name, val in dict_data.items():
                        
                        # Обновление таблицы client
                        cur.execute("""
                                    UPDATE client
                                       SET """ +  f'{val_name}' + """ = %s
                                     WHERE client_id = %s
                                    """, (val, client_id_check))
                        
                        # Фиксация изменений
                        conn.commit()

                        # Информирование пользователя об изменении данных в рамках таблицы client
                        print(f'Данные пользователя с id {client_id_check} изменены (таблица client)')

            # Учет наличия клиента в базе данных и заполненного списка телефонов phones
            if sum([int(None == n) for n in [client_id_check, phones]]) == 0:
                
                # Проверка корректности введенного телефона.
                # Результат - список корректных телефонов, введенных пользователем
                correct_phones_list = self.check_phones(phones)

                # Учет наличия заполненного списка correct_phones_list
                if len(correct_phones_list) != 0:
                    
                    # Вывод таблицы telephone
                    cur.execute("""
                                SELECT tel_id, telephone
                                  FROM telephone
                                 WHERE client_id=%s;
                                """, (client_id_check,))
                    
                    # Фиксация изменений и вывод данных из таблицы telephone 
                    # (id телефона, номер телефона). Результат - список telephone_tooples
                    telephone_tooples = cur.fetchall()

                    # Учет наличия заполненного списка telephone_tooples
                    if len(telephone_tooples) != 0:

                        # Вывод: 1) списка существующих id телефонов; 2) списка существующих телефонов
                        telephone_id_list = [t[0] for t in telephone_tooples if len(telephone_tooples) != 0]
                        telephone_number_list = [t[1] for t in telephone_tooples if len(telephone_tooples) != 0]

                    # Учет наличия одинакого размера списков telephone_number_list и telephone_number_list
                        ## correct_phones_list - список корректных телефонов, введенных пользователем
                        ## telephone_number_list - список телефонов, существующих в базе данных
                    # Пользователь не может изменить только один номер, если до этого он ввел несколько телефонов
                    if len(correct_phones_list) == len(telephone_number_list):

                        # Ввод значений из списков correct_phones_list и telephone_id_list
                            ## correct_phones_list - список корректных телефонов, введенных пользователем
                            ## telephone_id_list - список id телефонов, существующих в базе данных
                        for phone_number, phone_id in zip(correct_phones_list, telephone_id_list):
                            
                            # Обновление таблицы client
                            cur.execute("""
                                        UPDATE telephone
                                           SET telephone = %s
                                         WHERE client_id = %s AND tel_id = %s;
                                        """, (phone_number, client_id_check, phone_id))

                            # Фиксация изменений
                            conn.commit()

                        # Информирование пользователя об изменении данных в рамках таблицы telephone
                        print(f'Данные пользователя с id {client_id_check} изменены (таблица telephone)')

                    # Вывод ошибки в случае несоответствия списка введенных телефонов количеству телефонов, существующих в базе данных
                    else:
                        print(f'Список телефонов пользователя c id {client_id_check} не соответствует количеству телефонов в базе данных telephone. У пользователя их {len(telephone_number_list)}')

                # Вывод ошибки в случае отсутствия корректных номеров телефона в списке phones
                else:
                    print(f'У существующего пользователя c id {client_id_check} отсутствует корректный номер телефона')


    # Метод, позволяющий удалить телефон для существующего клиента
    def delete_phone(self, conn, client_id: int, phone: str):
        
        # Создание запроса на изменение базы данных
        with conn.cursor() as cur:

            # Проверка наличия клиента в базе данных
            client_id_check = self.check_client(cur, client_id)
            
            # Учет наличия клиента в базе данных
            if client_id_check is not None:
                
                # Проверка корректности введенного телефона
                phone_check = self.check_phones([phone])

                # Учет наличия корректного телефона
                if len(phone_check) != 0:
                    
                    # Вывод таблицы, содержащей телефоны клиентов (telephone)
                    cur.execute("""
                                SELECT * 
                                FROM telephone
                                WHERE client_id = %s;
                                """, (client_id,))

                    # Фиксация изменений и вывод данных из таблицы telephone                      
                    telephone_tooples = cur.fetchall()

                    # Формирование словаря для заполнения данных
                    dict_data = {}

                    # Учет наличия заполненного списка telephone_tooples
                    if len(telephone_tooples) != 0:

                        # Вывод списков: 1) существующих id телефонов; 2) существующих телефонов
                        telephone_id_list = [t[0] for t in telephone_tooples if len(telephone_tooples) != 0]
                        telephone_number_list = [t[1] for t in telephone_tooples if len(telephone_tooples) != 0]
                        
                        # Заполнение словаря dict_data существующими телефонами и их id в базе данных
                        for number, id in zip(telephone_number_list, telephone_id_list):
                            dict_data[number] = id
                    
                    # Учет наличия введенного телефона в списке существующих телефонов (т.е. в базе данных)
                    if phone_check[0] in telephone_number_list:
                        
                        # Получение id введенного телефона из словаря dict_data
                        id_tel = dict_data.get(phone_check[0])

                        # Удаление телефона из таблицы telephone
                        cur.execute("""
                                    DELETE 
                                      FROM telephone
                                     WHERE tel_id = %s 
                                    """, (id_tel,))

                        # Фиксация изменений
                        conn.commit()
                        
                        # Информирование пользователя об успешном удалении телефонных данных
                        print(f'Удаление телефона пользователя {client_id_check} произведено успешно')

                    # Вывод ошибки в случае отсутствия телефона в базе данных
                    else:
                        print(f'Выбранный телефон для удаления отсутствует в базе данных. Id пользователя - {client_id_check}')

                # Вывод ошибки в случае ввода пользователем некорректного телефона
                else:
                    print(f'Выбранный телефон для удаления не является корректным. Id пользователя - {client_id_check}')


    # Метод, позволяющий удалить существующего клиента
    def delete_client(self, conn, client_id: int):
        
        # Создание запроса на изменение базы данных
        with conn.cursor() as cur:
            
            # Проверка наличия клиента в базе данных
            client_id_check = self.check_client(cur, client_id)

            # Учет наличия клиента в базе данных
            if client_id_check is not None:

                # Удаление клиента из базы данных
                cur.execute("""
                            DELETE 
                              FROM client
                             WHERE client_id = %s;
                            """, (client_id_check,))
                
                # Фиксация изменений
                conn.commit()
            
                # Информирование об удалении клиента из базы данных
                print(f'Клиент {client_id_check} удален')


    # Метод для поиска клиента
    def find_client(self, conn, first_name: str = None, last_name: str = None, email: str = None, phone: str = None):
        
        # Создание запроса на изменение базы данных
        with conn.cursor() as cur:

            # Учет отсутствия незаполненных имен, фамилий, email и/или телефонов в базе данных
            if sum([int(None == n) for n in [first_name, last_name, email, phone]]) < 4:

                # Формирование словаря для заполнения данных
                dict_data = {}

                # Учет всевозможных названий столбцов и их значений
                for name, val in zip(["client_name", "client_surname", "email", "telephone"], 
                                       [first_name, last_name, email, phone]):
                    
                    # Заполнение словаря dict_data данными, введенными пользователем
                    if val is not None:
                        dict_data[name] = val
                
                # Создание листа, содержащего условия оператора WHERE
                # ([col1 = val1, col2 = val2, ... ])
                query_where_condition_list = []
                
                # Заполнение списка query_where_condition_list
                for col, val in dict_data.items():
                    query_where_condition_list.append(col + ' = ' + f"'{val}'")
                
                # Вывод объединения таблиц client и telephone
                cur.execute("""
                            SELECT client_name, client_surname, email, telephone
                            FROM client c
                            LEFT JOIN telephone t
                                   ON c.client_id = t.client_id
                            WHERE """ + ' AND '.join(query_where_condition_list) + ';')

                # Фиксация изменений и вывод результата поиска клиента
                client_data = cur.fetchall()

                # Учет наличия заполненного списка client_data
                if len(client_data) != 0:
                    
                    # Выведение результата поиска клиента
                    print(client_data)
                
                # Выведение информации об отсутствии данных по заданным параметрам
                else:
                    print('Не удалось найти клиента по заданным параметрам')


# Реализация написанных выше методов, тестирование созданных методов
if __name__ == '__main__':

    # Подключение к базе данных
    conn = psycopg2.connect(database='test', user='postgres', password='postgres', host='localhost')
    
    # Вызов класса Database
    database = Database()

    # 1. Создание таблиц в базе данных
    print('1. Создание таблиц в базе данных:')
    database.create_db(conn)

    # 2. Добавление новых клиентов в базу данных
    print(' ', '2. Добавление новых клиентов в базу данных:', sep = '\n')
    database.add_client(conn, 'Василий', 'Чапаев', 'chapai@mail.ru', ['8(800)235-55-55'])
    database.add_client(conn, 'Дмитрий', 'Фурманов', 'furman@bk.ru')
    database.add_client(conn, 'Петька', 'Исаев', 'petyka@mail.ru', ['8(92910230)2330394-552812812-552182181'])
    database.add_client(conn, 'Сергей', 'Бороздин', 'sergey@yandex.ru', ['8(800)888-88-88', '+78009999999', '800555 55  55'])
    database.add_client(conn, '', 'Жихарев', 'ziharev@bk.ru', ['8(800)666-66-66'])
    database.add_client(conn, 'Анка', '', 'Anka@outlook.com', ['8(800)777-77-77'])
    database.add_client(conn, 'Петрович', 'Потапов', 'potapov@yandex@yandex.kz', ['+78008888888'])
    database.add_client(conn, 'Иван', 'Кутяков', ' ')
    
    # 3. Добавление телефона существующих клиентов
    print(' ', '3. Добавление телефона существующих клиентов:', sep = '\n')
    database.add_phone(conn, 3, '+79752587777')
    database.add_phone(conn, 1, '202 39 023 09 23 23 3209 32')
    database.add_phone(conn, 20, '+79757777778')

    # 4. Изменение данных существующих клиентов
    print(' ', '4. Изменение данных существующих клиентов:', sep = '\n')
    database.change_client(conn, 1, last_name = 'Чапай')
    database.change_client(conn, 2, 'Дима')
    database.change_client(conn, 4, email = 'borozdin@mail.ru')
    database.change_client(conn, 3, phones = ['+79756666666'])
    database.change_client(conn, 4, phones = ['+78008888887'])
    database.change_client(conn, 4, email = 'borozdin@ya.ru', phones = ['+78008888810', '+78008888811', '+78008888812'])
    database.change_client(conn, 20, phones = ['+79757777778'])

    # 5. Удаление телефонов существующих клиентов
    print(' ', '5. Удаление телефонов существующих клиентов:', sep = '\n')
    database.delete_phone(conn, 4, '+78008888812')
    database.delete_phone(conn, 1, '202 39 023 09 23 23 3209 32')
    database.delete_phone(conn, 20, '+79757777778')

    # 6. Удаление существующих клиентов
    print(' ', '6. Удаление существующих клиентов:', sep = '\n')
    database.delete_client(conn, 2)
    database.delete_client(conn, 4)
    database.delete_client(conn, 20)

    # 7. Поиск существующих клиентов
    print(' ', '7. Поиск существующих клиентов:', sep = '\n')
    database.find_client(conn, last_name = 'Чапай')
    database.find_client(conn, 'Петька')
    database.find_client(conn, email = 'chapai@mail.ru')
    database.find_client(conn, phone = '+79756666666')
    database.find_client(conn, 'Иван', 'Иванов')

    # Завершение запроса на изменение базы данных
    conn.close()