import psycopg2
import psycopg2.extensions
from psycopg2.sql import SQL, Identifier
from pprint import pprint


class Database():
    def __init__(self):
        self.conn = psycopg2.connect(database='netology_db', user='postgres', password='pos0295485!')
        self.cursor = self.conn.cursor()

    def query_database(self, sql_statement, *args):
        self.cursor.execute(sql_statement, *args)
        return self.cursor

    def commit_query(self):
        return self.conn.commit()

    def fetch_one(self, sql_statement, *args):
        cursor = self.query_database(sql_statement, *args)
        return cursor.fetchone()

    def fetch_all_f(self, sql_statement, *args):
        cursor = self.query_database(sql_statement, *args)
        return cursor.fetchall()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def testing(self):
        self.cursor.execute(
            """
            SELECT *
            FROM Client;
            """
        )

        self.cursor.execute(
            """
            SELECT *
            FROM Phone;
            """
        )
        return print(self.cursor.fetchall())


class CreateTables(Database):
    def create_client_table(self):
        sql_statement = """
        CREATE TABLE IF NOT EXISTS Client (
        client_id    SERIAL PRIMARY KEY,
        name    VARCHAR(40) NOT NULL,
        surname VARCHAR(40) NOT NULL,
        email   VARCHAR(40) NOT NULL UNIQUE);
        """

        client_table = Database.query_database(self, sql_statement)
        Database.commit_query(self)
        print("Таблица клиентов создана")
        return client_table

    def create_phone_table(self):
        sql_statement = """
        CREATE TABLE IF NOT EXISTS Phone (
        phone_id    SERIAL PRIMARY KEY,
        phone_number    VARCHAR(15) NOT NULL UNIQUE,
        client_id   INTEGER NOT NULL REFERENCES Client(client_id));
        """
        phone_table = Database.query_database(self, sql_statement)
        Database.commit_query(self)
        print("Таблица телефонов создана")
        return phone_table


class OperateDatabase(CreateTables):
    def add_client(self, name, surname, email, phone_number=None):
        sql_statement = """
        SELECT client_id
          FROM Client
         WHERE name = %s OR surname = %s OR email = %s;
        """
        client_in_database = CreateTables.fetch_one(self, sql_statement, (name, surname, email))
        print(f"Add_client: The Client in Database is:>> {client_in_database}")
        sql_statement2 = """
        INSERT INTO Client (name, surname, email)
        VALUES (%s, %s, %s)  RETURNING client_id;
        """
        sql_statement3 = """
        INSERT INTO Phone (phone_number, client_id)
        VALUES (%s, %s) RETURNING phone_id, phone_number;
        """
        if not client_in_database:
            CreateTables.query_database(self, sql_statement2, (name, surname, email))
            print('Add_client: часть 1 готова')
            if phone_number:
                CreateTables.query_database(self, sql_statement3, (
                phone_number, Database.fetch_one(self, sql_statement, (name, surname, email))[0]))
                print('Add_client: Клиент с номером добавлен')
                return True
            CreateTables.commit_query(self)
            return True
        print("Add_client: Клиент уже есть в базе")

    def add_phone(self, client_id, phone_number):
        sql_statement = """
        SELECT client_id
          FROM Client
         WHERE client_id = %s;
        """
        client_in_database = CreateTables.fetch_one(self, sql_statement, (client_id, ))
        print(f"Add_phone: The Client in Database is:>> {client_in_database}")
        sql_statement2 = """
        INSERT INTO Phone (phone_number, client_id)
        VALUES (%s, %s) RETURNING client_id;
        """

        if client_in_database:
            CreateTables.query_database(self, sql_statement2, (
            phone_number, Database.fetch_one(self, sql_statement, (client_id, ))))
            print('Add_phone: Номер добавлен')
            CreateTables.commit_query(self)
            return True
        print("Add_phone: Такого клиента нет в базе")

    def change_data(self, client_id, name=None, surname=None, email=None, phone_number=None):
        sql_statement = """
        SELECT client_id
          FROM Client
         WHERE client_id = %s;
        """
        client_in_database = CreateTables.fetch_one(self, sql_statement, (client_id, ))
        data = {'name': name, 'surname': surname, 'email': email, 'phone_number': phone_number}
        sql_statement2 = """
        UPDATE Client
           SET {} = %s
         WHERE client_id = %s;
        """
        sql_statement3 = """
        SELECT *
          FROM Client
         WHERE client_id = %s;
        """
        if client_in_database:
            for key, arg in data.items():
                if arg:
                    Database.query_database(self, SQL(sql_statement2).format(Identifier(key)), (arg, client_id))
                Database.query_database(self, sql_statement3, (client_id, ))
            print(Database.fetch_all_f(self, sql_statement3, (client_id, )))
            return True
        print('Change_data: Такой Клиент уже существует')

    def delete_phone(self, client_id, phone_number):
        sql_statement = """
        SELECT client_id
          FROM Client
         WHERE client_id = %s;
        """
        client_in_database = CreateTables.fetch_one(self, sql_statement, (client_id, ))
        sql_statement2 = """
        DELETE
          FROM Phone
         WHERE client_id = %s AND phone_number = %s;
        """
        if client_in_database:
            CreateTables.query_database(self, sql_statement2, (client_id, phone_number))
            CreateTables.commit_query(self)
            print(f'Delete_phone: Телефон {phone_number} удален')
            return True
        print('Delete_phone: Такого клиента нет в базе')

    def delete_client(self, client_id):
        sql_statement = """
        SELECT client_id
          FROM Client
         WHERE client_id = %s;
        """
        client_in_database = CreateTables.fetch_one(self, sql_statement, (client_id, ))
        sql_statement2 = """
        DELETE
          FROM Phone
         WHERE client_id = %s;
        """
        sql_statement3 = """
        DELETE
          FROM Client
         WHERE client_id = %s;
        """
        if client_in_database:
            CreateTables.query_database(self, sql_statement2, (client_id, ))
            CreateTables.query_database(self, sql_statement3, (client_id,))
            # print(CreateTables.fetch_all_f(self, sql_statement2, (client_id, )))
            # Database.commit_query(self)
            print('Delete_client: Клиент удален')
            return True
        print('Delete_client: Такого клиента нет в базе')

    def find_client(self, name=None, surname=None, email=None, phone_number=None):
        data = {'name': name, 'surname': surname, 'email': email, 'phone_number': phone_number}

        sql_statement = """
        SELECT c.client_id, c.name, c.surname, p.phone_number
          FROM Client AS c
               LEFT JOIN Phone AS p
               ON c.client_id = p.client_id
         WHERE {} = %s;
        """

        for key, arg in data.items():
            if arg:
                Database.query_database(self, SQL(sql_statement).format(Identifier(key)), (arg, ))
                pprint(Database.fetch_all_f(self, SQL(sql_statement).format(Identifier(key)), (arg, )))
                print('Find_client: Клиент найден')
                return True


conn = psycopg2.connect(database='netology_db', user='postgres', password='pos0295485!')
with conn.cursor() as cur:

    cur.execute("""
        SELECT c.client_id, c.name, c.surname, p.phone_number
          FROM Client AS c
               LEFT JOIN Phone AS p
               ON c.client_id = p.client_id

        """)
    pprint(cur.fetchall())
    # # -- сброс таблиц
    # cur.execute("""
    #     DROP TABLE Phone;
    #     DROP TABLE Client;
    #     """)
    # conn.commit()
conn.close()

# --Тестирование/наполнение

test = CreateTables()
test.create_client_table()
test.create_phone_table()
test = OperateDatabase()
test.add_client('Zoya', 'Cosm', 'cc3zf2zs2ssffz@mail.ru', '+79263503777')
test.add_client('Paul', 'Zac', 'ppf2fss32sz@mail.ru')
test.add_client('Kris', 'Kov', 'kris.kov@gmail.com')
test.add_client('Pan', 'Devil', 'fromhell@list.ru', '+79998526655')
test.delete_phone('1', '+79998887777')
test.add_phone('1', '+79998887777')
test.change_data('1', 'Palet')
test.change_data('3', 'Piter', None, 'trustme@yandex.ru')
# test2 = Database()
# test2.testing()
test.delete_client('4')
test.find_client('Palet')
# --
