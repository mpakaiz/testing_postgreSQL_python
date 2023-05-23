import psycopg2
import psycopg2.extensions


# conn = psycopg2.connect(database='netology_db', user='postgres', password='pos0295485!')
# with conn.cursor() as cur:
#
#     cur.execute("""
#         DROP TABLE Phone;
#         DROP TABLE Client;
#         """)
# conn.close()

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
    # def fetch_one(self, sql_statement, *args):
    #     result = self.query_database(sql_statement, *args)
    #     if result is None:
    #         return False
    #     return result.fetchone()

    def fetch_all(self, sql_statement, *args):
        result = self.query_database(sql_statement, *args)
        if result is None:
            return None
        return result.fetchall()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

class CreateTables(Database):
    def create_client_table(self):
        sql_statement = """
        CREATE TABLE IF NOT EXISTS Client
        (client_id SERIAL PRIMARY KEY,
        name VARCHAR(40) NOT NULL,
        surname VARCHAR(40) NOT NULL,
        email VARCHAR(40) UNIQUE);
        """

        client_table = Database.query_database(self, sql_statement)
        Database.commit_query(self)
        print("Таблица клиентов создана")
        return client_table

    def create_phone_table(self):
        sql_statement = """
        CREATE TABLE IF NOT EXISTS Phone(
        phone_id SERIAL PRIMARY KEY,
        phone_number VARCHAR(15) NOT NULL,
        client_id INTEGER NOT NULL REFERENCES Client(client_id));
        """
        phone_table = Database.query_database(self, sql_statement)
        Database.commit_query(self)
        print("Таблица телефонов создана")
        return phone_table

test = CreateTables()
test.create_client_table()
test.create_phone_table()


class OperateDatabase(CreateTables):
    def add_client(self, name, surname, email, phone_number=None):
        sql_statement = """
        SELECT client_id
        FROM Client        
        WHERE name = %s OR surname = %s OR email = %s;
        """
        client_in_database = CreateTables.fetch_one(self, sql_statement, (name, surname, email))
        print(f"The Client in Database is:>> {client_in_database}")
        sql_statement2 = """
        INSERT INTO Client (name, surname, email) VALUES (%s, %s, %s) RETURNING client_id;
        """
        print(self.fetch_one(sql_statement2, (name, surname, email)))
        sql_statement3 = """
        INSERT INTO Phone(phone_number, client_id) VALUES(%s, %s) RETURNING phone_id, phone_number;
        """
        if not client_in_database:
            if phone_number:
                CreateTables.query_database(self, sql_statement2, (name, surname, email))
                CreateTables.commit_query(self)
                print('часть 1 готова')
                print(Database.fetch_one(self, sql_statement2, (name, surname, email, phone_number)))
                CreateTables.query_database(self, sql_statement3, (phone_number, Database.fetch_one(self, sql_statement2, (name, surname, email, phone_number))))
                CreateTables.commit_query(self)
                print('Клиент с номером добавлен')
                # self.conn.close()
                return True
            CreateTables.query_database(self, sql_statement2, (name, surname, email))
                # self.conn.close()
            print('Клиент добавлен без номера')
            return True
            # self.conn.close()
        print('Такой Клиент уже есть в базе')
        return False



test = OperateDatabase()
# print(test.add_client('Zoya', 'Cosm', 'cc3zf2zs2ssffz@mail.ru', '+79263503777'))
test.add_client('Paul', 'Zac', 'ppf2fss32sz@mail.ru')
