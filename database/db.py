import mysql.connector
from mysql.connector import Error

class DatabaseController:

    def __init__(self, host, user, password, database=None):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.connect()  # Connect once upon instantiation

    def connect(self):
        try:
            # Create the DB if not exists
            temp_conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            temp_cursor = temp_conn.cursor()
            temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            temp_conn.commit()
            temp_cursor.close()
            temp_conn.close()

            # Now connect to the actual DB and store the connection
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                print(f"[DATABASE]: Connected to database '{self.database}'")

        except Error as e:
            print(f"[DATABASE]: Error while connecting to MySQL: {e}")

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Connection closed")

    def create_table(self, table_name, columns):
        try:
            cursor = self.connection.cursor()
            columns_with_types = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types})")
            print(f"[DATABASE]: Table '{table_name}' created or already exists.")
            cursor.close()
        except Error as e:
            print(f"[DATABASE]: Error while creating table: {e}")
    
    def insert_many(self, table_name, data_list):
        try:
            if not data_list:
                return

            print(f"[DATABASE]: Inserting {len(data_list)} rows into '{table_name}' table")
            cursor = self.connection.cursor()

            columns = ", ".join(data_list[0].keys())
            placeholders = ", ".join(["%s"] * len(data_list[0]))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            values = [tuple(data.values()) for data in data_list]
            cursor.executemany(sql, values)

            self.connection.commit()
            print(f"[DATABASE]: {len(data_list)} rows inserted into '{table_name}' table.")
            cursor.close()
        except Error as e:
            print(f"Error during batch insert: {e}")
            

    def fetch_batch(self, table_name, batch_size=100, offset=0, where_clause=None):
        query = f"SELECT * FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        query += f" LIMIT {batch_size} OFFSET {offset}"
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Error as e:
            print(f"[DATABASE]: Error fetching batch: {e}")
            return []

        

