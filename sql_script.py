import psycopg2
import pandas as pd

class sql_script:
    def __init__(self, db_host, db_name, db_user, db_password, db_port):
        """Initialize connection parameters and connect to the database."""
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_port = db_port
        self.conn = None
        self.cur = None

        self.connect()

    def connect(self):
        """Establish connection to the database."""
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                port=self.db_port
            )
            self.cur = self.conn.cursor()
            print("Connected to the database successfully!")
        except Exception as e:
            print(f"Database connection failed: {e}")

    def table_exists(self, table_name):
        self.cur.execute("""
                SELECT EXISTS(
                    SELECT FROM information_schema.tables
                         WHERE table_name = %s    
                );
                         """, (table_name,))
        return self.cur.fetchone()[0]

    def create_table(self, df, table_name):
        
        column_string = ", ".join([f'"{col}" TEXT' for col in df.columns])
        print(f"Columns: {column_string}")
        create_table_sql = f"CREATE TABLE {table_name} ({column_string})"
        self.cur.execute(create_table_sql)
        add_primary_key = f"ALTER TABLE {table_name} ADD CONSTRAINT id PRIMARY KEY(id);"
        self.cur.execute(add_primary_key)
        print(f'Table {table_name} created!')
    
    def insert_table(self, df, table_name="reddit_posts"):
        """
        Insert a pandas DataFrame into the specified PostgreSQL table.

        Parameters:
        - df (pd.DataFrame): DataFrame containing the data to insert
        - table_name (str): Name of the table (default is 'reddit_posts')
        """
        if self.conn is None:
            print("No active database connection.")
            return
        
        # Open a cursor
        cursor = self.cur;


        if not self.table_exists(table_name):
            print(f"⚠️ Table '{table_name}' does not exist. Creating table...")
            self.create_table(df, table_name)
        

        # Generate column names dynamically
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))

        # Insert data row by row
        insert_query = f"""INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) 
        ON CONFLICT(id) DO UPDATE SET title=EXCLUDED.title, score=EXCLUDED.score, num_comments=EXCLUDED.num_comments"""

        try:
            for row in df.itertuples(index=False, name=None):
                cursor.execute(insert_query, row)

            self.conn.commit()
            print(f"Successfully inserted {len(df)} records into {table_name}.")
        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting data: {e}")
        finally:
            cursor.close()

    def close_connection(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

