import psycopg2
import logging


class PgProcessing:

    def __init__(self, env):
        self.username = env.get('username')
        self.password = env.get('password')
        self.hostname = env.get('hostname')
        self.port = env.get('port')
        self.database = env.get('database')
        self.__connect_to_pg__()

    def __connect_to_pg__(self):
        self.connection = psycopg2.connect(
            user=self.username,
            password=self.password,
            host=self.hostname,
            port=self.port,
            database=self.database)
        self.cursor = self.connection.cursor()

    def create_table(self):
        self.cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS real_estate_properrties.IndexHrApartments 
                    (id INT PRIMARY KEY NOT NULL,
                    Title VARCHAR(1000) NULL,
                    Location VARCHAR(50) NULL,
                    PriceEuro INT NULL,
                    PriceKuna INT NULL,
                    Area INT NULL,
                    cijenakvadrateuro DECIMAL NULL,
                    cijenakvadratkuna DECIMAL NULL,
                    Posted VARCHAR(100) NULL,
                    Link VARCHAR(500) NULL,
                    Photo VARCHAR(500) NULL);
            """)

    def insert_data(self, df):

        for index, row in df.iterrows():
            self.cursor.execute(
                """
                    INSERT INTO real_estate_properrties.IndexHrApartments(
                        id,
                        title,
                        location,
                        priceeuro,
                        pricekuna,
                        area,
                        cijenakvadrateuro,
                        cijenakvadratkuna,
                        posted,
                        link,
                        photo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """,
                        (row.Id,
                         row.Title,
                         row.Location,
                         row.PriceEuro,
                         row.PriceKuna,
                         row.Area,
                         row.CijenaKvadratEuro,
                         row.CijenaKvadratKuna,
                         row.Posted,
                         row.Link,
                         row.Photo,))

    def commit_connections(self):
        self.connection.commit()
        logging.info(f'Podatci s Index.hr oglasnika uredno zapisani u bazu [commit ok].')

    def rollback_connections(self):
        self.connection.rollback()
        logging.info(f'Zapisivanje podataka s Index.hr oglasnika uredno zaustavljeno [rollback ok].')

    def closing_connections(self):
        self.connection.close()
        logging.info(f'Zatvorena konekcije prema Postgresql Serveru.')
