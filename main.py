from logic.indexHrScrape import IndexHrScrape
from logic.pgProcessing import PgProcessing
from logic.config import get_environment_variables
import logging
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

try:
    pocetak = time.time()
    logging.basicConfig(filename='logs.txt',
                        filemode='a',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info('Početak izvršavanja zadatka.')

    envVar = get_environment_variables()
    scrape = IndexHrScrape()
    pg = PgProcessing(envVar.get('pg'))

    list_page_content = scrape.get_page_content(15)
    df = scrape.get_df(list_page_content)
    df.to_csv(r'jupyter\index_apartments.csv', index=False)
    pg_create_table = pg.create_table()
    pg_insert_data = pg.insert_data(df)

except Exception:
    logging.exception('Dogodila se greška sljedećeg sadržaja:')
    pg.rollback_connections()
else:
    pg.commit_connections()
    logging.info('Uspješno izvršen zadatak.')
finally:
    pg.closing_connections()
    logging.info(f'Obrada trajala: {time.strftime("%H sati, %M minuta i %S sekundi.", time.gmtime(time.time() - pocetak))}\n')
    #logging.info('\n')
