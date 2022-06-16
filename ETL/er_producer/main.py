import asyncio
import json
import logging
from typing import Dict

import fire
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from faker import Faker
from kafka import KafkaProducer

from ._mysql import (
    create_connection,
    create_database,
    execute_query,
    execute_read_query
)

# noinspection PyArgumentList
logging.basicConfig(filename='logs/er_producer.log', level=logging.DEBUG)
logger = logging.getLogger('er_producer')


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DataSource:

    def initialize(self) -> None:
        connection = create_connection('localhost', 'root', '')

        create_database_query = 'CREATE DATABASE etl'
        create_database(connection, create_database_query)

        connection = create_connection('localhost', 'root', '', 'etl')

        drop_table = """
        DROP TABLE IF EXISTS products;
        """
        execute_query(connection, drop_table)

        create_table = """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT AUTO_INCREMENT, 
            name TEXT NOT NULL,
            description TEXT NOT NULL, 
            tags TEXT NOT NULL, 
            PRIMARY KEY (product_id)
        ) ENGINE = InnoDB
        """
        execute_query(connection, create_table)

        fake_data_generator = Faker()
        counter: int = 0
        while counter < 1000:
            name: str = fake_data_generator.bothify(text='PN-????-####')
            description: str = fake_data_generator.paragraph(nb_sentences=1)
            tags: str = 'none'  # TODO Add tags

            create_users = f"""
            INSERT INTO
            `products` (`name`, `description`, `tags`)
            VALUES
            ('{name}', '{description}', '{tags}');
            """
            execute_query(connection, create_users)

            counter = counter + 1

        select_products_query = "SELECT * FROM products"
        products = execute_read_query(connection, select_products_query)

        # Debug.
        import pprint
        pprint.pprint(products)


class Producer:

    def __init__(self):
        self.connection = create_connection('localhost', 'root', '', 'etl')
        self.scheduler = AsyncIOScheduler(timezone='Europe/Athens')
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.produce_count: int = 0

    def produce_message(self, payload: Dict) -> None:
        try:
            self.producer.send('products-topic', payload)
        except Exception as e:
            logger.error('Failed to produce message', exc_info=e)

    def produce_messages(self) -> None:
        logger.info(f'Producing messages ({self.produce_count})')
        print(f'Producing messages ({self.produce_count})')
        offset = self.produce_count * 10
        select_products_query = f'SELECT * FROM products LIMIT {offset}, 10;'
        products = execute_read_query(self.connection, select_products_query)
        for product in products:
            p_id, p_name, p_description, p_tags = product
            logger.info(f'- Producing message for Product with product_id : {p_id}')
            print(f'- Producing message for Product with product_id : {p_id}')
            self.produce_message(payload={
                'product_id': p_id,
                'product_name': p_name,
                'product_description': p_description,
                'product_tags': p_tags,
            })
        self.produce_count = self.produce_count + 1

    def invoke(self) -> None:
        self.scheduler.add_job(self.produce_messages, 'cron', second='0,20,40')
        self.scheduler.start()

        try:
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            logger.info('Exiting...')


# noinspection PyMethodMayBeStatic
class CommandLineInterface(object):

    def initialize_datasource(self) -> None:
        datasource = DataSource()
        datasource.initialize()

    def invoke_producer(self) -> None:
        producer = Producer()
        producer.invoke()


def main():
    fire.Fire(CommandLineInterface)
