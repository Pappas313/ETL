import asyncio
import json
import logging
import random
from typing import Dict, List

import fire
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from faker import Faker
from kafka import KafkaProducer

from ._neo4j_neomodel import User

USERS_COUNT: int = 500

# noinspection PyArgumentList
# logging.basicConfig(filename='logs/graph_producer.log', level=logging.DEBUG)
logger = logging.getLogger('graph_producer')


def random_but_existing_product_ids() -> List[int]:
    random_number_of_product_ids: int = random.randint(1, 10)
    product_ids: List[int] = []
    counter: int = 0
    while counter < random_number_of_product_ids:
        random_product_id: int = random.randint(1, 999)
        product_ids.append(random_product_id)
        counter = counter + 1
    return product_ids


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DataSource:

    def initialize(self) -> None:
        fake_data_generator = Faker()

        # Create 500 Users
        # 1 root and 499 random user.
        # root user exists to ensure that all User will be traversed by the producer.
        # graph traversal: Depth First Search starting from the root User.

        root = User(natural_key=0, name='ROOT', product_ids=random_but_existing_product_ids()).save()

        counter: int = 1
        while counter < USERS_COUNT:
            random_name = fake_data_generator.name()
            User(natural_key=counter, name=random_name, product_ids=random_but_existing_product_ids()).save()
            counter = counter + 1

        # Iterate Users and create random relations.
        # - Random number of friends (1 - 6)
        # - Random friend for each slot.
        counter: int = 1
        while counter < USERS_COUNT:
            user = User.nodes.get_or_none(natural_key=counter)
            if user is None:
                logger.error(f'User with natural_key {counter} does not exist!')
                counter = counter + 1
                continue

            root.has_friend.connect(user)

            number_of_friends = random.randint(1, 6)

            inner_counter: int = 0
            while inner_counter < number_of_friends:
                random_friend_natural_key = random.randint(1, USERS_COUNT - 1)
                if random_friend_natural_key == counter:
                    inner_counter = inner_counter + 1
                    continue

                friend = User.nodes.get_or_none(natural_key=random_friend_natural_key)
                if friend is None:
                    logger.error(f'User (Friend) with natural_key {random_friend_natural_key} does not exist!')
                    inner_counter = inner_counter + 1
                    continue

                user.has_friend.connect(friend)

                inner_counter = inner_counter + 1

            counter = counter + 1


class Producer:

    def __init__(self):
        self.scheduler = AsyncIOScheduler(timezone='Europe/Athens')
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.produce_count: int = 0

        # TODO TEMPORARY! CHANGE ASAP!
        users_nodes = User.nodes.all()
        user_list = []
        for user_node in users_nodes:
            user = {
                'user_id': user_node.natural_key,
                'user_name': user_node.name,
                'user_product_ids': user_node.product_ids
            }
            user_list.append(user)
        chunk: int = 5
        self.user_slices = [user_list[i:i+chunk] for i in range(0, len(user_list), chunk)]

    def produce_message(self, payload: Dict) -> None:
        try:
            self.producer.send('users-topic', payload)
        except Exception as e:
            logger.error('Failed to produce message', exc_info=e)

    def produce_messages(self) -> None:
        logger.info(f'Producing messages ({self.produce_count})')
        print(f'Producing messages ({self.produce_count})')

        # TODO TEMPORARY!
        try:
            user_list = self.user_slices[self.produce_count]
        except IndexError:
            return

        for user in user_list:
            logger.info(f'- Producing message for User with user_id : {user["user_id"]}')
            print(f'- Producing message for User with user_id : {user["user_id"]}')
            self.produce_message(payload=user)
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
