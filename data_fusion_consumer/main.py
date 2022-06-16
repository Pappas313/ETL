import json
import logging
import time

import fire
from kafka import KafkaConsumer
from pymongo import MongoClient

# noinspection PyArgumentList
logging.basicConfig(filename='logs/data_fusion_consumer.log', level=logging.DEBUG)
logger = logging.getLogger('data_fusion_consumer')

mongo_client = MongoClient('localhost', 27017)
mongo_database = mongo_client.get_database('etl')

# mongo_database.create_collection('product')
products_collection = mongo_database.get_collection('product')

# mongo_database.create_collection('user_cache')
user_cache_collection = mongo_database.get_collection('user_cache')


def _process_user_cache(_id, _product_id) -> None:
    results = user_cache_collection.find({'product_id', _product_id})
    user_id_list = []
    for result in results:
        user_id_list.append(result['user_id'])
    if len(user_id_list) <= 0:
        return
    products_collection.update_one(
        {
            '_id': _id
        },
        {
            '$set': {
                'user_id_list': user_id_list,
            }
        }
    )


def process_product_payload(payload) -> None:
    try:
        product = products_collection.find_one({'product_id': payload['product_id']})
    except:
        product = None
    if product is not None:
        logger.debug(f'Product with ID {payload["product_id"]} already exists! Aborting...')
        return
    product_document = {
        # '_id': None  # Will be populated automatically.
        **payload,
        'user_id_list': []  # Init.
    }
    result = products_collection.insert_one(product_document)
    _id = result.inserted_id
    _process_user_cache(_id, product_document)


def process_user_payload(payload) -> None:
    user_product_associations = []
    for product_id in payload['user_product_ids']:
        user_product_associations.append({
            'user_id': payload['user_id'],
            'product_id': product_id
        })

    for user_product_association in user_product_associations:
        user_id = user_product_association['user_id']
        product_id = user_product_association['product_id']
        try:
            product_document = products_collection.find_one({'product_id': product_id})
        except:
            product_document = None
        if product_document is None:
            print(
                f'Product with ID : {product_id} does not exist. '
                f'Adding user_product_association to queue to later processing')

            user_cache_collection.insert_one({
                **user_product_association
            })

            continue
        if user_id not in product_document['user_id_list']:
            user_id_list = list(product_document['user_id_list'])
            user_id_list.append(user_id)
            products_collection.update_one(
                {
                    '_id': product_document['_id']
                },
                {
                    '$set': {
                        'user_id_list': user_id_list,
                    }
                }
            )


class Consumer:

    def __init__(self):
        pass

    def invoke(self) -> None:
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        consumer.subscribe(['products-topic', 'users-topic'])

        while True:
            try:
                messages = consumer.poll(10.0)

                if not messages:
                    time.sleep(10)

                for key, value in messages.items():
                    for record in value:
                        try:
                            topic = record.topic
                            payload = record.value
                            logger.info(f'Processing message with ID {None} from topic : {topic}')
                            if topic == 'products-topic':
                                process_product_payload(payload)
                            elif topic == 'users-topic':
                                process_user_payload(payload)
                            else:
                                continue
                        except Exception as e:
                            print(f'Error: {e}')
            except Exception as e:
                print(f'Unhandled error! : {e}')  # TODO Handle.
            finally:
                # consumer.close()  # TODO Complete
                pass


# noinspection PyMethodMayBeStatic
class CommandLineInterface(object):

    def invoke_consumer(self) -> None:
        consumer = Consumer()
        consumer.invoke()


def main():
    fire.Fire(CommandLineInterface)
