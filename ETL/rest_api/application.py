from flask import Flask
from flask_restful import Resource, Api

from typing import Dict, Optional, List
from pymongo import MongoClient

app = Flask(__name__)
api = Api(app)


mongo_client = MongoClient('localhost', 27017)
mongo_database = mongo_client.get_database('etl')
products_collection = mongo_database.get_collection('product')


def get_product(product_id) -> Optional[Dict]:
    try:
        product_document = products_collection.find_one({'product_id': product_id})

        # Clean `sensitive` information.
        del product_document['_id']
        del product_document['user_id_list']

        return product_document
    except:
        return None


def get_products_by_user_id(user_id) -> List[Dict]:
    try:
        result = []
        product_documents = products_collection.find({'user_id_list': user_id})
        for product_document in product_documents:

            # Clean `sensitive` information.
            del product_document['_id']
            del product_document['user_id_list']

            result.append(product_document)

        return result
    except:
        return []


# noinspection PyMethodMayBeStatic
class ProductResource(Resource):

    def get(self, product_id):
        product = get_product(product_id=int(product_id))
        return product, 200 if product is not None else 404


# noinspection PyMethodMayBeStatic
class UserProductResource(Resource):

    def get(self, user_id):
        # IDEA: Check if user exists.
        product_list = get_products_by_user_id(user_id=int(user_id))
        return product_list, 200


# Resource based REST API.
api.add_resource(ProductResource, '/api/products/<int:product_id>')
api.add_resource(UserProductResource, '/api/users/<int:user_id>/products')


def main():
    app.run(debug=True)
