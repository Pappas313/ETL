from neomodel import StructuredNode, StringProperty, IntegerProperty, ArrayProperty, RelationshipTo, config, db

config.DATABASE_URL = 'bolt://neo4j:test@localhost:7687'


class User(StructuredNode):

    natural_key = IntegerProperty(unique_index=True)  # For convenient User fetching/retrieval.
    name = StringProperty()
    product_ids = ArrayProperty(IntegerProperty())

    has_friend = RelationshipTo('User', 'HAS_FRIEND')


def run_cypher_query(query, params):
    # TODO Implement.
    results, meta = db.cypher_query(query, params)
    import pprint
    pprint.pprint(results)
    pprint.pprint(meta)
