# ETL Assignment

## Manual

```
# Activate virtual env.
# Example (linux): source venv/bin/activate
# Example (Windows): venv\Scripts\activate.bat

# Install Dependencies
pip install -r requirements.txt

# Start MySQL, Neo4j, MongoDB, Kafka, and ZooKeeper.
cd docker
docker-compose -f application.yml up -d

cd ..
python -m er_producer initialize_datasource     # Init MySQL and Generate random data
python -m graph_producer initialize_datasource  # Init Neo4j and Generate random data

# Open 4 windows and then run:
python -m er_producer invoke_producer
python -m graph_producer invoke_producer
python -m data_fusion_consumer invoke_consumer
python -m rest_api
```

## Dependencies

```
APScheduler               # Scheduler.
asyncio                   # Asynchronous concurrent code.
Faker                     # Fake data generator.
fire                      # CLI Generator.
mysql-connector-python    # Python MySQL module.
neo4j                     # Python Neo4j module.
kafka-python              # Python Kafka module.
neomodel
pymongo                   # Python MongoDB module.
flask
flask_restful
```
