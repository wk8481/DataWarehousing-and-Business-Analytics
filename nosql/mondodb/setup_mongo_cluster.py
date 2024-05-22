import subprocess
import time
from pymongo import MongoClient
from pymongo.errors import OperationFailure

MONGO_PATH = r'"C:\Program Files\MongoDB\Server\7.0\bin"'

def start_mongo_process(command):
    process = subprocess.Popen(f"{MONGO_PATH}\\{command}", shell=True)
    return process

# Start the Config Server
start_mongo_process('mongod --configsvr --replSet "configrs" --dbpath c:/data/rsconfigdb/rep1 --port 31001')

# Start Shard 1
start_mongo_process('mongod --shardsvr --replSet "rsShard1" --dbpath c:/data/rsShard1/rep1 --port 30002')

# Start Shard 2
start_mongo_process('mongod --shardsvr --replSet "rsShard2" --dbpath c:/data/rsShard2/rep1 --port 30003')

# Start Shard 3
start_mongo_process('mongod --shardsvr --replSet "rsShard3" --dbpath c:/data/rsShard3/rep1 --port 30004')

# Start the Mongos Router
start_mongo_process('mongos --configdb configrs/localhost:31001 --port 27020')

# Give MongoDB instances time to start up
time.sleep(10)

def initiate_replica_set(client, config):
    try:
        client.admin.command("replSetInitiate", config)
        print(f"Replica set {config['_id']} initiated successfully.")
    except OperationFailure as e:
        print(f"Error initiating replica set {config['_id']}: {e}")

# Connect to Config Server and initiate the replica set
config_rs_config = {
    "_id": "configrs",
    "configsvr": True,
    "members": [
        {"_id": 0, "host": "localhost:31001"}
    ]
}
client = MongoClient("mongodb://localhost:31001/")
initiate_replica_set(client, config_rs_config)

# Connect to Shard 1 and initiate the replica set
rs1_config = {
    "_id": "rsShard1",
    "members": [
        {"_id": 0, "host": "localhost:30002"}
    ]
}
client = MongoClient("mongodb://localhost:30002/")
initiate_replica_set(client, rs1_config)

# Connect to Shard 2 and initiate the replica set
rs2_config = {
    "_id": "rsShard2",
    "members": [
        {"_id": 0, "host": "localhost:30003"}
    ]
}
client = MongoClient("mongodb://localhost:30003/")
initiate_replica_set(client, rs2_config)

# Connect to Shard 3 and initiate the replica set
rs3_config = {
    "_id": "rsShard3",
    "members": [
        {"_id": 0, "host": "localhost:30004"}
    ]
}
client = MongoClient("mongodb://localhost:30004/")
initiate_replica_set(client, rs3_config)

# Connect to the Mongos Router and add the shards
mongos_client = MongoClient("mongodb://localhost:27020/")

try:
    mongos_client.admin.command("addShard", "rsShard1/localhost:30002")
    mongos_client.admin.command("addShard", "rsShard2/localhost:30003")
    mongos_client.admin.command("addShard", "rsShard3/localhost:30004")
    print("Shards added successfully.")
except OperationFailure as e:
    print(f"Error adding shards: {e}")

# Enable sharding on the database and shard the collection
try:
    mongos_client.admin.command("enableSharding", "mydatabase")
    mongos_client.admin.command("shardCollection", "mydatabase.treasures", key={"city_name": 1})
    print("Sharding enabled and collection sharded successfully.")
except OperationFailure as e:
    print(f"Error enabling sharding: {e}")
