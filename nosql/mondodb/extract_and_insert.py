import pyodbc
from pymongo import MongoClient
from config import SERVER, DATABASE_OP, DSN, USERNAME, PASSWORD, DRIVER

# Connect to the CATCHEM database
conn_str = f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_OP}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Execute the SQL query to retrieve the dataset
query = """
    SELECT
        t.id AS treasure_id,
        t.difficulty,
        t.terrain,
        c.city_name,
        co.name AS country_name,
        co.code3 AS country_code,
        s.container_size,
        s.description AS stage_description,
        s.latitude AS stage_latitude,
        s.longitude AS stage_longitude,
        s.sequence_number,
        s.type AS stage_type,
        s.visibility
    FROM
        treasure AS t
    INNER JOIN
        city AS c ON t.city_city_id = c.city_id
    INNER JOIN
        country AS co ON c.country_code = co.code
    INNER JOIN
        treasure_stages AS ts ON t.id = ts.treasure_id
    INNER JOIN
        stage AS s ON ts.stages_id = s.id
"""
cursor.execute(query)
treasures_data = cursor.fetchall()

# Close the connection to the CATCHEM database
conn.close()

# Connect to MongoDB through the mongos router
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["treasure_database"]
treasures_collection = db["treasures"]

# Insert data into the MongoDB collection
for treasure in treasures_data:
    treasures_collection.insert_one({
        "treasure_id": treasure[0],
        "difficulty": treasure[1],
        "terrain": treasure[2],
        "city_name": treasure[3],
        "country_name": treasure[4],
        "country_code": treasure[5],
        "stage": {
            "container_size": treasure[6],
            "description": treasure[7],
            "latitude": treasure[8],
            "longitude": treasure[9],
            "sequence_number": treasure[10],
            "type": treasure[11],
            "visibility": treasure[12]
        }
    })

# Enable sharding on the database and shard the collection
admin_db = mongo_client.admin
admin_db.command("enableSharding", "treasure_database")
admin_db.command("shardCollection", "treasure_database.treasures", key={"city_name": 1})

# Print confirmation message
print("Sharding enabled and collection sharded successfully.")
