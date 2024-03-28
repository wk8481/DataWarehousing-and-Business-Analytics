import pyodbc
import pandas as pd

from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
from dwh import establish_connection

# TODO: complete function for create dim table
def create_dim_treasure_type_table(conn):
    """
    Create "dimTreasureType" table if it doesn't exist in the data warehouse
    arg: (pyodbc.Connection): Database connection object
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dimTreasureType')
        BEGIN
            CREATE TABLE dimTreasureType (
                treasureType_SK INT IDENTITY(1,1) PRIMARY KEY,
                difficulty INT NOT NULL,
                terrain INT NOT NULL,
                size INT NOT NULL,
                visibility INT NULL
            )
        END
        """)
        cursor.commit()
        print("Table 'dimTreasureType' created  successfully or already exist")
    except pyodbc.Error as e:
        print(f"Error creating table 'dimTreasureType': {e}")


    



def fill_dim_treasure_type_table(cursor):
    """
    Fills the 'dimTreasureType' table with all possible combinations.
    Args:
        cursor (pyodbc.Cursor): Cursor object for executing SQL commands.
    """
    try:
        for difficulty in range(5):  # Assuming 0-4 range for difficulty
            for terrain in range(5):  # Assuming 0-4 range for terrain
                for size in range(1, 4):  # Assuming 1-3 range for size
                    for visibility in range(3):  # Assuming 0-2 range for visibility
                        insert_query = """
                        INSERT INTO dimTreasureType (difficulty, terrain, size, visibility)
                        VALUES (?, ?, ?, ?)
                        """
                        cursor.execute(insert_query, (difficulty, terrain, size, visibility))

        cursor.commit()
        print("Treasure types inserted into 'dimTreasureType' table successfully.")
    except pyodbc.Error as e:
        print(f"Error inserting into 'dimTreasureType' table: {e}")


def main():

    conn_dwh = establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)

    if conn_dwh:
        cursor = conn_dwh.cursor()
        # create dimTreasureType table
        create_dim_treasure_type_table(conn_dwh)

        # Fill dimTreasureType table with all possible combinations
        fill_dim_treasure_type_table(cursor)

        # Close cursor and connection
        cursor.close()
        conn_dwh.close()


if __name__ == "__main__":
    main()












# # Get unique treasure IDs with difficulty and terrain
# treasure_id_query = """
#             SELECT DISTINCT treasure_id, diffucutlty, terrain
#             FROM catchem.dbo.treasure
#             """
# cursor_op.execute(treasure_id_query)
# treasure_data = cursor_op.fetchall()
#
#
#
#
# # Create Pandas DataFrame
# df = pd.DataFrame(treasure_data, columns=['id', 'difficulty', 'terrain'])
#
# # Group by difficulty and terrain for unique combinations
# df_grouped = df.groupby(['difficulty', 'terrain']).agg(id=('id', 'first'))
#
# # Calculate size (number of stages) for each combination
# size_query = """
#                 SELECT ts.treasure_id, COUNT(*) AS size
#                 FROM catchem.dbo.treasure_stages ts
#                 GROUP BY ts.treasure_id
#                 """
# cursor_op.execute(size_query)
# size_data = cursor_op.fetchall()
# size_df = pd.DataFrame(size_data, columns=['treasure_id', 'size'])
#
# # Merge size data using ID as the key
# df_merged = df_grouped.merge(size_df, how='left', on='id')
#
# # Get visibility data, assuming mapping between stage type and visibility
# visibility_query = """
#                 SELECT s.type, s.visibility
#                 FROM catchem.dbo.stage s
#                 """
# cursor_op.execute(visibility_query)
# visibility_data = cursor_op.fetchall()
# visibility_df = pd.DataFrame(visibility_data, columns=['type', 'visibility'])
#
# # Option 1: Using apply (easy to understand)
# df_merged['Visibility'] = df_merged['id'].apply(
#                 lambda x: visibility_df[visibility_df['type'] == x]['visibility'].iloc[0]
#             )
#
# # Option 2: Vectorized using merge (potentially more efficient)
# # df_merged = df_merged.merge(visibility_df.rename(columns={'type': 'id'}), how='left', on='id')
# # df_merged['Visibility'] = df_merged['visibility'].fillna(0)  # Handle missing values (if applicable)
#
# # Insert data into dimTreasureType (excluding auto-generated ID)
# for index, row in df_merged.iterrows():
#                 difficulty = row['difficulty']
#                 terrain = row['terrain']
#                 size = row['size']
#                 visibility = row['Visibility']
#
#                 insert_query = f"""
#                     INSERT INTO dimTreasureType (difficulty, Terrain, Size, Visibility)
#                     VALUES (?, ?, ?, ?)
#                     """
#                 cursor_dwh.execute(insert_query, (difficulty, terrain, size, visibility))
#
# # Commit changes
# conn_dwh.commit()
# print(f"Successfully filled 'dimTreasureType' table.")
#
# Close the cursors and connections
# cursor_op.close()
# cursor_dwh.close()
# conn_op.close()
# conn_dwh.close()






#
#
# # Single SQL query with Common Table Expressions (CTEs)
# query = """
# WITH TreasureData AS (
#     SELECT DISTINCT t.id, t.difficulty, t.terrain
#     FROM catchem.dbo.treasure t
# ),
# StageVisibility AS (
#     SELECT s.type, s.visibility
#     FROM catchem.dbo.stage s
# ),
# TreasureSize AS (
#     SELECT COUNT(*) AS size
#     FROM catchem.dbo.stage s
#     WHERE
#     catchem.dbo.treasure_stages ts
#     GROUP BY ts.treasure_id
# )
#
# -- INSERT INTO dimTreasureType (id, difficulty, terrain, size, visibility)  -- Assuming separate ID
# -- OR:
# -- INSERT INTO dimTreasureType (difficulty, terrain, size, visibility)  -- Using existing ID
#
# --SELECT
# --    td.id,  -- Replace with generated ID if using surrogate key
# --    td.difficulty,
# --    td.terrain,
# --    ts.size,
# --    sv.visibility
# --FROM TreasureData td
# --INNER JOIN StageVisibility sv ON td.id = sv.type  -- Assuming type maps to visibility
# --INNER JOIN TreasureSize ts ON td.id = ts.treasure_id;
#
#   -- Uncomment and include this line
# """
#
# cursor_op.execute(query)
#
# for row in cursor_op.fetchall():
#   # insert data into the datawarehouse
#   insert_query = """
#   INSERT INTO dimTreasureType (Difficulty, Terrain, Size, Visibility)
#   VALUES (?,?,?,?)
#   """
#   cursor_dwh.execute(insert_query, row)
#
# conn_dwh.commit()
#
#
# print(f"Successfully populated 'dimTreasureType' table.")
#
# # Close the cursor and connection
# cursor_op.close()
# cursor_dwh.close()
# conn_op.close()
# conn_dwh.close()
#
#
# # CREATE TABLE dimTreasureType (
# #   TreasureType_SK INT IDENTITY(1,1) PRIMARY KEY,  -- Auto-incrementing surrogate key
# #   Difficulty INT NOT NULL,
# #   Terrain INT NOT NULL,
# #   Size INT NOT NULL,  -- Number of stages
# #   Visibility INT NOT NULL
# # );




#
# def fetch_stage_data(cursor_op):
#     """
#     Fetches data from the 'stage' table in the operational database.
#     Args:
#         cursor_op: The cursor object for the operational database.
#     Returns:
#         List of tuples containing stage data (visibility).
#     """
#     query = """
#     SELECT visibility
#     FROM [catchem].[dbo].[stage]
#     """
#     cursor_op.execute(query)
#     return cursor_op.fetchall()
#
# def fetch_treasure_data(cursor_op):
#     """
#     Fetches data from the 'treasure' table in the operational database.
#     Args:
#         cursor_op: The cursor object for the operational database.
#     Returns:
#         List of tuples containing treasure data (id, difficulty, terrain).
#     """
#     query = """
#     SELECT id, difficulty, terrain
#     FROM [catchem].[dbo].[treasure]
#     """
#     cursor_op.execute(query)
#     return cursor_op.fetchall()
#
# def populate_dim_treasure_type(cursor_dwh, stage_data, treasure_data):
#     """
#     Populates the 'dimTreasureType' table with treasure-related data.
#     Args:
#         cursor_dwh: The cursor object for the 'catchem_dwh' database.
#         stage_data: List of tuples containing stage data (visibility).
#         treasure_data: List of tuples containing treasure data (id, difficulty, terrain).
#     """
#     query = """
#     INSERT INTO dimTreasureType (Difficulty, Terrain, Size, Visibility)
#     VALUES (?, ?, ?, ?)
#     """
#
#     total_treasures = len(treasure_data)
#
#     # Initialize tqdm with total number of treasures for progress bar
#     with tqdm(total=total_treasures, desc="Populating dimTreasureType", unit="treasure") as pbar:
#         for treasure in treasure_data:
#             treasure_id = treasure[0]
#             difficulty = treasure[1]
#             terrain = treasure[2]
#
#             # Subquery to count the number of stages associated with the current treasure
#             subquery = """
#             SELECT COUNT(*)
#             FROM [catchem].[dbo].[treasure_stages]
#             WHERE treasure_id = ?
#             """
#             cursor_dwh.execute(subquery, (treasure_id,))
#             num_stages = cursor_dwh.fetchone()[0]
#
#             for stage in stage_data:
#                 if stage[0] == treasure_id:
#                     visibility = stage[0]
#                     break
#             else:
#               # If no matching stage found, set visibility to a default value or handle as needed
#               visibility = 0  # Default value, change to your desired default
#
#             # Execute the INSERT query
#             cursor_dwh.execute(query, (difficulty, terrain, num_stages, visibility))
#
#             # Increment progress bar
#             pbar.update(1)
#
#     conn_dwh.commit()  # Commit changes
#
#     print(f"Successfully populated 'dimTreasureType' table.")
#
# def main():
#     # Establish connection to operational and DWH databases
#     conn_op = establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
#     conn_dwh = establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
#
#     cursor_op = conn_op.cursor()
#     cursor_dwh = conn_dwh.cursor()
#
#     try:
#         # Fetch data from operational tables
#         stage_data = fetch_stage_data(cursor_op)
#         treasure_data = fetch_treasure_data(cursor_op)
#
#         # Populate dimTreasureType table
#         populate_dim_treasure_type(cursor_dwh, stage_data, treasure_data)
#
#         print("All tasks completed successfully.")
#     except Exception as e:
#         conn_dwh.rollback()  # Rollback changes if an error occurs
#         print(f"Error: {str(e)}")
#     finally:
#         # Close cursors and connections
#         cursor_op.close()
#         cursor_dwh.close()
#         conn_op.close()
#         conn_dwh.close()
#
# # Execute the main function
# if __name__ == "__main__":
#     main()






