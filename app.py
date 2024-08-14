from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
from pyspark.sql.functions import col, weekofyear, year
from pyspark.sql.functions import to_timestamp, date_format, concat_ws, col, to_date



def fix_timestamp(dataframe):
    dataframe = dataframe.withColumn("new_timestamp", to_date("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    return dataframe

def parse(session, dataset_path):

    # Load Dataset from Source
    dataframe = session.read.json(dataset_path)

    # Convert the 'timestamp' column to timestamp type and add it as a new column
    dataframe = fix_timestamp(dataframe)
    # Save dataframes to seperate parquet files to disk based on event type
    app_loaded_events = dataframe.filter(dataframe.event == "app_loaded")
    registered_events = dataframe.filter(dataframe.event == "registered")
    app_loaded_events.write.mode("overwrite").parquet("user/events/app_loaded/")
    registered_events.write.mode("overwrite").parquet("user/events/registered/")

query = """
    SELECT DISTINCT r.initiator_id
    FROM registered r
    JOIN apploaded a
    ON r.initiator_id = a.initiator_id
    AND 
    ((r.year = a.year AND r.week_of_year = a.week_of_year-1)
    OR (r.year = a.year + 1 AND (r.week_of_year = 1 AND a.week_of_year = 53)))

    """

query2 = '''
    SELECT DISTINCT initiator_id FROM registered
'''

def statistics(session):
    # Load the parquet files into DataFrames
    app_loaded_df = session.read.parquet("user/events/app_loaded/")
    registered_df = session.read.parquet("user/events/registered/")

    # Extract year and week from the timestamp
    app_loaded_df = app_loaded_df.withColumn("year", year("new_timestamp")).withColumn("week_of_year", weekofyear("new_timestamp"))
    registered_df = registered_df.withColumn("year", year("new_timestamp")).withColumn("week_of_year", weekofyear("new_timestamp"))

    # Create temporary views for SQL operations
    app_loaded_df.createOrReplaceTempView("apploaded")
    registered_df.createOrReplaceTempView("registered")

    # Run Queries
    dfsql = session.sql(query) 
    dfsql_all_users = session.sql(query2)

    # Calculate the statistics
    registered_users = dfsql.count()  # users that registered themselves the week after loading the app
    total_users_count = dfsql_all_users.count()  # total users
    percentage = (registered_users / total_users_count) * 100

    return percentage

session = SparkSession.builder.appName("Running PySpark Application....")\
.master("local[*]")\
.getOrCreate()

print(session.sparkContext.appName)

'''Parse Mode'''
print("***PARSE MODE***")
dataset_path = "dataset.json"
parse(session, dataset_path)

'''Statistics Mode'''
print("***STATISTICS MODE***")
print("Percentage of Users who registered one week after loading the application: ", statistics(session))
