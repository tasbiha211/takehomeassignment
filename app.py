from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
from pyspark.sql.functions import col, weekofyear, year


def parse(session):
    dataframe = session.read.json("dataset.json")
    dataframe.printSchema()
    dataframe.show()
    app_loaded_events = dataframe.filter(dataframe.event == "app_loaded")
    registered_events = dataframe.filter(dataframe.event == "registered")
    app_loaded_events.write.mode("overwrite").parquet("user/events/app_loaded/")
    registered_events.write.mode("overwrite").parquet("user/events/registered/")


def statistics(session):
    app_loaded_df = session.read.parquet("user/events/app_loaded/")
    registered_df = session.read.parquet("user/events/registered/")

    app_loaded_df = app_loaded_df.withColumn("year", year("timestamp")).withColumn("week_of_year", weekofyear("timestamp"))    

    registered_df = registered_df.withColumn("year", year("timestamp")).withColumn("week_of_year", weekofyear("timestamp"))   

    # Extract year and week from the timestamp
    app_loaded_df.printSchema()
    registered_df.printSchema()
    return app_loaded_df, registered_df


print("Application Started")
session = SparkSession.builder.appName("first pyspark application")\
.master("local[*]")\
.getOrCreate()


print(session.sparkContext.appName)

'''Modes'''
parse(session)
app_loaded_df, registered_df = statistics(session)

app_loaded_df.createOrReplaceTempView("apploaded")
registered_df.createOrReplaceTempView("registered")


query = """
SELECT DISTINCT r.initiator_id
FROM registered r
JOIN apploaded a
ON r.initiator_id = a.initiator_id
AND (
    (r.year = a.year AND r.week_of_year = a.week_of_year - 1)
    OR 
    (r.year = a.year - 1 AND r.week_of_year = 52 AND a.week_of_year = 1)
)
"""


dfsql = session.sql(query)
dfsql.printSchema()
dfsql.show()

registered_users = dfsql.count() ## users that registered themselves week after loading app

total_users_count = app_loaded_df.select("initiator_id").distinct().count()  ## total users

percentage = (registered_users / total_users_count) * 100

print(percentage)
