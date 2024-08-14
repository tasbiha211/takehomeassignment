import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, weekofyear, year, to_date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
from pyspark.sql.functions import col, weekofyear, year
from pyspark.sql.functions import to_timestamp, date_format, concat_ws, col, to_date
from app import fix_timestamp, query, parse  # Import the function

class TestFixTimestampFunction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("TestFixTimestamp").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_fix_timestamp(self):
        # Define schema for the test data
        schema = StructType([
            StructField("event", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

        # Create test data
        data = [
            {"event": "app_loaded", "timestamp": "2020-11-03T06:24:42.000Z"},
            {"event": "registered", "timestamp": "2020-12-11T07:00:14.000Z"},
            {"event": "app_loaded", "timestamp": "2021-11-17T06:24:42.000Z"},
        ]

        # Create a DataFrame
        dataframe = self.spark.createDataFrame(data, schema)

        # Call the fix_timestamp function
        result_df = fix_timestamp(dataframe)

        # Collect results
        result = result_df.select(col("new_timestamp").cast("string")).collect()

        # Expected dates in YYYY-MM-DD format
        expected_dates = ["2020-11-03", "2020-12-11", "2021-11-17"]

        # Assert the new_timestamp is correct
        for row, expected_date in zip(result, expected_dates):
            date_str = row["new_timestamp"]
            self.assertEqual(date_str, expected_date, f"Expected {expected_date}, but got {date_str}")

    def test_weeks_in_different_years(self):
        # Define schema for the test data
        schema = StructType([
            StructField("initiator_id", StringType()),
            StructField("event", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

        # Create test data
        app_loaded_data = [
            {"initiator_id":"1", "event": "app_loaded", "timestamp": "2020-12-29T06:24:42.000Z"},
            {"initiator_id":"2", "event": "app_loaded", "timestamp": "2021-12-26T07:00:14.000Z"},
            {"initiator_id":"3", "event": "app_loaded", "timestamp": "2021-11-17T06:24:42.000Z"},
            {"initiator_id":"4", "event": "app_loaded", "timestamp": "2021-11-18T06:24:42.000Z"},
            
        ]

        registered_data = [
            {"initiator_id":"1", "event": "registered", "timestamp": "2021-01-02T06:24:42.000Z"},
            {"initiator_id":"2", "event": "registered", "timestamp": "2022-01-01T07:00:14.000Z"},
            {"initiator_id":"3", "event": "registered", "timestamp": "2021-12-17T06:24:42.000Z"},
            {"initiator_id":"4", "event": "registered", "timestamp": "2021-12-18T06:24:42.000Z"},
            
        ]
         # Create a DataFrame
        app_loaded_df = self.spark.createDataFrame(app_loaded_data, schema)
        registered_df = self.spark.createDataFrame(registered_data, schema)

        app_loaded_df = app_loaded_df.withColumn("new_timestamp", to_date("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        registered_df = registered_df.withColumn("new_timestamp", to_date("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        app_loaded_df = app_loaded_df.withColumn("year", year("new_timestamp")).withColumn("week_of_year", weekofyear("new_timestamp"))
        registered_df = registered_df.withColumn("year", year("new_timestamp")).withColumn("week_of_year", weekofyear("new_timestamp"))

        # Select specific columns from app_loaded_df and show
        app_loaded_df.select("initiator_id", "event", "new_timestamp", "year", "week_of_year").show(truncate=False)

        # Select specific columns from registered_df and show
        registered_df.select("initiator_id", "event", "new_timestamp", "year", "week_of_year").show(truncate=False)

       

        app_loaded_df.createOrReplaceTempView("apploaded")
        registered_df.createOrReplaceTempView("registered")


        dfsql = self.spark.sql(query)

        result = dfsql.count() 
        expected_result =2
        print(result)

        self.assertEqual(expected_result, result)


# Run the tests
if __name__ == '__main__':
    unittest.main()
