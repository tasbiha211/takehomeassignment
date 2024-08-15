import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import to_date, year, weekofyear
from app import fix_timestamp, query  # Import the function


class TestSparkApp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("TestFixTimestamp").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        # Define schema for test data
        self.schema = StructType([
            StructField("initiator_id", StringType(), True),
            StructField("event", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

    def create_dataframe(self, data):
        return self.spark.createDataFrame(data, self.schema)

    def process_dataframe(self, dataframe):
        return dataframe.withColumn("new_timestamp", to_date("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
                        .withColumn("year", year("new_timestamp")) \
                        .withColumn("week_of_year", weekofyear("new_timestamp"))
    
    def run_query(self, app_loaded_data, registered_data):
         # Create and process DataFrames
        app_loaded_df = self.process_dataframe(self.create_dataframe(app_loaded_data))
        registered_df = self.process_dataframe(self.create_dataframe(registered_data))

        # Create temporary views for SQL
        app_loaded_df.createOrReplaceTempView("apploaded")
        registered_df.createOrReplaceTempView("registered")

        app_loaded_df.show()
        registered_df.show()

        # Execute query
        dfsql = self.spark.sql(query)
        return dfsql

    def test_fix_timestamp(self):

        '''
        Test to check if the timestamp is in the expected format i.e. Y-M-D
        '''
        print("Testing timestamp format")
        data = [
            {"event": "app_loaded", "timestamp": "2020-11-03T06:24:42.000Z"},
            {"event": "registered", "timestamp": "2020-12-11T07:00:14.000Z"},
            {"event": "app_loaded", "timestamp": "2021-11-17T06:24:42.000Z"},
        ]
    
        # Create and process the DataFrame
        dataframe = self.create_dataframe(data)
        result_df = fix_timestamp(dataframe)

        # Collect results
        result = result_df.select("new_timestamp").collect()

        # Expected dates in YYYY-MM-DD format
        expected_dates = ["2020-11-03", "2020-12-11", "2021-11-17"]

        # Assert the new_timestamp is correct
        for row, expected_date in zip(result, expected_dates):
            date_str = row["new_timestamp"].strftime("%Y-%m-%d")
            self.assertEqual(date_str, expected_date, f"Expected {expected_date}, but got {date_str}")

    def test_weeks_in_different_years(self):
        

        '''
        Test to check if the query produces correct result when week for registered event
        is in the next year. 
        '''
        print("Testing 2 weeks in different years")
        # Create Dataset

        registered_data = [
            {"initiator_id": "1", "event": "registered", "timestamp": "2020-12-29T06:24:42.000Z"}, ## year different, last week
            {"initiator_id": "2", "event": "registered", "timestamp": "2021-12-26T07:00:14.000Z"}, ## year different, last week of the year
            {"initiator_id": "3", "event": "registered", "timestamp": "2021-12-29T07:00:14.000Z"}, ## year different, week same

        ]
        app_loaded_data = [
            {"initiator_id": "1", "event": "app_loaded", "timestamp": "2021-01-06T06:24:42.000Z"}, ## year different, first week of next year
            {"initiator_id": "2", "event": "app_loaded", "timestamp": "2022-01-01T07:00:14.000Z"}, ## year different, last week 
            {"initiator_id": "3", "event": "app_loaded", "timestamp": "2022-01-01T07:00:14.000Z"} ## year different, week same
        ]



        dfsql = self.run_query(app_loaded_data, registered_data)
        # Expected result
        expected_result = 2

        # Actual Result
        result = dfsql.count()

        # Check if equal
        self.assertEqual(expected_result, result, f"Expected {expected_result} but got {result}")

    def test_multiple_app_loaded_events(self):

        '''
        Test to check if the query works correctly for multiple app loaded events
        '''
        print("Testing Multiple App load events")
        # Create Dataset
        registered_data = [
            {"initiator_id": "1", "event": "registered", "timestamp": "2020-09-01T06:24:42.000Z"},
            {"initiator_id": "2", "event": "registered", "timestamp": "2020-11-18T06:24:42.000Z"},
        ]

        app_loaded_data = [
            {"initiator_id": "1", "event": "app_loaded", "timestamp": "2020-09-08T06:24:42.000Z"},
            {"initiator_id": "1", "event": "app_loaded", "timestamp": "2020-09-22T06:24:42.000Z"},
            {"initiator_id": "2", "event": "app_loaded", "timestamp": "2020-11-22T06:24:42.000Z"},
            {"initiator_id": "2", "event": "app_loaded", "timestamp": "2020-11-30T06:24:42.000Z"},
        ]

        dfsql = self.run_query(app_loaded_data, registered_data)

        # Expected result
        expected_result = 1

        # Actual Result
        result = dfsql.count()

        # Check if equal
        self.assertEqual(expected_result, result, f"Expected {expected_result} but got {result}")



# Run the tests
if __name__ == '__main__':
    unittest.main()
