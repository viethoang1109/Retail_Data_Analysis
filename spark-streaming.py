# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a spark session
spark = SparkSession \
        .builder \
        .appName("Retail Data Analysis - Spark Streaming") \
        .getOrCreate()
        
# Set log level to ERROR
spark.sparkContext.setLogLevel('ERROR')

# Read Retail Dataset from Kafka topic : real-time-project
retail_data_source = spark  \
              .readStream  \
              .format("kafka")  \
              .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
              .option("subscribe","real-time-project")  \
              .load()

# Defining the Data Structure for source retail data (json format).              
retail_datastructure = StructType([
                            StructField("invoice_no", LongType()),
                            StructField("country", StringType()),
                            StructField("timestamp", TimestampType()),
                            StructField("type", StringType()),
                            StructField("items", ArrayType(StructType([
                                                                StructField("SKU", StringType()),
                                                                StructField("title", StringType()),
                                                                StructField("unit_price", FloatType()),
                                                                StructField("quantity", IntegerType())
                                                            ])))
                                ])

# Converting source data from json format to dataframe (table).            
retail_data_formatted =  retail_data_source \
                 .select(from_json(col("value").cast("string"), retail_datastructure).alias("retail_data")) \
                 .select("retail_data.*")

# Defining utility functions to create metrics for each invoice related to retail data: total_cost, total_items, is_order, is_return
def calculate_total_cost(items, type):
    '''
        Calculate total cost associated for each invoice.
        total_cost = sum of (unit_price * quantity) for all items.
        If the type is Return then the total cost will be negative which represents amount loss.
    ''' 
    total_cost = 0
    for item in items:
        total_cost = total_cost + (item[2] * item[3])
    if type == 'RETURN':
        return total_cost * -1
    return total_cost

def calculate_total_items(items):
    '''
        Calculate total no. of items present in each invoice (quantity).
    '''
    total_items = 0
    for item in items:
        total_items = total_items + item[3]
    return total_items

def check_order_type(type):
    '''
        To check whether the type is Order or not
            1 - ORDER Type
            0 - Not an Order Type
    '''
    if type == 'ORDER':
        return 1
    return 0
    
def check_return_type(type):
    '''
        To check whether the type is Return or not
            1 - Return Type
            0 - Not a Return Type
    '''
    if type == 'RETURN':
        return 1
    return 0

# Converting utility functions to UDFs
get_total_cost = udf(calculate_total_cost, DoubleType())
get_total_items = udf(calculate_total_items, IntegerType())
get_order_flag = udf(check_order_type, IntegerType())
get_return_flag = udf(check_return_type, IntegerType())

# Derive the columns/metrics: total_cost, total_items, is_order, is_return from the source dataframe and select the required columns
retail_data_transformed = retail_data_formatted \
                          .withColumn("total_cost", get_total_cost(retail_data_formatted.items, retail_data_formatted.type)) \
                          .withColumn("total_items", get_total_items(retail_data_formatted.items)) \
                          .withColumn("is_order", get_order_flag(retail_data_formatted.type)) \
                          .withColumn("is_return", get_return_flag(retail_data_formatted.type)) \
                          .select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return")
              
# Write the retail transformed data to console
retail_data_sink = retail_data_transformed \
                  .writeStream \
                  .outputMode("append") \
                  .format("console") \
                  .option("truncate", "false") \
                  .trigger(processingTime="1 minute") \
                  .start()

# Calculate time based KPIs: OPM (Orders Per Minute), total_sale_volume, average_transaction_size, rate_of_return.
retail_time_kpi = retail_data_transformed \
                  .withWatermark("timestamp","1 minute") \
                  .groupBy(window("timestamp", "1 minute", "1 minute")) \
                  .agg(count("invoice_no").alias("OPM"),
                       sum("total_cost").alias("total_sale_volume"),
                       avg("total_cost").alias("average_transaction_size"),
                       avg("is_return").alias("rate_of_return")) \
                  .select("window", 
                          "OPM",
                          "total_sale_volume",
                          "average_transaction_size",
                          "rate_of_return")

# Write retail data time based KPI in HDFS (JSON format)
retail_time_kpi_sink = retail_time_kpi \
                       .writeStream \
                       .format("json") \
                       .outputMode("append") \
                       .option("truncate", "false") \
                       .option("path", "Timebased-KPI") \
                       .option("checkpointLocation", "Timebased-Checkpoint") \
                       .trigger(processingTime="1 minute") \
                       .start()

# Calculate time-country based KPIs: OPM (Orders Per Minute), total_sale_volume, rate_of_return.
retail_time_country_kpi = retail_data_transformed \
                  .withWatermark("timestamp","1 minute") \
                  .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
                  .agg(count("invoice_no").alias("OPM"),
                       sum("total_cost").alias("total_sale_volume"),
                       avg("is_return").alias("rate_of_return")) \
                  .select("window", 
                          "country",
                          "OPM",
                          "total_sale_volume",
                          "rate_of_return")

# Write retail data time and country based KPI in HDFS (JSON format)
retail_time_country_kpi_sink = retail_time_country_kpi \
                       .writeStream \
                       .format("json") \
                       .outputMode("append") \
                       .option("truncate", "false") \
                       .option("path", "Country-and-timebased-KPI") \
                       .option("checkpointLocation", "Country-and-timebased-Checkpoint") \
                       .trigger(processingTime="1 minute") \
                       .start()

# Keep all the streams alive until it is terminated
retail_data_sink.awaitTermination()
retail_time_kpi_sink.awaitTermination()
retail_time_country_kpi_sink.awaitTermination()
