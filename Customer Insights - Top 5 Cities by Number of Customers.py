from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql.types import *
import datetime
import random

spark = SparkSession.builder.appName("DataMgmtApplication") \
        .master("local[*]") \
        .getOrCreate()

customers_data = [
(1, "john@example.com", "New York"),
(2, "sarah@gmail.com", "London"),
(3, "mike@yahoo.com", "New York"),
(4, "lisa@example.com", "Paris"),
(5, "dave@outlook.com", "London"),
(6, "anna@gmail.com", "Berlin")
]

customers = spark.createDataFrame(customers_data, ["customer_id", "email", "city"])

top_cities = customers.groupBy("city").count().orderBy(desc("count")).limit(5)
top_cities.show(5)
