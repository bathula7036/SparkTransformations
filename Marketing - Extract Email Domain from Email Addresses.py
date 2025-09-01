from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql.types import *
import datetime
import random

spark = SparkSession.builder.appName("DataMgmtApplication") \
        .master("local[*]") \
        .getOrCreate()

customer_data = [
  (1, "john@example.com", "NYC"),
  (2, "RUK@gmail.com", "NJ"),
  (4, "RAM@yahoo.com", "CA"),
  (3, "ROAKU@outlook.com", "TX"),
]

customers = spark.createDataFrame(customer_data, ["customer_id", "email", "city"])
                                  
email_domains = customers.withColumn("domain", split(col("email"), "@").getItem(1))
email_domains.select("email", "domain").show()

user_name = customers.withColumn("user_name", split(col("email"), "@").getItem(0))
user_name.show()
