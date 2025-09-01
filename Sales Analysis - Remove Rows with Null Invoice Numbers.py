from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql.types import *
import datetime
import random

spark = SparkSession.builder.appName("DataMgmtApplication") \
        .master("local[*]") \
        .getOrCreate()

sales_data = [
(1001, "A101", 25.99),
(None, "B202", 49.99),
(1003, None, 15.49),
(1004, "C303", 75.00)
]

sales = spark.createDataFrame(sales_data, ["invoice_id", "product_id", "amount"])
sales.show()

clean_sales = sales.dropna(subset=["invoice_id"])
clean_sales.show()

clean_sales = sales.dropna(subset= ["product_id"])
clean_sales.show()
