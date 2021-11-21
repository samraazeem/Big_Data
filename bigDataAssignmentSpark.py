from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
import pandas


## create spark session
appName = "PySpark Example - JSON file to Spark Data Frame"
master = "local"

spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# read data source
json_file_path = '/home/azureuser/spark/demographic_info.json'
dataframe = spark.read.json(json_file_path)
#dataframe = spark.read.option("multiline", "true") \
#                      .json(json_file_path)
print(dataframe.count())
#using select query
df = dataframe.select("_id", "name", "age", "gender", "isActive", "balance", "company", "eyeColor", "email", "phone")

#using filter
df = df.filter(df.isActive == "true")

df = df.withColumn("Age_Group", when((df.age >= 13) & (df.age < 20), lit("Teenager")) \
      .when((df.age >= 20) & (df.age < 40), lit("Adult")) \
      .when(df.age >= 40, lit("Old")) \
      .otherwise(lit("NA")))

df.show(5)
df.groupby('gender').agg({'balance': 'max'}).show(2)

df.groupby('gender').orderby('balance').show()

#create temp view table
dataframe.createTempView("sample_data_view")
spark.sql('''
    SELECT _id, name, age, gender, isActive, balance, company, eyeColor, email, phone
    FROM sample_data_view
    WHERE isActive == "true"
    ''').show(10, False)

#convert to rdd
rdd_object = df.rdd
rdd_object.saveAsTextFile("./rdd_text")

#end spark session
spark.stop()

