    CREATE TABLE mobile(
    `User ID` INT,
    `Device Model` STRING,
    `Operating System` STRING,
    `App Usage Time (min/day)` INT,
    `Screen On Time (hours/day)` DOUBLE,
    `Battery Drain (mAh/day)` INT,
    `Number of Apps Installed` INT,
    `Data Usage (MB/day)` INT,
    `Age` INT,
    `Gender` STRING,
    `User Behavior Class` INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");



loading datbe in hive

LOAD DATA INPATH '/project/mobile_user.csv' INTO TABLE mobile;

LOAD DATA INPATH '/mobile_user.csv' INTO TABLE mobile;



SELECT * FROM mobile;


read csv file from HDFS

from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("mobile_user CSV to Hive") \
    .enableHiveSupport() \
    .getOrCreate()


mobile_df = spark.read.option("header", True) \
    .csv("hdfs:/mobile_user.csv")

Renaming the table

mobile_df = mobile_df \
    .withColumnRenamed("User ID", "user_id") \
    .withColumnRenamed("Device Model", "device_model") \
    .withColumnRenamed("Operating System", "operating_system") \
    .withColumnRenamed("App Usage Time (min/day)", "app_usage") \
    .withColumnRenamed("Screen On Time (hours/day)", "screen_time") \
    .withColumnRenamed("Battery Drain (mAh/day)", "battery_drain") \
    .withColumnRenamed("Number of Apps Installed", "apps_installed") \
    .withColumnRenamed("Data Usage (MB/day)", "data_usage") \
    .withColumnRenamed("Age", "age") \
    .withColumnRenamed("Gender", "gender") \
    .withColumnRenamed("User Behavior Class", "user_behavior")

write to data frame

mobile_df.write.mode("overwrite").saveAsTable("default.newmobile")

verifi data loaded into new hive table

spark.sql("SELECT * FROM default.newmobile").show()

 -----------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression




mobile_df = mobile_df.na.drop()  # Drop rows with null values

assembler = VectorAssembler(
    inputCols=["screen_time", "app_usage", "battery_drain", "apps_installed"],
    outputCol="features",
    handleInvalid="skip")

assembled_df = assembler.transform(mobile_df).select("features", "screen_time")


train_data, test_data = assembled_df.randomSplit([0.7, 0.3])


lr = LinearRegression(labelCol="screen_time")
lr_model = lr.fit(train_data)


HBase

create 'my_mobile', 'cf'
-------------------------------------------------------------------------------

from pyspark.sql import functions as F

mobile_df = mobile_df \
    .withColumn("screen_time", F.col("screen_time").cast("double")) \
    .withColumn("app_usage", F.col("app_usage").cast("double")) \
    .withColumn("battery_drain", F.col("battery_drain").cast("double")) \
    .withColumn("apps_installed", F.col("apps_installed").cast("double"))


def write_to_hbase_partition(partition):
    connection = happybase.Connection('master')
    connection.open()
    table = connection.table('my_mobile')
    for row in partition:
        row_key, column, value = row
        table.put(row_key, {column: value})
    connection.close()

-------------------------------------------------------------------------	

docker compose cp /home/mohammed/mobile_user.csv/file master:/data/


wget url
hdfs dfs -put /mobile_user.csv
-------------------------------------------------------------------------------

def to_hbase_format(row):
    return (row.user_id, {  
        'cf:device_model': str(row.device_model),     
        'cf:operating_system': str(row.operating_system),
        'cf:app_usage': str(row.app_usage),
        'cf:screen_time': str(row.screen_time),
        'cf:battery_drain': str(row.battery_drain),
        'cf:apps_installed': str(row.apps_installed),
        'cf:data_usage': str(row.data_usage),
        'cf:age': str(row.age),
        'cf:gender': str(row.gender),
        'cf:user_behavior': str(row.user_behavior)
    })



def save_to_hbase(row):
    row_key, data = row
    connection = happybase.Connection('localhost')
    table = connection.table('my_mobile')  
    table.put(row_key, data)
