
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import unix_timestamp, to_date, col, from_unixtime, udf, year, month, hour, minute, to_date
from datetime import datetime


spark = SparkSession \
    .builder \
    .appName("StreamingProzessor") \
    .getOrCreate()


# ---- Custom function ----
def extractDate(t):
    dt = datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    return dt.strftime('%Y-%m-%d')
    
def calculateResponseTime(count):
    return 60 / count
    
def getDateTime(date, hour, minute):
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    time_obj = datetime.strptime(str(hour)+":"+str(minute), '%H:%M').time()
    return datetime.combine(date_obj, time_obj)

schema = StructType([ \
    StructField("code", StringType()), \
    StructField("client_id", IntegerType()), \
    StructField("loc_ts", IntegerType()), \
    StructField("length", IntegerType()), \
    StructField("op", StringType()), \
    StructField("err_code", IntegerType()), \
    StructField("time", StringType()), \
    StructField("thread_id", IntegerType())])

udf_getDate = udf(extractDate, StringType())
udf_calculateResponseTime = udf(calculateResponseTime, DoubleType())
udf_getDateTime = udf(getDateTime, TimestampType())

spark.udf.register("udf_getDate", udf_getDate)
spark.udf.register("udf_calculateResponseTime", udf_calculateResponseTime)
spark.udf.register("udf_getDateTime", udf_getDateTime)

# ---- End Custom function ----

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 9999) \
    .load()

df_withTime = lines.withColumn("datetime", from_unixtime(col("time")[0:10]))

df_withMinuteAndHour = df_withTime \
    .withColumn("minute", minute("datetime")) \
    .withColumn("hour", hour("datetime")) \
    .withColumn("month", month("datetime")) \
    .withColumn("date", udf_getDate("datetime")) \
    .withColumn("year", year("datetime"))
    
df_filtered = df_withMinuteAndHour.filter(col("code") == "res_snd")

df_result = df_filtered.groupBy("date", "month", "year", "hour", "minute").count()

df_result_calc = df_result \
    .withColumn("responseTime", udf_calculateResponseTime("count")) \
    .withColumn("datetime", udf_getDateTime("date", "hour", "minute"))
    
df_result_cleaned = df_result_calc \
    .drop("date") \
    .drop("hour") \
    .drop("minute")

df_result_cleaned.sort("datetime").show(300,False)

df_result_cleaned.registerTempTable("streamTemp")