
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("SreamingProzessor") \
    .getOrCreate()

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 9999) \
    .load()

words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

wordCounts = words \
    .groupBy("word") \
    .count()

query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query \
    .awaitTermination()