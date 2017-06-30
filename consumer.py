import findspark

from project_utils.inserts import insert_hashtags, insert_text, insert_screen_name, insert_keywords
from project_utils.writes import write_hashtags, write_texts, write_screen_names, write_keywords

findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

try:
    import json
except ImportError:
    import simplejson as json
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/luis.rivera157/spark/spark-2.1.1-bin-hadoop2.7/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar pyspark-shell'


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config("spark.sql.warehouse.dir", 'hdfs://master:9000/user/hive/warehouse').enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def consumer():
    context = StreamingContext(sc, 30)
    dStream = KafkaUtils.createDirectStream(context, ["trump"], {"metadata.broker.list": "136.145.216.152:9092,136.145.216.163:9092,136.145.216.168:9092,136.145.216.173:9092,136.145.216.175:9092"})
    dStream.foreachRDD(run)
    context.start()
    context.awaitTermination()


def run(time, rdd):
    rdd = rdd.map(lambda x: json.loads(x[1]))
    tweets = rdd.collect()
    spark = getSparkSessionInstance(rdd.context.getConf())

    # 4.a
    hashtags = [element["entities"]["hashtags"] for element in tweets if "entities" in element]
    hashtags = [x for x in hashtags if x]
    hashtags = [element[0]["text"] for element in hashtags]
    insert_hashtags(sc, hashtags, spark, time)
    global last_refresh_hashtags
    if datetime.now() > last_refresh_hashtags + timedelta(minutes=10):
        write_hashtags(spark)
        last_refresh_hashtags = datetime.now()

    # 4.b
    text = [element["text"] for element in tweets if "text" in element]
    insert_text(sc, text, spark, time)
    global last_refresh_texts
    if datetime.now() > last_refresh_texts + timedelta(minutes=10):
        write_texts(spark)
        last_refresh_texts = datetime.now()

    # 4.c
    screen_names = [element["user"]["screen_name"] for element in tweets if "user" in element]
    insert_screen_name(sc, screen_names, spark, time)
    global last_refresh_screen_name
    if datetime.now() > last_refresh_screen_name + timedelta(minutes=60):
        write_screen_names(spark)
        last_refresh_screen_name = datetime.now()

    # 5
    insert_keywords(sc, text, spark, time)
    global last_refresh_keywords
    if datetime.now() > last_refresh_keywords + timedelta(days=1):
        write_keywords(spark)
        last_refresh_keywords = datetime.now()

last_refresh_screen_name = None
last_refresh_texts = None
last_refresh_keywords = None
last_refresh_hashtags = None


if __name__ == "__main__":
    print("Started reading tweets")
    last_refresh_hashtags = datetime.now()
    last_refresh_texts = datetime.now()
    last_refresh_screen_name = datetime.now()
    last_refresh_keywords = datetime.now()
    print("Started at", datetime.now())
    sc = SparkContext(appName="Consumer")
    consumer()
