import findspark
findspark.init()

from pyspark.sql import Row
from stop_words import get_stop_words
try:
    import json
except ImportError:
    import simplejson as json


# 4.a
def insert_hashtags(sc, hashtags, spark, time):
    if hashtags:
        rdd_hashtags = sc.parallelize(hashtags)
        rdd_hashtags = rdd_hashtags.map(lambda x: x.lower())
        if rdd_hashtags.count() > 0:
            hashtags_data_frame = spark.createDataFrame(rdd_hashtags.map(lambda x: Row(hashtag=x, timestamp=time)))
            hashtags_data_frame.createOrReplaceTempView("hashtags")
            hashtags_data_frame = spark.sql("create database if not exists p2")
            hashtags_data_frame = spark.sql("use p2")
            hashtags_data_frame = spark.sql("select hashtag, timestamp from hashtags")
            hashtags_data_frame.write.mode("append").saveAsTable("hashtags")
            print("Inserted hashtags to table")
    else:
        print("No hashtags to insert")


# 4.b
def insert_text(sc, text, spark, time):
    if text:
        stop_words = get_stop_words('en')
        stop_words.extend(get_stop_words('spanish'))
        stop_words.append("rt")
        rdd_text = sc.parallelize(text)
        rdd_text = rdd_text.flatMap(lambda x: x.split()).map(lambda x: x.lower())
        rdd_text = rdd_text.filter(lambda x: x not in stop_words)
        if rdd_text.count() > 0:
            text_data_frame = spark.createDataFrame(rdd_text.map(lambda x: Row(text=x, timestamp=time)))
            text_data_frame.createOrReplaceTempView("text")
            text_data_frame = spark.sql("create database if not exists p2")
            text_data_frame = spark.sql("use p2")
            text_data_frame = spark.sql("select text, timestamp from text")
            text_data_frame.write.mode("append").saveAsTable("text")
            print("Inserted text into table")
    else:
        print("No text to insert")


# 4.c
def insert_screen_name(sc, screen_name, spark, time):
    if screen_name:
        rdd_text = sc.parallelize(screen_name)
        if rdd_text.count() > 0:
            screen_name_data_frame = spark.createDataFrame(rdd_text.map(lambda x: Row(sn=x, timestamp=time)))
            screen_name_data_frame.createOrReplaceTempView("screennames")
            screen_name_data_frame = spark.sql("create database if not exists p2")
            screen_name_data_frame = spark.sql("use p2")
            screen_name_data_frame = spark.sql("select sn, timestamp from screennames")
            screen_name_data_frame.write.mode("append").saveAsTable("screennames")
            print("Inserted screen name into table")
    else:
        print("No screen name to insert")


# 5
def insert_keywords(sc, text, spark, time):
    if text:
        rdd_keywords = sc.parallelize(text)
        rdd_keywords = rdd_keywords.flatMap(lambda x: x.split()).map(lambda x: x.lower())
        rdd_keywords = rdd_keywords.filter(lambda x: x in ["trump", "maga", "dictator", "impeach", "drain", "swamp"])
        if rdd_keywords.count() > 0:
            keyword_data_frame = spark.createDataFrame(rdd_keywords.map(lambda x: Row(keyword=x, timestamp=time)))
            keyword_data_frame.createOrReplaceTempView("keywords")
            keyword_data_frame = spark.sql("create database if not exists p2")
            keyword_data_frame = spark.sql("use p2")
            keyword_data_frame = spark.sql("select keyword, timestamp from keywords")
            keyword_data_frame.write.mode("append").saveAsTable("keywords")
            print("Inserted keywords into table")
    else:
        print("No keywords to insert")
