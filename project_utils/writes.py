import findspark
findspark.init()

from pyspark.sql import Row
from datetime import datetime, timedelta


def write_hashtags(spark):
    try:
        hashtags_data_frame = spark.sql("select hashtag, timestamp from hashtags")
        hashtags_rdd = hashtags_data_frame.rdd
        hashtags_rdd = hashtags_rdd.filter(lambda x: x["timestamp"] > datetime.now() - timedelta(minutes=60))
        hashtags_data_frame = spark.createDataFrame(hashtags_rdd.map(lambda x: Row(hashtag=x["hashtag"], timestamp=["timestamp"])))
        hashtags_data_frame.createOrReplaceTempView("last_hashtags")
        hashtags_count_data_frame = spark.sql("select hashtag, count(*) as c from last_hashtags group by hashtag order by c desc")
        now = datetime.now()
        hashtags_dict = hashtags_count_data_frame.rdd.map(lambda x: {"timestamp": now, "hashtag": x["hashtag"], "count": x["c"]}).take(10)
        f = open('output/hashtags.txt', 'a')
        f.write(str(hashtags_dict))
        f.write("\n")
        f.close()
        print("Hashtags appended to file")
    except:
        print("Error appending hashtags")
        pass


def write_texts(spark):
    try:
        texts_data_frame = spark.sql("select text, timestamp from text")
        texts_rdd = texts_data_frame.rdd
        texts_rdd = texts_rdd.filter(lambda x: x["timestamp"] > datetime.now() - timedelta(minutes=60))
        texts_data_frame = spark.createDataFrame(texts_rdd.map(lambda x: Row(text=x["text"], timestamp=["timestamp"])))
        texts_data_frame.createOrReplaceTempView("last_texts")
        texts_count_data_frame = spark.sql("select text, count(*) as cnt from last_texts group by text order by cnt desc")
        now = datetime.now()
        text_dict = texts_count_data_frame.rdd.map(lambda x: {"timestamp": now, "text": x["text"], "count": x["c"]}).take(10)
        f = open('output/texts.txt', 'a')
        f.write(str(text_dict))
        f.write("\n")
        f.close()
        print("Texts appended to file")
    except:
        print("Error appending texts")
        pass


def write_screen_names(spark):
    try:
        screen_names_data_frame = spark.sql("select sn, timestamp from screennames")
        screen_names_rdd = screen_names_data_frame.rdd
        screen_names_rdd = screen_names_rdd.filter(lambda x: x["timestamp"] > datetime.now() - timedelta(hours=12))
        screen_names_data_frame = spark.createDataFrame(screen_names_rdd.map(lambda x: Row(sn=x["sn"], timestamp=["timestamp"])))
        screen_names_data_frame.createOrReplaceTempView("last_screennames")
        screen_names_count_data_frame = spark.sql("select sn, count(*) as c from last_screennames group by sn order by c desc")
        now = datetime.now()
        screen_names_dict = screen_names_count_data_frame.rdd.map(lambda x: {"timestamp": now, "sn": x["sn"], "count": x["c"]}).take(10)
        f = open('output/screennames.txt', 'a')
        f.write(str(screen_names_dict))
        f.write("\n")
        f.close()
        print("Screennames appended to file")
    except:
        print("Error appending screennames")
        pass


def write_keywords(spark):
    try:
        keywords_data_frame = spark.sql("select keyword, timestamp from kwords")
        keywords_rdd = keywords_data_frame.rdd
        keywords_rdd = keywords_rdd.filter(lambda x: x["timestamp"] > datetime.now() - timedelta(hours=1))
        keywords_data_frame = spark.createDataFrame(keywords_rdd.map(lambda x: Row(keyword=x["keyword"], timestamp=["timestamp"])))
        keywords_data_frame.createOrReplaceTempView("last_keywords")
        keywords_count_data_frame = spark.sql("select keyword, count(*) as c from last_keywords group by keyword order by c desc")
        now = datetime.now()
        keyword_dict = keywords_count_data_frame.rdd.map(lambda x: {"timestamp": now, "keyword": x["keyword"], "count": x["c"]}).take(6)
        f = open('output/keywords.txt', 'a')
        f.write(str(keyword_dict))
        f.write("\n")
        f.close()
        print("Keywords appended to file")
    except:
        print("Error appending keywords")
        pass
