import plotly
from plotly.graph_objs import *
import datetime
#plotly.tools.set_credentials_file(username="USERNAME", api_key="API_KEY")


def text_graph():
    fi = open('output/texts.txt', 'r')

    tweets_texts = {}
    for line in fi:
        line = line[:-1]
        line = eval(line)
        for element in line:
            if element['text'] not in tweets_texts:
                tweets_texts[element['text']] = [(element['timestamp'], element['count'])]
            else:
                tweets_texts[element['text']].append((element['timestamp'], element['count']))

    fi.close()

    points = []
    for key, value in tweets_texts.items():
        x_list = []
        y_list = []
        for elements in value:
            x_list.append(elements[0])
            y_list.append(elements[1])
        points.append(Scatter(
            x=x_list,
            y=y_list,
            name=key
        ))

    data = Data(points)

    plotly.plotly.plot(data, filename = 'texts_graph')


def screenname_graph():
    fi = open('output/screennames.txt', 'r')

    tweets_screennames = {}
    for line in fi:
        line = line[:-1]
        line = eval(line)
        for element in line:
            if element['sn'] not in tweets_screennames:
                tweets_screennames[element['sn']] = [(element['timestamp'], element['count'])]
            else:
                tweets_screennames[element['sn']].append((element['timestamp'], element['count']))

    fi.close()

    points_list = []
    for key, value in tweets_screennames.items():
        x_coordinates = []
        y_coordinates = []
        for elements in value:
            x_coordinates.append(elements[0])
            y_coordinates.append(elements[1])
        points_list.append(Scatter(
            x=x_coordinates,
            y=y_coordinates,
            name=key
        ))

    data = Data(points_list)

    plotly.plotly.plot(data, filename = 'screennames_graph')


def keywords_graph():
    fi = open('output/keywords.txt', 'r')

    tweets_keywords = {}
    for line in fi:
        line = line[:-1]
        line = eval(line)
        for element in line:
            if element['keyword'] not in tweets_keywords:
                tweets_keywords[element['keyword']] = [(element['timestamp'], element['count'])]
            else:
                tweets_keywords[element['keyword']].append((element['timestamp'], element['count']))

    fi.close()

    points_list = []
    for key, value in tweets_keywords.items():
        x_coordinates = []
        y_coordinates = []
        for tup in value:
            x_coordinates.append(tup[0])
            y_coordinates.append(tup[1])
        points_list.append(Scatter(
            x=x_coordinates,
            y=y_coordinates,
            name=key
        ))

    data = Data(points_list)

    plotly.plotly.plot(data, filename='keywords_graph')


def hashtags_graph():
    fi = open('output/hashtags.txt', 'r')

    tweets_hashtags = {}
    for line in fi:
        line = line[:-1]
        line = eval(line)
        for element in line:
            if element['hashtag'] not in tweets_hashtags:
                tweets_hashtags[element['hashtag']] = [(element['timestamp'], element['count'])]
            else:
                tweets_hashtags[element['hashtag']].append((element['timestamp'], element['count']))

    fi.close()

    points_list = []
    i = 0

    for key, value in tweets_hashtags.items():
        if i >= 9:
            break
        x_coordinates = []
        y_coordinates = []
        for elements in value:
            x_coordinates.append(elements[0])
            y_coordinates.append(elements[1])
        points_list.append(Scatter(
            x=x_coordinates,
            y=y_coordinates,
            name=key
        ))
        i += 1

    data = Data(points_list)

    plotly.plotly.plotly.plot(data, filename='hashtags_graph')

text_graph()
screenname_graph()
keywords_graph()
hashtags_graph()