# spark-submit --master yarn-client followersToLength.py hdfs://hadoop2-0-0/data/twitter/part-03212

# Spark program to see if there is a relationship between the number of followers a user has
# and the average length of their tweets.
#
# Will display a plot when done.  
# To display, you'll need to install matplotlib
# Execute this to install:  yum install python-matplotlib

from __future__ import print_function
import sys
import json
import matplotlib.pyplot as plt
from pyspark import SparkContext


def getText(line):
    try:
        js = json.loads(line)
        hashtags = js["entities"]["hashtags"]
        date = js["created_at"].split()
        y = map(lambda v : v["text"], hashtags)
        hour = date[3].split(":")
        if y:
            for i in y:
              return [(date[0],hour[0],str(i))]
        else:
            return [(date[0],hour[0],"")]
    except Exception as a:
        return []

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="followersToLength")

    tweets = sc.textFile(sys.argv[1], 40)
    t = tweets.flatMap(getText)
    t.cache()

    trial = t.map(lambda (day,hour,hashtag) : ((day,hashtag),1)).reduceByKey(lambda a, b: a + b)
    trial_test = trial.map(lambda((day,hashtag),c) : (day,{"hashtag":hashtag,"count":c}) if hashtag else (day,{"hashtag":"space","count":-1}))
    trial_test_2 = trial_test.reduceByKey(lambda a, b: {'count': a['count'] if a['count']>b['count'] else b['count'], 'hashtag':a['hashtag'] if a['count']>b['count'] else b['hashtag']})

    trial_hour = t.map(lambda (day,hour,hashtag) : ((hour,hashtag),1)).reduceByKey(lambda a, b: a + b)
    trial_test_hour = trial.map(lambda((hour,hashtag),c) : (hour,{"hashtag":hashtag,"count":c}) if hashtag else (hour,{"hashtag":"space","count":-1}))
    trial_test_2_hour = trial_test.reduceByKey(lambda a, b: {'count': a['count'] if a['count']>b['count'] else b['count'], 'hashtag':a['hashtag'] if a['count']>b['count'] else b['hashtag']})

    print("Looking at %d tweets out of %d" % (t.count(), tweets.count()))

    trial_test_2.saveAsTextFile("day_file")
    trial_test_2_hour.saveAsTextFile("hour_file")

    # Stop your spark job.
    sc.stop()