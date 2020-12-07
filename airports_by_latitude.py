from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark import sql

import time
import re

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

def split_comma(line: str):
    splits = COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])


if __name__ == '__main__':
    start = time.time()
    conf = SparkConf().setAppName('by_latitude').setMaster("local[1]")
    sc = SparkContext(conf=conf)
    airports = sc.textFile("datos/airports.text")
    airports_big40 = airports.filter(lambda line: float(COMMA_DELIMITER.split(line)[6])>40)
    print(airports_big40.first())
    airports_name = airports_big40.map(split_comma)
    airports_name.saveAsTextFile('output/airports_lat_40')
    end = time.time()
    print(end - start)
    