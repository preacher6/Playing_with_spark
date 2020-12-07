import sys
from pyspark import SparkConf, SparkContext
import re

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

def split_comma(line: str):
    splits = COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])

if __name__ == '__main__':
    conf = SparkConf().setAppName('airports').setMaster("local[3]")
    sc = SparkContext(conf=conf)
    airports = sc.textFile("datos/airports.text")
    airports_in_usa = airports.filter(lambda line: COMMA_DELIMITER.split(line)[3]=="\"United States\"")
    
    airports_names_city = airports_in_usa.map(split_comma)
    airports_names_city.saveAsTextFile('output/airports_in_usa.text')
