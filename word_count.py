# -*- coding: utf8 -*-
from pyspark import SparkContext

if __name__.endswith("__main__"):
    
    sc = SparkContext("local[1]", "word count")
    sc.setLogLevel("ERROR")
    lines = sc.textFile("datos/word_count.text")
    words = lines.flatMap(lambda line: line.split(" "))
    word_count = words.countByValue()
    for word, count in word_count.items():
        print("{} : {}".format(word, count))