from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('Collect').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    
    input_words = ["spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop"]
    
    word_rdd = sc.parallelize(input_words)
    
    words = word_rdd.collect()
    
    for word in words:
        print(word)