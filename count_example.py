from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName('Different counts').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    
    input_words = ["spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop"]
    word_rdd = sc.parallelize(input_words)
    
    print('count: {}'.format(word_rdd.count()))
    word_rdd_byvalue = word_rdd.countByValue()
    print('countByValue:')
    for word, count in word_rdd_byvalue.items():
        print('{} : {}'.format(word, count))