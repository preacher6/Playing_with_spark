from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('reduce').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    
    input_integers = [1, 2, 3]
    integers_rdd = sc.parallelize(input_integers)
    
    product = integers_rdd.reduce(lambda x, y: x * y)
    print("product is {}".format(product))
    