from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName('Persistencia').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    
    input_integers = list(range(10))
    integer_rdd = sc.parallelize(input_integers)
    
    integer_rdd.persist(StorageLevel.MEMORY_ONLY)
    
    integer_rdd.reduce(lambda x, y: x * y)
    print(integer_rdd.count())