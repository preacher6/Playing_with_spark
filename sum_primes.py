from pyspark import SparkConf, SparkContext

if __name__=='__main__':
    conf = SparkConf().setAppName('Sum_primes').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    
    primes_rdd = sc.textFile('datos/prime_nums.text')
    primes_map = primes_rdd.flatMap(lambda line: line.split(' '))
    print(type(primes_map))
    #primes = primes_rdd.flatMap(lambda line: )
    
    for number in primes_map:
        print(number)