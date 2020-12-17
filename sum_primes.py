from pyspark import SparkConf, SparkContext

if __name__=='__main__':
    conf = SparkConf().setAppName('Sum_primes').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    
    primes_rdd = sc.textFile('datos/prime_nums.text')
    primes_map = primes_rdd.flatMap(lambda line: line.split('\t'))

    primes_map = primes_map.map(lambda line: int(line))

    print(primes_map.take(30))
    sum = primes_map.reduce(lambda x, y: x+y)
    print(f'La suma es: {sum}')
