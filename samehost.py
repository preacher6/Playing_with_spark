from pyspark import SparkConf, SparkContext

def is_not_header(line: str):
    return not (line.startswith("host") and "bytes" in line)

if __name__ == '__main__':
    conf = SparkConf().setAppName('logs').setMaster('local[1]')
    sc = SparkContext(conf=conf)
    july_logs = sc.textFile('datos/nasa_19950701.tsv.txt')
    august_logs = sc.textFile('datos/nasa_19950801.tsv.txt')
    
    july_logs_hosts = july_logs.map(lambda line: line.split("\t")[0])
    august_logs_hosts = august_logs.map(lambda line: line.split("\t")[0])
    
    intersection = july_logs_hosts.intersection((august_logs_hosts))
    
    clean_intersection = intersection.filter(is_not_header)
    clean_intersection.saveAsTextFile('output/samples4')