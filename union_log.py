from pyspark import SparkConf, SparkContext

def is_not_header(line: str):
    return not (line.startswith("host") and "bytes" in line)

if __name__ == '__main__':
    conf = SparkConf().setAppName('logs').setMaster('local[1]')
    sc = SparkContext(conf=conf)
    july_logs = sc.textFile('datos/nasa_19950701.tsv.txt')
    august_logs = sc.textFile('datos/nasa_19950801.tsv.txt')
    
    join_logs = july_logs.union(august_logs)
    
    clean_logs = join_logs.filter(is_not_header)
    sample = clean_logs.sample(withReplacement=True, fraction=0.1)
    sample.saveAsTextFile('output/samples3')