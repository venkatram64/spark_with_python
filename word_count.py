from pyspark import SparkConf, SparkContext

if __name__ == "__main__":

    sc = SparkContext(master="local[3]", appName="Spark Demo")
    sc.setLogLevel("ERROR")
    lines = sc.textFile("word_count.txt")

    """word_map = lines.map(lambda line: line.split(" "))
    wrdd = word_map.collect()
    
    for w in wrdd:
        print(w)
    print("*************")"""

    words = lines.flatMap(lambda line: line.split(" "))
    """rdd = words.collect()
    for w in rdd:
        print(w)
    print("**************")"""
    word_counts = words.countByValue()
    for word, count in word_counts.items():
        print('{}: {}'.format(word, count))
