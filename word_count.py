from pyspark import SparkConf, SparkContext

if __name__ == "__main__":

    sc = SparkContext(master="local[3]", appName="Spark Demo")
    sc.setLogLevel("ERROR")
    lines = sc.textFile("word_count.txt")
    print(lines.first())

    words = lines.flatMap(lambda line: line.split(" "))
    word_counts = words.countByValue()
    for word, count in word_counts.items():
        print('{}: {}'.format(word, count))
