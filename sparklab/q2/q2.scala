import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object WordCount {
  def main(args: Array[String]) = {
    val sc = getSC()
    val counts = doWordCount(sc)
    saveit(counts)
  }
  
  def getSC() = {
    val conf = new SparkConf().setAppName("wc")
    val sc = new SparkContext(conf)
    sc
  }
  def doWordCount(sc: SparkContext) = {
    val input = sc.textFile("/datasets/wap")
    val words = input.flatMap(_.split(" "))
    val kv = words.map(word => (word,1))
    val counts = kv.reduceByKey((x,y) => x+y)
    counts
  }
  words = line.split()
        for w in words:
            if len(w) > 2:
               yield (len(w), 1)
  }
  def saveit(counts: org.apache.spark.rdd.RDD[(String, Int)]) = {
    counts.saveAsTextFile("result")
  }
}
