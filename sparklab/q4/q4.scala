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
  def mapper(self, key, line):
      parts = line.split("\t")
        origin_state = parts[3]  
        destination_state = parts[5} 
        num_passengers = parts[7]
        
        #num_passengers leaving the state
        key = origin_state
        value = (num_passenger, "Outgoing")
        yield (key, value)

        #num_passengers arriving in the state
        key = destination_state
        value = (num_passengers, "Incoming")
        yield (key, value)
  def reducer(self, key, values):
        yield (key, (x,y))
        
  def saveit(counts: org.apache.spark.rdd.RDD[(String, Int)]) = {
    counts.saveAsTextFile("result")
  }
}
