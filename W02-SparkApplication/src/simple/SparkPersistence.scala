package simple

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage._

object SparkPersistence {
  
  
    def main(args:Array[String]): Unit = {

  val conf = new SparkConf().setAppName("Persisting RDD").setMaster("local[2]")
     val sc = new SparkContext(conf)

    val inputRdd =
      sc.parallelize(Array("this,is,a,ball","it,is,a,cat","julie,is,in,the,church "))
    val wordsRdd = inputRdd.flatMap(record => record.split(","))
    val wordLengthPairs = wordsRdd.map(word=> (word, word.length))
    val wordPairs = wordsRdd.map(word => (word,1))
    val reducedWordCountRdd = wordPairs.reduceByKey((x,y) => x+y)
    val filteredWordLengthPairs = wordLengthPairs.filter{case(word,length) =>
      length >=3}
    reducedWordCountRdd.cache()
    val joinedRdd = reducedWordCountRdd.join(filteredWordLengthPairs)
    joinedRdd.persist(StorageLevel.MEMORY_AND_DISK) //store back into the disk; control space for options
    val wordPairsCount = reducedWordCountRdd.count
    val wordPairsCollection = reducedWordCountRdd.take(10)
    val joinedRddCount = joinedRdd.count
    val joinedPairs = joinedRdd.collect()
    reducedWordCountRdd.unpersist() 
    joinedRdd.unpersist()  //unpersist examples
    
  }
    
}