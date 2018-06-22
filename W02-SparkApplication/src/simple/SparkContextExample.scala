package simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkContextExample {
   def main(args: Array[String]) {
    //val stocksPath = "hdfs://quickstart.cloudera/user/cloudera/stocks.txt"  //read from HDFS
    val stocksPath = "/home/cloudera/git/S-BEAD/W02-SparkApplication/data/stocks.txt" //read from local file
    val conf = new SparkConf().setAppName("Counting Lines").setMaster("local[2]") //test 
    val sc = new SparkContext(conf)
    val data = sc.textFile(stocksPath, 3) //chop into n jobs
    
    
    val totalLines = data.count() //do line counts
    println("Total number of Lines: %s".format(totalLines)) //print line counts
  }
}