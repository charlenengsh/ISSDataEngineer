package simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
   def main(args: Array[String]) {
     
   
    val conf = new SparkConf().setAppName("Counting Lines").setMaster("local[2]") //test 
    val sc = new SparkContext(conf)
    
    val inFile =  "/home/cloudera/git/S-BEAD/W02-SparkApplication/data/textfiles/WordCountFile" //read from local file
    
    //val stocksPath = "hdfs://quickstart.cloudera/user/cloudera/stocks.txt"  //read from HDFS
    val data = sc.textFile(inFile, 1) //chop into n jobs
    
      
    
    /*count words*/
      val wordcnt = data.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_) 
      wordcnt.foreach(println)
    
    /*total word count*/
       val totalwordcnt = data.flatMap(line => line.split(" ")).filter(s => !s.isEmpty)
       val totalcnt = totalwordcnt.count()
       println("Total words: %s".format(totalcnt))
       
       
      //ouput in file
      //wordcnt.saveAsTextFile("/home/cloudera/git/S-BEAD/W02-SparkApplication/data/textfiles/WordCountFile")
      
    sc.stop()
  }
   
  
}