package simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestWordCount {
   def main(args: Array[String]) {
     
    // val inputFile = args(0)
    // val outputFile = args(1) 
     val inFile =  "/home/cloudera/git/S-BEAD/W02-SparkApplication/data/textfiles/WordCountFile" //read from local file
     
     
     //val inFile = Option(inputFile).orElse("/home/cloudera/git/S-BEAD/W02-SparkApplication/data/textfiles/WordCountFile")
     
    //val stocksPath = "hdfs://quickstart.cloudera/user/cloudera/stocks.txt"  //read from HDFS
    //val myParagraph = inputFile
    
    val conf = new SparkConf().setAppName("Counting Lines").setMaster("local[2]") //test 
    val sc = new SparkContext(conf)
    val data = sc.textFile(inFile, 1) //chop into n jobs
    
    //count lines
    val totalLines = data.count() //do line counts
   // println("Total number of Lines: %s".format(totalLines)) //print line counts
    
    
     //def countWords(text: String) = text.split("[ ,!.]+").map(_.toLowerCase).groupBy(identity).mapValues(_.size)
    
    
    /*count words*/
      // Split up into words.
      val wordRdd = data.flatMap(line => line.split(" ")).filter(s => !s.isEmpty) 
      // Transform into word and count.
      //val wordcnt = wordRdd.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}  
      
      val totalcnt = wordRdd.count()
      
      print("Total words: %s".format(totalcnt))
      
     // val wordcnt = wordRdd.map(word => (word, 1)).reduceByKey(_+_) //count unique; map+reduce
      
     
 
          
        //println(count)
       
      
    //val wordStr = wordRdd.rdd.map(_.mkString(","))
    //val strWords = words.map(word => (word, 1)).
    //countWords(words.mkString(" "))
    
    
   // wordcnt.take(10).foreach(println)
      //output in Console
      // wordcnt.foreach(println)
     
      //ouput in file
      //wordcnt.saveAsTextFile("/home/cloudera/git/S-BEAD/W02-SparkApplication/data/textfiles/WordCountFile")
      
    //println("Total number of Words: %s".format(counts)) //print word counts
    
    sc.stop()
  }
   
  
}