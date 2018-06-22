package simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Example {
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf
    conf.setMaster("local[2]").setAppName("SparkTransformationExample")
    val sc = new SparkContext(conf)
    
    val data = sc.parallelize(Array(('A',1),('b',2),('c',3)))
    val data2 =sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val result = data.join(data2)
    println(result.collect().mkString(","))
    result.foreach(println)
  }
}