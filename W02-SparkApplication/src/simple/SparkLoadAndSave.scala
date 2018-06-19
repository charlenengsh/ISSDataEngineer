package simple

import org.apache.hadoop.io.{ Text, IntWritable }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Loading JSON file
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature

import java.io.{ StringWriter, StringReader }
import au.com.bytecode.opencsv.{ CSVWriter, CSVReader }
import scala.collection.JavaConverters._
import collection.JavaConverters._

object SparkLoadAndSave {
  case class Person(name: String, age: Int)

  case class Stocks(name: String, totalPrice: Long)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    conf.setMaster("local[2]").setAppName("Loading_SavingData")
    val sc = new SparkContext(conf)

    val mapper = new ObjectMapper()
    // Loading text file
    val input =
      sc.textFile("/home/cloudera/git/S-BEAD/W02-SparkApplication/data/stocks.txt")
    val wholeInput = sc.wholeTextFiles("/home/cloudera/git/S-BEAD/W02-SparkApplication/data/textfiles/")

    val result = wholeInput.mapValues { value =>
      val nums = value.split(" ").map(x => x.toDouble)
      nums.sum / nums.size.toDouble
    }

    result.saveAsTextFile("/home/padmac/data/outputFileWholeInput")

    // Loading JSON File
    val jsonInput = sc.textFile("/home/padmac/bigdata/spark-1.6.0-bin-hadoop2.6/examples/src/main/resources/people.json")
    val result1 = jsonInput.flatMap(record => {
      try {
        Some(mapper.readValue(record, classOf[Person]))
      } catch {
        case e: Exception => None
      }
    })
    result1.filter(person => person.age > 15).map(mapper.writeValueAsString(_)).
      saveAsTextFile("/home/padmac/data/outputFile")

    // Loading CSV

    val input1 = sc.textFile("/home/padmac/data/Stocks/stocks.csv")
    val result2 = input1.flatMap { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readAll().asScala.toList.map(x => Stocks(x(0), x(6).toLong))
    }

    result2.foreach(println)
    result2.map(stock => Array((stock.name, stock.totalPrice))).mapPartitions { stock =>
      val stringWriter = new StringWriter
      val csvWriter = new CSVWriter(stringWriter)

      csvWriter.writeAll(stock.toList.map(arr => arr.map(x => x._1 + x._2.toString)).asJava)
      Iterator(stringWriter.toString)
    }.saveAsTextFile("/home/padmac/data/CSVOutputFile")

    //Loading Sequence File

    val data = sc.sequenceFile("/sequenceFile/path", classOf[Text],
      classOf[IntWritable]).map { case (x, y) => (x.toString, y.get()) }
    val input3 = sc.parallelize(List(("Panda", 3), ("Kay", 6),
      ("Snail", 2)))
    input3.saveAsSequenceFile("hdfs://namenode:9000/sequenceOutputFile")
  }

}