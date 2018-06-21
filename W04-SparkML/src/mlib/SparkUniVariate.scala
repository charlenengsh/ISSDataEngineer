package mlib


import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameNaFunctions
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object SparkUniVariate {
   def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("spark://master:7077")
      .setAppName("Univariate_Analysis")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //Loading data
    val titanic_data =
      sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema","true")
        .load("hdfs://namenode:9000/titanic_data.csv")
    titanic_data.show(10)

    /* Mean value of Fare charged to board Titanic Ship using
MultiVariateStatisticalSummary */
    val fare_Details_Df = titanic_data.select("Fare")
    val fareObservations = fare_Details_Df.map{row =>
      Vectors.dense(row.getDouble(0))}
    val summary_Fare:MultivariateStatisticalSummary =
      Statistics.colStats(fareObservations)
    println("Mean of Fare: "+summary_Fare.mean)


    // Other way of finding the mean
    val fare_DetailsRdd = fare_Details_Df.map{row =>
      row.getDouble(0)}
    val meanValue = fare_DetailsRdd.mean()
    println("Mean Value of Fare From RDD: "+meanValue)
    // Median of the variable Fare
    val countOfFare = fare_DetailsRdd.count()
    val sortedFare_Rdd = fare_DetailsRdd.sortBy(fareVal => fareVal )
    val sortedFareRdd_WithIndex = sortedFare_Rdd.zipWithIndex()
    val median_Fare = if(countOfFare%2 ==1)
      sortedFareRdd_WithIndex.filter{case(fareVal:Double, index:Long) =>
        index == (countOfFare-1)/2}.first._1
    else{
      val elementAtFirstIndex =
        sortedFareRdd_WithIndex.filter{case(fareVal:Double,
        index:Long) => index == (countOfFare/2)-1}.first._1
      val elementAtSecondIndex =
        sortedFareRdd_WithIndex.filter{case(fareVal:Double,index:Long)
        => index == (countOfFare/2)}.first._1
      (elementAtFirstIndex+elementAtSecondIndex)/2.0
    }
    println("Median of Fare variable is: "+median_Fare)

    // Mode of the variable Fare
    val fareDetails_WithCount =
      fare_Details_Df.groupBy("Fare").count()
    val maxOccurrence_CountsDf =
      fareDetails_WithCount.select(max("count")).alias("MaxCount")
    val maxOccurrence = maxOccurrence_CountsDf.first().getLong(0)
    val fares_AtMaxOccurrence =
      fareDetails_WithCount.filter(fareDetails_WithCount("count") ===
        maxOccurrence)
    if(fares_AtMaxOccurrence.count() == 1)
      println ("Mode of Fare variable is: "+fares_AtMaxOccurrence.first().getDouble(0))

    else {
      val modeValues = fares_AtMaxOccurrence.collect().map{row =>
        row.getDouble(0)}
      println("Fare variable has more 1 mode: ")
      modeValues.foreach(println)
    }
    //Spread of the variable
    println("Variance is: "+summary_Fare.variance)


    // Univariate analysis for Categorical data
    val class_Details_Df = titanic_data.select("Pclass")
    val count_OfRows = class_Details_Df.count()
    println("Count of Pclass rows: "+count_OfRows)
    val classDetails_GroupedCount =
      class_Details_Df.groupBy("Pclass").count()
    val classDetails_PercentageDist =
      classDetails_GroupedCount.withColumn("PercentDistribution",
        classDetails_GroupedCount("count")/count_OfRows)
    classDetails_PercentageDist.show()
}
}