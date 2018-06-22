package sqlsamples

import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object SummaryStats {
  
   def main(args:Array[String]): Unit = {
     
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SummaryStats")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    
    val salary_Data = sqlContext.read.format ("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema", "true") //inferSchema is the grammar
      .load("/home/cloudera/git/S-BEAD/W03-SparkSQL/data/Salaries.csv")
      
      /* 
       * S/N,rank,discipline,yrs_since_phd,yrs_service,sex,salary
       * */
      salary_Data.show()
      
      // Gets summary of all numeric fields
      val summary = salary_Data.describe()
      summary.show()
    
      
      // Get Summary on subset of columns
      val summary_subsetColumns = salary_Data.describe("sex", "yrs_service", "salary")
      summary_subsetColumns.show()

      
      /*  
      val spkSession = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			//.setAppName("SummaryStats")
			.master("local[2]")
			.getOrCreate()
	    val scDF = spkSession.sparkContext
      import spkSession.implicits._
      
      val sqlcontext = new SQLContext(scDF)
      
      //Summary of Female 
     // val Fsalary_Data = conf.sql
      
 
      
      //Summary of Male
      */
   
    // Get subset of statistics
    val subset_summary = salary_Data.select(mean("yrs_service"), min("yrs_service"), max("yrs_service"))
    subset_summary.show()

    
    val selected_Df = salary_Data.select("yrs_service","yrs_since_phd", "salary")
    selected_Df.show()

    
    //errors
    val observations = selected_Df.rdd.map{
      row =>
        val yrs_service = row.getInt(0)
        val yrs_since_phd = row.getInt(1)
        val salary = row.getInt(0)//if(row.isNullAt(2)) 0.0 else row.getInt(2)
        Vectors.dense(yrs_service, yrs_since_phd,salary)}
    val summary1 = Statistics.colStats(observations)
    
    
    println("Mean: "+summary1.mean)
    println("Variance: "+summary1.variance)
    println("Num of Non-zeros: "+summary1.numNonzeros)
  
      
      sc.stop()
   }
  
}