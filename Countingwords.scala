
package com.project.wordcount

//import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Countingwords {
	def main(args: Array[String]) {
	  
				
		val conf = new SparkConf().setMaster("local").setAppName("Wordcount")
		val sc = new SparkContext(conf)
		
		// created RDD 
    val data = sc.textFile ("/Users/ravishaggarwal/Desktop/spark-2.2.0-bin-hadoop2.7/data.txt")
    
    val result = data.flatMap { line => {
      line.split(" ")
      
      
    } }
		
		//MAPING
    .map { words => (words, 1)
     

      }
    //REDUCING
    .reduceByKey(_+_)
   
   
    result.collect.foreach(println)
    
    
	}
}

