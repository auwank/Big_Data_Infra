package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max

/** Find the average Friends by name */
object FriendsByName {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val name = fields(1)
    val friends_num = fields(3).toInt
    (name, friends_num)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new  SparkConf().setMaster("local[*]").setAppName("FriendsByName").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named MinTemperatures
    // alternative: val sc = new SparkContext("local[*]", "MinTemperatures")
    val sc = new SparkContext(conf)
    
    // Read each line of input data
    val lines = sc.textFile("../fakefriends.csv")
    
    // Convert to (name, friends_num) tuples
    val parsedLines = lines.map(parseLine)
       
    // Add 1 in tuple with friends_num to calculate average
    val friends_1 = parsedLines.map((x) => (x._1, (x._2,1)))
    
    // Reduce by name to get total friends
    val friends_by_name = friends_1.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2))
    
    // Get average friends
    val friends_avg = friends_by_name.map((x) => (x._1, x._2._2, (x._2._1/x._2._2)))
    
    // Sort by num of friends(descending)
    val sortedResults = friends_avg.sortBy(-_._3)
    
    // Collect, format, and print the results
    val results = sortedResults.collect()
    
    for (result <- results) {
       val name = result._1
       val friendsByName = result._3
       val name_times = result._2
       //val formattedTemp = f"$temp%.2f F"
       println(s"$name_times people have name '$name'  who have $friendsByName friends on average.") 
    }
      
  }
}