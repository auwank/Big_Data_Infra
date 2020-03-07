package com.cellariot.spark
import scala.collection.mutable.ListBuffer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Find the superhero with the most co-appearances. */
object TopTenSuperhero {
  
  // Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }
  
  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new  SparkConf().setMaster("local[*]").setAppName("TopTenSuperhero").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named MostPopularSuperhero
    //alternative: val sc = new SparkContext("local[*]", "MostPopularSuperhero")
    val sc = new SparkContext(conf)   
    
    // Build up a hero ID -> name RDD
    val names = sc.textFile("../marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)
    
    // Load up the superhero co-apperarance data
    val lines = sc.textFile("../marvel-graph.txt")
    
    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurences)
    
    // Combine entries that span more than one line & sort in descending order
    val totalFriendsByCharacter = pairings.reduceByKey( (x,y) => x + y )
    val totalFriendsByCharacter_sorted = totalFriendsByCharacter.sortBy(-_._2)
    
    //collecting result
    val results = totalFriendsByCharacter_sorted.collect()
    
    // Taking top 10 results
    val res_10 = results.take(10)
     
    // list to populate with top 10 results
    var lst_1 = ListBuffer[String]()
   println("Top 10 superheroes are as below:") 
   
   // Population list with result
 	 for (x <- res_10) {
 		lst_1 += namesRdd.lookup(x._1)(0)                     //> 1
                                                  //| 2
                                                  //| 3
                                                  //| 4
 		}
    println(lst_1)
  }
}
