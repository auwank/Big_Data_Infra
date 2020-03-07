package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up top 10 words in a book as simply as possible. */
object TopTenWords {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new  SparkConf().setMaster("local[*]").setAppName("TopTenWords").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named WordCount
    //alternative: val sc = new SparkContext("local[*]", "WordCount")
    val sc = new SparkContext(conf) 
    
    // Read each line of my book into an RDD
    val input = sc.textFile("../book.txt")
    
    // Lower case & Split into words 
    val words = input.map(x => x.toLowerCase()).flatMap(x => x.split("\\W+"))
    
    // Eliminating common words
    val rdd2 = words.filter(x => (!x.contains("you"))&&(!x.contains("to"))
        &&(!x.contains("your"))&&(!x.contains("the"))&&(!x.contains("a"))
        &&(!x.contains("of"))&&(!x.contains("and")))
      
    // Count up the occurrences of each word & sort in descending order
    val wordCounts = rdd2.countByValue()
    val result = wordCounts.toSeq.sortBy(-_._2)
    
    //Printing top 10 words 
    println("Top 10 words by word count (excluding common words) are as below:")
    var p = 10 
    for (x <- result) {    
      if (p >= 0)
      {
  	println(x)
  	 p -= 1
  }      
    }    
  }
  }

