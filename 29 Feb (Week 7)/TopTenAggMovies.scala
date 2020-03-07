package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

/** Find the movies with the most ratings. */
object TopTenAggMovies {
 
    /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }
  
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new  SparkConf().setMaster("local[*]").setAppName("PopularMovies").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named PopularMovies
    //alternative: val sc = new SparkContext("local[*]", "PopularMovies")
    val sc = new SparkContext(conf)   
    
    // Read in each rating line
    val lines = sc.textFile("../ml-100k/u.data")
    
    // Map to (movieID, 1) (rating, 1) tuples
    val movies = lines.map(x => ((x.split("\t")(1).toInt), (x.split("\t")(2).toInt,1)))
    
    // movie aggregated ratings & rating count    
    val movies_count = movies.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2))
    
    // movie ratings count >200
    val movies_fltr = movies_count.filter(x => x._2._2 > 200)
    
    // sorting by descending aggregated rating
    val movies_sorted = movies_fltr.sortBy(-_._2._1)
    
    // mapping movie names to movieId
    var nameDict = sc.broadcast(loadMovieNames)
    val sortedMoviesWithNames = movies_sorted.map(x => (nameDict.value(x._1), x._2._1))
    
    // collecting result   
    val results = sortedMoviesWithNames.collect()
    println("Top 10 movies aggregated movies are as below:")
    // printing top 10 results
    var p = 10 
    var q = 1
    for (x <- results) {    
      if (p > 0)
      {
  	println(s"$q:$x")
  	 p -= 1
  	 q += 1
  }       
    } 
  }
}

