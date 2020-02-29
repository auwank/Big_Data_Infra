package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max

/** Find the max weather change */
object DateWeatherChange {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val date = fields(1)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, date, temperature)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new  SparkConf().setMaster("local[*]").setAppName("DateWeatherChange").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named MinTemperatures
    // alternative: val sc = new SparkContext("local[*]", "MinTemperatures")
    val sc = new SparkContext(conf)
    
    // Read each line of input data
    val lines = sc.textFile("../1800.csv")
    
    // Convert to (stationID, date, temperature)) tuples
    val parsedLines = lines.map(parseLine)
    
    // separating stations
    val parsedLines_ITE = parsedLines.filter(x => x._1 == "ITE00100554")
    val parsedLines_EZE = parsedLines.filter(x => x._1 == "EZE00100082")
    
    // getting date & temperature
    val ITE_temp = parsedLines_ITE.map(x => (x._2, x._3.toFloat))
    val EZE_temp = parsedLines_EZE.map(x => (x._2, x._3.toFloat))
    
    // temp diff for each day
    val ITE_temp_diff = ITE_temp.reduceByKey((x,y) => math.abs(x-y))  // we have date & temp difference
    val EZE_temp_diff = EZE_temp.reduceByKey((x,y) => math.abs(x-y))  // we have date & temp difference
    
    //getting max temp difference
    
    val ITE_max_temp_diff = ITE_temp_diff.reduce((x, y) =>  if (x._2 > y._2) x else y)
    val EZE_max_temp_diff = EZE_temp_diff.reduce((x, y) =>  if (x._2 > y._2) x else y )
    
    //collecting date & max temp difference
    
    val ITE_date_max_weather_chng = ITE_max_temp_diff._1  
    val ITE_max_weather_chng = ITE_max_temp_diff._2  
    val EZE_date_max_weather_chng = EZE_max_temp_diff._1  
    val EZE_max_weather_chng = EZE_max_temp_diff._2  
    
    //Checking max weather change between 2 stations
    if (ITE_max_weather_chng>EZE_max_weather_chng){
    println(s"max weather change is for station ITE00100554 of $ITE_max_weather_chng F on $ITE_date_max_weather_chng")}
    else{
    println(s"max weather change is for station EZE00100082 of $EZE_max_weather_chng F on $EZE_date_max_weather_chng")}
          
  }
}