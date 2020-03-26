package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object Boston_Housing_Linear_Reg {
  
  // Function for feature addition (column giving extreme high nitric oxide concentration as 1)
  def nox(x: Double) : Double = {
  	 if (x > 0.6) {
  	1     // extreme high nitric oxide
  } else {
  	0
  } 
  }
  
  // Function to create accessibility feature based on distance from employment center & radial highway
  def acc(x: Double, y:Double) : Double = { 
    if (x < 3.8 || y < 9.5){
      1 // accessible housing
    }
    else{
      0 // less accessible housing 
    }
  }
  
  /** Main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("Boston_Housing_Linear_Reg")
      .master("local[*]")     
      .getOrCreate()
    
  // Read input lines   
   val lines = spark.sparkContext.textFile("../BostonHousing.csv")
    
  //Remove header with column titles
    val ip_rdd_header = lines.map(row => row.split(",").map(_.trim.replace("\"", "")))     
    val ip_rdd_no_header = ip_rdd_header.mapPartitionsWithIndex {  (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      
  // Splitting input lines to get separate fields      
    val ip_rdd = ip_rdd_no_header.map(x => (x(0).toDouble, x(1).toDouble,
        x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble,
        (x(7)).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble, x(11).toDouble,
        x(12).toDouble, x(13).toDouble))    
               
  // Convert RDD to a DataFrame
    import spark.implicits._

    val columns = Seq("crim", "zn", "indus", "chas", "nox", "rm", "age", "dis", "rad", "tax", "ptratio", "b", "lstat", "medv")
    val df= ip_rdd.toDF(columns: _*)
    
  // Creating UDF to apply function to dataframe columns
    val UDF_nox = udf(nox _)
    val UDF_acc = udf(acc _)
      
  // Creating feature column "nox_ft" & "acc" by applying UDF
    val df_feat_1 = df.withColumn("nox_ft", UDF_nox(df("nox")))
    val df_feat_2 = df_feat_1.withColumn("acc", UDF_acc(df_feat_1("dis"),df_feat_1("rad")))
       
  // Creating feature vectors & label(value to predict) to create linear regression model     
    val assembler =  new VectorAssembler()
                     .setInputCols(Array("crim", "zn", "indus","chas","nox","rm","age","dis","rad","tax","ptratio","b","lstat","nox_ft","acc"))
                     .setOutputCol("features")

    val final_df = assembler.transform(df_feat_2).select($"medv".cast(DoubleType).as("label"), $"features")
    
  // Splitting data into training data and testing data
    val split = final_df.randomSplit(Array(0.7, 0.3))
    val train = split(0)
    val test = split(1)
       
  // Creating linear regression model
    val lr_model = new LinearRegression().setRegParam(0.3).setElasticNetParam(0.8).setMaxIter(100)  
    
  // Train the model using our training data
    val model = lr_model.fit(train)
    
  // Predicting using linear regression model 
    val pred = model.transform(test).cache()
    
  // Extract the predictions and the true labels.
    val pred_and_true = pred.select("prediction", "label").rdd.map(x => (x.getDouble(0), x.getDouble(1)))
    
  // Evaluating model performance thru RMSE
    val MSE = pred_and_true.map(x => math.pow((x._2 - x._1),2)).mean()
    val RMSE = math.pow(MSE, (0.5))
    println(s"\nRMSE score for model is: $RMSE\n")
    
  //  Print out the predicted and actual values for each point
    for (prediction <- pred_and_true) {
     println(prediction)
    }
    
  // Stop the session
    spark.stop()
  }
}