package com.alpine.training

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
 * These are some functions to help you learn Spark
 */
object ExampleFunctions {

  /**
   * This function takes a dataFrame and a column to sort. To do the sorting, it converts that column to
   * a normal Spark RDD (remember that mappings of this type are essentially free in Spark) and
   * uses Spark's award winning sortByKey function. Then it reconverts to a single column dataFrame
   * using the sqlContext from the input data frame and returns that one column, sorted
   */
  def sortOneColumn( dataFrame : DataFrame, colName : String ) : DataFrame = {
    //select the column and map to an rdd of just that value
    //note that when map is called on a data frame it is implicitly converted to an RDD of (key, value)
    //pairs where the key is from the column to be sorted
    //we have to convert to string since it could be any type
    val rdd = dataFrame.select(colName).map(r => (r.get(0).toString, 1))

    //sort the data by key
    val sorted = rdd.sortByKey()
    val sortedRowRDD = sorted.keys.map(x => Row(x))

    //define a new schema for the output dataframe
    val newSchema = StructType(Array(StructField("SortedColumn", StringType)))

    //create the dataFrame with a row rdd and the StructType
    dataFrame.sqlContext.createDataFrame(sortedRowRDD, newSchema)
  }

  /**
   * This function would be all the spark code one would need to add MlLib's kMeans algorithm to Alpine.
   * The function takes in the input data as a dataFrame
   */
  def kMeans(dataFrame: DataFrame, k : Int, numberIterations : Int ) : DataFrame = {
    //kmeans requires vectors of double type, so we have to map to get Double vectors
    val vectorRDD = dataFrame.map(row =>
      //the rows are of generic type, so we need to convert to string and then parse as a doulbe
         Vectors.dense(row.toSeq.map(rowValue => rowValue.toString.toDouble).toArray))

    //use the Kmeans class to compute the clusters
    val clusters = KMeans.train(vectorRDD, k, numberIterations )

    //predict each vector, and then create a row from the old vector with the prediction appended
    val predictionRowRDD = vectorRDD.map(v => Row.fromSeq( v.toArray ++ Array(clusters.predict(v))))

    //append a new "Cluster" column to the dataFrame
    val newSchema = dataFrame.schema.add("Cluster", IntegerType)
    dataFrame.sqlContext.createDataFrame(predictionRowRDD, newSchema)
  }

}
