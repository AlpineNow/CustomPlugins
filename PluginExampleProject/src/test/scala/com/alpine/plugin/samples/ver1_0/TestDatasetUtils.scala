package com.alpine.plugin.samples.ver1_0

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

/**
 * This show cases an alternate way of creating the DataFrame
 * Rather than creating a row RDD and a schema we can use a case class whose fields correspond
 * to the dataFrames schema
 */
case class IrisFlower(
  sepalLength: Double,
  sepalWidth: Double,
  petaLlength: Double,
  petalWidth: Double)


object IrisFlowerPrediction {
  /**
   * Creates a DataFrame of the iris dataset given an RDD of the String rows
   */
  def convertIrisRDDtoDF(input: RDD[String], sQLContext: SQLContext): DataFrame = {
    val inputWithType = input.map(_.split(",")).map(flower =>
      IrisFlower(
        flower(0).trim.toDouble,
        flower(1).trim.toDouble,
        flower(2).trim.toDouble,
        flower(3).trim.toDouble)
    )
    sQLContext.createDataFrame(inputWithType)
  }
}


object GolfData{
  val golfRows: Seq[String] = {
    "sunny,85,85,false,no" ::
      "sunny,80,90,true,no" ::
      "overcast,83,78,false,yes" ::
      "overcast,64,65,true,yes" ::
      "sunny,72,95,false,no" ::
      "sunny,69,70,false,yes" ::
      "overcast,72,90,true,yes" ::
      "rain,70,96,false,yes" ::
      "rain,68,80,false,yes" ::
      "rain,65,70,true,no" ::
      "rain,75,80,false,yes" ::
      "rain,71,80,true,no" ::
      "sunny,75,70,true,yes" ::
      "overcast,81,75,false,yes" ::
      Nil
  }

  val nullRows = Seq(
    Row.fromTuple( "overcast", null ,null ,"","yes" ),
    Row.fromTuple( null, null ,76L ,"","yes" )
  )

  val zeroRows = Seq(
  Row.fromTuple( "sunny", 0L, 1L, "false", "yes"),
  Row.fromTuple("sunny", 75L, 0L, "false", "yes")
  )


  val golfSchema = StructType(Array(
    StructField("outlook", StringType) ,
    StructField( "humidity", LongType, nullable = true ),
    StructField( "temperature", LongType, nullable = true),
    StructField("wind", StringType), StructField( "play" , StringType)))

  def createGolfDFWithNullRows(sc : SparkContext ) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    val rowRDD = sc.parallelize(nullRows)
    val badData = sqlContext.createDataFrame(rowRDD, golfSchema)
    //union of the bad and good rows
    createGolfDF(sc).unionAll(badData)
  }

  def createGolfDFWithNullAndZeroRows(sc : SparkContext) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    val rowRDD = sc.parallelize(zeroRows)
    val badData = sqlContext.createDataFrame(rowRDD, golfSchema)
    //union of the bad and good rows
    createGolfDFWithNullRows(sc).unionAll(badData)
  }

  def createGolfDF(sc : SparkContext) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    val rows = golfRows.map(s => {
      val split = s.split(",")
      Row.fromTuple(split(0), split(1).toLong, split(2).toLong, split(3), split(4))
    })
    val rowRDD = sc.parallelize(rows)

    sqlContext.createDataFrame(rowRDD, golfSchema)
  }


}
