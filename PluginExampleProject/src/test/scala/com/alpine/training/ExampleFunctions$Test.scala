package com.alpine.training


import com.alpine.plugin.test.utils.IrisFlowerPrediction
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class ExampleFunctions$Test extends FunSuite with BeforeAndAfterAll{
  self: org.scalatest.Suite  =>
  @transient var sc: SparkContext = _
  @transient var sqlContext : SQLContext = _
  override def beforeAll() {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.driver.host")
    sc = new SparkContext("local", "test")
    sqlContext = new SQLContext(sc )
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.driver.host")
    super.afterAll()
  }

  test("Test the sorting function"){
    val inputRowRDD = sc.parallelize(Array(
      Row.fromTuple(1, "e", 2 ),
      Row.fromTuple(1, "b", 8 ),
      Row.fromTuple(1, "c", 7 )
    ) )

    val inputSchema = StructType(Array(
      StructField("co1", IntegerType),
      StructField("col2", StringType),
      StructField("col3", IntegerType)
    ))

    val inputDF = sqlContext.createDataFrame(inputRowRDD, inputSchema)
    val sortedDF = ExampleFunctions.sortOneColumn(inputDF, "col2")
    val resultArray =  sortedDF.collect().map(_.getString(0))
    assert(resultArray(0).equals("b"))
    assert(resultArray(1).equals("c"))
    assert(resultArray(2).equals("e"))
  }


  test("simple test of k means function"){
    val inputRowRDD = sc.parallelize(Array(
      Row.fromTuple(1.0, 2.0 , 1.0 ),
      Row.fromTuple(2.0, 4.0 , 3.0 ),
      Row.fromTuple(3.0, 6.0 , 3.0 ),
      Row.fromTuple(4.0, 4.0 , 3.0 ),
      Row.fromTuple(1.0, 2.0 , 1.0 )
    ) )

    val inputSchema = StructType(Array(
      StructField("co1", DoubleType),
      StructField("col2", DoubleType),
      StructField("col3", DoubleType)
    ))

    val inputDF = sqlContext.createDataFrame(inputRowRDD, inputSchema)
    val clusteredDf = ExampleFunctions.kMeans(inputDF, 4, 10)
    assert(clusteredDf.schema.containsSlice(inputDF.schema))
    assert(clusteredDf.count() == inputDF.count())
  }

  test("Test the k means function on the Iris Dataset"){
    val path = "src/test/resources/irisDataSet"
    val irisData = sc.textFile(path )
    val irisDF = IrisFlowerPrediction.convertIrisRDDtoDF(irisData, sqlContext)
    val clusteredDf = ExampleFunctions.kMeans(irisDF, 4, 10)
    assert(clusteredDf.schema.containsSlice(irisDF.schema))
    assert(clusteredDf.count() == irisData.count())
  }
}



