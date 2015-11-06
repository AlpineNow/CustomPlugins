package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.test.mock.OperatorParametersMock
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * This show cases an alternate way of creating the DataFrame
 * Rather than creating a row RDD and a schema we can use a case class whose fields correspond
 * to the dataFrames schema
 */
case class IrisFlower(
   sepalLength: Double,
   sepalWidth: Double,
   petallength: Double,
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
class NumericFeatureTransformerJobTest extends SimpleAbstractSparkJobSuite {

  test("Test addendum and schema"){
    val path = "src/test/resources/irisDataSet"
    val irisData = sc.textFile(path )
    val irisDF = IrisFlowerPrediction.convertIrisRDDtoDF(irisData, sqlContext)

    val parametersMock = new OperatorParametersMock("TestNFT", "123")
    //add columnsToTransform
    ParameterMockUtil.addTabularColumns(parametersMock,
      NumericFeatureTransformerUtil.columnsToTransformKey,
      "sepalLength", "sepalWidth")

    parametersMock.setValue(NumericFeatureTransformerUtil.transformationTypeKey,
      NumericFeatureTransformerUtil.pow2)

    ParameterMockUtil.addHdfsParams(operatorParametersMock = parametersMock,
    outputName = "NumericTransformerTest")

    val nftSparkJobClass = new NumericFeatureTransformerJob

    val (dataFrame, addendum) = super.runDataFrameThroughDFTemplate(irisDF,
      nftSparkJobClass,
      params = parametersMock)

    //check that all the columns are included
    assert(dataFrame.schema.containsSlice(irisDF.schema))

    //check that the correct transformation type is being returned by the addendum
    assert(addendum(NumericFeatureTransformerUtil.transformationTypeVisualKey).toString
           .equals(NumericFeatureTransformerUtil.pow2))
  }

}
