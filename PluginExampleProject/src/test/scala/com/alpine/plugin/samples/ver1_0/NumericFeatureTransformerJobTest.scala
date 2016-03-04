package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.test.mock.OperatorParametersMock
import com.alpine.plugin.test.utils.{IrisFlowerPrediction, TestSparkContexts, OperatorParameterMockUtil, SimpleAbstractSparkJobSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NumericFeatureTransformerJobTest extends SimpleAbstractSparkJobSuite {

  //we need to import this in order to have a local Spark context to work with
  import TestSparkContexts._

  test("Test addendum and schema"){
    val path = "src/test/resources/irisDataSet"
    val irisData = sc.textFile(path )
    val irisDF = IrisFlowerPrediction.convertIrisRDDtoDF(irisData, sqlContext)

    val parametersMock = new OperatorParametersMock("TestNFT", "123")
    //add columnsToTransform
    OperatorParameterMockUtil.addTabularColumns(parametersMock,
      NumericFeatureTransformerUtil.columnsToTransformKey,
      "sepalLength", "sepalWidth")

    parametersMock.setValue(NumericFeatureTransformerUtil.transformationTypeKey,
      NumericFeatureTransformerUtil.pow2)

    OperatorParameterMockUtil.addHdfsParams(parametersMock, "NumericTransformerTest")

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
