package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.test.mock.OperatorParametersMock
import com.alpine.plugin.test.utils.{IrisFlowerPrediction, OperatorParameterMockUtil, SimpleAbstractSparkJobSuite, TestSparkContexts}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NumericFeatureTransformerJobTest extends SimpleAbstractSparkJobSuite {

  //we need to import this in order to have a local Spark context to work with
  import TestSparkContexts._

  test("Test addendum and schema"){
    val irisDF = IrisFlowerPrediction.createIrisDataFrame(sparkSession)

    val parametersMock = new OperatorParametersMock("TestNFT", "123")
    //add columnsToTransform
    OperatorParameterMockUtil.addTabularColumns(parametersMock,
      NumericFeatureTransformerUtil.columnsToTransformKey,
      "sepalLength", "sepalWidth")

    parametersMock.setValue(NumericFeatureTransformerUtil.transformationTypeKey,
      NumericFeatureTransformerUtil.pow2)

    OperatorParameterMockUtil.addHdfsParamsDefault(parametersMock, "NumericTransformerTest")

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
