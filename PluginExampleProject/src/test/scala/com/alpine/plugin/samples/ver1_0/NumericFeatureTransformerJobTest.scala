package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.test.mock.OperatorParametersMock
import com.alpine.plugin.test.utils.{ParameterMockUtil, SimpleAbstractSparkJobSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NumericFeatureTransformerJobTest extends SimpleAbstractSparkJobSuite {
  import com.alpine.plugin.core.spark.utils.TestSparkContexts._

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
