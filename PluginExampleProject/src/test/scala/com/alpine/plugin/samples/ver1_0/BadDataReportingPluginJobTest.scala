package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.test.mock.OperatorParametersMock
import com.alpine.plugin.test.utils.{GolfData, OperatorParameterMockUtil, TestSparkContexts, SimpleAbstractSparkJobSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BadDataReportingPluginJobTest extends SimpleAbstractSparkJobSuite {

  import TestSparkContexts._

  test("Test that null data is removed") {

    val operator = new BadDataReportingPluginJob
    val dataFrameInput = GolfData.createGolfDFWithNullRows(sc)
    val parameters =new OperatorParametersMock("TestBadData", "golf")
    parameters.setValue(HdfsParameterUtils.badDataReportParameterID,
      HdfsParameterUtils.badDataReportNO)
    parameters.setValue(BadDataConstants.badDataTypeParamId, BadDataConstants.ALL_NULL)
    parameters.setValue(BadDataConstants.styleTagParamId, BadDataConstants.DEFAULT_STYLE_TAG )

    OperatorParameterMockUtil.addHdfsParamsDefault(parameters, "BadDataReportingTest")
    val (r, _) =  runDataFrameThroughDFTemplate(dataFrameInput, operator, parameters)
    val justGoodData = GolfData.createGolfDF(sc)

    //using Subtraction in this way is a solution to test rdd equality
    val resultMinusGood = r.rdd.subtract(justGoodData.rdd).collect()
    val goodMinusResult = justGoodData.rdd.subtract(r.rdd).collect()
    assert(resultMinusGood.length == 0, "There is still bad data in output DF ")
    assert(goodMinusResult.length == 0, "There are extra rows in the data output")
 }

  test("Test that null and zero data is removed") {

    val operator = new BadDataReportingPluginJob
    val dataFrameInput = GolfData.createGolfDFWithNullAndZeroRows(sc)
    val parameters =new OperatorParametersMock("TestBadData", "golf")
    parameters.setValue(HdfsParameterUtils.badDataReportParameterID,
      HdfsParameterUtils.badDataReportNO)
    parameters.setValue(BadDataConstants.badDataTypeParamId, BadDataConstants.NULL_AND_ZERO)
    parameters.setValue(BadDataConstants.styleTagParamId, BadDataConstants.DEFAULT_STYLE_TAG )

    OperatorParameterMockUtil.addHdfsParamsDefault(parameters, "BadDataReportingTest")
    val (r, _) =  runDataFrameThroughDFTemplate(dataFrameInput, operator, parameters)
    val justGoodData = GolfData.createGolfDF(sc)

    //using Subtraction in this way is a solution to test rdd equality
    val resultMinusGood = r.rdd.subtract(justGoodData.rdd).collect()
    val goodMinusResult = justGoodData.rdd.subtract(r.rdd).collect()
    assert(resultMinusGood.length == 0, "There is still bad data in output DF ")
    assert(goodMinusResult.length == 0, "There are extra rows in the data output")
  }

}
