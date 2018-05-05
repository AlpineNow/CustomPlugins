package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.io.TSVAttributes
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.utils.{HdfsParameterUtils, HdfsStorageFormatType}
import com.alpine.plugin.test.mock._
import com.alpine.plugin.test.utils.{GolfData, OperatorParameterMockUtil, SimpleAbstractSparkJobSuite, TestSparkContexts}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

//Include this tag so that the tests can be run from the command line with mvn test
//this also means that this test will be run automatically with mvn package
@RunWith(classOf[JUnitRunner])
class ColumnFilterPluginTest extends SimpleAbstractSparkJobSuite  {
  import TestSparkContexts._
  test("Col Filter Spark Job"){
    //create the input data frame
    val inputRows = List(
      Row("Masha", 22),
      Row("Ulia", 21),
      Row("Nastya", 23))
    val inputSchema =
      StructType(List(
        StructField("name", StringType),
        StructField("age", IntegerType)))
    val input : RDD[Row] = sc.parallelize(inputRows)
    //create a dataFrame using that test data
    // and the sqlContext provided by the parent class
    val dataFrameInput = sparkSession.createDataFrame(input, inputSchema)

    val colFilterJob = new ColumnFilterJob
    val uuid = "1"
    val colFilterName = "testColumnFilter"
    //create a mock parameters object
    val parametersMock = new OperatorParametersMock(colFilterName, uuid)

    //add the columns to keep param
    OperatorParameterMockUtil.addTabularColumns(
      params = parametersMock,
      ColumnFilterUtil.COLUMNS_TO_KEEP_KEY , "name")
    //add the HDFS storage parameters
    OperatorParameterMockUtil.addHdfsParamsDefault(parametersMock, "ColumnFilterTestResults")

    val (result , _)  = runDataFrameThroughDFTemplate(
      dataFrame = dataFrameInput,
      operator = colFilterJob,
      params = parametersMock)

    val expectedRows =  Array(
      Row("Masha"),
      Row("Ulia"),
      Row("Nastya")
    )

    assert(!result.schema.fieldNames.contains("age"))
    assert(result.collect().sameElements(expectedRows))
  }

  /**
    * This test makes sure that the parameter IDs used in the GUI node and Spark Job match and that
    * the parameters are added correctly.
    */
  test("Test Col Filter GUI and Spark Job using Provided Golf Dataset"){

    //The Golf data object is provided in our test utils class.
    val dataFrameInput = GolfData.createGolfDF(sparkSession)
    val operatorGUI = new ColumnFilterGUINode

    val inputParameters = new OperatorParametersMock("2", "TestFullColumnFilter")
    OperatorParameterMockUtil.addTabularColumns(inputParameters, ColumnFilterUtil.COLUMNS_TO_KEEP_KEY, "outlook", "play")
    OperatorParameterMockUtil.addHdfsParamsDefault(inputParameters, "FullColumnFilterTestResults")

    val inputHdfs = HdfsDelimitedTabularDatasetDefault(
      "target/testResults",
      sparkUtils.convertSparkSQLSchemaToTabularSchema(dataFrameInput.schema),
      TSVAttributes.defaultCSV
    )

    val defaultParams = getNewParametersFromDataFrameGui(operatorGUI, inputHdfs, inputParameters)

    assert(defaultParams.contains(ColumnFilterUtil.COLUMNS_TO_KEEP_KEY))

    //the default storage format is TSV. Check that that value was added to the parameters in the GUI
    assert(HdfsParameterUtils.getHdfsStorageFormatType(defaultParams).equals(HdfsStorageFormatType.CSV))

    val (outputDF, _) = runDataFrameThroughEntireDFTemplate(operatorGUI,
    operatorJob = new ColumnFilterJob, inputParams = inputParameters, dataFrameInput = dataFrameInput )

    assert(outputDF.schema.fieldNames.toSet.equals(Set("outlook", "play")),
      "The output schema is incorrect.")
  }

}

