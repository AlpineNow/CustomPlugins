package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.test.mock.OperatorParametersMock
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


class ColumnFilterPluginTest extends SimpleAbstractSparkJobSuite  {

  test("Col Filter"){
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
    val dataFrameInput = sqlContext.createDataFrame(input, inputSchema)

    val colFilterJob = new ColumnFilterJob
    val uuid = "1"
    val colFilterName = "testColumnFilter"
    //create a mock parameters object
    val parametersMock = new OperatorParametersMock(colFilterName, uuid)

    //add the columns to keep param
    ParameterMockUtil.addTabularColumns(
      params = parametersMock,
      ColumnFilterUtil.COLUMNS_TO_KEEP_KEY , "name")
    //add the HDFS storage parameters
    ParameterMockUtil.addHdfsParams(parametersMock, "ColumnFilterTestResults")

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

}

