package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.Tuple2Default
import com.alpine.plugin.model.RegressionModelWrapper
import com.alpine.plugin.test.mock.{SimpleOperatorListener, OperatorParametersMock}
import com.alpine.plugin.test.utils.{OperatorParameterMockUtil, TestSparkContexts, SimpleAbstractSparkJobSuite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


/**
 * A test class for our linear regression algorithm and regression evaluator.
 * Demonstrates how to tests the behavior of two custom operators in sequence.
 *
 * Note: If you examine the results of the regression evaluator for the linear regression
 * model on this data you will notice that this algorithm does a very poor job on
 * data which isn't normalized.
 */
@RunWith(classOf[JUnitRunner])
class LinearRegressionTrainingJobTest extends SimpleAbstractSparkJobSuite {
  import TestSparkContexts._
  //values which we can use in both tests
  val outputDirectory = "target/testResults/regression"
  val listener = new SimpleOperatorListener

  //variables that will be assigned in both tests
  var regressionModel : RegressionModelWrapper =  _
  var manufacturedData : HdfsTabularDataset = _

  test("Test On ManufacturedData"){
    //model inputs
    val dependantVar = "columnC"
    val independentVar1 = "columnA"
    val independentVar2 = "columnB"

   //generate some dat to test the linear regression on
    val rows = Range(0, 50).map(i => Row.fromTuple(i.toDouble,
      i.toDouble, 2*i.toDouble+5))
    val schema = StructType(Array(
      StructField(independentVar1, DoubleType),
      StructField(independentVar2, DoubleType),
      StructField(dependantVar, DoubleType))
    )

    val df = sqlContext.createDataFrame(sc.parallelize(rows), schema)

    val localHDFSFile = createHdfsTabularDatasetLocal(dataFrame = df,
      opInfo = Some(OperatorInfo("123", "RegressionTest")),
    outputDirectory)

    //set the data to one of the class variables so we can use it in the next test
    manufacturedData = localHDFSFile

    //create the mock parameters object
    val params = new OperatorParametersMock("123", "RegressionTest")
    OperatorParameterMockUtil.addHdfsParams(params, "regressionOutput",
      outputDirectory = outputDirectory)
    OperatorParameterMockUtil.addTabularColumn(params, "dependentColumn",
      dependantVar)
    OperatorParameterMockUtil.addTabularColumns(params, "independentColumns",
      independentVar1, independentVar2 )

    val regressionOperator = new LinearRegressionTrainingJob

    val resultModel =
      regressionOperator.onExecution(sc,
      scala.collection.mutable.Map[String, String](),
        localHDFSFile, params, listener)

    regressionModel = resultModel

   //assert some things about the model we created
    assert (resultModel.model.dependentFeature.columnName.equals(dependantVar))
    assert (resultModel.model.inputFeatures.head.columnName.equals(independentVar1))
  }

  test("Test Regression Evaluator on Manufactured Data"){


    val operatorInfo = OperatorInfo("12345", "RegressionEvaluatorTest")
    val params = new OperatorParametersMock(operatorInfo.name, operatorInfo.uuid)
    OperatorParameterMockUtil.addHdfsParams(params, "RegressionEvaluatorTestOutput")

    val regEvalOperator = new RegressionEvaluatorJob

    //create a tuple with the results of the previous tests and the same
    val inputTuple = Tuple2Default("tuple", manufacturedData,
          regressionModel,
          Some(operatorInfo))

    //run the input data through the on-execution method
    val result = regEvalOperator.onExecution(sc, null,
          inputTuple, params, listener)

    val resultDataFrame = sparkUtils.getDataFrame(result)
    assert(resultDataFrame.collect().length == 1)

  }

}
