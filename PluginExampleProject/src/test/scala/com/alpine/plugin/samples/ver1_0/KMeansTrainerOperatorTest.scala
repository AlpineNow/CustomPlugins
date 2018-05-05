package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.io._
import com.alpine.plugin.core.utils.HdfsStorageFormatType
import com.alpine.plugin.core.visualization.{HtmlVisualModel, VisualModel}
import com.alpine.plugin.model.ClusteringModelWrapper
import com.alpine.plugin.test.mock._
import com.alpine.plugin.test.utils.{IrisFlowerPrediction, OperatorParameterMockUtil, SimpleAbstractSparkJobSuite, TestSparkContexts}
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KMeansTrainerOperatorTest extends SimpleAbstractSparkJobSuite with BeforeAndAfterAll {
  //we need to import this line in order to get the pre-defined Spark Local Context Test
  import TestSparkContexts._
  val outputPath = "target/results/Kmeans"
  var kMeansDefaultOutput : ClusteringModelWrapper = _
  var irisDF: DataFrame = _
  var inputHdfsDataset : HdfsDelimitedTabularDataset = _

  override protected def beforeAll(): Unit = {
    irisDF = IrisFlowerPrediction.createIrisDataFrame(sparkSession)
    inputHdfsDataset = this.createHdfsTabularDatasetLocal(irisDF, outputPath)
  }

  test("Test of K Means Plugin Spark Job using Iris dataset"){
    val parametersMock = new OperatorParametersMock("Iris Kmeans", "1776")
    OperatorParameterMockUtil.addTabularColumns(parametersMock,
      KMeansConstants.featuresParamId, irisDF.schema.fieldNames : _* )
    parametersMock.setValue(KMeansConstants.numClustersParamId, 5)
    parametersMock.setValue(KMeansConstants.numIterationsParamId, 100)
    OperatorParameterMockUtil.addHdfsParams(
      operatorParametersMock = parametersMock,
      outputName = "KMeansOutput",
      outputDirectory = outputPath,
      storageFormat = HdfsStorageFormatType.CSV,
      overwrite = true
    )

    val kMeansJob = new KMeansTrainerJob
    val resultModelWrapper: ClusteringModelWrapper = runInputThroughOperator[HdfsTabularDataset, ClusteringModelWrapper](
      inputHdfsDataset, kMeansJob, parametersMock)

    val clusterMessage = resultModelWrapper.addendum(KMeansConstants.visualOutputKey)
    println(clusterMessage)

    assert(resultModelWrapper.model.isInstanceOf[ExampleKMeansClusteringModel])
  }

  test("Test Entire K Means Operator with default parameters"){
    val guiNode = new KMeansTrainerGUINode
    val job = new KMeansTrainerJob

    val inputParameters = new OperatorParametersMock("2", "K Means Trainer Job")
    OperatorParameterMockUtil.addTabularColumns(inputParameters, KMeansConstants.featuresParamId,
      "sepalLength", "sepalWidth", "petalWidth")
    OperatorParameterMockUtil.addHdfsParamsDefault(inputParameters, "KMeansFullOperatorTestResult")
    //the result of the parameters will be assigned their default values.

    //run the onPlacement method, which will add parameters to the mock operator default
    val operatorDialogMock = new OperatorDialogMock(inputParameters, inputHdfsDataset)
    guiNode.onPlacement(operatorDialogMock, new OperatorDataSourceManagerMock(DataSourceMock("HdfsDataSource")),
      new OperatorSchemaManagerMock())


    val defaultParameters = operatorDialogMock.getNewParameters
    //assert that the number of clusters is set to the default value five
    assert(defaultParameters.getIntValue(KMeansConstants.numClustersParamId) == 5)

    //test the gui node and run time class.
    kMeansDefaultOutput = runInputThroughEntireOperator(inputHdfsDataset, guiNode, job, inputParameters)

    assert(kMeansDefaultOutput.model.isInstanceOf[ExampleKMeansClusteringModel])
    //check the output visualization method
    //we can use this to test some things about what the visualization will be.

  }

  test("K Means Visualization"){
    val guiNode = new KMeansTrainerGUINode
    val runtimeClass = new KMeansTrainerRuntime

    val inputParameters = new OperatorParametersMock("2", "K Means Trainer Job")
    OperatorParameterMockUtil.addTabularColumns(inputParameters, KMeansConstants.featuresParamId,
      "sepalLength", "sepalWidth", "petalWidth")
    OperatorParameterMockUtil.addHdfsParamsDefault(inputParameters, "KMeansFullOperatorTestResult")
    //the result of the parameters will be assigned their default values.
    //run the onPlacement method, which will add parameters to the mock operator default
    val operatorDialogMock = new OperatorDialogMock(inputParameters, inputHdfsDataset)
    guiNode.onPlacement(operatorDialogMock, new OperatorDataSourceManagerMock(DataSourceMock("HdfsDataSource")),
      new OperatorSchemaManagerMock())

    val defaultParameters = operatorDialogMock.getNewParameters
    //assert that the number of clusters is set to the default value five
    assert(defaultParameters.getIntValue(KMeansConstants.numClustersParamId) == 5)

    val visualModels: VisualModel = runtimeClass.createVisualResults(
      new SparkExecutionContextMock(null), inputHdfsDataset, kMeansDefaultOutput, defaultParameters, new SimpleOperatorListener
    )
    //cast to the html model so we can see what the text looks like.
    val visualizationText = visualModels.asInstanceOf[HtmlVisualModel].html
    assert(visualizationText.contains("<table ><tr><td style = \"padding-right:10px;\" >"))
  }

}
