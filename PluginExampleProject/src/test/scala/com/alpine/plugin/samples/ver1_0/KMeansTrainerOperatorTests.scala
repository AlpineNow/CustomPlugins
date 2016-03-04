package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.io._
import com.alpine.plugin.core.visualization.VisualModel
import com.alpine.plugin.model.ClusteringModelWrapper
import com.alpine.plugin.test.mock._
import com.alpine.plugin.test.utils.{IrisFlowerPrediction, OperatorParameterMockUtil, TestSparkContexts, SimpleAbstractSparkJobSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KMeansTrainerOperatorTests extends SimpleAbstractSparkJobSuite {
  //we need to import this line in order to get the pre-defined Spark Local Context Test
  import TestSparkContexts._
  val outputPath = "target/results/Kmeans"
  var kMeansDefaultOutput : ClusteringModelWrapper = _

  var inputHdfsDataset : HdfsDelimitedTabularDataset = _
  test("Test of K Means Plugin Spark Job using Iris dataset"){
    val irisDataPath = "src/test/resources/irisDataSet"
    val irisData = sc.textFile(irisDataPath )
    val irisDF = IrisFlowerPrediction.convertIrisRDDtoDF(irisData, sqlContext)

    val parametersMock = new OperatorParametersMock("Iris Kmeans", "1776")
    OperatorParameterMockUtil.addTabularColumns(parametersMock,
      KMeansConstants.featuresParamId, irisDF.schema.fieldNames : _* )
    parametersMock.setValue(KMeansConstants.numClustersParamId, 5)
    parametersMock.setValue(KMeansConstants.numIterationsParamId, 100)
    OperatorParameterMockUtil.addHdfsParams(parametersMock, "KMeansOutput", outputDirectory = outputPath)

     inputHdfsDataset = this.createHdfsTabularDatasetLocal(irisDF, Some(parametersMock.operatorInfo()),
    outputPath)

    val kMeansJob = new KMeansTrainerJob
    val resultModelWrapper: ClusteringModelWrapper = runInputThroughOperator[HdfsTabularDataset, ClusteringModelWrapper](
      inputHdfsDataset, kMeansJob, parametersMock)

    val clusterMessage = resultModelWrapper.addendum.get(KMeansConstants.visualOutputKey).get
    println(clusterMessage)

    assert(resultModelWrapper.model.isInstanceOf[ExampleKMeansClusteringModel])
  }

  test("Test Entire K Means Operator with default parameters"){
    val guiNode = new KMeansTrainerGUINode
    val job = new KMeansTrainerJob

    val inputParameters = new OperatorParametersMock("2", "K Means Trainer Job")
    OperatorParameterMockUtil.addTabularColumns(inputParameters, KMeansConstants.featuresParamId,
      "sepalLength", "sepalWidth", "petaLlength", "petalWidth")
    OperatorParameterMockUtil.addHdfsParams(inputParameters, "KMeansFullOperatorTestResult")
    //the result of the parameters will be assigned their default values.

    //run the onPlacement method, which will add parameters to the mock operator default
    val operatorDialogMock = new OperatorDialogMock(inputParameters, inputHdfsDataset, Some(inputHdfsDataset.tabularSchema))
    guiNode.onPlacement(operatorDialogMock, new OperatorDataSourceManagerMock(new DataSourceMock("HdfsDataSource")),
      new OperatorSchemaManagerMock(Some(inputHdfsDataset.tabularSchema)))


    val defaultParameters = operatorDialogMock.getNewParameters
    //assert that the number of clusters is set to the default value five
    assert(defaultParameters.getIntValue(KMeansConstants.numClustersParamId) == 5)

    //test the gui node and run time class.
    kMeansDefaultOutput = runInputThroughEntireOperator(inputHdfsDataset, guiNode,
      job, inputParameters, Some(inputHdfsDataset.tabularSchema))

    assert(kMeansDefaultOutput.model.isInstanceOf[ExampleKMeansClusteringModel])
    //check the output visualization method
    //we can use this to test some things about what the visualization will be.

  }

  test("K Means Visualizaton"){
    val guiNode = new KMeansTrainerGUINode
    val job = new KMeansTrainerJob

    val inputParameters = new OperatorParametersMock("2", "K Means Trainer Job")
    OperatorParameterMockUtil.addTabularColumns(inputParameters, KMeansConstants.featuresParamId,
      "sepalLength", "sepalWidth", "petaLlength", "petalWidth")
    OperatorParameterMockUtil.addHdfsParams(inputParameters, "KMeansFullOperatorTestResult")
    //the resut of the parameters will be assigned their default values.
    //run the onPlacement method, which will add parameters to the mock operator default
    val operatorDialogMock = new OperatorDialogMock(inputParameters, inputHdfsDataset, Some(inputHdfsDataset.tabularSchema))
    guiNode.onPlacement(operatorDialogMock, new OperatorDataSourceManagerMock(new DataSourceMock("HdfsDataSource")),
      new OperatorSchemaManagerMock(Some(inputHdfsDataset.tabularSchema)))

    val defaultParameters = operatorDialogMock.getNewParameters
    //assert that the number of clusters is set to the default value five
    assert(defaultParameters.getIntValue(KMeansConstants.numClustersParamId) == 5)

    val visualModels: VisualModel = guiNode.onOutputVisualization(defaultParameters, kMeansDefaultOutput,
      new VisualModelFactoryMock)
    //cast to the html model so we can see what the text looks like.
    val visualizationText = visualModels.asInstanceOf[HtmlVisualModel].text
    println(visualizationText)
  }

}
