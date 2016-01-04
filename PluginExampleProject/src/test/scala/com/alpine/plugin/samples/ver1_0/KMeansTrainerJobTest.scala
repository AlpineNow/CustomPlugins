package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.io._
import com.alpine.plugin.model.ClusteringModelWrapper
import com.alpine.plugin.test.mock.{SimpleOperatorListener, OperatorParametersMock}
import com.alpine.plugin.test.utils.{ParameterMockUtil, SimpleAbstractSparkJobSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KMeansTrainerJobTest extends SimpleAbstractSparkJobSuite {
  //we need to import this line in order to get the pre-defined Spark Local Context Test
  import com.alpine.plugin.core.spark.utils.TestSparkContexts._
  val outputPath = "target/results/Kmeans"
  test("Test of K Means Plugin Spark Job using Iris dataset"){
    val l = new SimpleOperatorListener
    val irisDataPath = "src/test/resources/irisDataSet"
    val irisData = sc.textFile(irisDataPath )
    val irisDF = IrisFlowerPrediction.convertIrisRDDtoDF(irisData, sqlContext)

    val parametersMock = new OperatorParametersMock("Iris Kmeans", "1776")
    ParameterMockUtil.addTabularColumns(parametersMock,
      KMeansConstants.featuresParamId, irisDF.schema.fieldNames : _* )
    parametersMock.setValue(KMeansConstants.numClustersParamId, 5)
    parametersMock.setValue(KMeansConstants.numIterationsParamId, 100)
    ParameterMockUtil.addHdfsParams(parametersMock, "KMeansOutput", outputDirectory = outputPath)

    val inputHdfsDataset = this.createHdfsTabularDatasetLocal(irisDF, Some(parametersMock.operatorInfo()),
    outputPath)

    val kMeansJob = new KMeansTrainerJob
    val resultModelWrapper = runInputThroughOperator[HdfsTabularDataset, ClusteringModelWrapper](
      inputHdfsDataset, kMeansJob, parametersMock)

    val clusterMessage = resultModelWrapper.addendum.get(KMeansConstants.visualOutputKey).get
    println(clusterMessage)

    assert(resultModelWrapper.model.isInstanceOf[ExampleKMeansClusteringModel])
  }

}
