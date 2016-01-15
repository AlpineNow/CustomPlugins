package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.test.SimpleOperatorListener
import com.alpine.plugin.test.mock.OperatorParametersMock

class KMeansTrainerJobTest extends  SimpleAbstractSparkJobSuite {

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
    val resultModelWrapper = kMeansJob.onExecution(sc,appConf = scala.collection.mutable.Map[String,String]()
      ,inputHdfsDataset, parametersMock, l)

    val clusterMessage = resultModelWrapper.addendum.get(KMeansConstants.visualOutputKey).get
    println(clusterMessage)

    assert(resultModelWrapper.model.isInstanceOf[ExampleKMeansClusteringModel])
  }

}
