package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, HdfsTabularDataset, OperatorSchemaManager}
import com.alpine.plugin.core.spark.utils.{MLlibUtils, SparkRuntimeUtils}
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.HtmlTabulator
import com.alpine.plugin.core.visualization.{HtmlVisualModel, VisualModel, VisualModelFactory}
import com.alpine.plugin.model.ClusteringModelWrapper
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class KMeansTrainerSignature extends OperatorSignature[KMeansTrainerGUINode, KMeansTrainerRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - K Means Trainer",
      category = "Plugin Sample - Spark",
      author = Some("Rachel Warren"),
      version = 1,
      helpURL = None,
      icon = None,
      toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
        "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
        " of the operator and are no more than fifty words.")
    )
  }
}

/**
  * Contains Constants that will be used throughout all the classes in the operator.
  */
object KMeansConstants {
  val featuresParamId = "features"
  val numClustersParamId = "k"
  val numIterationsParamId = "n"
  val visualOutputKey = "key"
}

class KMeansTrainerGUINode extends OperatorGUINode[HdfsTabularDataset, ClusteringModelWrapper] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addTabularDatasetColumnCheckboxes(id = KMeansConstants.featuresParamId,
      label = "Input Features", columnFilter = ColumnFilter.NumericOnly, selectionGroupId = "main")

    operatorDialog.addIntegerBox(id = KMeansConstants.numClustersParamId, label = "Number of Clusters",
      min = 0, max = 1000, defaultValue = 5)

    operatorDialog.addIntegerBox(id = KMeansConstants.numIterationsParamId, label = "Number of Iterations",
      min = 0, max = 1000, defaultValue = 20)
  }

  /**
    * We only need to override this method since we want the custom visualization behavior wherein
    * we have a tab that displays the value of the cluster centers.
    */
  override def onOutputVisualization(params: OperatorParameters, output: ClusteringModelWrapper,
                                     visualFactory: VisualModelFactory): VisualModel = {
    //get the Table from the addendum
    val clusterTable = output.addendum.getOrElse(KMeansConstants.visualOutputKey,
      "No Cluster Table Found").toString

    HtmlVisualModel(clusterTable)
  }
}

class KMeansTrainerRuntime extends SparkRuntimeWithIOTypedJob[
  KMeansTrainerJob, HdfsTabularDataset,
  ClusteringModelWrapper] {}

class KMeansTrainerJob extends SparkIOTypedPluginJob[HdfsTabularDataset, ClusteringModelWrapper] {
  override def onExecution(sparkContext: SparkContext,
                           appConf: mutable.Map[String, String],
                           input: HdfsTabularDataset,
                           operatorParameters: OperatorParameters,
                           listener: OperatorListener): ClusteringModelWrapper = {

    val sparkRuntimeUtils = new SparkRuntimeUtils(sparkContext)
    val inputData: DataFrame = sparkRuntimeUtils.getDataFrame(input)

    val (inputDataSetUUID, inputFeatureNames) =
      operatorParameters.getTabularDatasetSelectedColumns(KMeansConstants.featuresParamId)
    listener.notifyMessage("Generating features from the input dataset " + inputDataSetUUID)

    val selectedData = inputData.select(inputFeatureNames.head, inputFeatureNames.tail: _*)
    //add to Vector to MLLibUtils
    val vectorRDD: RDD[Vector] = selectedData.map(row => {
      val rowAsDoubles: Array[Double] = row.toSeq.map(x => MLlibUtils.anyToDouble(x)).toArray
      Vectors.dense(rowAsDoubles)
    })

    vectorRDD.cache()

    val k = operatorParameters.getIntValue(KMeansConstants.numClustersParamId)
    val n = operatorParameters.getIntValue(KMeansConstants.numIterationsParamId)

    val mllibModel: KMeansModel = KMeans.train(vectorRDD, k, n)

    listener.notifyMessage("The model was trained with " + k + " clusters and " + n + " iterations")

    val inputFeatureColumnDefs = inputFeatureNames.map(name => ColumnDef(name, ColumnType.Double))

    val clusters: Array[Array[Double]] = mllibModel.clusterCenters.map(_.toArray)
    val clusterNames = clusters.indices.map(index => "Cluster_" + index)

    val alpineModel = new ExampleKMeansClusteringModel(inputFeatureColumnDefs, clusters, clusterNames)

    //this will be used in the visualization to create a tab that shows which clusters were trained
    val clusterAddendum = makeAddendum(inputFeatureNames, clusters, clusterNames)

    new ClusteringModelWrapper(model = alpineModel, addendum = clusterAddendum)
  }

  /**
    * Create the Map used in the "addendum" in the creation of the Clustering Model Wrapper.
    * The information will be available at output visualization, so we can use it to display
    * a table with information about the centers.
    */

  private def makeAddendum(inputFeatureNames: Array[String], clusters: Array[Array[Double]], labels: Seq[String]) = {

    val clusterTableHeader = "ClusterName" :: inputFeatureNames.toList

    val clusterTableBody = labels.zip(clusters).map {
      case (name, vector) => name :: vector.map(_.toString).toList
    }
    val clusterTable = HtmlTabulator.format(clusterTableHeader :: clusterTableBody.toList)

    Map[String, AnyRef](KMeansConstants.visualOutputKey -> clusterTable)
  }
}