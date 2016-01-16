package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.{ColumnDef, OperatorSchemaManager, HdfsTabularDataset}
import com.alpine.plugin.core.spark.utils.{MLlibUtils, SparkRuntimeUtils}
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}
import com.alpine.plugin.core._
import com.alpine.plugin.model.ClusteringModelWrapper
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class KMeansTrainerSignature extends OperatorSignature[KMeansTrainerGUINode, KMeansTrainerRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - K Means Trainer",
      category = "Plugin Sample - Spark",
      author = "Rachel Warren",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
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

class KMeansTrainerGUINode extends OperatorGUINode[HdfsTabularDataset, ClusteringModelWrapper]{
  override def onPlacement(operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addTabularDatasetColumnCheckboxes(id = KMeansConstants.featuresParamId,
      label = "Input Features" , columnFilter = ColumnFilter.NumericOnly, selectionGroupId = "main")

    operatorDialog.addIntegerBox(id = KMeansConstants.numClustersParamId, label = "Number of Clusters",
      min = 0, max = 1000, defaultValue = 5 )

    operatorDialog.addIntegerBox(id = KMeansConstants.numIterationsParamId, label = "Number of Iterations",
      min = 0, max = 1000, defaultValue = 5 )
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

    //create a text visual model
    visualFactory.createHtmlTextVisualization(clusterTable)
  }
}

class KMeansTrainerRuntime extends SparkRuntimeWithIOTypedJob[
  KMeansTrainerJob, HdfsTabularDataset,
  ClusteringModelWrapper] {}

class KMeansTrainerJob extends SparkIOTypedPluginJob[HdfsTabularDataset, ClusteringModelWrapper]{
  override def onExecution(sparkContext: SparkContext,
    appConf: mutable.Map[String, String],
    input: HdfsTabularDataset,
    operatorParameters: OperatorParameters,
    listener: OperatorListener): ClusteringModelWrapper = {

    val sparkRuntimeUtils = new SparkRuntimeUtils(sparkContext)
    val inputData : DataFrame = sparkRuntimeUtils.getDataFrame(input)

    val (inputDataSetUUID, inputFeatureNames) =
      operatorParameters.getTabularDatasetSelectedColumns(KMeansConstants.featuresParamId)
    listener.notifyMessage("Generating features from the input dataset " + inputDataSetUUID)

    val selectedData = inputData.select(inputFeatureNames.head, inputFeatureNames.tail : _*)
    //add to Vector to MLLibUtils
    val vectorRDD : RDD[Vector] = selectedData.map(row => {
      val rowAsDoubles: Array[Double] =  row.toSeq.map(x => MLlibUtils.anyToDouble(x)).toArray
      Vectors.dense(rowAsDoubles)
    })

    val k = operatorParameters.getIntValue(KMeansConstants.numClustersParamId)
    val n = operatorParameters.getIntValue(KMeansConstants.numIterationsParamId)

    val mllibModel: KMeansModel = KMeans.train(vectorRDD, k, n )

    listener.notifyMessage("The model was trained with " + k + " clusters and "  + n + " iterations")

    val clusters : Array[DenseVector] = mllibModel.clusterCenters.map(_.toDense)

    //filter the input column definitions base on the value of the input features parameters
    //in order to get a sequence of the full Alpine Column Definitions  for the input feature
    val inputFeatureColumnDefs: Array[ColumnDef] =
      input.tabularSchema.getDefinedColumns.filter( columnDef =>
        inputFeatureNames.contains(columnDef.columnName)).toArray

    val clusterWithName = clusters.zipWithIndex.map{
      case (vector, index) => ("Cluster_"+index)
    }

    val labels = clusterWithName.toSeq
    val clustersAsDoubleArrays = clusters.map(_.toArray)


    val alpineModel = new ExampleKMeansClusteringModel(inputFeatureColumnDefs,
      clustersAsDoubleArrays, labels)

    //this will be used in the visualization to create a tab that shows which clusters were trained
    val clusterAddendum = makeAddendum(inputFeatureNames, clusters, labels)

    new ClusteringModelWrapper(modelName = "K Means Model", model = alpineModel, sourceOperatorInfo =
      Some(operatorParameters.operatorInfo), addendum = clusterAddendum)
  }

  /**
   * Create the Map used in the "addendum" in the creation of the Clustering Model Wrapper.
   * The information will be available at output visualization, so we can use it to display
   * a table with information about the centers.
   */

  def makeAddendum(inputFeatureNames : Array[String], clusters : Array[DenseVector],
    labels : Seq[String]) = {

    val clusterTableHeader = "ClusterName" :: inputFeatureNames.toList

    val clusterTableBody = labels.zip(clusters).map {
      case (name, vector) => name :: vector.values.map(_.toString).toList
    }
    val clusterTable = htmlFormat(clusterTableHeader :: clusterTableBody.toList)

    Map[String, AnyRef](KMeansConstants.visualOutputKey -> clusterTable)
  }

    /**
      * Allows the user to inject her own style tag into the html table's td elements.
      * The table will be built with <td style = \" + styleTag + \" > " for each of the cells.
      * NOTE: This is availible as a util function in 5.8
      * @param table a sequence of rows in the table, where each row is a sequence of strings.
      * @param styleTag an css style element. The default to string method uses "padding-right:10px;"
      * @return the html string representation of the table
      */
    def htmlFormat(table: Seq[Seq[String]], styleTag: String = "padding-right:10px;") = table match {
      case Seq() => ""
      case _ =>
        "<table >" + table.map(tr => "<tr>" +
          tr.map(td => "<td style = \"" + styleTag + "\" >" + td + "</td>").mkString + "</tr>").mkString +
          "</table>"
    }
  }
