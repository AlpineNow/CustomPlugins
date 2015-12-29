package com.alpine.plugin.samples.ver1_0

import com.alpine.model.ClusteringRowModel
import com.alpine.model.pack.util.CastedDoubleSeq
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.{ColumnDef, HdfsTabularDataset, OperatorSchemaManager}
import com.alpine.plugin.core.spark.utils.{MLlibUtils, SparkRuntimeUtils}
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core._
import com.alpine.plugin.core.utils.HtmlTabulator
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}
import com.alpine.plugin.model.ClusteringModelWrapper
import com.alpine.result.ClusteringResult
import com.alpine.transformer.{ClusteringTransformer, CategoricalTransformer}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
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
   * The information will be avalible at output visualization, so we can use it to display
   * a table with information about the centers.
   */

  def makeAddendum(inputFeatureNames : Array[String], clusters : Array[DenseVector],
    labels : Seq[String]) = {

    val clusterTableHeader = "ClusterName" :: inputFeatureNames.toList

    val clusterTableBody = labels.zip(clusters).map{
      case (name, vector) => name ::  vector.values.map(_.toString).toList
    }
    val clusterTable = HtmlTabulator.format(clusterTableHeader :: clusterTableBody.toList )

    Map[String, AnyRef](KMeansConstants.visualOutputKey -> clusterTable)
  }
}

/**
 * Defines the Clustering model, extends the Alpine ClusteringRowModel class.
 * Note: I have designed this class so that it takes some parameters
 * (which we get from the parameters of the Operator and from the from the MLLib Model)
 * but the uer may design the class so that it takes any parameter that are convenient).
 *
 * NOTE: We have an existing K Means Model and transformer included in the Model SDK. They
 * are called KMeansModel and KMeansModelTransformer. However this sample is intended to show how
 * one could write more custom code in a transformation model.
 *
 * NOTE: In order to use this with the existing Alpine Predictor, this class CANNOT contain any Spark Dependencies.
 * The reason is the the Alpine Spark Predictors are used with Map Reduce
 *
 * @param inputFeatures A sequence of Alpine Column Definitions for the columns that were used as
 *                      input features in training the model. Prediction will require a dataset that
 *                      has these input features present.
 *
 * @param clusters An array of Dense vectors representing the cluster centers trained by the model.
 *                 In this example these come from the MLLib KMeans model, but there is no restriction on
 *                 how the centers are defined. In fact in this instance they are only used by the Transformer
 *                 which uses them to score each row.
 * @param classLabels - the names of the clusters
 */
class ExampleKMeansClusteringModel(val inputFeatures : Seq[ColumnDef], clusters : Array[Array[Double]],
override val classLabels: Seq[String])
  extends ClusteringRowModel {

  /**
   * The 'transformer for this model' which is the class which has methods to score each row.
   * In this case I have used a custom transformer defined in the 'KMeansTransformer' class below.
   */
  def transformer: CategoricalTransformer[_ <: ClusteringResult] =
    new ExampleKMeansTransformer(clusters, classLabels)
}

/**
 * The class used to score the row. We extend CategoricalTransform because each row gets sorted into
 * a discrete category.
 *
 * NOTE: In order to use this with the existing Alpine Predictor, this class CANNOT contain any Spark Dependencies.
 * The reason is the the Alpine Spark Predictors are used with Map Reduce
 * @param clusters Passed from the parameters in the Example KMeansClusteringModel, these represent
 *                 the cluster centers derived from the training model.
 * @param classLabels - the names of the clusters
 */
class ExampleKMeansTransformer( clusters : Array[Array[Double]],
  override val classLabels : Seq[String] ) extends  ClusteringTransformer {
  val numClasses = classLabels.length

  override def scoreDistances(row: Row): Array[Double] = {
    val doubleRow = CastedDoubleSeq(row)

    val distances = Array.ofDim[Double](numClasses)
    var i = 0
    while (i < numClasses) {
      distances(i) = distanceWithoutSpark(doubleRow.toArray, clusters(i))
      i += 1
    }
    distances
  }

  /**
   * This function computes the square distance between two vectors.
   * If we didn't care about using Alpine's Map-Reduce Predictors we could keep the clusters as
   * dense vectors an use Vectors.sqdist(cluster, vector)
   */
  private def distanceWithoutSpark(row: Array[Double], cluster: Array[Double]): Double = {
    val dim = row.length
    var d = 0d
    var i = 0
    while(i < dim) {
      val diff = row(i) - cluster(i)
      d += diff * diff
      i += 1
    }
    math.sqrt(d)
  }
}
