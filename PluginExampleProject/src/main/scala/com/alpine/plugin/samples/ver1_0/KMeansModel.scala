package com.alpine.plugin.samples.ver1_0

import com.alpine.model.ClusteringRowModel
import com.alpine.model.pack.util.CastedDoubleSeq
import com.alpine.plugin.core.io.ColumnDef
import com.alpine.transformer.ClusteringTransformer


/**
  * Defines the Clustering model, extends the Alpine ClusteringRowModel class.
  * Note: I have designed this class so that it takes some parameters
  * (which we get from the parameters of the Operator and from the from the MLlib Model)
  * but the user may design the class so that it takes any parameter that are convenient).
  *
  * NOTE: We have an existing K-Means model and transformer included in the Model SDK. They
  * are called KMeansModel and KMeansModelTransformer. However, this sample is intended to show how
  * one could write more custom code in a transformation model.
  *
  * NOTE: In order to use these models with the existing Alpine Predictor, this class CANNOT contain any Spark dependencies.
  * This is because the Alpine Spark predictors are used with MapReduce.
  *
  * @param inputFeatures A sequence of Alpine column definitions for the columns that were used as
  *                      input features in training the model. Prediction will require a dataset that
  *                      has these input features present.
  * @param clusters      An array of dense vectors representing the cluster centers trained by the model.
  *                      In this example these come from the MLlib K-Means model, but there is no restriction on
  *                      how the centers are defined. In fact, in this instance they are only used by the transformer
  *                      which uses them to score each row.
  * @param classLabels   - the names of the clusters
  */
class ExampleKMeansClusteringModel(val inputFeatures: Seq[ColumnDef], clusters: Array[Array[Double]],
                                   override val classLabels: Seq[String])
  extends ClusteringRowModel {
  /**
    * The 'transformer for this model' which is the class which has methods to score each row.
    * In this case I have used a custom transformer defined in the 'KMeansTransformer' class below.
    */
  def transformer =
    new ExampleKMeansTransformer(clusters, classLabels)
}

/**
  * The class used to score the row. We extend CategoricalTransform because each row gets sorted into
  * a discrete category.
  *
  * NOTE: In order to use this with the existing Alpine Predictor, this class CANNOT contain any Spark Dependencies.
  * The reason is the the Alpine Spark predictors are used with MapReduce.
  *
  * @param clusters    Passed from the parameters in the Example KMeansClusteringModel, these represent
  *                    the cluster centers derived from the training model.
  * @param classLabels - the names of the clusters
  */
class ExampleKMeansTransformer(clusters: Array[Array[Double]],
                               override val classLabels: Seq[String]) extends ClusteringTransformer {
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
    * If we didn't care about using Alpine's MapReduce predictors we could keep the clusters as
    * dense vectors and use Vectors.sqdist(cluster, vector)
    */
  private def distanceWithoutSpark(row: Array[Double], cluster: Array[Double]): Double = {
    val dim = row.length
    var d = 0d
    var i = 0
    while (i < dim) {
      val diff = row(i) - cluster(i)
      d += diff * diff
      i += 1
    }
    math.sqrt(d)
  }
}
