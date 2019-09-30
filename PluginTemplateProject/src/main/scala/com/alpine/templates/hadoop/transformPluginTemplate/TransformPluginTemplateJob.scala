package com.alpine.templates.hadoop.transformPluginTemplate

import com.alpine.plugin.core.{OperatorListener, OperatorParameters}
import com.alpine.plugin.core.spark.templates.SparkDataFrameJob
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import org.apache.spark.sql.DataFrame

class TransformPluginTemplateJob extends SparkDataFrameJob{
  import TransformPluginTemplateUtils._

  // TODO: Your Spark code goes in here. The transform function should return a Spark DataFrame.
  //  Current example shows how to collect user parameters and return the filtered DataFrame, enhance with your logic.
  override def transform(parameters: OperatorParameters,
                         dataFrame: DataFrame,
                         sparkUtils: SparkRuntimeUtils,
                         listener: OperatorListener): DataFrame = {
    // grab the selected column names
    val columnNamesToKeep = parameters.getTabularDatasetSelectedColumns(parameterID)._2
    // returns the columns from the DF containing the column names
    val columnsToKeep = columnNamesToKeep.map(dataFrame.col)

    // returns a data frame with only the selected columns included
    dataFrame.select(columnsToKeep: _*)
  }

}

