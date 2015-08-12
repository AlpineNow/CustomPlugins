package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, OperatorSchemaManager, TabularSchemaOutline}
import com.alpine.plugin.core.spark.templates._
import com.alpine.plugin.core.spark.utils.SparkUtils
import com.alpine.plugin.core.{OperatorListener, OperatorMetadata, OperatorParameters, OperatorSignature}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}


/**
 * An example of a very simple pugin that take a  group by parameter and a a few numeric features and computers Sum
 * of each column
 *
 * Class to define:
 * 1. SIGNATURE CLASS (AggregationPluginSignature)  which has the type of the GUI-NODE CLASS (AggregationGuiNode)
 * and the RUNTIME CLASS (AggregationRuntime): Defines some metadata about the operator in particular its name,
 * author and optionally, version.
 *
 * 2. GUINODE Class(AggregationGUINode) defines the gui box for when the user clicks on the operator.
 * ***The onPlacement( ... ) method defines the parameters that will appear when you place the operator
 * ***The getSparkOutputSchema( ... )  defines the schema for the output (in this case as a scala STRUCTTYPE  used
 * to defined dataframe schemas.
 *
 * 3. RUNTIME CLASS (AggregationRuntime) which takes the SPARKJOB (AggregationPluginSparkJob) as a type parameter: Defines the run time behavior.
 * It is in this class where the spark environment is set up. To customize the spark settings override the getSparkJobConfiguration class.
 *
 * 4. PLUGIN SPARK JOB CLASS (AggregationPluginSparkJob) This is the logic of the spark job, which should be contained in the
 * "transform method which takes in the input dataset as a DataFrame and
 * produces the output as a dataFrame.
 */



class AggregationSignature extends OperatorSignature[
  AggregationGUINode,
  AggregationRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Aggregate By Product",
      author = "Rachel Warren",
      version = 1,
      category = "Transformation",
      helpURL = "",
      iconNamePrefix = "")
  }
}

object AggregationConstants {
  val GROUP_BY_PARAM_KEY = "group_by"
  val AGGREGATION_PARAM_KEY  = "Aggregate"
}

/**
 * Static object which defines the output column names so that they can be reused in the GUI (for the design time
 * schema) and to define the actual runtime output schema.
 */
object AggregationOutputSchema {
  //a method that defines the column defs.  for the output schema so they can be used in the GUI Node and spark job.
  def getColumnDefs(operatorParameters: OperatorParameters) : Array[ColumnDef] = {
    val (_, aggregationCols)  = operatorParameters.getTabularDatasetSelectedColumns(AggregationConstants.AGGREGATION_PARAM_KEY)
    val newNames = Array.ofDim[ColumnDef](aggregationCols.length+1)
    newNames(0) = ColumnDef("GroupKey", ColumnType.String)
    var i = 0
    while(i < aggregationCols.length){
      newNames(i+1) = ColumnDef(aggregationCols(i)+"_PRODUCT", ColumnType.Double)
      i+=1
    }
    newNames
  }
}

class AggregationGUINode extends SparkDataFrameGUINode[AggregationPluginSparkJob] {
  /*
   * Define the parameters here
   */
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    // Setup operator specific options
    operatorDialog.addTabularDatasetColumnDropdownBox(AggregationConstants.GROUP_BY_PARAM_KEY,
      "Column to Group By",
      ColumnFilter.CategoricalOnly,
      "main")
    operatorDialog.addTabularDatasetColumnCheckboxes(AggregationConstants.AGGREGATION_PARAM_KEY,
      "Columns to Aggregate",
      ColumnFilter.NumericOnly,
      "main"
    )
    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
  }

  override def defineOutputSchemaColumns(inputSchema: TabularSchemaOutline,
                                         params: OperatorParameters ) : Array[ColumnDef] = {
    AggregationOutputSchema.getColumnDefs(params) }
}

class AggregationRuntime extends SparkDataFrameRuntime[AggregationPluginSparkJob]{}

class AggregationPluginSparkJob extends SparkDataFrameJob {

  def transform(operatorParameters: OperatorParameters, inputDataFrame: DataFrame, sparkUtils: SparkUtils,
                listener: OperatorListener): DataFrame = {
    //get parameters
    val (_, groupByCol) =
      operatorParameters.getTabularDatasetSelectedColumn(AggregationConstants.GROUP_BY_PARAM_KEY)
    val ( _, colsToAggregate) =
      operatorParameters.getTabularDatasetSelectedColumns(AggregationConstants.AGGREGATION_PARAM_KEY)

    //write message to the UI
    listener.notifyMessage("Starting the spark job now")

    val selectedData = inputDataFrame.select(groupByCol, colsToAggregate : _*)
    val rowRDD =  selectedData.map(row => {
      val key = row.getString(0)
      val rest = Range(1, row.length).map(i => row.getDouble(i)).toList
      (key, rest)
    }).reduceByKey( (rowA, rowB) => rowA.zip(rowB).map{case (a, b) => a*b})
      .map{case (key, values) => Row.fromSeq( (key :: values).toSeq)}

    val newSchema = getSchema(operatorParameters,
      sparkUtils)
    //create a data frame with the row RDD and the schema
    inputDataFrame.sqlContext.createDataFrame( rowRDD, newSchema)
  }

  //convert column definitions used at design time to DataFrame schema.
  def getSchema(operatorParameters: OperatorParameters, sparkUtils : SparkUtils) : StructType = {
    val newColumns = AggregationOutputSchema.getColumnDefs(operatorParameters)
    StructType( newColumns.map( newCol  =>
      StructField(newCol.columnName,
        sparkUtils.convertColumnTypeToSparkSQLDataType(newCol.columnType), true))
      .updated(0, StructField(newColumns(0).columnName,
      StringType, true)) )
  }
}