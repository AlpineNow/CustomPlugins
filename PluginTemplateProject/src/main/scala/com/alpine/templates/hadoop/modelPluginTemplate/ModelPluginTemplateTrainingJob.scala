package com.alpine.templates.hadoop.modelPluginTemplate

import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.multiple.{CombinerModel, PipelineRegressionModel}
import com.alpine.model.pack.preprocess.{OneHotEncodedFeature, OneHotEncodingModel}
import com.alpine.plugin.core.{OperatorListener, OperatorParameters}
import com.alpine.plugin.core.io.{ColumnType, HdfsTabularDataset}
import com.alpine.plugin.core.spark.{AlpineSparkEnvironment, SparkIOTypedPluginJob}
import com.alpine.plugin.core.utils.{AddendumWriter, HtmlTabulator}
import com.alpine.plugin.model.RegressionModelWrapper

import scala.collection.mutable.ListBuffer

// TODO: Job class that does the heavy lifting. Include your spark code here
class ModelPluginTemplateTrainingJob extends SparkIOTypedPluginJob[HdfsTabularDataset, RegressionModelWrapper]
{
  import  ModelPluginTemplateUtils._

  override def onExecution(alpineSparkEnvironment: AlpineSparkEnvironment,
                           input: HdfsTabularDataset,
                           operatorParameters: OperatorParameters,
                           listener: OperatorListener): RegressionModelWrapper = {

    val inputDataFrame = alpineSparkEnvironment.getSparkUtils.getDataFrame(input)

    val dependentColumn = operatorParameters.getTabularDatasetSelectedColumnName(stringDependentColumn);
    val depColumnDef = input.tabularSchema.getDefinedColumns.find(c => c.columnName.equals(dependentColumn)).get;
    val independentColumnNames = operatorParameters.getTabularDatasetSelectedColumnNames(stringIndependentColumns);
    val categoricalPredDefs = input.tabularSchema.getDefinedColumns
      .filter(c =>c.columnType == ColumnType.String && independentColumnNames.contains(c.columnName));
    val categoricalPredNames = categoricalPredDefs.map(_.columnName);
    val continuousPredDefs = input.tabularSchema.getDefinedColumns
      .filter(c =>c.columnType != ColumnType.String && independentColumnNames.contains(c.columnName));
    val continuousPredNames = continuousPredDefs.map(_.columnName);
    // TODO: Collect Model params

    // TODO: Do spark modeling

    // TODO: Collect weights from the model, should be of the form Seq(predictorName, weight)
    val weightsMsg = new ListBuffer[Seq[String]]
    // TODO: Collect metrics from the model, should be of the form Seq(metricName, value)
    val metricsMsg = new ListBuffer[Seq[String]]

    // Prepare additional display items in results section
    // Summary tab display
    // TODO: Add any parameters to be displayed in summary output here as string list
    val messageList = List(
      List(s"$stringParamSelected ", ""),
      List(s"$stringDependentColumn:", dependentColumn),
      List(s"$stringIndependentColumns: ", independentColumnNames.mkString(" ")),
      AddendumWriter.emptyRow,
      List(s"$stringOutput "));

    val summaryMsg = HtmlTabulator.format(messageList);
    val addendum = Map(stringSummaryID -> summaryMsg, stringWeightsAddendumID -> weightsMsg,stringGOFID -> metricsMsg);

    // Create a pipeline model to use in scoring/model deployment
    // This will enable the operator to be connected to the predictor operator downstream for scoring
    // This sample shows use of Regression model. If it is a classification model this class will have to extend
    // ClassificationModelWrapper.
    // TODO: Fill out if categorical predictors are present else remove
    val encodedFeaturesBuffer = new ListBuffer[OneHotEncodedFeature]
    val encodedFeatures = {
      encodedFeaturesBuffer.append(
        new OneHotEncodedFeature(
          Array("","",""),// TODO: Fill it with one hot encoded categories in order
          Some(" ")));// TODO: Fill it with base one hot encoded category
      encodedFeaturesBuffer.result();
    }

    val unitModel: UnitModel = UnitModel(continuousPredDefs);
    val oneHotEncodingModel: OneHotEncodingModel = OneHotEncodingModel(encodedFeatures, categoricalPredDefs);
    val preProcessing: CombinerModel = CombinerModel.make(Seq(unitModel, oneHotEncodingModel));

    val rowModel = ModelPluginTemplateModel.make(
      coefficients = Seq(),
      inputFeatures = categoricalPredDefs ++ continuousPredDefs,
      0.0,
      dependentFeatureName = dependentColumn,
      identifier = stringIdentifier);

    new RegressionModelWrapper(
      PipelineRegressionModel(Seq(preProcessing), rowModel, stringIdentifier),
      addendum);
  }

}

