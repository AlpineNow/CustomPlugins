package com.alpine.templates.hadoop.modelPluginTemplate

import com.alpine.plugin.core.{OperatorListener, OperatorParameters}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, HdfsTabularDataset}
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.AddendumWriter
import com.alpine.plugin.core.visualization.{CompositeVisualModel, HtmlVisualModel, TabularVisualModel, VisualModel}
import com.alpine.plugin.model.RegressionModelWrapper

import java.util

class ModelPluginTemplateRuntime extends
  SparkRuntimeWithIOTypedJob[ModelPluginTemplateTrainingJob, HdfsTabularDataset, RegressionModelWrapper]
{
  import ModelPluginTemplateUtils._

  override def createVisualResults(context: SparkExecutionContext,
                                   input: HdfsTabularDataset,
                                   output: RegressionModelWrapper,
                                   params: OperatorParameters,
                                   listener: OperatorListener): VisualModel = {

    val summary = output.addendum.getOrElse(stringSummaryID, "Error").toString;
    val weightsInfo = addendumListInfoToScala(output.addendum(stringWeightsAddendumID)
      .asInstanceOf[util.ArrayList[util.ArrayList[String]]]);
    val trainingMetrics = addendumListInfoToScala(output.addendum(stringGOFID)
      .asInstanceOf[util.ArrayList[util.ArrayList[String]]]);

    val composite = new CompositeVisualModel;

    val summaryModel = HtmlVisualModel(summary);
    composite.addVisualModel(AddendumWriter.SUMMARY_DISPLAY_NAME, summaryModel);

    if (trainingMetrics.nonEmpty) {
      val trainingTableModel = TabularVisualModel(
        trainingMetrics,
        Seq[ColumnDef](ColumnDef(stringStatistic, ColumnType.String), ColumnDef(stringValue, ColumnType.Double)));

      composite.addVisualModel(stringGOF, trainingTableModel);

      val weightsTableModel = TabularVisualModel(weightsInfo,
        Seq[ColumnDef](ColumnDef(stringPredictors, ColumnType.String),
          ColumnDef(stringWeights, ColumnType.Double)));

      composite.addVisualModel(stringWeights, weightsTableModel);
    }
    else {
      val weightsTableModel = TabularVisualModel(weightsInfo,
        Seq[ColumnDef](ColumnDef(stringPredictors, ColumnType.String),
          ColumnDef(stringWeights, ColumnType.Double)));

      composite.addVisualModel(stringWeights, weightsTableModel);
    }

    composite;
  }
}

