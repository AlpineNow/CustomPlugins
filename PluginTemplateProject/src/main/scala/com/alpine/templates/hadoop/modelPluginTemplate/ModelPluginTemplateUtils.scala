package com.alpine.templates.hadoop.modelPluginTemplate

object ModelPluginTemplateUtils {
  // Output
  val stringParamSelected = "<b>Parameters selected<b>";
  val stringOutput = "<b>Output<b>";
  val stringStorageLevel = "MEMORY_AND_DISK";

  // Input params
  val stringDependentColumnID = "dependentColumnID";
  val stringDependentColumn = "Dependent Column";

  val stringIndependentColumnsID = "independentColumnsID";
  val stringIndependentColumns = "Independent Columns";

  // Internal
  val stringIdentifier = "ModelID";//replace
  val stringSummaryID = "summaryID";
  val stringWeightsAddendumID = "weightsAddendumID";
  val stringGOFID = "goodnessOfFitID";

  val stringPredictors = "Predictors";
  val stringWeights = "Weights";
  val stringStatistic = "Statistic";
  val stringValue = "Value";
  val stringGOF = "Goodness of Fit";
  // Error messages
  val stringGaussianLinkError = "Error building model"

  // Methods
  def addendumListInfoToScala(weightsInfo: java.util.ArrayList[java.util.ArrayList[String]]): Seq[Seq[String]] = {
    weightsInfo.toArray.toSeq.map(_.asInstanceOf[java.util.ArrayList[String]].toArray.toSeq.map(_.toString))
  }

}

