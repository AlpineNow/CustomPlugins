/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0

import com.alpine.features.{DoubleType, FeatureDesc, StringType}
import com.alpine.model.ClassificationRowModel
import com.alpine.plugin.core.{OperatorMetadata, _}
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntime}
import com.alpine.plugin.model._
import com.alpine.transformer.ClassificationTransformer

/**
 * A plugin that demonstrates implementing a custom classification model.
 */
class ExampleClassificationModelSignature extends OperatorSignature[
  ExampleClassificationModelGUINode,
  ExampleClassificationModelRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "ExampleClassificationModel",
      category = "Classification",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class ExampleClassificationModelGUINode extends OperatorGUINode[
  IONone,
  ClassificationModelWrapper] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
  }
}

class ExampleClassificationModel extends ClassificationRowModel {
  override def transformer = {
    class ExampleClassificationTransformer extends ClassificationTransformer {
      def scoreConfidences(row: Row): Array[Double] = {
        Array(0.7, 0.3)
      }

      def classLabels: Seq[String] = Array("yes", "no").toSeq
    }

    new ExampleClassificationTransformer()
  }

  def inputFeatures: Seq[FeatureDesc[_]] =
    Array(
      FeatureDesc("humidity", DoubleType()),
      FeatureDesc("temperature", DoubleType())
    )

  def classLabels: Seq[String] = Array("yes", "no").toSeq

  def dependentFeature = new FeatureDesc("play", StringType())
}

class ExampleClassificationModelRuntime extends SparkRuntime[
  IONone,
  ClassificationModelWrapper] {
  override def onExecution(
    context: SparkExecutionContext,
    input: IONone,
    params: OperatorParameters,
    listener: OperatorListener): ClassificationModelWrapper = {
    return new ClassificationModelWrapper(
      "Example classification model",
      new ExampleClassificationModel()
    )
  }

  override def onStop(context: SparkExecutionContext, listener: OperatorListener) {}
}