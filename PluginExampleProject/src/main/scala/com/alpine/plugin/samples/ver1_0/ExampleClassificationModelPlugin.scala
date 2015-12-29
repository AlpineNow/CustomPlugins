/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 *
 * BSD 3-Clause License
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.alpine.plugin.samples.ver1_0

import com.alpine.model.ClassificationRowModel
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntime}
import com.alpine.plugin.core.{OperatorMetadata, _}
import com.alpine.plugin.model._
import com.alpine.transformer.ClassificationTransformer

/**
 * A plugin that demonstrates implementing a custom classification model.
 * The model works only with the golf dataset:
 * which has the Following schema:
     val golfSchema = TabularSchema(
    Seq(
      ColumnDef("outlook", ColumnType.String),
      ColumnDef("temperature", ColumnType.Double),
      ColumnDef("humidity", ColumnType.Double),
      ColumnDef("wind", ColumnType.String),
      ColumnDef("play", ColumnType.String)))

 * where the "play" column has yes or no values.
 * The model scores that data with 70% confidence for 'yes' and
 * 30% confidence for 'no' at each row.

 */
class ExampleClassificationModelSignature extends OperatorSignature[
  ExampleClassificationModelGUINode,
  ExampleClassificationModelRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - ExampleClassificationModel",
      category = "Plugin Sample - Spark",
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

/**
 * ExampleClassification Transformer class shows how to transform each row
 * of data for this model. In this case, the transformation is to produce
 * an array of score confidences, since this Transformer extends
 * ClassificationTransformer
 * An instance of this class is used for the Transformer of the ExampleClassificationModel
 */
class ExampleClassificationTransformer extends ClassificationTransformer {
  /**
   * Scores one new row of data using this model.
   * @param row an alpine row object representing one line in the prediction data
   * @return An array of confidence scores to assign to that row.
   */
  def scoreConfidences(row: Row): Array[Double] = {
    Array(0.7, 0.3)
  }

  /**
   * The labels for the confidences. In this case, they are the values of
   * the "play" column, yes or no.
   * @return
   */
  def classLabels: Seq[String] = Array("yes", "no").toSeq
}

/**
 * The New model that we are creating.
 * Uses the ExampleClassificationTransformer to define how each row
 * of data should be scored. It is the information in this class that will
 * be passed on to the Alpine Classifiers, predictors, and Evaluators.
 */
class ExampleClassificationModel extends ClassificationRowModel {
  /**
   * The model object which defines the logic that this model should apply
   * to each row of data.
   * @return A Transformer which describes some function to apply to each
   *         row of data. In this case, because this is a classification model,
   *         the function provides a score for each row of prediction data
   */
  override def transformer : ExampleClassificationTransformer = {
    new ExampleClassificationTransformer()
  }

  /**
   * The columns used as input features in this model.
   */
  def inputFeatures: Seq[ColumnDef] =
    Array(
      ColumnDef("humidity", ColumnType.Double),
      ColumnDef("temperature", ColumnType.Double)
    )

  /**
   * Must correspond to the equivalent field in the transformer.
   * Defines the labels for the classes predicted by this model.
   */
  def classLabels: Seq[String] = Array("yes", "no").toSeq

  /**
   * The column definition for the dependent feature
   */
  def dependentFeature: ColumnDef = new ColumnDef("play", ColumnType.String)
}

/**
 * Note that because this model doesn't have to do any actual reading and
 * writing of the data, it need not launch a spark job.
 * We simply create the new model in the runtime class and return a ClassificationModelWrapper
 * object that contains that model.
 *
 * Thus, instead of overriding SparkRuntimeWithIOTypedJob we override the more generic
 * SparkRuntime class and we don't implement any SparkJob class.
 */
class ExampleClassificationModelRuntime extends SparkRuntime[
  IONone,
  ClassificationModelWrapper] {
  override def onExecution(
    context: SparkExecutionContext,
    input: IONone,
    params: OperatorParameters,
    listener: OperatorListener): ClassificationModelWrapper = {

    val exampleClassificationModel = new ExampleClassificationModel()

    //return the ClassificationModelWrapper object which extends IOBase
    new ClassificationModelWrapper(
      "Example classification model",
       exampleClassificationModel,
      Some(params.operatorInfo)
    )
  }

  /**
   * Since we aren't launching and then stopping a Spark Context as the default
   * implementation of SparkRuntime does, we have to override the on stop method.
   */
  override def onStop(context: SparkExecutionContext, listener: OperatorListener) {}
}