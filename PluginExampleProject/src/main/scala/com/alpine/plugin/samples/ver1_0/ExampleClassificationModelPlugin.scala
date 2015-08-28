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