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

import com.alpine.plugin.core._
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.spark._
import com.alpine.plugin.core.io.defaults.IOStringDefault
import com.alpine.plugin.model.ClassificationModelWrapper

/**
 * Demonstrates using Tuple4 as an input.
 * Must be used with:
 * - 1) An Hdfs tabular dataset
 * - 2) An Alpine Classification model i.e. Logistic Regression
 * - 3) An I.O string type, like the result of the Hello World plugin
 * - 4) A second Hdfs tabular dataset
 */
class Tuple4SampleSignature extends OperatorSignature[
  Tuple4SampleGUINode,
  Tuple4SampleRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - Tuple4 Input Viewer",
    category = "Plugin Sample - Local",
    author = Some("Sung Chung"),
    version = 1,
    helpURL = None,
    icon = None,
    toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
      "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
      " of the operator and are no more than fifty words.")
  )
}

class Tuple4SampleGUINode extends OperatorGUINode[
  Tuple4[
    HdfsTabularDataset,
    ClassificationModelWrapper,
    IOString,
    HdfsTabularDataset],
  IOString] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {}
}

class Tuple4SampleRuntime extends SparkRuntime[
  Tuple4[
    HdfsTabularDataset,
    ClassificationModelWrapper,
    IOString,
    HdfsTabularDataset],
  IOString] {
  override def onExecution(
    context: SparkExecutionContext,
    input: Tuple4[
      HdfsTabularDataset,
      ClassificationModelWrapper,
      IOString,
      HdfsTabularDataset],
    params: OperatorParameters,
    listener: OperatorListener): IOString = {
    IOStringDefault(
      "The first input is a dataset with the path '" + input._1.path + "'\n" +
      "The second input is a model of type'" + input._2.model.getClass + "'\n" +
      "The third input is a string with the value '" + input._3.value + "'\n" +
      "The fourth input is a dataset with the path '" + input._4.path + "'\n"
    )
  }

  override def onStop(
    context: SparkExecutionContext,
    listener: OperatorListener): Unit = {}
}
