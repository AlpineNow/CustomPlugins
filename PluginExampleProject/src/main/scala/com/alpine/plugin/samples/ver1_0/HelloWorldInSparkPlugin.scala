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
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.spark.{AlpineSparkEnvironment, SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.HdfsParameterUtils

class HelloWorldInSparkSignature extends OperatorSignature[
  HelloWorldInSparkGUINode,
  HelloWorldInSparkRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - Hello World In Spark",
    category = "Plugin Sample - Spark",
    author = Some("Alpine Data"),
    version = 1,
    helpURL = None,
    icon = None,
    toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
      "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
      " of the operator and are no more than fifty words.")
  )
}

class HelloWorldInSparkGUINode extends OperatorGUINode[
  IONone,
  HdfsDelimitedTabularDataset] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

    val outputSchema = TabularSchema(Seq(ColumnDef("HelloWorld", ColumnType.String)))
    operatorSchemaManager.setOutputSchema(outputSchema)
  }
}

class HelloWorldInSparkRuntime extends SparkRuntimeWithIOTypedJob[
  HelloWorldInSparkJob,
  IONone,
  HdfsDelimitedTabularDataset] {}

class HelloWorldInSparkJob extends SparkIOTypedPluginJob[
  IONone,
  HdfsDelimitedTabularDataset] {
  override def onExecution(alpineSparkEnvironment: AlpineSparkEnvironment,
                           input: IONone,
                           operatorParameters: OperatorParameters,
                           listener: OperatorListener): HdfsDelimitedTabularDataset = {
    val sparkUtils = alpineSparkEnvironment.getSparkUtils

    val outputPathStr = HdfsParameterUtils.getOutputPath(operatorParameters)
    val overwrite = HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)

    sparkUtils.deleteOrFailIfExists(outputPathStr, overwrite)

    val outputSchema = TabularSchema(Seq(ColumnDef("HelloWorld", ColumnType.String)))
    val rdd = alpineSparkEnvironment.sparkSession.sparkContext.parallelize(List("Hello World"))
    rdd.saveAsTextFile(outputPathStr)
    HdfsDelimitedTabularDatasetDefault(
      outputPathStr,
      outputSchema,
      TSVAttributes.defaultCSV
    )
  }
}
