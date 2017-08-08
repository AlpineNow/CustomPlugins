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
import com.alpine.plugin.core.io.defaults.IOStringDefault
import com.alpine.plugin.core.spark._

/**
  * Demonstrates using IOList as an input.
  */
class IOListSampleSignature extends OperatorSignature[
  IOListSampleGUINode,
  IOListSampleRuntime] {

  def getMetadata: OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - IOList Input Viewer",
      category = "Plugin Sample - Local",
      author = Some("Alpine Data"),
      version = 1,
      helpURL = None,
      icon = None,
      toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
        "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
        " of the operator and are no more than fifty words.")
    )
  }
}

class IOListSampleGUINode extends OperatorGUINode[
  IOList[HdfsTabularDataset],
  IOString] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addParentOperatorDropdownBox(
      id = "dataset1",
      label = "Number One Dataset",
      required = false
    )

    operatorDialog.addParentOperatorDropdownBox(
      id = "dataset2",
      label = "Number Two Dataset",
      required = false
    )
  }
}

/**
  * As with the Tuple4 SamplePlugin, we don't launch a Spark Job
  * since we are not ever using the data stored on HDFS. Instead this plugin
  * simply returns a string object with a message about the paths to the datasets
  * selected as the number one and number two dataset.
  */
class IOListSampleRuntime extends SparkRuntime[
  IOList[HdfsTabularDataset],
  IOString] {
  override def onExecution(context: SparkExecutionContext,
                           input: IOList[HdfsTabularDataset],
                           params: OperatorParameters,
                           listener: OperatorListener): IOString = {
    val dataset1UUID = params.getStringValue("dataset1")
    val dataset2UUID = params.getStringValue("dataset2")

    val dataset1Index = input.sources.indexWhere(o => o.uuid == dataset1UUID)
    val dataset2Index = input.sources.indexWhere(o => o.uuid == dataset2UUID)

    if (dataset1Index > -1 && dataset2Index > -1) {
      val dataset1 = input.elements(dataset1Index)
      val dataset2 = input.elements(dataset2Index)

      IOStringDefault(
        "The number one dataset has the path '" + dataset1.path +
          "' and is of the type '" + dataset1.getClass.getCanonicalName + "'\n" +
          "The number two dataset has the path '" + dataset2.path +
          "' and is of the type '" + dataset2.getClass.getCanonicalName + "'\n"
      )
    } else {
      IOStringDefault(
        "Could not find both of the parent datasets."
      )
    }
  }

  override def onStop(context: SparkExecutionContext,
                      listener: OperatorListener): Unit = {}
}