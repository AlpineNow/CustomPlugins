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

/**
 * Demonstrates using IOList as an input.
 */
class IOListSampleSignature extends OperatorSignature[
  IOListSampleGUINode,
  IOListSampleRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - IOList Input Viewer",
      category = "Plugin Sample - Local",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class IOListSampleGUINode extends OperatorGUINode[
  IOList[HdfsTabularDataset],
  IOString] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addParentOperatorDropdownBox(
      id = "dataset1",
      label = "Number One Dataset"
    )

    operatorDialog.addParentOperatorDropdownBox(
      id = "dataset2",
      label = "Number Two Dataset"
    )
  }
}

class IOListSampleRuntime extends SparkRuntime[
  IOList[HdfsTabularDataset],
  IOString] {
  override def onExecution(
    context: SparkExecutionContext,
    input: IOList[HdfsTabularDataset],
    params: OperatorParameters,
    listener: OperatorListener): IOString = {
    val dataset1UUID = params.getStringValue("dataset1")
    val dataset2UUID = params.getStringValue("dataset2")
    var dataset1Path: String = ""
    var dataset1Type: String = ""
    var dataset2Path: String = ""
    var dataset2Type: String = ""
    val itr = input.elements.iterator
    while (itr.hasNext) {
      val dataset = itr.next()
      if (dataset.sourceOperatorInfo.get.uuid.equals(dataset1UUID)) {
        dataset1Path = dataset.path
        dataset1Type = dataset.getClass.getCanonicalName
      } else if (dataset.sourceOperatorInfo.get.uuid.equals(dataset2UUID)) {
        dataset2Path = dataset.path
        dataset2Type = dataset.getClass.getCanonicalName
      }
    }

    new IOStringDefault(
      "The number one dataset has the path '" + dataset1Path +
        "' and is of the type '" + dataset1Type + "'\n" +
      "The number two dataset has the path '" + dataset2Path +
        "' and is of the type '" + dataset2Type + "'\n",
      Some(params.operatorInfo)
    )
  }

  override def onStop(
    context: SparkExecutionContext,
    listener: OperatorListener): Unit = {}
}