/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core._
import com.alpine.plugin.core.io.{IOFactory, OperatorSchemaManager, HdfsBinaryFile, IONone}
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntime}

class HdfsBinaryFileSignature extends OperatorSignature[
  HdfsBinaryFileGUINode,
  HdfsBinaryFileRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "HdfsBinaryFile",
      category = "Dataset",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class HdfsBinaryFileGUINode extends OperatorGUINode[
  IONone,
  HdfsBinaryFile] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addHdfsFileSelector(
      id = "sourcePath",
      label = "Output Path",
      defaultPath = "",
      dataSourceManager = operatorDataSourceManager
    )
  }
}

class HdfsBinaryFileRuntime extends SparkRuntime[
  IONone,
  HdfsBinaryFile] {
  override def onExecution(
    context: SparkExecutionContext,
    input: IONone,
    params: OperatorParameters,
    listener: OperatorListener,
    ioFactory: IOFactory): HdfsBinaryFile = {
    val sourcePath = params.getStringValue("sourcePath")
    ioFactory.createHdfsBinaryFile(sourcePath)
  }

  override def onStop(
    context: SparkExecutionContext,
    listener: OperatorListener): Unit = {}
}
