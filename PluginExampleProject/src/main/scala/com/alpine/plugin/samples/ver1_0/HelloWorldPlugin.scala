/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.IOStringDefault
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntime}

class HelloWorldSignature extends OperatorSignature[
  HelloWorldGUINode,
  HelloWorldRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "HelloWorld",
      category = "Standalone",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class HelloWorldGUINode extends OperatorGUINode[
  IONone,
  IOString] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
  }
}

class HelloWorldRuntime extends SparkRuntime[
  IONone,
  IOString] {
  override def onExecution(
    context: SparkExecutionContext,
    input: IONone,
    params: OperatorParameters,
    listener: OperatorListener): IOString = {
    new IOStringDefault("Hello World")
  }

  override def onStop(
    context: SparkExecutionContext,
    listener: OperatorListener): Unit = {}
}
