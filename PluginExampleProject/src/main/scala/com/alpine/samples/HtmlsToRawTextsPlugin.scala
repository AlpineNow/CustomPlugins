/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.samples

import com.alpine.plugin.core.{OperatorMetadata, _}
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.jsoup.Jsoup

import scala.collection.mutable

class HtmlsToRawTextsSignature extends OperatorSignature[
  HtmlsToRawTextsGUINode,
  HtmlsToRawTextsRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "HtmlsToRawTexts",
      category = "Transformation",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class HtmlsToRawTextsGUINode extends OperatorGUINode[
  HdfsHtmlDataset,
  HdfsRawTextDataset] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addHdfsDirectorySelector(
      id = "outputPath",
      label = "Output Path",
      defaultPath = "",
      dataSourceManager = operatorDataSourceManager
    )
  }
}

class HtmlsToRawTextsRuntime extends SparkRuntimeWithIOTypedJob[
  HtmlsToRawTextsJob,
  HdfsHtmlDataset,
  HdfsRawTextDataset] {}

class HtmlsToRawTextsJob extends
  SparkIOTypedPluginJob[HdfsHtmlDataset, HdfsRawTextDataset] {
  override def onExecution(
    sparkContext: SparkContext,
    appConf: mutable.Map[String, String],
    input: HdfsHtmlDataset,
    operatorParameters: OperatorParameters,
    listener: OperatorListener,
    ioFactory: IOFactory): HdfsRawTextDataset = {
    val inputPath = input.getPath()
    val hdfs = FileSystem.get(sparkContext.hadoopConfiguration)

    // Delete the output path if it already exists.
    val outputPathStr = operatorParameters.getStringValue("outputPath")
    val outputPath = new Path(outputPathStr)

    // Delete the output path if it already exists.
    if (hdfs.exists(outputPath)) {
      hdfs.delete(outputPath, true)
    }

    // List all the directories under this directory.
    val fileStatuses = hdfs.listStatus(new Path(inputPath))
    for (fileStatus <- fileStatuses) {
      // If this is a directory, convert all the HTML documents under this
      // directory into raw text files under the output directory under the
      // same sub-directory name.
      if (fileStatus.isDirectory) {
        val subDirName = fileStatus.getPath.getName
        var pathStr = fileStatus.getPath.toString
        if (pathStr.startsWith("hdfs://")) {
          pathStr = pathStr.substring(7)
          pathStr = "/" + pathStr.split("/", 2)(1) + "/*"
        }

        val htmlRdd = sparkContext.textFile(pathStr)
        listener.notifyMessage("Stripping HTML files under " + pathStr)
        // One file per partition.
        val stripped = htmlRdd.mapPartitions(lines => {
          val partitionContent = lines.mkString
          Array(Jsoup.parse(partitionContent).text()).toIterator
        })

        stripped.saveAsTextFile(new Path(outputPath, subDirName).toString)
      }
    }

    ioFactory.createHdfsRawTextDataset(outputPathStr)
  }
}
