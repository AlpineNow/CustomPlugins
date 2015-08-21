/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0

import scala.collection.mutable

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsRawTextDatasetDefault
import com.alpine.plugin.core.spark.utils.SparkUtils
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.core.{OperatorMetadata, _}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.jsoup.Jsoup

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
    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)
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
    listener: OperatorListener): HdfsRawTextDataset = {
    val inputPath = input.getPath()
    val hdfs = FileSystem.get(sparkContext.hadoopConfiguration)

    // We use the standard output parameters.
    val outputPathStr = HdfsParameterUtils.getOutputPath(operatorParameters)
    val outputPath = new Path(outputPathStr)

    if (HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)) {
      new SparkUtils(sparkContext).deleteFilePathIfExists(outputPathStr)
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

    new HdfsRawTextDatasetDefault(outputPathStr)
  }
}
