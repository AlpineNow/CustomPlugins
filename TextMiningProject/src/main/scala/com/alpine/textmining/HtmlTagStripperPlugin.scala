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

package com.alpine.textmining

import com.alpine.plugin.core.icon.{StarBurst, OperatorIcon}

import scala.collection.mutable

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsRawTextDatasetDefault
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.jsoup.Jsoup

class HtmlTagStripperSignature extends OperatorSignature[
  HtmlTagStripperGUINode,
  HtmlTagStripperRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Text Mining - HTML Tag Stripper",
      category = "Text Mining",
      author = Some("Sung Chung"),
      version = 1,
      helpURL = None,
      icon = None,
      toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
        "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
        " of the operator and are no more than fifty words.")
    )
  }
}

class HtmlTagStripperGUINode extends OperatorGUINode[
  HdfsHtmlDataset,
  HdfsRawTextDataset] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)
  }

  override def onOutputVisualization(
    params: OperatorParameters,
    output: HdfsRawTextDataset,
    visualFactory: VisualModelFactory): VisualModel = {
    visualFactory.createTextVisualization(
      output.addendum("numDocs").toString +
        " HTML documents stripped."
    )
  }
}

class HtmlTagStripperRuntime extends SparkRuntimeWithIOTypedJob[
  HtmlTagStripperJob,
  HdfsHtmlDataset,
  HdfsRawTextDataset] {}

class HtmlTagStripperJob extends
  SparkIOTypedPluginJob[HdfsHtmlDataset, HdfsRawTextDataset] {
  override def onExecution(
    sparkContext: SparkContext,
    appConf: mutable.Map[String, String],
    input: HdfsHtmlDataset,
    operatorParameters: OperatorParameters,
    listener: OperatorListener): HdfsRawTextDataset = {
    val inputPath = input.path
    val hdfs = FileSystem.get(sparkContext.hadoopConfiguration)

    // We use the standard output parameters.
    val outputPathStr = HdfsParameterUtils.getOutputPath(operatorParameters)
    val outputPath = new Path(outputPathStr)

    if (HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)) {
      new SparkRuntimeUtils(sparkContext).deleteFilePathIfExists(outputPathStr)
    }

    var numTotalDocuments = 0

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

        numTotalDocuments += htmlRdd.partitions.length
      }
    }

    new HdfsRawTextDatasetDefault(
      outputPathStr,
      Some(operatorParameters.operatorInfo),
      Map("numDocs" -> numTotalDocuments.toString)
    )
  }
}
