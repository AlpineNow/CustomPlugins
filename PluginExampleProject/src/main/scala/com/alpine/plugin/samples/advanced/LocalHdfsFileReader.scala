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

package com.alpine.plugin.samples.advanced

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.{OperatorFailedException, SparkExecutionContext, SparkRuntime}
import com.alpine.plugin.core._
import com.alpine.plugin.core.io.defaults.IONoneDefault
import com.alpine.plugin.core.spark.utils.{FileTooLargeException, HDFSFileFilter, HDFSFileReaderUtils, RowWithIndex}
import com.alpine.plugin.core.utils.AddendumWriter
import com.alpine.plugin.core.visualization.{CompositeVisualModel, HtmlVisualModel, VisualModel}
import org.apache.hadoop.fs.Path

import scala.util.Try

/**
  * This operator demonstrates how to read an HdfsTabularDataset input locally on the Alpine Server, without running a Spark job.
  * This is useful for operators that attempt to export data to an external server or tool, that can be accessed via the Alpine Server.
  * For this purpose the sdk class HDFSFileReaderUtils can be leveraged: it supports different input storage format (CSV, Parquet, Avro)
  * as well as compressed input files.
  * It also shows an example of how to read a parameter from the alpine config file, in order to keep it configurable by the admin of an Alpine instance.
  * (here we try to read the maximum file size (MB) to read from the alpine conf).
  *
  *
  * Usage: the user selects a numeric column from the input, a double number to find, and it returns a visual message in UI indicating the total
  * number of occurrences of this number in the column selected. It is a terminal operator.
  */

object LocalHdfsFileReaderUtil {
  val COLUMN_TO_ANALYZE = "columnToAnalyze"
  val NUMBER_TO_FIND = "numberToFind"
  //configuration parameter name that we will try to read in the alpine.conf file
  val co_config_max_file_size_mb = "local_hdfs_reader.max_file_size_mb"
}

class LocalHDFSFileReaderSignature extends OperatorSignature[
  LocalHDFSFileReaderGUINode,
  LocalHDFSFileReaderRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - Local HDFS File Reader",
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

/** *
  * The LocalHDFSFileReaderGUINode defines the design time behavior of the plugin. It extends
  * the template GUI class "OperatorGUINode".
  */
class LocalHDFSFileReaderGUINode extends OperatorGUINode[HdfsTabularDataset, IONone] {
  override def onPlacement(operatorDialog: OperatorDialog,
      operatorDataSourceManager: OperatorDataSourceManager,
      operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addTabularDatasetColumnDropdownBox(LocalHdfsFileReaderUtil.COLUMN_TO_ANALYZE,
      "Column to Analyze", ColumnFilter.NumericOnly, "main")

    operatorDialog.addDoubleBox(LocalHdfsFileReaderUtil.NUMBER_TO_FIND, "Count Occurrences Of Number",
      Double.MinValue, Double.MaxValue, inclusiveMin = true, inclusiveMax = true, 1d)
  }
}

/**
  * What happens when the alpine user clicks the "run button".
  */
class LocalHDFSFileReaderRuntime extends SparkRuntime[HdfsTabularDataset, IONone] {

  import LocalHdfsFileReaderUtil._

  override def createVisualResults(
      context: SparkExecutionContext,
      input: HdfsTabularDataset,
      output: IONone,
      params: OperatorParameters,
      listener: OperatorListener): VisualModel = {

    val summary = output.addendum(AddendumWriter.summaryKey).asInstanceOf[String]
    val model = new CompositeVisualModel

    //The UI output is a single HTMLVisualModel with a message indicating the number of occurrences of the number specified
    //were found in the input column selected.
    model.addVisualModel("Output", HtmlVisualModel(summary))
    model
  }

  override def onExecution(context: SparkExecutionContext,
      input: HdfsTabularDataset, params: OperatorParameters,
      listener: OperatorListener): IONone = {

    //Getting the user inputs specified via UI
    val columnToAnalyze = params.getTabularDatasetSelectedColumnName(COLUMN_TO_ANALYZE)
    val columnIndex = input.tabularSchema.getDefinedColumns.indexWhere(_.columnName == columnToAnalyze)
    val numberToFind = params.getDoubleValue(NUMBER_TO_FIND)

    //We try to read the value of "local_hdfs_reader.max_file_size_mb" in the alpine conf, accessible via context.config.entries.
    //We use a default value of 1000MB if the parameter is not specified in the conf.
    val limitInputFileSizeMB = Try(context.config.entries.get(co_config_max_file_size_mb).toString.toDouble).getOrElse(1000d)

    //instantiates the class HDFSFileReaderUtils to leverage all utils to read HDFS files locally (storage format, compression)
    val hdfsReaderUtils = new HDFSFileReaderUtils(context)


    //initialize the counter of number to find in the column selected
    var counterForNumber = 0

    //This is the function that defines how to handle each part file content, which we can pass directly as argument to the HDFSFileReaderUtils methods (like openFileGeneralAndProcessLines).
    //It takes as argument Iterator[RowWithIndex], and increments the counter if the value in column selected is equal to the number to find.
    def partFileResultHandler(iter: Iterator[RowWithIndex]) =
    iter.foreach(indexedRow => {
      val number = Try(indexedRow.values(columnIndex).toDouble)
      if (number.isSuccess && number.get == numberToFind) {
        counterForNumber += 1
      }
    })

    //getting all relevant HDFS part files in the input directory path (containing the actual rows of data to analyze)
    val allPartFiles: Seq[Path] = HDFSFileFilter.getAllPartFiles(context, input.path)
    try {
      //Read locally all relevant part files from the input dataset directory path,
      //and process each file content with the function `partFileResultHandler` defined earlier.
      //The method `openFileGeneralAndProcessLines` handles storage format and compression automatically
      for (file <- allPartFiles) {
        hdfsReaderUtils.openFileGeneralAndProcessLines(file, input, partFileResultHandler, Some(limitInputFileSizeMB))
      }
    }
    catch {
      //catch FileTooLargeException to modify the error message and specify how the user can modify the file size limit parameter value
      case ftl: FileTooLargeException => throw new FileTooLargeException(
        ftl.actualSizeMB, ftl.fileSizeLimitMB, ftl.getMessage + s"This limit is configurable in the alpine.conf (custom_operator.$co_config_max_file_size_mb).")
    }

    val message = s"Total occurrences of number ${numberToFind.toString} in column $columnToAnalyze: ${counterForNumber.toString}."

    IONoneDefault(Map(AddendumWriter.summaryKey -> message))
  }

  override def onStop(
      context: SparkExecutionContext,
      listener: OperatorListener) = {
  }
}