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

import scala.collection.mutable

import com.alpine.plugin.core.{OperatorMetadata, _}
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.HdfsParameterUtils
import opennlp.tools.tokenize.{Tokenizer, TokenizerME, TokenizerModel}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RawTextDataToClassifierDataSignature extends OperatorSignature[
  RawTextDataToClassifierDataGUINode,
  RawTextDataToClassifierDataRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "RawTextDataToClassifierData",
      category = "Transformation",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class RawTextDataToClassifierDataGUINode extends OperatorGUINode[
  Tuple3[HdfsRawTextDataset, HdfsDelimitedTabularDataset, HdfsBinaryFile],
  HdfsDelimitedTabularDataset] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {

    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

    operatorDialog.addDropdownBox(
      id = "labelSyntax",
      label = "How Label is Chosen",
      values = scala.collection.Seq("Path Name"),
      defaultSelection = "Path Name"
    )

    val columnDefs = new mutable.ArrayBuffer[ColumnDef]()
    // The only thing we know for certain is that there will be a label column.
    columnDefs += ColumnDef("Label", ColumnType.String)
    var i = 0
    while (i < 1000) {
      columnDefs += ColumnDef("Word" + (i + 1).toString, ColumnType.Int)
      i += 1
    }

    val outputSchema = TabularSchema(columnDefs)
    operatorSchemaManager.setOutputSchema(outputSchema)
  }
}

class RawTextDataToClassifierDataRuntime extends SparkRuntimeWithIOTypedJob[
  RawTextDataToClassifierDataJob,
  Tuple3[HdfsRawTextDataset, HdfsDelimitedTabularDataset, HdfsBinaryFile],
  HdfsDelimitedTabularDataset] {}

class RawTextDataToClassifierDataJob extends SparkIOTypedPluginJob[
  Tuple3[HdfsRawTextDataset, HdfsDelimitedTabularDataset, HdfsBinaryFile],
  HdfsDelimitedTabularDataset] {
  override def onExecution(
    sparkContext: SparkContext,
    appConf: mutable.Map[String, String],
    input: Tuple3[HdfsRawTextDataset, HdfsDelimitedTabularDataset, HdfsBinaryFile],
    operatorParameters: OperatorParameters,
    listener: OperatorListener): HdfsDelimitedTabularDataset = {
    val conf = new Configuration()
    val dfs = FileSystem.get(conf)
    val inputRawFiles = input.getT1()
    val dictionaryFile = input.getT2()
    val tokenizerConfigFile = input.getT3()
    val tokenizerConfigPath = tokenizerConfigFile.getPath()
    val inputPath = inputRawFiles.getPath()
    val outputPathStr = HdfsParameterUtils.getOutputPath(operatorParameters)
    val delimiter = dictionaryFile.getDelimiter

    // Load the dictionary first.
    val dicRdd = sparkContext.textFile(dictionaryFile.getPath())
    val allWordCntMap = dicRdd.mapPartitions(lines => {
      val wordCntMap = mutable.Map[String, Int]()
      while (lines.hasNext) {
        val line = lines.next()
        val splitElems = line.split(delimiter)
        wordCntMap += (splitElems(0) -> splitElems(1).toInt)
      }

      Array(wordCntMap).toIterator
    }).reduce((s1, s2) => s1 ++ s2)

    // Sort all word cnt map by counts.
    val sortedWordCnt = allWordCntMap.toList.sortBy(-_._2)

    val numWords = math.min(allWordCntMap.size, 1000)

    val columnDefs = new mutable.ArrayBuffer[ColumnDef]()
    columnDefs += ColumnDef("Label", ColumnType.String)

    // Now, assign a feature index to each word.
    val wordIndices = mutable.Map[String, Int]()
    val itr = sortedWordCnt.iterator
    var i = 0
    while (i < 1000 && itr.hasNext) {
      val wordCnt = itr.next()
      wordIndices.put(wordCnt._1, i)
      columnDefs += ColumnDef("Word" + (i + 1).toString, ColumnType.Long)
      i += 1
    }

    // Find child directories.
    val childDirs = dfs.listStatus(new Path(inputPath))
    val rdds = new Array[RDD[String]](childDirs.length)
    i = 0
    while (i < childDirs.length) {
      val f = childDirs(i)
      val label = f.getPath.getName
      val labelRdd = sparkContext.textFile(f.getPath.toString)
      rdds(i) = labelRdd.mapPartitions(
        lines => {
          val hdfs = FileSystem.get(new Configuration)
          // Read the tokenizer config.
          val tokenizerStream = hdfs.open(new Path(tokenizerConfigPath))
          val model: TokenizerModel = new TokenizerModel(tokenizerStream)
          val tokenizer: Tokenizer = new TokenizerME(model)
          tokenizerStream.close()
          val content = lines.mkString
          val featurizedRow = Array.fill[Int](numWords)(0)
          val tokens = tokenizer.tokenize(content)
          var i = 0
          while (i < tokens.length) {
            val token = tokens(i)
            if (wordIndices.contains(token)) {
              val wordIndex = wordIndices(token)
              featurizedRow(wordIndex) += 1
            }
            i += 1
          }

          val delimitedRow = label + "\t" + featurizedRow.mkString("\t")
          Array(delimitedRow).toIterator
        }
      )

      i += 1
    }

    // Get a union of all the RDDs.
    var unionRdd: RDD[String] = rdds(0)
    var rddIndex = 1
    while (rddIndex < rdds.length) {
      unionRdd = unionRdd.union(rdds(rddIndex))
      rddIndex += 1
    }

    if (HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)) {
      new SparkRuntimeUtils(sparkContext).deleteFilePathIfExists(outputPathStr)
    }
    unionRdd.saveAsTextFile(outputPathStr)

    new HdfsDelimitedTabularDatasetDefault(
      outputPathStr,
      TabularSchema(columnDefs),
      "\t",
      "\\",
      "\"",
      false
    )
  }
}