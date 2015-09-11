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

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.HdfsParameterUtils
import opennlp.tools.tokenize.{Tokenizer, TokenizerME, TokenizerModel}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

/**
 * The companion object is used during operator registration.
 * This object applies to all instances of the operator.
 */
class SparkWordCountSignature extends OperatorSignature[
  SparkWordCountGUINode,
  SparkWordCountRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "WordCount (Spark)",
      category = "Transformation",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = "test"
    )
  }
}

// This word counter uses Open NLP's tokenizer. The Open NLP tokenizer requires
// a binary configuration file.
class SparkWordCountGUINode extends
  OperatorGUINode[Tuple2[HdfsRawTextDataset, HdfsBinaryFile], HdfsDelimitedTabularDataset] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    // Add input parameters for selecting the output directory.
    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)
    operatorSchemaManager.setOutputSchema(WordCounter.createOutputSchema())
  }

}

/**
 * A sample code leveraging the Alpine Plugin API to perform word counting using Spark.
 */
class SparkWordCountRuntime extends SparkRuntimeWithIOTypedJob[
  WordCounter,
  Tuple2[HdfsRawTextDataset, HdfsBinaryFile],
  HdfsDelimitedTabularDataset] {}

object WordCounter {
  def createOutputSchema(): TabularSchema = {
    TabularSchema(Array(
      ColumnDef("Word", ColumnType.String),
      ColumnDef("Count", ColumnType.Long)
    ))
  }
}

class WordCounter extends
  SparkIOTypedPluginJob[Tuple2[HdfsRawTextDataset, HdfsBinaryFile], HdfsDelimitedTabularDataset] {
  override def onExecution(
    sparkContext: SparkContext,
    appConf: mutable.Map[String, String],
    input: Tuple2[HdfsRawTextDataset, HdfsBinaryFile],
    operatorParameters: OperatorParameters,
    listener: OperatorListener): HdfsDelimitedTabularDataset = {
    val inputData = input.getT1()
    val tokenizerConfigFile = input.getT2()
    val tokenizerConfigPath = tokenizerConfigFile.getPath()
    val rawPath = inputData.getPath()
    val textRdd = sparkContext.textFile(new Path(rawPath, "*/*").toString)
    val outputPathStr = HdfsParameterUtils.getOutputPath(operatorParameters)
    val wordCntFunc: Iterator[String] => Iterator[(String, Int)] =
      (lines: Iterator[String]) => {
        val stopWords = Set[String](
          "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"
        )
        val hdfs = FileSystem.get(new Configuration)
        // Read the tokenizer config.
        val tokenizerStream = hdfs.open(new Path(tokenizerConfigPath))
        val model: TokenizerModel = new TokenizerModel(tokenizerStream)
        val tokenizer: Tokenizer = new TokenizerME(model)
        tokenizerStream.close()

        // Get token counts without the stop words.
        val tokenCounts = mutable.Map[String, Int]()
        while (lines.hasNext) {
          val line = lines.next()
          val tokens = tokenizer.tokenize(line)
          var i = 0
          while (i < tokens.length) {
            val token = tokens(i)
            if (!stopWords.contains(token)) {
              if (!tokenCounts.contains(token)) {
                tokenCounts(token) = 0
              }

              tokenCounts(token) += 1
            }

            i += 1
          }
        }

        tokenCounts.toIterator
      }

    val outputRdd =
      textRdd.
        mapPartitions(wordCntFunc).
        reduceByKey(_ + _).
        map(t => t._1 + "\t" + t._2)


    if (HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)) {
      new SparkRuntimeUtils(sparkContext).deleteFilePathIfExists(outputPathStr)
    }

    outputRdd.saveAsTextFile(outputPathStr)
    new HdfsDelimitedTabularDatasetDefault(
      outputPathStr,
      WordCounter.createOutputSchema(),
      "\t",
      "\\",
      "\"",
      false
    )
  }
}
