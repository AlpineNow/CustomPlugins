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

import com.alpine.plugin.core.io.defaults.AbstractHdfsDelimitedTabularDataset

import scala.collection.mutable

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}
import opennlp.tools.tokenize.{Tokenizer, TokenizerME, TokenizerModel}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

/**
 * A word dictionary builder computes various statistics about
 * words in raw text documents.
 */
class WordDictionaryBuilderSignature extends OperatorSignature[
  WordDictionaryBuilderGUINode,
  WordDictionaryBuilderRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Text Mining - Dictionary Builder",
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

object WordDictionarySchema {
  def createSchema: TabularSchema = {
    TabularSchema.apply(
      Seq(
        ColumnDef("Word", ColumnType.String),
        ColumnDef("TotalCountInCorpus", ColumnType.Long),
        ColumnDef("DocCount", ColumnType.Long)
      )
    )
  }
}

/**
 * A word dictionary of all valid words that occur within the given
 * corpus.
 * @param corpusPath The root corpus directory.
 * @param corpusDocCount The total count of all the documents in the corpus.
 * @param corpusWordCount The total count of all the words in the corpus.
 * @param lowerCased Whether all the words have been lower cased.
 * @param path The dictionary path.
 */
case class WordDictionaryFile(
  corpusPath: String,
  corpusDocCount: Long,
  corpusWordCount: Long,
  lowerCased: Boolean,
  override val path: String,
  override val sourceOperatorInfo: Option[OperatorInfo])
  extends AbstractHdfsDelimitedTabularDataset(path, WordDictionarySchema.createSchema, TSVAttributes.default, sourceOperatorInfo, Map[String, AnyRef]()) {
  def getCorpusPath: String = corpusPath
  def getCorpusDocCount: Long = corpusDocCount
  def getCorpusWordCount: Long = corpusWordCount
  def isLowerCased: Boolean = lowerCased
}

class WordDictionaryBuilderGUINode extends
  OperatorGUINode[
    Tuple2[HdfsRawTextDataset, HdfsOpenNLPModelFile],
    WordDictionaryFile
  ] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {

    // Add input parameters for selecting the output directory.
    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

    operatorDialog.addRadioButtons(
      id = "useLowerCase",
      label = "Use lower case tokens",
      values = Seq("true", "false"),
      defaultSelection = "true"
    )

    operatorDialog.addRadioButtons(
      id = "filterStopWords",
      label = "Filter stop words",
      values = Seq("true", "false"),
      defaultSelection = "true"
    )

    operatorSchemaManager.setOutputSchema(
      WordDictionarySchema.createSchema
    )
  }

  override def onOutputVisualization(
    params: OperatorParameters,
    output: WordDictionaryFile,
    visualFactory: VisualModelFactory): VisualModel = {
    val composite = visualFactory.createCompositeVisualModel()
    val dataVisualization =
      visualFactory.createTabularDatasetVisualization(output)
    val statsSummary =
      visualFactory.createTextVisualization(
        "Corpus Statistics\n" +
        "=================\n" +
        "Corpus path : " + output.getCorpusPath + "\n" +
        "Corpus document count : " + output.getCorpusDocCount + "\n" +
        "Corpus total word count : " + output.getCorpusWordCount
      )

    composite.addVisualModel("Dictionary", dataVisualization)
    composite.addVisualModel("Corpus Statistics", statsSummary)
    composite
  }
}

/**
 * A needed boilerplate.
 */
class WordDictionaryBuilderRuntime extends SparkRuntimeWithIOTypedJob[
  WordDictionaryBuilderJob,
  Tuple2[HdfsRawTextDataset, HdfsOpenNLPModelFile],
  WordDictionaryFile
] {}

/**
 * Used by the token statistics calculator.
 * @param count The total count of the token (in a document or a corpus).
 * @param docCount The total count of documents that contain the token.
 */
case class TokenStats(var count: Long, var docCount: Long)

class WordDictionaryBuilderJob extends
  SparkIOTypedPluginJob[
    Tuple2[HdfsRawTextDataset, HdfsOpenNLPModelFile],
    WordDictionaryFile
  ] {
  override def onExecution(
    sparkContext: SparkContext,
    appConf: mutable.Map[String, String],
    input: Tuple2[HdfsRawTextDataset, HdfsOpenNLPModelFile],
    operatorParameters: OperatorParameters,
    listener: OperatorListener
  ): WordDictionaryFile = {
    val inputData = input._1
    val tokenizerModelFile = input._2

    // Check that it is the tokenizer model file that we wanted.
    if (!tokenizerModelFile.getModelType.equals(OpenNLPModelType.Tokenizer)) {
      throw new IllegalArgumentException(
        "Expected a tokenizer model file but instead found " +
          tokenizerModelFile.getModelType
      )
    }

    val tokenizerModelPath = tokenizerModelFile.path
    val corpusPath = inputData.path
    val corpusRdd = sparkContext.textFile(
      new Path(corpusPath, "*/*").toString
    )

    val outputPathStr = HdfsParameterUtils.getOutputPath(operatorParameters)
    val useLowerCase =
      operatorParameters.getStringValue("useLowerCase").toBoolean
    val filterStopWords =
      operatorParameters.getStringValue("filterStopWords").toBoolean

    val wordStatsFunc: Iterator[String] => Iterator[(String, TokenStats)] =
      (lines: Iterator[String]) => {
        // The following tokens are always removed from the dictionary.
        val illegalTokens = Set[String](
          "<", ",", ">", ".", "?", "/", ";", ":", "\"", "'", "[", "]", "{", "}", "|", "\\", "+", "=", "-", "_", "`", "~", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")"
        )

        val stopWords = Set[String](
          "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "don't", "should", "now"
        )

        val hdfs = FileSystem.get(new Configuration)

        // Read the tokenizer model.
        val tokenizerStream = hdfs.open(new Path(tokenizerModelPath))
        val model: TokenizerModel = new TokenizerModel(tokenizerStream)
        val tokenizer: Tokenizer = new TokenizerME(model)
        tokenizerStream.close()

        // Get token counts without the stop words.
        val tokenStats = mutable.Map[String, TokenStats]()
        while (lines.hasNext) {
          val line = lines.next()
          val tokens = tokenizer.tokenize(line)
          var i = 0
          while (i < tokens.length) {
            val token =
              if (useLowerCase) {
                tokens(i).toLowerCase
              } else {
                tokens(i)
              }
            // Illegal string and stop word filtering are always done
            // with lower case tokens.
            val lowerCaseToken = token.toLowerCase
            if (!illegalTokens.contains(lowerCaseToken)) {
              if (!filterStopWords ||
                (filterStopWords && !stopWords.contains(lowerCaseToken))) {
                if (!tokenStats.contains(token)) {
                  tokenStats(token) = TokenStats(0L, 1L)
                }

                tokenStats(token).count += 1L
              }
            }

            i += 1
          }
        }

        tokenStats.toIterator
      }

    val outputRdd =
      corpusRdd.
        mapPartitions(wordStatsFunc).
        reduceByKey((ts1, ts2) => TokenStats(ts1.count + ts2.count, ts1.docCount + ts2.docCount))

    if (HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)) {
      new SparkRuntimeUtils(sparkContext).deleteFilePathIfExists(outputPathStr)
    }

    outputRdd.map(
      t => t._1 + "\t" + t._2.count + "\t" + t._2.docCount
    ).saveAsTextFile(outputPathStr)

    // Now compute the entire corpus stats.
    val totalTokenCount =
      outputRdd.map(_._2.count).reduce(
        _ + _
      )
    val numDocs = corpusRdd.partitions.length

    new WordDictionaryFile(
      corpusPath,
      numDocs,
      totalTokenCount,
      useLowerCase,
      outputPathStr,
      Some(operatorParameters.operatorInfo)
    )
  }
}
