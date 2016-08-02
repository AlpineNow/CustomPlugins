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

import scala.collection.mutable

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}
import opennlp.tools.tokenize.{Tokenizer, TokenizerME, TokenizerModel}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RawTextToTabularDataSignature extends OperatorSignature[
  RawTextToTabularDataGUINode,
  RawTextToTabularDataRuntime] {
  def getMetadata: OperatorMetadata = {
    new OperatorMetadata(
      name = "Text Mining - Text to Table Converter",
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

object TextFeatures {
  // Document features.
  val docFeat_rootPathLabel = "Root Path"
  val docFeat_numWordsLabel = "Number of Words"
  val docFeat_normalizedNumWordsLabel = "Normalized Number of Words"

  // Word features.
  val wordFeat_rawCountsLabel = "Raw Counts"
  val wordFeat_normalizedCountsLabel = "Normalized Counts"
  val wordFeat_tfIdfLabel = "TF-IDF"

  val labelToColString = Map[String, String](
    docFeat_rootPathLabel -> "RootPath",
    docFeat_numWordsLabel -> "NumWordsInDoc",
    docFeat_normalizedNumWordsLabel -> "NormalizedNumWordsInDoc",
    wordFeat_rawCountsLabel -> "RawCount",
    wordFeat_normalizedCountsLabel -> "NormalizedCount",
    wordFeat_tfIdfLabel -> "TFIDF"
  )
}

class RawTextToTabularDataGUINode extends OperatorGUINode[
  Tuple3[HdfsRawTextDataset, WordDictionaryFile, HdfsOpenNLPModelFile],
  HdfsDelimitedTabularDataset] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {

    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

    // A 'document' is considered to an individual raw text file.
    // This will convert one document into one row of a tabular dataset.
    // We want to add 'document' level features.
    operatorDialog.addCheckboxes(
      id = "documentFeatures",
      label = "Document Features to Add",
      values = Seq(
        TextFeatures.docFeat_rootPathLabel,
        TextFeatures.docFeat_numWordsLabel,
        TextFeatures.docFeat_normalizedNumWordsLabel
      ),
      defaultSelections = Seq(
        TextFeatures.docFeat_rootPathLabel,
        TextFeatures.docFeat_numWordsLabel,
        TextFeatures.docFeat_normalizedNumWordsLabel
      )
    )

    // We have a dictionary of all the words used in the entire corpus.
    // We want to detect the occurrences of some of the words in individual
    // documents and compute some statistics or measures about those words.
    // These will form word features for individual documents.
    operatorDialog.addIntegerBox(
      id = "numWords",
      label = "Number of Unique Words to Consider",
      min = 1,
      max = Int.MaxValue,
      defaultValue = 1000
    )

    // How the words are chosen from the dictionary for word feature
    // calculations.
    operatorDialog.addDropdownBox(
      id = "wordCriteria",
      label = "How Words are Chosen for Consideration",
      values = Seq(
        "Highest Counts",
        "Most Documents",
        "Lowest Counts",
        "Fewest Documents"
      ),
      defaultSelection = "Most Documents"
    )

    // Word features that we want to compute.
    operatorDialog.addCheckboxes(
      id = "wordFeatures",
      label = "Word Features to Add",
      values = Seq(
        TextFeatures.wordFeat_rawCountsLabel,
        TextFeatures.wordFeat_normalizedCountsLabel,
        TextFeatures.wordFeat_tfIdfLabel
      ),
      defaultSelections = Seq(
        TextFeatures.wordFeat_rawCountsLabel,
        TextFeatures.wordFeat_normalizedCountsLabel,
        TextFeatures.wordFeat_tfIdfLabel
      )
    )

    // For the normalized word features, the smoothing
    // constant we want to add to the denominator.
    operatorDialog.addDoubleBox(
      id = "smoothingConstant",
      label = "Smoothing Constant for 'Normalized Counts'",
      min = 0.0,
      max = Double.MaxValue,
      inclusiveMin = true,
      inclusiveMax = true,
      defaultValue = 1.5
    )

    operatorSchemaManager.setOutputSchema(
      DocumentFeatureManager.createOutputSchema(
        1000,
        Seq(
          TextFeatures.docFeat_rootPathLabel,
          TextFeatures.docFeat_numWordsLabel,
          TextFeatures.docFeat_normalizedNumWordsLabel
        ),
        Seq(
          TextFeatures.wordFeat_rawCountsLabel,
          TextFeatures.wordFeat_normalizedCountsLabel,
          TextFeatures.wordFeat_tfIdfLabel
        )
      )
    )
  }

  override def onInputOrParameterChange(inputSchemas: Map[String, TabularSchema],
                                        params: OperatorParameters,
                                        operatorSchemaManager: OperatorSchemaManager): OperatorStatus = {
    operatorSchemaManager.setOutputSchema(
      DocumentFeatureManager.createOutputSchema(
        params.getIntValue("numWords"),
        params.getStringArrayValue("documentFeatures").toSeq,
        params.getStringArrayValue("wordFeatures").toSeq
      )
    )

    OperatorStatus(isValid = true, msg = None)
  }

  override def onOutputVisualization(
    params: OperatorParameters,
    output: HdfsDelimitedTabularDataset,
    visualFactory: VisualModelFactory): VisualModel = {
    val composite = visualFactory.createCompositeVisualModel()
    val dataset = visualFactory.createTabularDatasetVisualization(output)
    val wordMapInfo =
      visualFactory.createTextVisualization(
        output.addendum("wordMap").toString
      )
    composite.addVisualModel("Dataset", dataset)
    composite.addVisualModel("Word Map", wordMapInfo)
    composite
  }
}

object DocumentFeatureManager {
  def addColumnDef(
    columnDefs: mutable.ArrayBuffer[ColumnDef],
    label: String,
    typeValue: ColumnType.TypeValue): Unit = {
    columnDefs +=
      ColumnDef(
        TextFeatures.labelToColString(label),
        typeValue
      )
  }

  def addColumnDef(
    columnDefs: mutable.ArrayBuffer[ColumnDef],
    colPrefix: String,
    label: String,
    typeValue: ColumnType.TypeValue): Unit = {
    columnDefs +=
      ColumnDef(
        colPrefix + "_" + TextFeatures.labelToColString(label),
        typeValue
      )
  }

  def createDocumentFeatureColumnDefs(
    docFeatures: Seq[String]): mutable.ArrayBuffer[ColumnDef] = {
    val columnDefs = new mutable.ArrayBuffer[ColumnDef]()
    docFeatures.foreach {
      case TextFeatures.docFeat_rootPathLabel =>
        addColumnDef(columnDefs, TextFeatures.docFeat_rootPathLabel, ColumnType.String)
      case TextFeatures.docFeat_numWordsLabel =>
        addColumnDef(columnDefs, TextFeatures.docFeat_numWordsLabel, ColumnType.Double)
      case TextFeatures.docFeat_normalizedNumWordsLabel =>
        addColumnDef(columnDefs, TextFeatures.docFeat_normalizedNumWordsLabel, ColumnType.Double)
    }

    columnDefs
  }

  def addWordFeatures(
    columnDefs: mutable.ArrayBuffer[ColumnDef],
    columnName: String,
    wordFeatures: Seq[String]): Unit = {
    wordFeatures.foreach {
      case TextFeatures.wordFeat_rawCountsLabel =>
        addColumnDef(
          columnDefs,
          columnName,
          TextFeatures.wordFeat_rawCountsLabel,
          ColumnType.Double
        )
      case TextFeatures.wordFeat_normalizedCountsLabel =>
        addColumnDef(
          columnDefs,
          columnName,
          TextFeatures.wordFeat_normalizedCountsLabel,
          ColumnType.Double
        )

      case TextFeatures.wordFeat_tfIdfLabel =>
        addColumnDef(
          columnDefs,
          columnName,
          TextFeatures.wordFeat_tfIdfLabel,
          ColumnType.Double
        )
    }
  }

  def createOutputSchema(
    numWords: Int,
    docFeatures: Seq[String],
    wordFeatures: Seq[String]): TabularSchema = {
    val columnDefs = createDocumentFeatureColumnDefs(docFeatures)

    var i = 0
    while (i < numWords) {
      val colName = "Word" + (i + 1).toString
      addWordFeatures(columnDefs, colName, wordFeatures)

      i += 1
    }

    TabularSchema(columnDefs)
  }
}

class RawTextToTabularDataRuntime extends SparkRuntimeWithIOTypedJob[
  RawTextToTabularDataJob,
  Tuple3[HdfsRawTextDataset, WordDictionaryFile, HdfsOpenNLPModelFile],
  HdfsDelimitedTabularDataset] {}

object WordUtils {
  def sortTokenStatsByCriteria(
    wordCriteria: String,
    wordStatsMap: mutable.Map[String, TokenStats]
  ): List[(String, TokenStats)] = {
    if (wordCriteria.equals("Highest Counts")) {
      wordStatsMap.toList.sortBy(-_._2.count)
    } else if (wordCriteria.equals("Lowest Counts")) {
      wordStatsMap.toList.sortBy(_._2.count)
    } else if (wordCriteria.equals("Most Documents")) {
      wordStatsMap.toList.sortBy(-_._2.docCount)
    } else {
      wordStatsMap.toList.sortBy(_._2.docCount)
    }
  }
}

class RawTextToTabularDataJob extends SparkIOTypedPluginJob[
  Tuple3[HdfsRawTextDataset, WordDictionaryFile, HdfsOpenNLPModelFile],
  HdfsDelimitedTabularDataset] {
  override def onExecution(
    sparkContext: SparkContext,
    appConf: mutable.Map[String, String],
    input: Tuple3[HdfsRawTextDataset, WordDictionaryFile, HdfsOpenNLPModelFile],
    operatorParameters: OperatorParameters,
    listener: OperatorListener): HdfsDelimitedTabularDataset = {

    val conf = new Configuration()
    val dfs = FileSystem.get(conf)
    val inputRawFiles = input._1
    val dictionaryFile = input._2
    val tokenizerModelFile = input._3

    if (!tokenizerModelFile.getModelType.equals(OpenNLPModelType.Tokenizer)) {
      throw new IllegalArgumentException(
        "Expected a tokenizer model file but instead found " +
          tokenizerModelFile.getModelType
      )
    }

    val tokenizerModelPath = tokenizerModelFile.path
    val inputPath = inputRawFiles.path
    val outputPathStr = HdfsParameterUtils.getOutputPath(operatorParameters)
    val delimiter = dictionaryFile.tsvAttributes.delimiter

    val documentFeaturesToAdd =
      operatorParameters.getStringArrayValue("documentFeatures")
    val maxNumWords = operatorParameters.getIntValue("numWords")
    val wordCriteria = operatorParameters.getStringValue("wordCriteria")
    val wordFeaturesToAdd =
      operatorParameters.getStringArrayValue("wordFeatures")
    val smoothingConstant =
      operatorParameters.getDoubleValue("smoothingConstant")

    val numTotalDocs = dictionaryFile.getCorpusDocCount
    val numTotalWords = dictionaryFile.getCorpusWordCount
    val lowerCased = dictionaryFile.isLowerCased

    // Load the corpus word dictionary first.
    val dictRdd = sparkContext.textFile(dictionaryFile.path)
    val toBeUsedWordStats = dictRdd.mapPartitions(lines => {
      val wordStatsMap = mutable.Map[String, TokenStats]()
      while (lines.hasNext) {
        val line = lines.next()
        val splitElems = line.split(delimiter)
        wordStatsMap +=
          (
            splitElems(0) ->
              TokenStats(
                splitElems(1).toLong,
                splitElems(2).toLong
              )
          )
      }

      Array(wordStatsMap).toIterator
    }).reduce((s1, s2) => {
      val combined = s1 ++ s2
      val reduced: mutable.Map[String, TokenStats] =
        if (combined.size > maxNumWords) {
          val r = mutable.Map[String, TokenStats]()
          val sortedList =
            WordUtils.sortTokenStatsByCriteria(wordCriteria, combined)

          sortedList.foreach(
            p => {
              if (r.size < maxNumWords) {
                r.put(p._1, p._2)
              }
            }
          )
          r
        } else {
          combined
        }
      reduced
    })

    // Sort all word cnt map by raw counts.
    val sortedWordStats =
      WordUtils.sortTokenStatsByCriteria(wordCriteria, toBeUsedWordStats)

    val numWords = math.min(sortedWordStats.size, maxNumWords)

    val columnDefs = DocumentFeatureManager.createDocumentFeatureColumnDefs(
      documentFeaturesToAdd.toSeq
    )

    // This is the mapping information from a word to a column.
    val wordMapOutputBuilder = new mutable.StringBuilder()

    // Now, assign a feature index to each word.
    // Also remember the number of docs that the word appeared in.
    val wordIndices = mutable.Map[String, (Int, Long)]()
    val itr = sortedWordStats.iterator
    var i = 0
    while (i < numWords && itr.hasNext) {
      val wordStats = itr.next()
      wordIndices.put(wordStats._1, (i, wordStats._2.docCount))
      val columnName = "Word" + (i + 1).toString
      DocumentFeatureManager.addWordFeatures(
        columnDefs,
        columnName,
        wordFeaturesToAdd.toSeq
      )

      wordMapOutputBuilder.
        append("'" + columnName + "'").
        append(" -> '").
        append(wordStats._1).
        append("', raw count : ").
        append(wordStats._2.count).
        append(", doc count : ").
        append(wordStats._2.docCount).
        append("\n")

      i += 1
    }

    val numWordFeatures = wordFeaturesToAdd.length

    // Find child directories.
    val childDirs = dfs.listStatus(new Path(inputPath))

    // Each child directory should contain related corpus of documents.
    val rdds = new Array[RDD[String]](childDirs.length)
    i = 0
    while (i < childDirs.length) {
      val f = childDirs(i)
      val path = f.getPath.getName
      val pathRdd = sparkContext.textFile(f.getPath.toString)
      rdds(i) = pathRdd.mapPartitions(
        lines => {
          val hdfs = FileSystem.get(new Configuration)
          // Read the tokenizer config.
          val tokenizerStream = hdfs.open(new Path(tokenizerModelPath))
          val model: TokenizerModel = new TokenizerModel(tokenizerStream)
          val tokenizer: Tokenizer = new TokenizerME(model)
          tokenizerStream.close()

          val content = lines.mkString
          val wordFeatures = Array.fill[Double](numWords * numWordFeatures)(0.0)
          val tokens = tokenizer.tokenize(content)
          val docSize = tokens.length
          var j = 0
          while (j < tokens.length) {
            val token =
              if (lowerCased) {
                tokens(j).toLowerCase
              } else {
                tokens(j)
              }
            if (wordIndices.contains(token)) {
              val wordIndex = wordIndices(token)._1
              val numDocsWithWord = wordIndices(token)._2
              val featureOffset = wordIndex * numWordFeatures

              // Add all the word features for this word.
              var k = 0
              while (k < wordFeaturesToAdd.length) {
                val wordFeature = wordFeaturesToAdd(k)
                wordFeature match {
                  case TextFeatures.wordFeat_rawCountsLabel =>
                    wordFeatures(featureOffset + k) += 1.0
                  case TextFeatures.wordFeat_normalizedCountsLabel =>
                    wordFeatures(featureOffset + k) += 1.0 / (docSize.toDouble + smoothingConstant)
                  case TextFeatures.wordFeat_tfIdfLabel =>
                    wordFeatures(featureOffset + k) +=
                      math.log(numTotalDocs.toDouble / numDocsWithWord.toDouble)
                }

                k += 1
              }
            }

            j += 1
          }

          // Now build the full document feature row.
          val rowBuilder = new mutable.StringBuilder()
          j = 0
          while (j < documentFeaturesToAdd.length) {
            val documentFeature = documentFeaturesToAdd(j)
            documentFeature match {
              case TextFeatures.docFeat_rootPathLabel =>
                rowBuilder.append(path).append("\t")
              case TextFeatures.docFeat_numWordsLabel =>
                rowBuilder.append(docSize.toString).append("\t")
              case TextFeatures.docFeat_normalizedNumWordsLabel =>
                rowBuilder.append(docSize.toDouble / numTotalWords.toDouble).append("\t")
            }

            j += 1
          }

          rowBuilder.append(wordFeatures.mkString("\t"))
          Array(rowBuilder.toString()).toIterator
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

    new SparkRuntimeUtils(sparkContext).deleteOrFailIfExists(
      path = outputPathStr,
      overwrite = HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)
    )

    unionRdd.saveAsTextFile(outputPathStr)

    new HdfsDelimitedTabularDatasetDefault(
      outputPathStr,
      TabularSchema(columnDefs),
      TSVAttributes.default,
      Some(operatorParameters.operatorInfo),
      Map("wordMap" -> wordMapOutputBuilder.toString())
    )
  }
}