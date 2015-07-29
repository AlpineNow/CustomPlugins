/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.utils.OutputParameterUtils

import collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import opennlp.tools.tokenize.{TokenizerME, Tokenizer, TokenizerModel}

import com.alpine.plugin.core._
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.OperatorMetadata
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.spark.{SparkRuntimeWithIOTypedJob, SparkIOTypedPluginJob}
import com.alpine.plugin.core.dialog.OperatorDialog

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
    OutputParameterUtils
      .addStandardHDFSOutputParameters(operatorDialog, "RawTextToClassifier", operatorDataSourceManager)

    operatorDialog.addDropdownBox(
      id = "labelSyntax",
      label = "How Label is Chosen",
      values = scala.collection.Seq("Path Name"),
      defaultSelection = "Path Name"
    )

    // This can have a variable number of columns.
    val schemaOutline = operatorSchemaManager.createTabularSchemaOutline(
      minNumCols = 1001, // Stupid hack to pre-define columns.
      maxNumCols = Int.MaxValue
    )

    // The only thing we know for certain is that there will be a label column.
    schemaOutline.addColumnDef(ColumnDef("Label", ColumnType.String))
    var i = 0
    while (i < 1000) {
      schemaOutline.addColumnDef(ColumnDef("Word" + (i + 1).toString, ColumnType.Int))
      i += 1
    }

    operatorSchemaManager.setOutputSchemaOutline(schemaOutline)
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
    listener: OperatorListener,
    ioFactory: IOFactory): HdfsDelimitedTabularDataset = {
    val conf = new Configuration()
    val dfs = FileSystem.get(conf)
    val inputRawFiles = input.getT1()
    val dictionaryFile = input.getT2()
    val tokenizerConfigFile = input.getT3()
    val tokenizerConfigPath = tokenizerConfigFile.getPath()
    val inputPath = inputRawFiles.getPath()
    val outputPathStr = OutputParameterUtils.getOutputPath(operatorParameters)
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

    val schemaOutline = ioFactory.createTabularSchemaOutline(
      minNumCols = numWords + 1,
      maxNumCols = numWords + 1
    )

    // Define the output column types (label and word counts).
    schemaOutline.addColumnDef(ColumnDef("Label", ColumnType.String))

    // Now, assign a feature index to each word.
    val wordIndices = mutable.Map[String, Int]()
    val itr = sortedWordCnt.iterator
    var i = 0
    while (i < 1000 && itr.hasNext) {
      val wordCnt = itr.next()
      wordIndices.put(wordCnt._1, i)
      schemaOutline.addColumnDef(ColumnDef("Word" + (i + 1).toString, ColumnType.Long))
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

    val outputPath = new Path(outputPathStr)
    val driverHdfs = FileSystem.get(sparkContext.hadoopConfiguration)
    if (driverHdfs.exists(outputPath)) {
      driverHdfs.delete(outputPath, true)
    }
    unionRdd.saveAsTextFile(outputPathStr)

    ioFactory.createHdfsDelimitedTabularDataset(
      path = outputPathStr,
      delimiter = "\t",
      escapeStr = "\\",
      quoteStr = "\"",
      containsHeader = false,
      schemaOutline = schemaOutline
    )
  }
}