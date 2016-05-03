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

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.{HdfsParameterUtils, HdfsStorageFormat, SparkParameterUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

class SparkRandomDatasetGeneratorSignature extends OperatorSignature[
  SparkRandomDatasetGeneratorGUINode,
  SparkRandomDatasetGeneratorRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - Random Data Generator",
      category = "Plugin Sample - Spark",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

object SparkRandomDatasetGeneratorConstants {
  val doubleColumnsParam = "numDoubleColumns"
  val intColumnsParam = "numIntColumns"
  val stringColumnsParam = "numStringColumns"
  val numRowsParam = "numRows"

  def defineOutputSchema(numDoubleCols: Int,
                         numIntCols: Int,
                         numStringCols: Int,
                         tabularFormatAttributes: TabularFormatAttributes) = {
    TabularSchema(
      (1 to numDoubleCols).map(i => ColumnDef("DoubleCol" + i.toString, ColumnType.Double)) ++
        (1 to numIntCols).map(i => ColumnDef("IntCol" + i.toString, ColumnType.Int)) ++
        (1 to numStringCols).map(i => ColumnDef("StringCol" + i.toString, ColumnType.String)),
      tabularFormatAttributes
    )

  }
}

class SparkRandomDatasetGeneratorGUINode
  extends OperatorGUINode[IONone, HdfsTabularDataset] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addIntegerBox(
      SparkRandomDatasetGeneratorConstants.doubleColumnsParam,
      "Number of double columns to generate.",
      0,
      Int.MaxValue,
      1
    )

    operatorDialog.addIntegerBox(
      SparkRandomDatasetGeneratorConstants.intColumnsParam,
      "Number of integer columns to generate.",
      0,
      Int.MaxValue,
      1
    )

    operatorDialog.addIntegerBox(
      SparkRandomDatasetGeneratorConstants.stringColumnsParam,
      "Number of string columns to generate.",
      0,
      Int.MaxValue,
      1
    )

    operatorDialog.addIntegerBox(
      SparkRandomDatasetGeneratorConstants.numRowsParam,
      "Number of rows to generate.",
      1,
      Int.MaxValue,
      1000
    )

    HdfsParameterUtils.addHdfsStorageFormatParameter(operatorDialog, HdfsStorageFormat.TSV)
    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

    SparkParameterUtils.addStandardSparkOptions(
      operatorDialog,
      defaultNumExecutors = 2,
      defaultExecutorMemoryMB = 1024,
      defaultDriverMemoryMB = 1024,
      defaultNumExecutorCores = 1
    )

    operatorSchemaManager.setOutputSchema(
      // Not defining the attribute argument means we're defaulting to TSV.
      TabularSchema(
        Seq(
          ColumnDef("DoubleCol1", ColumnType.Double),
          ColumnDef("IntCol1", ColumnType.Int),
          ColumnDef("StringCol1", ColumnType.String)
        )
      )
    )
  }

  override def onInputOrParameterChange(inputSchemas: Map[String, TabularSchema],
                                        params: OperatorParameters,
                                        operatorSchemaManager: OperatorSchemaManager): OperatorStatus = {
    val numDoubleCols = params.getIntValue(SparkRandomDatasetGeneratorConstants.doubleColumnsParam)
    val numIntCols = params.getIntValue(SparkRandomDatasetGeneratorConstants.intColumnsParam)
    val numStringCols = params.getIntValue(SparkRandomDatasetGeneratorConstants.stringColumnsParam)
    val totalNumCols = numDoubleCols + numIntCols + numStringCols
    if (totalNumCols <= 0) {
      OperatorStatus(isValid = false, msg = "The total number of columns has to be greater than 0.")
    } else {
      // We have to re-define the output schema based on the number of column
      // parameters. Additionally, we have to take into account the user
      // potentially selecting different output formats.
      val tabularFormatAttributes = HdfsParameterUtils.getTabularFormatAttributes(HdfsParameterUtils.getHdfsStorageFormat(params))
      val outputSchema = SparkRandomDatasetGeneratorConstants.defineOutputSchema(numDoubleCols,
        numIntCols, numStringCols, tabularFormatAttributes)
      operatorSchemaManager.setOutputSchema(outputSchema)

      OperatorStatus(isValid = true)
    }
  }
}

class SparkRandomDatasetGeneratorRuntime
  extends SparkRuntimeWithIOTypedJob[RandomDatasetGeneratorJob, IONone, HdfsTabularDataset] {}

class RandomDatasetGeneratorJob extends SparkIOTypedPluginJob[IONone, HdfsTabularDataset] {
  override def onExecution(sparkContext: SparkContext,
                           appConf: mutable.Map[String, String],
                           input: IONone,
                           params: OperatorParameters,
                           listener: OperatorListener): HdfsTabularDataset = {

    val sparkUtils = new SparkRuntimeUtils(sparkContext)
    val numDoubleCols = params.getIntValue("numDoubleColumns")
    val numIntCols = params.getIntValue("numIntColumns")
    val numStringCols = params.getIntValue("numStringColumns")
    val numRows = params.getIntValue("numRows")
    val numExecutors = sparkContext.getExecutorMemoryStatus.size
    val numRowsPerPartition = math.ceil(numRows.toDouble / numExecutors.toDouble).toInt
    val randomDataRdd = sparkContext.parallelize(Seq[Int](), numExecutors).mapPartitionsWithIndex {
      (partitionId, _) => {
        val rng = new scala.util.Random(partitionId)
        (1 to numRowsPerPartition).map(
          _ =>
            org.apache.spark.sql.Row.fromSeq(
              (1 to numDoubleCols).map(_ => rng.nextDouble()) ++
                (1 to numIntCols).map(_ => rng.nextInt()) ++
                (1 to numStringCols).map(
                  _ =>
                    rng.nextPrintableChar().toString +
                      rng.nextPrintableChar().toString +
                      rng.nextPrintableChar().toString +
                      rng.nextPrintableChar().toString +
                      rng.nextPrintableChar().toString
                )
            )
        ).iterator
      }
    }

    val outputPath = HdfsParameterUtils.getOutputPath(params)
    val overwrite = HdfsParameterUtils.getOverwriteParameterValue(params)
    val storageFormat = HdfsParameterUtils.getHdfsStorageFormat(params)

    val tabularFormatAttributes =
      HdfsParameterUtils.getTabularFormatAttributes(storageFormat)

    //use our utils object to get the AlpineOutputSchema which we can then convert to Spark SQL with the
    //the SparkRuntimeUtils class.
    val outputSchema = SparkRandomDatasetGeneratorConstants.defineOutputSchema(numDoubleCols,
      numIntCols, numStringCols, tabularFormatAttributes)

    val sqlContext = new SQLContext(sparkContext)
    val outputDF =
      sqlContext.createDataFrame(randomDataRdd, sparkUtils.convertTabularSchemaToSparkSQLSchema(outputSchema))

    //use sparkUtils to save the DataFrame and return the HdfsTabularDataset object
    sparkUtils.saveDataFrame(
      outputPath,
      outputDF,
      storageFormat,
      overwrite,
      Some(params.operatorInfo)
    )
  }
}
