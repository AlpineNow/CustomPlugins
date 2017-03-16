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
import com.alpine.plugin.core.dialog.{SparkParameter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.spark.{AutoTunerOptions, SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.{HdfsStorageFormatType, HdfsParameterUtils, SparkParameterUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

class SparkRandomDatasetGeneratorSignature extends OperatorSignature[
  SparkRandomDatasetGeneratorGUINode,
  SparkRandomDatasetGeneratorRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - Random Data Generator",
    category = "Plugin Sample - Spark",
    author = Some("Jenny Thompson"),
    version = 1,
    helpURL = None,
    icon = None,
    toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
      "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
      " of the operator and are no more than fifty words.")
  )
}

object SparkRandomDatasetGeneratorConstants {
  val doubleColumnsParam = "numDoubleColumns"
  val intColumnsParam = "numIntColumns"
  val stringColumnsParam = "numStringColumns"
  val numRowsParam = "numRows"

  def defineOutputSchema(numDoubleCols: Int,
                         numIntCols: Int,
                         numStringCols: Int
                        ): TabularSchema = {
    TabularSchema(
      (1 to numDoubleCols).map(i => ColumnDef("DoubleCol" + i.toString, ColumnType.Double)) ++
        (1 to numIntCols).map(i => ColumnDef("IntCol" + i.toString, ColumnType.Int)) ++
        (1 to numStringCols).map(i => ColumnDef("StringCol" + i.toString, ColumnType.String))
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

    HdfsParameterUtils.addHdfsStorageFormatParameter(operatorDialog, HdfsStorageFormatType.CSV)
    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

    SparkParameterUtils.addStandardSparkOptions(operatorDialog, List[SparkParameter]())

    operatorSchemaManager.setOutputSchema(
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
      val outputSchema = SparkRandomDatasetGeneratorConstants.defineOutputSchema(numDoubleCols,
        numIntCols, numStringCols)
      operatorSchemaManager.setOutputSchema(outputSchema)

      OperatorStatus(isValid = true)
    }
  }
}

class SparkRandomDatasetGeneratorRuntime
  extends SparkRuntimeWithIOTypedJob[RandomDatasetGeneratorJob, IONone, HdfsTabularDataset] {
  /**
    * We use this method to change how the Spark AutoTuner Works.
    * By default the auto tuner spins up enough workers to read the input data into memory. We estimate
    * the size of the input data in memory as the file size times some scalar, which can be
    * set in this method.
    * (See the documentation for the 'AutoTunerOptions' class for more details.)
    *
    * However, since this operator has no input data, we need to override this value so that
    * it will correspond with the computational resources we need.
    * There are many possible ways of doing this, one simple way, is just to assume that the
    * operator requires more resources as the number of rows increases. (Note that this value
    * will be reported in the "view status" feed of the alpine UI as  the "size of input data in memory").
    */

  override def getAutoTuningOptions(parameters: OperatorParameters, input: IONone): AutoTunerOptions = {
    val fileSizeMultiplier = parameters.getIntValue("numRows")/2000.0 //we will increase the file size base on the number of rows
    AutoTunerOptions(driverMemoryFraction = 0.5, //this doesn't require much work from the driver so we set this value to less than one.
      fileSizeMultiplier = fileSizeMultiplier)
  }
}

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
    val storageFormat = HdfsParameterUtils.getHdfsStorageFormatType(params)

    //use our utils object to get the AlpineOutputSchema which we can then convert to Spark SQL with the
    //the SparkRuntimeUtils class.
    val outputSchema = SparkRandomDatasetGeneratorConstants.defineOutputSchema(numDoubleCols,
      numIntCols, numStringCols)

    val sqlContext = new SQLContext(sparkContext)
    val outputDF =
      sqlContext.createDataFrame(randomDataRdd, sparkUtils.convertTabularSchemaToSparkSQLSchema(outputSchema))

    //use sparkUtils to save the DataFrame and return the HdfsTabularDataset object

    sparkUtils.saveDataFrame(
      outputPath,
      outputDF,
      storageFormat,
      overwrite,
      Some(params.operatorInfo),
      Map[String, AnyRef](),
      TSVAttributes.defaultCSV //the default is a comma delimited file. But you may specify different delimiters or escape characters with this parameter
    )
  }
}
