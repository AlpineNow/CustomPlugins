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
import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.model.RegressionModelWrapper

/**
 * This is the design-time code for the Regression evaluator operator.
 * It takes an input dataset and a Regression model to create a set of metrics
 * evaluating the quality of the model on the dataset.
 *
 * The result is a dataset containing one row, where each statistic is in a separate column.
 */
class RegressionEvaluatorSignature extends OperatorSignature[
  RegressionEvaluatorGUINode,
  RegressionEvaluatorRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Regression Evaluator",
      category = "Model Evaluator",
      author = "Jenny Thompson",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class RegressionEvaluatorGUINode extends OperatorGUINode[
  Tuple2[HdfsTabularDataset, RegressionModelWrapper],
  HdfsTabularDataset
  ] {

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

    val columnDefs = Array(
      ColumnDef("explainedVariance", ColumnType.Double),
      ColumnDef("meanAbsoluteError", ColumnType.Double),
      ColumnDef("meanSquaredError", ColumnType.Double),
      ColumnDef("rootMeanSquaredError", ColumnType.Double),
      ColumnDef("r2", ColumnType.Double)
    )

    val expectedOutputFormat = TabularFormatAttributes.createTSVFormat()

    val outputSchema = TabularSchema(columnDefs, expectedOutputFormat)
    operatorSchemaManager.setOutputSchema(outputSchema)
  }

}

