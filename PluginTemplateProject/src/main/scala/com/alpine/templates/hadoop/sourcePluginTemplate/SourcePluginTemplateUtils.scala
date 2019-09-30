package com.alpine.templates.hadoop.sourcePluginTemplate

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, TabularSchema}

object SourcePluginTemplateUtils {
  val numberRowsParamKey = "number"
  val numberRowsParamName = "Number of things"

  def getOutputSchema(params: OperatorParameters) : TabularSchema = {
    // create an return a tabular schema object
    TabularSchema(Seq(
      ColumnDef("Thing", ColumnType.String), // the first field in our output dataset
      ColumnDef("Number", ColumnType.Int))  // the second field in our output dataset
    )
  }
}

