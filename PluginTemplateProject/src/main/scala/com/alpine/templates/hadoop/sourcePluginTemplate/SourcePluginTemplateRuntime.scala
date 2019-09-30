package com.alpine.templates.hadoop.sourcePluginTemplate

import com.alpine.plugin.core.io.{HdfsTabularDataset, IONone}
import com.alpine.plugin.core.spark.SparkRuntimeWithIOTypedJob

class SourcePluginTemplateRuntime
  extends SparkRuntimeWithIOTypedJob[SourcePluginTemplateJob, IONone, HdfsTabularDataset] {

}

