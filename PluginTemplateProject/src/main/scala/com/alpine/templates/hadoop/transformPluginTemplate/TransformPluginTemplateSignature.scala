package com.alpine.templates.hadoop.transformPluginTemplate

import com.alpine.plugin.core.{OperatorMetadata, OperatorSignature}

// TODO: Class that ADL uses to get metadata about the operator, replace name, author, help, icon, description text
class TransformPluginTemplateSignature
  extends OperatorSignature[TransformPluginTemplateGUINode, TransformPluginTemplateRuntime]{

  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "operator name",
      category = "Transformations",
      author = Some("Tibco"),
      version = 1,
      helpURL = None,
      icon = None,
      toolTipText = Some("A brief description of the operator.")
    )
  }

}

