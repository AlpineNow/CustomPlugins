package com.alpine.templates.hadoop.modelPluginTemplate

import com.alpine.plugin.core.{OperatorCategories, OperatorMetadata, OperatorSignature}

// TODO: Class that ADL uses to get metadata about the operator, replace name, author, help, icon, description text
class ModelPluginTemplateSignature extends OperatorSignature [ModelPluginTemplateGUINode, ModelPluginTemplateRuntime]
{
  def getMetadata: OperatorMetadata = {
    new OperatorMetadata(
      name="Operator name",
      category = OperatorCategories.MODEL,
      author = Some("Tibco SDS"),
      version = 1,
      helpURL = Some(OperatorMetadata.alpineHelpLink("Model+Plugin+Template")),
      icon = None,
      toolTipText = Some("Brief description about the operator.")
    )
  }
}

