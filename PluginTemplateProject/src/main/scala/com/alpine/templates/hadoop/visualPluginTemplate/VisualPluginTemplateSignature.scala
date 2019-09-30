package com.alpine.templates.hadoop.visualPluginTemplate

import com.alpine.plugin.core.{OperatorMetadata, OperatorSignature}

// TODO: Class that ADL uses to get metadata about the operator, replace name, author, help, icon, description text
class VisualPluginTemplateSignature extends OperatorSignature[VisualPluginTemplateGUINode, VisualPluginTemplateRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "operator name",
    category = "Explore",
    author = Some("Alpine Data"),
    version = 1,
    helpURL = None,
    icon = None,
    toolTipText = Some("An Explore Operator that draws a graph."),
    usesJavascript = true);

}
