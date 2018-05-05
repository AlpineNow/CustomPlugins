package com.alpine.plugin.samples.advanced

/*
 * Copyright (c) 2017 Alpine Data Labs
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

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog._
import com.alpine.plugin.core.io.defaults.IONoneDefault
import com.alpine.plugin.core.io.{HdfsTabularDataset, IONone, OperatorSchemaManager}
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntime}
import com.alpine.plugin.core.visualization._

import scala.util.Try

/**
  * A demonstration of UI property elements available to the custom operator developer
  */

class DialogElementExplorerSignature extends OperatorSignature[
  DialogElementExplorerGUINode,
  DialogElementExplorerRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - Property Element Explorer",
    category = "Plugin Sample - Spark",
    author = Some("Alpine Data"),
    version = 1,
    helpURL = None,
    icon = None,
    toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
      "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
      " of the operator and are no more than fifty words."),
    usesJavascript = false
  )
}

object DialogElementExplorerUtil {
  /**
    * Key for the column selection parameter.
    */
  val WIDGET_INTEGER = "widget_integer"
  val WIDGET_DOUBLE = "widget_double"
  val WIDGET_STRING = "widget_string"
  val WIDGET_SCRIPT_EDIT = "widget_script_edit"
  val WIDGET_PARENT_SEL = "widget_parent_sel"
  val WIDGET_CHORUS_FILE = "widget_chorus_file"
  val WIDGET_COLUMN_SEL = "widget_column_sel"
  val WIDGET_DROPDOWN_BOX = "widget_dropdown_box"
  val WIDGET_RADIO_BTNS = "widget_radio_btns"
  val WIDGET_ROW_DIALOG = "widget_row_dialog"
}

/**
  * The DialogElementExplorerGUINode defines the design time behavior of the plugin. It extends
  * the template GUI class "SparkDataFrameGUINode".
  * It is in this class that we define the parameters which the user will see when they click on the
  * operator and the design time output schema.
  */
class DialogElementExplorerGUINode extends OperatorGUINode[HdfsTabularDataset, IONone] {
  override def onPlacement(operatorDialog: OperatorDialog,
      operatorDataSourceManager: OperatorDataSourceManager,
      operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addDialogElement(
      IntegerDialogElementSetup(DialogElementExplorerUtil.WIDGET_INTEGER, "Integer", isRequired = false,
        Option.apply(NumericBound(0.0, inclusive = true)), Some(NumericBound(100.0, inclusive = false)),
        Option.apply("1")))

    operatorDialog.addDialogElement(NumericDialogElementSetup(DialogElementExplorerUtil.WIDGET_DOUBLE, "Double", isRequired = false,
      Option.apply(NumericBound(0.0, inclusive = true)), Some(NumericBound(100.0, inclusive = false)),
      Option.apply("0.0")))

    operatorDialog.addDialogElement(StringDialogElementSetup(DialogElementExplorerUtil.WIDGET_STRING, "String", isRequired = false,
      Option.apply("Hello World"), None, isLarge = false, isPassword = false))

    operatorDialog.addDialogElement(
      ScriptEditPopupSetup(DialogElementExplorerUtil.WIDGET_SCRIPT_EDIT, "Script Edit", isRequired = false, "sql"))

    operatorDialog.addDialogElement(ParentOperatorSelectorSetup(DialogElementExplorerUtil.WIDGET_PARENT_SEL, "Parent Select", isRequired = false))

    operatorDialog.addDialogElement(ChorusFileSelectorSetup(DialogElementExplorerUtil.WIDGET_CHORUS_FILE, "File Select", isRequired = false,
      extensionFilter = Set[String](".csv", ".txt"), None))

    operatorDialog.addDialogElement(TabularDatasetColumnDropdownSetup(DialogElementExplorerUtil.WIDGET_COLUMN_SEL, "Column Select", isRequired = false,
      ColumnFilter.NumericOnly, "main", None))

    operatorDialog.addDialogElement(TabularDatasetColumnDropdownSetup(DialogElementExplorerUtil.WIDGET_COLUMN_SEL+"2", "Column Select 2", isRequired = false,
      ColumnFilter.All, "main", None))

    operatorDialog.addDialogElement(DropdownBoxSetup(DialogElementExplorerUtil.WIDGET_DROPDOWN_BOX, "Dropdown", isRequired = false,
      Option.apply(Seq("Red", "Green", "Blue")), Option.apply("Blue")))

    operatorDialog.addDialogElement(RadioButtonSetup(DialogElementExplorerUtil.WIDGET_RADIO_BTNS, "Radio Buttons", isRequired = false,
      Seq("Mario", "Luigi", "Peach"), "Mario"))

    operatorDialog.addDialogElement(RowDialogSetup(DialogElementExplorerUtil.WIDGET_ROW_DIALOG, "Row Dialog", "Show Dialog", isRequired = false,
      Seq(
        RowDialogElement("a_str", "A String",
          StringDialogElementSetup("a_str", "String", isRequired = true, Some("Hello World"), None, isLarge = false, isPassword = false), 15, 100),
        RowDialogElement("an_int", "An Int",
          IntegerDialogElementSetup("an_int", "An Int", isRequired = true, Some(NumericBound(-1000, inclusive = true)), Some(NumericBound(1000, inclusive = true)), Some("55")), 15, 100),
        RowDialogElement("a_double", "A Double",
          NumericDialogElementSetup("a_double", "A Double", isRequired = true, Some(NumericBound(-3.14, inclusive = true)), Some(NumericBound(3.14, inclusive = true)), Some("1.808")), 15, 100),
        RowDialogElement("a_dd", "A Dropdown",
          DropdownBoxSetup("a_dd", "A Dropdown", isRequired = true, Some(Seq("Alpha", "Beta", "Gamma")), None), 15, 100),
        RowDialogElement("a_column", "A Column Picker",
          TabularDatasetColumnDropdownSetup("a_column", "A Column Picker", isRequired = true, null, selectionGroupId = "main", None), 15, 100),
        RowDialogElement("a_radio", "Radio Buttons",
          RadioButtonSetup("a_radio", "Radio Buttons", isRequired = true, Seq("Red", "Green", "Blue"), "Blue"), 15, 100)
      ), Some(new RowDialogElementValidator)))
  }
}


class RowDialogElementValidator extends RowDialogValidator {
  override def validate(values: List[IRowDialogRow]): RowDialogValidation = {

    val dropdown_a_dd_values = values.flatMap(_.getRowDialogValues.filter(_.getName == "a_dd").map(_.getValue))
    val dropdownContainsAlpha: Boolean = dropdown_a_dd_values.exists(_.equals("Alpha"))

    if (dropdownContainsAlpha) {
      RowDialogValidation(isValid = false, "You may not select the value 'Alpha' for 'A Dropdown' in the Row Dialog.")
    } else {
      RowDialogValidation(isValid = true, "")
    }
  }
}

/**
  * What happens when the alpine user clicks the "run button".
  */
class DialogElementExplorerRuntime extends SparkRuntime[HdfsTabularDataset, IONone] {

  /**
    * @param context  Execution context of the operator.
    * @param input    The input to the operator.
    * @param output   The output from the execution.
    * @param params   The parameter values to the operator.
    * @param listener The listener object to communicate information back to
    *                 the console.
    * @return VisualModel
    */
  override def createVisualResults(context: SparkExecutionContext,
      input: HdfsTabularDataset,
      output: IONone,
      params: OperatorParameters,
      listener: OperatorListener): VisualModel = {

    /** *
      * Shows which relevant method in OperatorParameters should be used to retrieve each dialog element selected value(s), depending on the type of widget:
      */
    HtmlVisualModel(
      "Selected values: <br><br>" +
        //use getIntValue for a IntegerDialogElementSetup dialog element
        "Integer Box (" + DialogElementExplorerUtil.WIDGET_INTEGER + "): " + params.getIntValue(DialogElementExplorerUtil.WIDGET_INTEGER) + "<br>" +
        //use getDoubleValue for a NumericDialogElementSetup dialog element
        "Numeric Box (" + DialogElementExplorerUtil.WIDGET_DOUBLE + "): " + params.getDoubleValue(DialogElementExplorerUtil.WIDGET_DOUBLE) + "<br>" +
        "String Box (" + DialogElementExplorerUtil.WIDGET_STRING + "): " + params.getStringValue(DialogElementExplorerUtil.WIDGET_STRING) + "<br>" +
        "Script Window (" + DialogElementExplorerUtil.WIDGET_SCRIPT_EDIT + "): " + params.getStringValue(DialogElementExplorerUtil.WIDGET_SCRIPT_EDIT) + "<br>" +
        "Parent Dropdown (" + DialogElementExplorerUtil.WIDGET_PARENT_SEL + "): " + params.getStringValue(DialogElementExplorerUtil.WIDGET_PARENT_SEL) + "<br>" +
        //use getChorusFile for a ChorusFileSelectorSetup dialog element
        "Chorus File (" + DialogElementExplorerUtil.WIDGET_CHORUS_FILE + ") - name: " + Try(params.getChorusFile(DialogElementExplorerUtil.WIDGET_CHORUS_FILE).get.name).getOrElse("") + "<br>" +
        //use getTabularDatasetSelectedColumnName for a TabularDatasetColumnDropdownSetup dialog element
        "Tabular Dataset Dropdown (" + DialogElementExplorerUtil.WIDGET_COLUMN_SEL + "): " + params.getTabularDatasetSelectedColumnName(DialogElementExplorerUtil.WIDGET_COLUMN_SEL) + "<br>" +
        "Dropdown Box (" + DialogElementExplorerUtil.WIDGET_DROPDOWN_BOX + "): " + params.getStringValue(DialogElementExplorerUtil.WIDGET_DROPDOWN_BOX) + "<br>" +
        "Radio Buttons (" + DialogElementExplorerUtil.WIDGET_RADIO_BTNS + "): " + params.getStringValue(DialogElementExplorerUtil.WIDGET_RADIO_BTNS) + "<br>" +
        //use getDialogRowsAsArray for a RowDialogSetup dialog element
        "Row Dialog Window (" + DialogElementExplorerUtil.WIDGET_ROW_DIALOG + ") - number of rows defined: " + params.getDialogRowsAsArray(DialogElementExplorerUtil.WIDGET_ROW_DIALOG).length + "<br>" +
        ""
    )
  }

  override def onExecution(context: SparkExecutionContext, input: HdfsTabularDataset, params: OperatorParameters,
      listener: OperatorListener): IONone = {
    new IONoneDefault(Map[String, AnyRef]())
  }

  override def onStop(context: SparkExecutionContext, listener: OperatorListener): Unit = {
  }
}
