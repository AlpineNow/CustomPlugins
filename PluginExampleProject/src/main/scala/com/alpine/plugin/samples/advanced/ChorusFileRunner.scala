package com.alpine.plugin.samples.advanced

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ChorusFile, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.IONoneDefault
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntime}
import com.alpine.plugin.core.utils.{ChorusAPICaller, ChorusUtils}
import com.alpine.plugin.core.visualization.{TextVisualModel, VisualModel}

/**
  * This is a hadoop custom operator that displays the functionality to create a workfile selector
  * dropdown. The custom operator only allows the user to select .txt and .pynb (python notebook)
  * files. It then downloads those files and displays the text of that workfile.
  * If a python notebook was selected it runs the notebook.
  *
  * Finally, the operator creates a new workfile in the workspace
  *
  */
class ChorusFileRunnerSignature extends OperatorSignature[ChorusFileRunnerGUI, ChorusFileRunnerRuntime] {

  override def getMetadata: OperatorMetadata =
    new OperatorMetadata("Sample - Chorus File Dropdown",
      "Plugin Sample - Local",
      Some("Alpine Data"),
      1,
      None,
      None,
      Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
        "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
        " of the operator and are no more than fifty words."))
}

class ChorusFileRunnerGUI extends OperatorGUINode[IONone, IONone] {

  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager
  ): Unit = {
    operatorDialog.addChorusFileDropdownBox(id = "file",
      label = "Select a File From the Workspace",
      extensionFilter = Set[String](".ipynb", ".txt"), isRequired = true)
  }

}

class ChorusFileRunnerRuntime extends SparkRuntime[IONone, IONone] {

  override def onExecution(
    context: SparkExecutionContext, input: IONone,
    params: OperatorParameters,
    listener: OperatorListener
  ): IONone = {

    // At runtime is is okay to call .get on required Chorus files, since we only run valid operators.
    val chorusFileObject: ChorusFile = params.getChorusFile("file").get

    //retrieve a wrapper for the chorus api from the parameters object
    val chorusAPICaller: ChorusAPICaller = context.chorusAPICaller
    //run notebook
    if (chorusFileObject.extension == ".ipynb") {
      val pythonNotebook = chorusAPICaller.runNotebook(chorusFileObject.id)
      if (pythonNotebook.isSuccess) {
        listener.notifyMessage("The python notebook was last updated by the user on " + pythonNotebook.get.userModifiedAt)
      } else {
        listener.notifyError("The python notebook failed to run: " + pythonNotebook.failed.get.getMessage)
      }
    }
    //download file
    val text = chorusAPICaller.readWorkfileAsText(chorusFileObject.id)

    listener.notifyMessage(
      "The chorus file id is: " + chorusFileObject.id +
        ", the file name is: " + chorusFileObject.name
    )

    //create new workfile in this workspace
    val currentWorkfileId = context.workflowInfo.workflowID
    val tempDir = context.recommendedTempDir

    val newChorusFile = ChorusUtils.writeTextChorusFile("Exported_from_Spark_Op",
      "The is a file that was exported from the runtime class of a Spark operator",
      context, newVersionIfExists = true)

    if (newChorusFile.isSuccess) {
      listener.notifyMessage("The new chorus file: " +
        newChorusFile.get + " was written successfully")
    } else {
      listener.notifyError("Writing new chorus file failed with error: " +
        newChorusFile.failed.get.getMessage)
    }

    IONoneDefault(input.addendum.+("key" -> text))
  }

  override def createVisualResults(
    context: SparkExecutionContext,
    input: IONone,
    output: IONone,
    params: OperatorParameters,
    listener: OperatorListener
  ): VisualModel = {
    TextVisualModel(output.addendum("key").toString)
  }

  override def onStop(context: SparkExecutionContext, listener: OperatorListener): Unit = {}
}