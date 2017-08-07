package com.alpine.plugin.samples.advanced

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.db.{DBExecutionContext, DBRuntime}
import com.alpine.plugin.core.dialog.{ChorusFile, OperatorDialog}
import com.alpine.plugin.core.io.defaults.IONoneDefault
import com.alpine.plugin.core.io.{IONone, OperatorSchemaManager}
import com.alpine.plugin.core.utils.ChorusUtils
import com.alpine.plugin.core.visualization.{TextVisualModel, VisualModel}
import com.alpine.plugin.{EmptyIOMetadata, OperatorDesignContext}

/**
  * Demonstrates the functionality of the new chorus file drop down box on database and the
  * ChorusAPICaller object, which can be used to make several different calls to the chorus API
  *
  * The custom operator has two chorus file drop down boxes. It performs different API actions
  * with these two file
  * 1: The first allows the user to select a text file, python notebook.
  * from the workspace.
  * Then the custom operator will download the file at runtime and in the createVisualResults.
  * It will show the text of the workfile.
  *
  * Lastly, the operator creates a new workfile in the workspace called
  * NOTE: This is a work in progress to test the chorus API functionality, it should be simplified
  * and re-written before release to display the functionality in a user friendly way.
  */
class DBChorusFileRunnerSignature extends OperatorSignature[DBChorusFileRunnerGUI,
  DBChorusFileRunnerRuntime] {
  /**
    * This must be implemented by every operator to provide metadata
    * about the operator itself.
    *
    * @return Metadata about this operator. E.g. the version, the author, the
    *         category, etc.
    */
  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    "Sample - DB Chorus File Dropdown",
    "Plugin Sample - Local",
    Some("Alpine Data"),
    1,
    None,
    None,
    Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
      "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
      " of the operator and are no more than fifty words."))
}

class DBChorusFileRunnerGUI extends OperatorGUINode[IONone, IONone] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addChorusFileDropdownBox(
      id = "file",
      label = "Select a File From the Workspace",
      extensionFilter = Set(".ipynb", ".txt"),
      isRequired = true)

    operatorDialog.addStringBox("newFileName", "Name of New Workfile", "exported_file.txt", ".+", required = true)
  }

  /**
    * Currently serves as a test that
    * A) The workfile selector can be retrieved at design time.
    * B) That the chorus api caller can be used to download a workflow at design time.
    * WARNING: Doing this is not advised, as it could make the operator interaction laggy.
    */
  override def getOperatorStatus(context: OperatorDesignContext): OperatorStatus = {
    // This is called at design-time, so the parameter may be empty,
    // as the user may not have selected one yet.
    context.parameters.getChorusFile("file") match {
      case Some(chorusFile) =>
        val text: String = context.chorusAPICaller.readWorkfileAsText(chorusFile.id).get
      case None => // Do nothing.
    }
    OperatorStatus(isValid = true, msg = None, EmptyIOMetadata())
  }

}

class DBChorusFileRunnerRuntime extends DBRuntime[IONone, IONone] {

  override def onExecution(context: DBExecutionContext,
                           input: IONone, params: OperatorParameters,
                           listener: OperatorListener): IONone = {

    // At runtime is is okay to call .get on required Chorus files, since we only run valid operators.
    val chorusFileObject: ChorusFile = params.getChorusFile("file").get
    listener.notifyMessage(
      "The chorus file id is: " + chorusFileObject.id +
      ", the file name is: " + chorusFileObject.name
    )

    //download first file and get content.
    val text = context.chorusAPICaller.readWorkfileAsText(params.getChorusFile("file").get.id).get

    val newFileName = params.getStringValue("newFileName")
    //write new chorus file
    val newChorusFile = ChorusUtils.writeTextChorusFile(
      newFileName,
      "I am a test file written to chorus",
      context, newVersionIfExists = true)

    if (newChorusFile.isSuccess) {
      listener.notifyMessage("The new chorus file: " +
        newChorusFile.get + " was written successfully")
    } else {
      listener.notifyError("Writing new chorus file failed with error: " +
        newChorusFile.failed.get.getMessage)
    }

    //return text of downloaded workflow for visualization
    IONoneDefault(input.addendum.+("key" -> text))
  }

  override def createVisualResults(context: DBExecutionContext,
                                   input: IONone,
                                   output: IONone,
                                   params: OperatorParameters,
                                   listener: OperatorListener): VisualModel = {
    val text = context.chorusAPICaller.readWorkfileAsText(params.getChorusFile("file").get.id).get
    TextVisualModel(output.addendum("key").toString)
  }
}
