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

package com.alpine.textmining

import java.io.FileNotFoundException

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io.defaults.AbstractHdfsFile
import com.alpine.plugin.core.io.{IONone, OperatorSchemaManager}
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntime}
import com.alpine.plugin.core.visualization.{TextVisualModel, VisualModel, VisualModelFactory}
import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}
import opennlp.tools.postag.{POSModel, POSTaggerME}
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}

object OpenNLPModelType {
  val Tokenizer = "Tokenizer"
  val SentenceDetector = "Sentence Detector"
  val POSTagger = "POS Tagger"
  val NameFinder = "Name Finder"
}

/**
 * This plugin demonstrates how developers can select and validate
 * externally defined file types that could be used subsequently by other
 * plugins. This plugin checks whether the given HDFS file is a valid Open
 * NLP model file and if it is, passes it along to the next operator for
 * further uses.
 */
class HdfsOpenNLPModelFileSignature extends OperatorSignature[
  HdfsOpenNLPModelFileGUINode,
  HdfsOpenNLPModelFileRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Text Mining - Open NLP Model File",
      category = "Text Mining",
      author = Some("Sung Chung"),
      version = 1,
      helpURL = None,
      icon = None,
      toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
        "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
        " of the operator and are no more than fifty words.")
    )
  }
}

case class HdfsOpenNLPModelFile(
  override val path: String,
  modelType: String
) extends AbstractHdfsFile(path, Map.empty()) {
  def getModelType: String = modelType
}

class HdfsOpenNLPModelFileGUINode extends OperatorGUINode[
  IONone,
  HdfsOpenNLPModelFile] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addHdfsFileSelector(
      id = "sourcePath",
      label = "Output Path",
      defaultPath = ""
    )

    operatorDialog.addDropdownBox(
      id = "openNlpModelType",
      label = "Open NLP Model Type",
      values = Seq(
        OpenNLPModelType.Tokenizer,
        OpenNLPModelType.SentenceDetector,
        OpenNLPModelType.POSTagger,
        OpenNLPModelType.NameFinder
      ),
      defaultSelection = OpenNLPModelType.Tokenizer
    )
  }

  override def onOutputVisualization(
    params: OperatorParameters,
    output: HdfsOpenNLPModelFile,
    visualFactory: VisualModelFactory): VisualModel = {
    TextVisualModel(
      "Successfully verified an OpenNLP model file " +
        output.path + " that is of the type " + output.getModelType
    )
  }
}

class HdfsOpenNLPModelFileRuntime extends SparkRuntime[
  IONone,
  HdfsOpenNLPModelFile] {
  override def onExecution(
    context: SparkExecutionContext,
    input: IONone,
    params: OperatorParameters,
    listener: OperatorListener): HdfsOpenNLPModelFile = {
    val sourcePath = params.getStringValue("sourcePath")
    val expectedModelType = params.getStringValue("openNlpModelType")
    // Make sure that the model type is as we expected.
    val modelFileStream = context.openPath(sourcePath)
    if (modelFileStream == null) {
      throw new FileNotFoundException(sourcePath + " doesn't exist.")
    }

    try {
      expectedModelType match {
        case OpenNLPModelType.Tokenizer => {
          try {
            val model = new TokenizerModel(modelFileStream)
            new TokenizerME(model)
          } catch {
            case t: Throwable =>
              throw new IllegalArgumentException(
                sourcePath + " is not a valid Tokenizer model file : " +
                 t.toString
              )
          }
        }

        case OpenNLPModelType.SentenceDetector => {
          try {
            val model = new SentenceModel(modelFileStream)
            new SentenceDetectorME(model)
          } catch {
            case t: Throwable =>
              throw new IllegalArgumentException(
                sourcePath + " is not a valid sentence model file : " +
                  t.toString
              )
          }
        }

        case OpenNLPModelType.POSTagger => {
          try {
            val model = new POSModel(modelFileStream)
            new POSTaggerME(model)
          } catch {
            case t: Throwable =>
              throw new IllegalArgumentException(
                sourcePath + " is not a valid POS tagger model file : " +
                  t.toString
              )
          }
        }

        case OpenNLPModelType.NameFinder => {
          try {
            val model = new TokenNameFinderModel(modelFileStream)
            new NameFinderME(model)
          } catch {
            case t: Throwable =>
              throw new IllegalArgumentException(
                sourcePath + " is not a valid name finder model file : " +
                  t.toString
              )
          }
        }
      }
    } finally {
      modelFileStream.close()
    }

    HdfsOpenNLPModelFile(sourcePath, expectedModelType)
  }

  override def onStop(
    context: SparkExecutionContext,
    listener: OperatorListener): Unit = {}
}
