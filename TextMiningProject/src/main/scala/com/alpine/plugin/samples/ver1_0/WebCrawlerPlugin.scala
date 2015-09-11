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

import java.io.File

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsHtmlDatasetDefault
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntime}
import com.alpine.plugin.core.utils.HdfsParameterUtils
import edu.uci.ics.crawler4j.crawler._
import edu.uci.ics.crawler4j.fetcher.PageFetcher
import edu.uci.ics.crawler4j.robotstxt.{RobotstxtConfig, RobotstxtServer}
import edu.uci.ics.crawler4j.url._

class WebCrawlerSignature extends OperatorSignature[
  WebCrawlerGUINode,
  WebCrawlerRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "WebCrawler",
      category = "DataCollection",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

object PluginCrawler {
  var storePath: String = _
  var sparkExecutionContext: SparkExecutionContext = _
  var limitToSeedPrefixes: Boolean = true
  var seeds: Array[WebURL] = _
  var operatorListener: OperatorListener = _
  def setStorePath(sp: String): Unit = {
    storePath = sp
  }

  def setSparkExecutionContext(sparkExeContext: SparkExecutionContext): Unit = {
    sparkExecutionContext = sparkExeContext
  }

  def writeFile(urlContent: Array[Byte], relativePath: String): Unit = {
    val path = new File(storePath, relativePath)
    if (!sparkExecutionContext.exists(path.getParent)) {
      sparkExecutionContext.mkdir(path.getParent)
    }

    val stream = sparkExecutionContext.createPath(
      path.toString,
      overwrite = true)
    stream.write(urlContent)
    stream.close()
  }
}

class PluginCrawler extends WebCrawler {
  override def shouldVisit(referringPage: Page, url: WebURL): Boolean = {
    val validPrefix =
      if (PluginCrawler.limitToSeedPrefixes) {
        var matchesSeed = false
        PluginCrawler.seeds.foreach(
          seed => {
            if (url.toString.indexOf(seed.toString) >= 0) {
              matchesSeed = true
            }
          }
        )
        matchesSeed
      } else {
        true
      }
    validPrefix
  }

  override def visit(page: Page): Unit = {
    val urlDomain = page.getWebURL.getDomain
    val urlPath = page.getWebURL.getPath
    val storePath =
      if (urlPath != null && !urlPath.isEmpty) {
        urlDomain + "/" + urlPath.replaceAll("/", "-").replaceAll("_", "-")
      } else {
        urlDomain + "/-"
      }
    if (page.getContentType.toLowerCase.contains("html")) {
      PluginCrawler.operatorListener.notifyMessage(
        "Storing " + page.getWebURL.toString +
        " to " + new File(PluginCrawler.storePath, storePath).toPath.toString
      )
      PluginCrawler.writeFile(page.getContentData, storePath)
    }
  }
}

class WebCrawlerGUINode extends OperatorGUINode[
  IONone,
  HdfsHtmlDataset] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    // Add a data source selection box.
    operatorDialog.addDataSourceDropdownBox(
      id = "dataSourceSelector",
      label = "Data Source",
      dataSourceManager = operatorDataSourceManager
    )

    // Add where in the Hdfs this will get stored.
  HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

    operatorDialog.addRadioButtons(
      "limitToSeedPrefixes",
      "Limit crawling to seed prefixes",
      scala.collection.Seq("Yes", "No"),
      "Yes"
    )

    // Copied from : https://gist.github.com/dperini/729294
    val regexForValidUrl =
      "(?:(?:https?|ftp):\\/\\/)(?:\\S+(?::\\S*)?@)?(?:(?!(?:10|127)(?:\\.\\d{1,3}){3})(?!(?:169\\.254|192\\.168)(?:\\.\\d{1,3}){2})(?!172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}(?:\\.(?:[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))|(?:(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)(?:\\.(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)*(?:\\.(?:[a-z\\u00a1-\\uffff]{2,}))\\.?)(?::\\d{2,5})?(?:[/?#]\\S*)?"

    operatorDialog.addStringBox(
      id = "urlSeeds",
      label = "Comma Separated Url Seeds",
      defaultValue = "",
      regex = "^(" + regexForValidUrl + ")" + "(," + regexForValidUrl + ")*$",
      width = 100,
      height = 100
    )

    operatorDialog.addIntegerBox(
      id = "numCrawlers",
      label = "Number of concurrent crawlers",
      min = 0,
      max = 256,
      defaultValue = 7
    )

    operatorDialog.addIntegerBox(
      id = "maxDepth",
      label = "Maximum crawling depth",
      min = 0,
      max = Integer.MAX_VALUE,
      defaultValue = 5
    )

    operatorDialog.addStringBox(
      id = "tmpFolder",
      label = "Temporary Local Crawler Folder",
      defaultValue = "/tmp/",
      regex = ".+",
      width = 0,
      height = 0
    )
  }
}

class WebCrawlerRuntime extends SparkRuntime[
  IONone,
  HdfsHtmlDataset] {
  var crawlController: CrawlController = null
  override def onExecution(
    context: SparkExecutionContext,
    input: IONone,
    params: OperatorParameters,
    listener: OperatorListener): HdfsHtmlDataset = {
    val outputPath = HdfsParameterUtils.getOutputPath(params)
    val limitToSeedPrefixes = params.getStringValue("limitToSeedPrefixes")
    PluginCrawler.setStorePath(outputPath)
    PluginCrawler.setSparkExecutionContext(context)
    if (limitToSeedPrefixes.equals("Yes")) {
      PluginCrawler.limitToSeedPrefixes = true
    } else {
      PluginCrawler.limitToSeedPrefixes = false
    }
    val urlSeeds = params.getStringValue("urlSeeds").split(",").map(
      url => URLCanonicalizer.getCanonicalURL(url)
    )
    PluginCrawler.seeds = urlSeeds.map(
      url => {
        val webUrl = new WebURL
        webUrl.setURL(url)
        webUrl
      }
    )
    val tmpFolder = params.getStringValue("tmpFolder")
    val numCrawlers = params.getIntValue("numCrawlers")
    val maxDepth = params.getIntValue("maxDepth")
    val crawlerConfig = new CrawlConfig
    crawlerConfig.setCrawlStorageFolder(tmpFolder)
    crawlerConfig.setMaxDepthOfCrawling(maxDepth)
    val pageFetcher = new PageFetcher(crawlerConfig)
    val robotstxtConfig = new RobotstxtConfig
    val robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher)
    crawlController = new CrawlController(
      crawlerConfig,
      pageFetcher,
      robotstxtServer
    )

    urlSeeds.foreach(seed => crawlController.addSeed(seed))

    if (!context.exists(outputPath)) {
      context.mkdir(outputPath)
    }

    listener.notifyMessage("Crawler is starting with the following options :")
    listener.notifyMessage("  HDFS output path : " + outputPath)
    listener.notifyMessage("  Limit to seed prefixes : " + limitToSeedPrefixes)
    listener.notifyMessage("  URL seeds : " + params.getStringValue("urlSeeds"))
    listener.notifyMessage("  Temporary Local Crawling Folder : " + params.getStringValue("tmpFolder"))
    listener.notifyMessage("  Number of Crawlers : " + numCrawlers.toString)
    listener.notifyMessage("  Maximum crawling depth : " + maxDepth.toString)

    PluginCrawler.operatorListener = listener

    crawlController.start(classOf[PluginCrawler], numCrawlers)
    listener.notifyMessage("Finished running the crawler !")
    new HdfsHtmlDatasetDefault(outputPath)
  }

  override def onStop(
    context: SparkExecutionContext,
    listener: OperatorListener): Unit = {
    if (crawlController != null) {
      crawlController.shutdown()
      listener.notifyMessage("Crawler successfully stopped!")
    }
  }
}