/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0

import java.io.File

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsHtmlDatasetDefault
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntime}
import com.alpine.plugin.core.utils.HDFSParameterUtils
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
    HDFSParameterUtils
          .addStandardHDFSOutputParameters(operatorDialog, operatorDataSourceManager)


    operatorDialog.addRadioButtons(
      "limitToSeedPrefixes",
      "Limit crawling to seed prefixes",
      scala.collection.Seq("Yes", "No"),
      "Yes"
    )

    operatorDialog.addStringBox(
      id = "urlSeeds",
      label = "Comma Separated Url Seeds",
      defaultValue = "",
      regex = ".+",
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
    val outputPath = HDFSParameterUtils.getOutputPath(params)
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