package com.alpine.plugin.samples.ver1_0

import java.util
import com.alpine.plugin.core.io.{HdfsTabularDataset, OperatorInfo}
import com.alpine.plugin.core.spark.SparkIOTypedPluginJob
import com.alpine.plugin.core.spark.templates.SparkDataFrameJob
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.test.mock.{SimpleOperatorListener, OperatorParametersMock}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait SparkContextTest extends BeforeAndAfterAll {
  self: org.scalatest.Suite  =>
  @transient var sc: SparkContext = _
  @transient var sqlContext : SQLContext = _

  override def beforeAll() {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.driver.host")
    sc = new SparkContext("local", "test")
    sqlContext = new SQLContext(sc )
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.driver.host")
    super.afterAll()
  }
}


class SimpleAbstractSparkJobSuite extends FunSuite with SparkContextTest {

  lazy val sparkUtils = {
    new SparkRuntimeUtils(sc)
  }

  def createHdfsTabularDatasetLocal(dataFrame: DataFrame, opInfo: Option[OperatorInfo], path : String) = {
   sparkUtils.deleteFilePathIfExists(path)
    sparkUtils.saveAsTSV(path , dataFrame, opInfo)
  }

  def runDataFrameThroughDFTemplate(dataFrame: DataFrame, operator : SparkDataFrameJob, params : OperatorParametersMock) = {
    operator.transformWithAddendum(params, dataFrame, sparkUtils, new SimpleOperatorListener)
  }

  def runDataFrameThroughOperator(dataFrame: DataFrame,
    operator: SparkIOTypedPluginJob[HdfsTabularDataset, HdfsTabularDataset],
    appConf: collection.mutable.Map[String, String] = collection.mutable.Map.empty,
    parameters: OperatorParametersMock): DataFrame = {
    val hdfsTabularDataset = createHdfsTabularDatasetLocal(dataFrame, Some(parameters.operatorInfo()), "target/restResults")
    val result = operator.onExecution(sc, appConf, hdfsTabularDataset, parameters, new SimpleOperatorListener)
    sparkUtils.getDataFrame(result)
  }

}

object ParameterMockUtil{

  def addTabularColumns(params : OperatorParametersMock, paramId : String , colNames : String* ) : Unit  = {
    val map : util.HashMap[String ,util.ArrayList[String]]  = new util.HashMap[String ,util.ArrayList[String]]()
    val arrayList = new util.ArrayList[String ]()
    colNames.foreach(colName => arrayList.add(arrayList.size(), colName))
     map.put("group", arrayList)
    params.setValue(paramId, map)
  }

  def addTabularColumn(params : OperatorParametersMock, paramId : String , colName : String ) : Unit  = {
    val map : util.HashMap[String ,String]  = new util.HashMap[String ,String]()
    map.put("group", colName)
    params.setValue(paramId, map)
  }

  def addHdfsParams( operatorParametersMock: OperatorParametersMock,
    outputName : String,
    outputDirectory : String = "target/testResults", storageFormat : String = "TSV",
    overwrite : String = "true" ) : OperatorParametersMock = {
    operatorParametersMock.setValue(HdfsParameterUtils.outputDirectoryParameterID, outputDirectory)
    operatorParametersMock.setValue(HdfsParameterUtils.outputNameParameterID, outputName)
    operatorParametersMock.setValue("storageFormat", "TSV")
    operatorParametersMock.setValue(HdfsParameterUtils.overwriteParameterID, "true")
    operatorParametersMock
  }
}
