import com.alpine.plugin.core.utils.HDFSParameterUtils
import com.alpine.plugin.samples.ver1_0.ColumnFilterJob
import com.alpine.plugin.test.AbstractSparkJobSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ColumnFilterPluginTest extends AbstractSparkJobSuite {

  test("local context test") {
    val operator = new ColumnFilterJob
    val input = sc.parallelize(List(Row("Masha", 22), Row("Ulia", 21), Row("Nastya", 23)))
    val schema =
      StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val dataFrameInput = sContext.createDataFrame(input, schema)
    assert(dataFrameInput.schema.fieldNames.contains("age"))
    val parameters = getOperatorParameters(("columnsToKeep", "name"))

    parameters.setValue(HDFSParameterUtils.outputDirectoryParameterID, cluster.createDirectory())
    parameters.setValue(HDFSParameterUtils.outputNameParameterID, "column_filter_plugin_test")
    parameters.setValue("storageFormat", "TSV")

    val result = runDataFrameThroughOperator(dataFrameInput, operator, parameters = parameters)
    assert(!result.schema.fieldNames.contains("age"))
  }

}
