import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.samples.ver1_0.ColumnFilterJob
import com.alpine.plugin.test.AbstractSparkJobSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ColumnFilterPluginTest extends AbstractSparkJobSuite {
   val russianGirls = List(Row("Masha", 22), Row("Ulia", 21), Row("Nastya", 23))
  val russianGirlsShema =
    StructType(List(StructField("name", StringType), StructField("age", IntegerType)))

  test("local context test") {
    val operator = new ColumnFilterJob
    val input = sc.parallelize(russianGirls)

    //create a dataFrame using that test data
    val dataFrameInput = sContext.createDataFrame(input, russianGirlsShema)
    assert(dataFrameInput.schema.fieldNames.contains("age"))
    val parameters = getOperatorParameters(("columnsToKeep", "name"))

    parameters.setValue(HdfsParameterUtils.outputDirectoryParameterID, cluster.createDirectory())
    parameters.setValue(HdfsParameterUtils.outputNameParameterID, "column_filter_plugin_test")
    parameters.setValue("storageFormat", "TSV")

    val result = runDataFrameThroughOperator(dataFrameInput, operator, parameters = parameters)
    assert(!result.schema.fieldNames.contains("age"))
  }

}
