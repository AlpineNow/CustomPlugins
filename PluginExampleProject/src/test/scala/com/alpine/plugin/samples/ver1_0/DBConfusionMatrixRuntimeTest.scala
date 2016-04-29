package com.alpine.plugin.samples.ver1_0

import com.alpine.model.pack.ml.{MultiLogisticRegressionModel, SingleLogisticRegression}
import com.alpine.plugin.core.io.defaults.{DBTableDefault, Tuple2Default}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, TabularSchema}
import com.alpine.plugin.model.ClassificationModelWrapper
import com.alpine.sql.DatabaseType.TypeValue
import com.alpine.sql.SQLGenerator
import org.scalatest.FunSpec

class DBConfusionMatrixRuntimeTest extends FunSpec {

  describe("testGetCreateTableSQL") {

    val lor = new MultiLogisticRegressionModel(Seq(
      SingleLogisticRegression(
        "yes",
        Seq(2.0, 3.0).map(java.lang.Double.valueOf), 4.0
      )),
      "no",
      "play",
      Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long))
    )

    val wrapper: ClassificationModelWrapper = new ClassificationModelWrapper("dummy name", lor, None)

    val mockSQLGenerator = new SQLGenerator {
      override def useAliasForSelectSubQueries: Boolean = true

      override def getStandardDeviationFunctionName: String = ???

      override def quoteObjectName(schemaName: String, objectName: String): String = ???

      override def getCreateTableAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, whereClause: String): String = ???

      override def quoteChar: String = ???

      override def getVarianceFunctionName: String = ???

      @scala.deprecated("Please use quoteIdentifier instead [Paul]")
      override def escapeColumnName(s: String): String = quoteIdentifier(s)

      override def dbType: TypeValue = ???

      override def getModuloExpression(dividend: String, divisor: String): String = ???

      override def quoteIdentifier(s: String): String = s""""$s""""
    }

    it("Should work with good input") {
      val dbTable = new DBTableDefault(
        schemaName = "demo",
        tableName = "golfnew",
        tabularSchema = TabularSchema(
          Seq(
            ColumnDef("temperature", ColumnType.Long),
            ColumnDef("humidity", ColumnType.Long),
            ColumnDef("play", ColumnType.String)
          )
        ),
        isView = true,
        dbName = "dummy DB Name",
        dbURL = "dummy URL",
        sourceOperatorInfo = None
      )

      val input = new Tuple2Default(
        "dummy display name",
        wrapper,
        dbTable,
        None
      )

      val actualSQL = (new DBConfusionMatrixRuntime).getCreateTableSQL(
        mockSQLGenerator,
        input, isView = false,
        fullOutputName = "\"output_schema\".\"output_table\""
      )

      // println(actualSQL)

      val expectedSQL =
        """CREATE TABLE "output_schema"."output_table" AS (SELECT
          | "play" AS "Observed",
          | "PRED" AS "Predicted",
          | COUNT(*) AS "N"
          | FROM (SELECT "column_0" AS "play", (CASE WHEN ("baseVal" > "ce0") THEN 'no' ELSE 'yes' END) AS "PRED" FROM (SELECT "column_0" AS "column_0", 1 / "sum" AS "baseVal", "e0" / "sum" AS "ce0" FROM (SELECT "column_0" AS "column_0", 1 + "e0" AS "sum", "e0" AS "e0" FROM (SELECT "play" AS "column_0", EXP(4.0 + "temperature" * 2.0 + "humidity" * 3.0) AS "e0" FROM "demo"."golfnew") AS alias_0) AS alias_1) AS alias_2) AS alias_3
          | GROUP BY "play", "PRED");""".stripMargin

      assert(expectedSQL === actualSQL)
    }

    it("Should fail with bad input") {
      val dbTable = new DBTableDefault(
        schemaName = "demo",
        tableName = "golfnew",
        tabularSchema = TabularSchema(
          Seq(
            ColumnDef("temperature", ColumnType.Long),
            ColumnDef("humidity", ColumnType.Long)
          )
        ),
        isView = true,
        dbName = "dummy DB Name",
        dbURL = "dummy URL",
        sourceOperatorInfo = None
      )

      val input = new Tuple2Default(
        "dummy display name",
        wrapper,
        dbTable,
        None
      )

      /**
        * Should fail because the column "play" is not in the input dataset.
        */
      intercept[RuntimeException] {
        (new DBConfusionMatrixRuntime).getCreateTableSQL(
          mockSQLGenerator, input, isView = false,
          "\"output_schema\".\"output_table\""
        )
      }
    }

  }

}