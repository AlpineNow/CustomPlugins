package com.alpine.plugin.samples.ver1_0

import com.alpine.model.pack.ml.{MultiLogisticRegressionModel, SingleLogisticRegression}
import com.alpine.plugin.core.io.defaults.{DBTableDefault, Tuple2Default}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, TabularSchema}
import com.alpine.plugin.model.ClassificationModelWrapper
import com.alpine.plugin.test.mock.SimpleSQLGenerator
import org.apache.commons.lang3.StringUtils
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DBConfusionMatrixRuntimeTest extends FunSpec {

  describe("testGetCreateTableSQL") {

    val lor = MultiLogisticRegressionModel(Seq(
      SingleLogisticRegression(
        "yes",
        Seq(2.0, 3.0).map(java.lang.Double.valueOf), 4.0
      )),
      "no",
      "play",
      Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long))
    )

    val wrapper: ClassificationModelWrapper = new ClassificationModelWrapper(lor)

    val mockSQLGenerator = new SimpleSQLGenerator()

    it("Should work with good input") {
      val dbTable = DBTableDefault(
        schemaName = "demo",
        tableName = "golfnew",
        tabularSchema = TabularSchema(
          Seq(
            ColumnDef("temperature", ColumnType.Long),
            ColumnDef("humidity", ColumnType.Long),
            ColumnDef("play", ColumnType.String)
          )
        )
      )

      val input = Tuple2Default(wrapper, dbTable)

      val actualSQL = (new DBConfusionMatrixRuntime).getCreateTableSQL(
        mockSQLGenerator,
        input, isView = false,
        fullOutputName = "\"output_schema\".\"output_table\""
      )


      val expectedSQL =
        """CREATE TABLE "output_schema"."output_table" AS (SELECT
          | "play" AS "Observed",
          | "PRED" AS "Predicted",
          | COUNT(*) AS "N"
          | FROM (SELECT "column_0" AS "play", CASE WHEN "baseVal" IS NULL OR "ce0" IS NULL THEN NULL ELSE (CASE WHEN ("baseVal" > "ce0") THEN 'no' ELSE 'yes' END) END AS "PRED" FROM (SELECT "column_0" AS "column_0", 1 / "sum" AS "baseVal", "e0" / "sum" AS "ce0" FROM (SELECT "column_0" AS "column_0", 1 + "e0" AS "sum", "e0" AS "e0" FROM (SELECT "play" AS "column_0", EXP(4.0 + "temperature" * 2.0 + "humidity" * 3.0) AS "e0" FROM "demo"."golfnew") AS alias_0) AS alias_1) AS alias_2) AS alias_3
          | GROUP BY "play", "PRED")""".stripMargin

//      println(actualSQL)
//      println(expectedSQL)

      assert(StringUtils.normalizeSpace(expectedSQL) === StringUtils.normalizeSpace(actualSQL))
    }

    it("Should fail with bad input") {
      val dbTable = DBTableDefault(
        schemaName = "demo",
        tableName = "golfnew",
        tabularSchema = TabularSchema(
          Seq(
            ColumnDef("temperature", ColumnType.Long),
            ColumnDef("humidity", ColumnType.Long)
          )
        )
      )

      val input = Tuple2Default(wrapper, dbTable)

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