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
package com.alpine.plugin.samples.ver1_0.JavaDBTransformer;

import com.alpine.plugin.core.OperatorListener;
import com.alpine.plugin.core.OperatorParameters;
import com.alpine.plugin.core.db.DBConnectionInfo;
import com.alpine.plugin.core.db.DBExecutionContext;
import com.alpine.plugin.core.db.DBRuntime;
import com.alpine.plugin.core.io.ColumnDef;
import com.alpine.plugin.core.io.DBTable;
import com.alpine.plugin.core.io.TabularSchema;
import com.alpine.plugin.core.io.defaults.DBTableDefault;
import com.alpine.plugin.core.utils.DBParameterUtils;
import com.alpine.plugin.util.JavaConversionUtils;
import com.alpine.sql.SQLGenerator;

import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

/**
 * The runtime behavior of a database transformation. Will compute the square or cube of some user selected columns. Note: Will only work on
 * GreenPlum and Postgres databases.
 */
@SuppressWarnings("WeakerAccess")
public class JavaDBTransformerRuntime extends DBRuntime<DBTable, DBTable> {

    public DBTable onExecution(
        DBExecutionContext context,
        DBTable input,
        OperatorParameters params,
        OperatorListener listener
    ) {
        String[] cols2transform = params.getTabularDatasetSelectedColumns(DBTransformerConstants.COLUMNS_TO_TRANSFORM_PARAM)._2();
        String transformationType = params.getStringValue(DBTransformerConstants.TRANSFORMATION_TYPE_PARAM);
        SQLGenerator generator = context.getSQLGenerator();
        try {
            //get parameter values using the DBParameterUtils class
            String outputSchemaName = DBParameterUtils.getDBOutputSchemaParam(params);
            String tableName = DBParameterUtils.getResultTableName(params);
            Boolean isView = DBParameterUtils.getIsViewParam(params);
            Boolean dropIfExists = DBParameterUtils.getDropIfExistsParameterValue(params);
            DBConnectionInfo connectionInfo = context.getDBConnectionInfo();

            String fullOutputName = generator.quoteObjectName(outputSchemaName, tableName);
            //Create an sql statement object and build the statement according to the values
            //of the parameters and the input data.

            if (dropIfExists) {
                //drop the table if it already exists
                try (Statement stmtTable = connectionInfo.connection().createStatement()) {
                    listener.notifyMessage("Dropping table if it exists.");
                    stmtTable.execute(("DROP TABLE IF EXISTS " + fullOutputName + " CASCADE;"));
                } catch (Exception e) {
                    listener.notifyMessage("A view of the name " + fullOutputName + "exists");
                }

                //drop the view if it already exists
                try (Statement stmtView = connectionInfo.connection().createStatement()) {
                    listener.notifyMessage("Dropping view if it exists.");
                    stmtView.execute(("DROP VIEW IF EXISTS " + fullOutputName + " CASCADE;"));
                } catch (Exception e) {
                    listener.notifyMessage("A view of the name " + fullOutputName + "exists");
                }
            } //end if

            //use a string builder to create a the sql statement
            StringBuilder columnSQL = new StringBuilder();

            TabularSchema inputSchemaOutline = input.tabularSchema();
            Seq<ColumnDef> inputColSeq = inputSchemaOutline.getDefinedColumns().toSeq();
            List<ColumnDef> inputCols = JavaConversions.asJavaList(inputColSeq);

            //select all of the input Columns (append them to the select statement)
            for (ColumnDef inputCol : inputCols) {
                columnSQL.append(generator.quoteIdentifier(inputCol.columnName())).append(", ");
            }

            String power;
            if (DBTransformerConstants.TRANSFORMATION_TYPE_POW2.equals(transformationType)) {
                power = "2";
            } else {
                power = "3";
            }
            //Perform the transformation according to the value set for the transformation type
            //parameter and the columns to transform parameter
            for (int i = 0; i < cols2transform.length - 1; i++) {
                String columnName = cols2transform[i];
                columnSQL
                    .append("POWER(").append(generator.quoteIdentifier(columnName)).append(", ").append(power)
                    .append(") AS ")
                    .append(columnName + "_pow" + power)
                    .append(", ");
            }
            //add the last column
            String columnName = cols2transform[cols2transform.length - 1];
            columnSQL
                .append("POWER(").append(generator.quoteIdentifier(columnName)).append(", ").append(power)
                .append(") AS ")
                .append(columnName + "_pow" + power);

            //create a new table/view according to the database output parameters.
            StringBuilder createTableStatement = new StringBuilder();
            if (isView) {
                createTableStatement
                    .append("CREATE VIEW ").append(fullOutputName).append(" AS (")
                    .append("SELECT ")
                    .append(columnSQL)
                    .append(" FROM ")
                    .append(generator.quoteObjectName(input.schemaName(), input.tableName()))
                    .append(");");
            } else {
                createTableStatement.append(
                    generator.getCreateTableAsSelectSQL(
                        columnSQL.toString(),
                        generator.quoteObjectName(input.schemaName(), input.tableName()),
                        fullOutputName,
                        null // null safe
                    )
                );
            }

            //execute the statement
            Statement stmt = connectionInfo.connection().createStatement();
            listener.notifyMessage("Executing the sql statement \n" + createTableStatement.toString());
            stmt.execute(createTableStatement.toString());
            stmt.close();

            //create the Tabular schema, which is required for the DBTable object.
            //Use the "transformSchema" method defined in the DBTransformerConstants class so that
            //the runtime schema will be consistent with the one defined in the GUI node.
            TabularSchema outputTabularSchema = DBTransformerConstants
                .transformSchema(
                    input.tabularSchema(),
                    cols2transform,
                    transformationType
                );

            //create an empty addendum object
            HashMap<String, Object> addendum = new HashMap<>();

            //create the DBTable object
            return new DBTableDefault(
                outputSchemaName,
                tableName,
                outputTabularSchema,
                isView,
                connectionInfo.name(),
                connectionInfo.url(),
                Option.apply(params.operatorInfo()),
                JavaConversionUtils.toImmutableMap(addendum)
            );
        } catch (Exception sqlException) {
            //report Exception to the progress bar, but handle it since this method is not
            //annotated to throw an exception.
            listener.notifyError("Failed due to exception. " + sqlException.getMessage());
            return null;
        }
    }
}
