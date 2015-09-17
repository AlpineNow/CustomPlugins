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
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

/**
 * The runtime behavior of a database transformation. Will compute the square
 * or cube of some user selected columns.
 * Note: Will only work on GreenPlum and Postgres databases.
 */
public class JavaDBTransformerRuntime extends DBRuntime<DBTable, DBTable> {

    public DBTable onExecution(DBExecutionContext context, DBTable input,
                               OperatorParameters params, OperatorListener listener) {
        String[] cols2transform = params.getTabularDatasetSelectedColumns(
                DBTransformerConstants.COLUMNS_TO_TRANSFORM_PARAM)._2();
        String transformationType = params.getStringValue(
                DBTransformerConstants.TRANSFORMATION_TYPE_PARAM);
        try {
            //get parameter values using the utils class
            String outputSchemaName = DBParameterUtils.getDBOutputSchemaParam(params);
            String tableName = DBParameterUtils.getResultTableName(params);
            Boolean isView = DBParameterUtils.getIsViewParam(params);
            Boolean overwrite = DBParameterUtils.getOverwriteParameterValue(params);
            DBConnectionInfo connectionInfo = context.getDBConnectionInfo();
            String fullOutputName = getQuotedSchemaTableName(outputSchemaName, tableName);

            //a SQL object
            Statement stmtTable = connectionInfo.connection().createStatement();
            if (overwrite) {
                //drop the table if it already exists
                try {
                    listener.notifyMessage("Dropping table if it exists.");
                    stmtTable.execute(("DROP TABLE IF EXISTS " + fullOutputName + " CASCADE;"));
                } catch (Exception e) {
                    listener.notifyMessage("A view of the name " + fullOutputName + "exists");
                } finally {
                    stmtTable.close();
                }

                Statement stmtView = connectionInfo.connection().createStatement();
                stmtView.execute(("DROP VIEW IF EXISTS " + fullOutputName + " CASCADE;"));
                stmtView.close();
            } //end if

            StringBuilder createTableStatement = new StringBuilder();
            //use a string builder to create a the sql statement
            if (isView) {
                createTableStatement.append("CREATE VIEW " + fullOutputName + " AS (");
            } else {
                createTableStatement.append("CREATE TABLE " + fullOutputName + " AS (");
            }
            createTableStatement.append("SELECT ");
            TabularSchema inputSchemaOutline = input.tabularSchema();
            Seq<ColumnDef> inputColSeq = inputSchemaOutline.getDefinedColumns().toSeq();
            List<ColumnDef> inputCols = JavaConversions.asJavaList(inputColSeq);

            for (ColumnDef inputCol : inputCols) {
                String columnName = quoteName(inputCol.columnName());
                createTableStatement.append(columnName + ", ");
            }

            String power;
            if (DBTransformerConstants.TRANSFORMATION_TYPE_POW2.equals(transformationType)) {
                power = "2";
            } else {
                power = "3";
            }

            for (int i = 0; i < cols2transform.length - 1; i++) {
                String columnName = cols2transform[i];
                createTableStatement.append("POWER(" + quoteName(columnName) + ", " + power + ") AS ");
                createTableStatement.append(columnName + "_pow" + power);

                createTableStatement.append(", ");
            }
            //add the last column
            String columnName = cols2transform[cols2transform.length - 1];
            createTableStatement.append("POWER(" + quoteName(columnName) + ", " + power + ") AS ");
            createTableStatement.append(columnName + "_pow" + power);

            createTableStatement.append(" FROM " + getQuotedSchemaTableName(input.schemaName(), input.tableName()) + ");");
            //execute the statement
            Statement stmt = connectionInfo.connection().createStatement();
            listener.notifyMessage("Executing the sql statement \n"
                    + createTableStatement.toString());
            stmt.execute(createTableStatement.toString());
            stmt.close();

            TabularSchema outputTabularSchema = DBTransformerConstants
                    .transformSchema(
                            input.tabularSchema(),
                            cols2transform,
                            transformationType
                    );

            HashMap<String, Object> addendum = new HashMap<String, Object>();
            addendum.put("TestValue1", 1);

            return new DBTableDefault(
                    outputSchemaName, tableName,
                    outputTabularSchema, isView,
                    connectionInfo.name(), connectionInfo.url(),
                    Option.apply(params.operatorInfo()),
                    JavaConversionUtils.toImmutableMap(addendum)
            );
        } catch (Exception sqlException) {
            listener.notifyError("Failed due to exception. " + sqlException.getMessage());
            return null;
        }
    }

    /**
     * Create the output format as "schemaName"."tableName".
     */
    private String getQuotedSchemaTableName(String schemaName, String tableName) {
        return quoteName(schemaName) + "." + quoteName(tableName);
    }

    private String quoteName(String colName) {
        return "\"" + colName + "\"";
    }
}

