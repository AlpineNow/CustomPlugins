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
import com.alpine.sql.SQLExecutor;
import com.alpine.sql.SQLGenerator;

import java.sql.SQLException;
import java.sql.Statement;
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
    ) throws SQLException {
        String[] cols2transform = params.getTabularDatasetSelectedColumns(JavaDBTransformerUtil.COLUMNS_TO_TRANSFORM_PARAM)._2();
        String transformationType = params.getStringValue(JavaDBTransformerUtil.TRANSFORMATION_TYPE_PARAM);
        SQLGenerator sqlGenerator = context.getSQLGenerator();
        //get parameter values using the DBParameterUtils class
        String outputSchemaName = DBParameterUtils.getDBOutputSchemaParam(params);
        String tableName = DBParameterUtils.getResultTableName(params);
        Boolean isView = DBParameterUtils.getIsViewParam(params);
        Boolean dropIfExists = DBParameterUtils.getDropIfExistsParameterValue(params);
        DBConnectionInfo connectionInfo = context.getDBConnectionInfo();

        String fullOutputName = sqlGenerator.quoteObjectName(outputSchemaName, tableName);

        if (dropIfExists) {
            final SQLExecutor sqlExecutor = context.getSQLExecutor();
            sqlExecutor.ddlDropTableOrViewIfExists(fullOutputName);
        }

        //Create an sql statement object and build the statement according to the values
        //of the parameters and the input data.

        //use a string builder to create SQL of to select from the table.
        StringBuilder columnSQL = new StringBuilder();

        TabularSchema inputSchemaOutline = input.tabularSchema();
        List<ColumnDef> inputCols = inputSchemaOutline.getDefinedColumnsAsJavaList();

        //select all of the input Columns (append them to the select statement)
        for (ColumnDef inputCol : inputCols) {
            columnSQL.append(sqlGenerator.quoteIdentifier(inputCol.columnName())).append(", ");
        }

        String power;
        if (JavaDBTransformerUtil.TRANSFORMATION_TYPE_POW2.equals(transformationType)) {
            power = "2";
        } else {
            power = "3";
        }
        //Perform the transformation according to the value set for the transformation type
        //parameter and the columns to transform parameter
        boolean started = false;
        for (String columnName : cols2transform) {
            if (started) {
                columnSQL.append(", ");
            } else {
                started = true;
            }
            columnSQL
                    .append("POWER(").append(sqlGenerator.quoteIdentifier(columnName)).append(", ").append(power)
                    .append(") AS ")
                    .append(sqlGenerator.quoteIdentifier(JavaDBTransformerUtil.getOutputColumnName(columnName, transformationType)));
        }

        //create a new table/view according to the database output parameters.
        StringBuilder createTableStatement = new StringBuilder();
        createTableStatement.append(
                sqlGenerator.getCreateTableOrViewAsSelectSQL(
                        columnSQL.toString(),
                        sqlGenerator.quoteObjectName(input.schemaName(), input.tableName()),
                        fullOutputName,
                        isView
                )
        );

        //execute the statement
        // Use try-with-resources to be sure the statement will be closed.
        try (Statement stmt = connectionInfo.connection().createStatement()) {
            listener.notifyMessage("Executing the sql statement \n" + createTableStatement.toString());
            stmt.execute(createTableStatement.toString());
        }

        //create the Tabular schema, which is required for the DBTable object.
        //Use the "transformSchema" method defined in the DBTransformerConstants class so that
        //the runtime schema will be consistent with the one defined in the GUI node.
        TabularSchema outputTabularSchema = JavaDBTransformerUtil
                .transformSchema(
                        input.tabularSchema(),
                        cols2transform,
                        transformationType
                );

        //create the DBTable object
        return DBTableDefault.apply(
                outputSchemaName,
                tableName,
                outputTabularSchema
        );
    }
}
