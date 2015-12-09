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


import com.alpine.JavaPluginUtils.ScalaConversionUtils;
import com.alpine.plugin.core.OperatorGUINode;
import com.alpine.plugin.core.OperatorParameters;
import com.alpine.plugin.core.OperatorStatus;
import com.alpine.plugin.core.datasource.OperatorDataSourceManager;
import com.alpine.plugin.core.dialog.ColumnFilter;
import com.alpine.plugin.core.dialog.OperatorDialog;
import com.alpine.plugin.core.io.*;
import com.alpine.plugin.core.utils.DBParameterUtils;
import scala.Option;
import scala.collection.immutable.Map;

public class JavaDBNumericTransformerGUINode extends OperatorGUINode<DBTable, DBTable> {

    public static final String SELECTION_GROUP_ID_MAIN = "main";

    public void onPlacement(OperatorDialog operatorDialog,
                            OperatorDataSourceManager operatorDataSourceManager,
                            OperatorSchemaManager operatorSchemaManager) {
        //Parameter addition may throw exceptions, however this method cannot
        //throw an exception so the user must handle those exceptions.
        try {
            //add the parameters
            operatorDialog.addTabularDatasetColumnCheckboxes(
                    DBTransformerConstants.COLUMNS_TO_TRANSFORM_PARAM,
                    "Columns to Transform",
                    ColumnFilter.All(),
                    SELECTION_GROUP_ID_MAIN,
                    true
            );
            //the operator dialog methods require scala collection types.
            //use the ScalaConversionUtils class to generate those in Java.
            scala.collection.Seq<String> transformationTypes = ScalaConversionUtils.scalaSeq(
                    DBTransformerConstants.TRANSFORMATION_TYPE_POW2,
                    DBTransformerConstants.TRANSFORMATION_TYPE_POW3);
            operatorDialog.addDropdownBox(
                    DBTransformerConstants.TRANSFORMATION_TYPE_PARAM,
                    "Transformation Type",
                    transformationTypes,
                    DBTransformerConstants.TRANSFORMATION_TYPE_POW2
            );
            DBParameterUtils.addStandardDatabaseOutputParameters(operatorDialog,
                    operatorDataSourceManager, "");
        } catch (Exception e) {
            //print a message if the dialog element couldn't be added
            System.out.println("Dialog element failed due to exception: " +
                    e.getMessage());
        }
    }

    /**
     * Updates the schema manager with the new output schema
     * whenever the parameters or inputs are changed.
     * Called by the onInputOrParameterChange method
     */
    private void updateOutputSchema(Map<String, TabularSchema> inputSchemas,
                                    OperatorParameters params,
                                    OperatorSchemaManager operatorSchemaManager) {
        //only update the output schema if something has been connected to this operator
        //(if the list of input schemas is nonEmpty)
        if (!inputSchemas.isEmpty()) {
            TabularSchema inputSchema = inputSchemas.values().iterator().next();
            //add columns to the output schema only if the input Schemas are not empty

            if (inputSchema.getDefinedColumns().nonEmpty()) {
                String[] colsToTransform = params.getTabularDatasetSelectedColumns(
                        DBTransformerConstants.COLUMNS_TO_TRANSFORM_PARAM)._2();
                TabularSchema outputSchema = DBTransformerConstants.transformSchema(
                        inputSchema,
                        colsToTransform,
                        params.getStringValue(DBTransformerConstants.TRANSFORMATION_TYPE_PARAM)
                );
                operatorSchemaManager.setOutputSchema(outputSchema);
            }
        }
    }

    public OperatorStatus onInputOrParameterChange(Map<String, TabularSchema> inputSchemas,
                                                   OperatorParameters params,
                                                   OperatorSchemaManager operatorSchemaManager) {
        this.updateOutputSchema(inputSchemas, params, operatorSchemaManager);
        scala.Option<String> msg = Option.empty();
        return new OperatorStatus(true, msg);
    }
}
