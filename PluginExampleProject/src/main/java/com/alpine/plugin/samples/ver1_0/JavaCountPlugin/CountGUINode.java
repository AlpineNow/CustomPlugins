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
package com.alpine.plugin.samples.ver1_0.JavaCountPlugin;

import com.alpine.plugin.core.OperatorParameters;
import com.alpine.plugin.core.datasource.OperatorDataSourceManager;
import com.alpine.plugin.core.dialog.ColumnFilter;
import com.alpine.plugin.core.dialog.OperatorDialog;
import com.alpine.plugin.core.io.ColumnDef;
import com.alpine.plugin.core.io.ColumnType;
import com.alpine.plugin.core.io.OperatorSchemaManager;
import com.alpine.plugin.core.io.TabularSchema;
import com.alpine.plugin.core.spark.templates.SparkDataFrameGUINode;
import com.alpine.plugin.util.JavaConversionUtils;
import scala.collection.Seq;

import java.util.Arrays;

/**
 * Design time behavior for the Count Plugin. Defines the parameters and
 * output schema.
 */
class CountGUINode extends SparkDataFrameGUINode<CountPluginSparkJob> {

    public void onPlacement(OperatorDialog operatorDialog,
                            OperatorDataSourceManager operatorDataSourceManager,
                            OperatorSchemaManager operatorSchemaManager) {
        try {
            operatorDialog.addTabularDatasetColumnDropdownBox(CountPluginSparkJob.GroupByParamKey,
                    "Column to Group By",
                    ColumnFilter.CategoricalOnly(),
                    "main",
                    true
            );
        } catch (Exception e) {
            System.out.println("Error could not set up parameters for Count operator");
        }
        //call the super class onPlacement method which adds the standard output parameters
        super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager);
    }

    public Seq<ColumnDef> defineOutputSchemaColumns(TabularSchema inputSchema,
                                                    OperatorParameters params) {
        String groupByVar = params.getTabularDatasetSelectedColumn(
                CountPluginSparkJob.GroupByParamKey)._2();
        return JavaConversionUtils.toSeq(Arrays.asList(new ColumnDef(groupByVar, ColumnType.String()),
                new ColumnDef("GroupCounts", ColumnType.Long())));
    }
}
