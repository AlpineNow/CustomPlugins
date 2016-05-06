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

import com.alpine.plugin.core.io.ColumnDef;
import com.alpine.plugin.core.io.ColumnType;
import com.alpine.plugin.core.io.TabularSchema;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

class DBTransformerConstants {

    static final String COLUMNS_TO_TRANSFORM_PARAM = "cols_to_transform";
    static final String TRANSFORMATION_TYPE_PARAM = "transformation_type";

    static final String TRANSFORMATION_TYPE_POW2 = "Pow2";
    static final String TRANSFORMATION_TYPE_POW3 = "Pow3";

    static TabularSchema transformSchema(TabularSchema inputSchema,
                                         String[] columnsToTransform,
                                         String transformationType) {

        Seq<ColumnDef> inputCols = inputSchema.getDefinedColumns();
        List<ColumnDef> outputColumnDefs = new ArrayList<ColumnDef>(inputCols.size() + columnsToTransform.length);

        for (int i = 0; i < inputCols.length(); i++) {
            outputColumnDefs.add(inputCols.apply(i));
        }

        for (String columnToTransform : columnsToTransform) {
            outputColumnDefs.add(
                    new ColumnDef(
                            columnToTransform + "_" + transformationType,
                            new ColumnType.TypeValue("DOUBLE PRECISION")
                    )
            );
        }
        return TabularSchema.apply(outputColumnDefs);
    }
}
