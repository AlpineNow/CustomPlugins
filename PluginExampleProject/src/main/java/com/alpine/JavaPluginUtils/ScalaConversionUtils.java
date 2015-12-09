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
package com.alpine.JavaPluginUtils;

import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.List;

import java.util.Arrays;

/**
 * Utility functions to use scala collection types natively in Java.
 */
public class ScalaConversionUtils {
    /**
     * Builds a scala list natively in Java.
     *
     * @param args objects or collection
     * @param <T>  Object type of the collection
     * @return A Scala list containing args
     */
    public static <T> List<T> scalaList(T... args) {
        return scalaSeq(args).toList();
    }

    /**
     * Build a scala sequence natively in Java.
     * Using the syntax : Seq(a1, a2, a3... )
     *
     * @param args Java objects or collections
     */
    public static <T> Seq<T> scalaSeq(T... args) {
        return JavaConversions.asScalaBuffer(
                Arrays.asList(args));
    }

    /**
     * Utility Function to create the scala option type None in Java
     */
    public static <T> scala.Option<T> None() {
        return Option.empty();
    }

    /**
     * Utility function to create scala option type Some(Value) in Java.
     */
    public static <T> scala.Option<T> Some(T value) {
        return Option.apply(value);
    }
}


