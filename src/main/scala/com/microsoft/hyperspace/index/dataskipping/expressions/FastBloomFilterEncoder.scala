/*
 * Copyright (2021) The Hyperspace Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.hyperspace.index.dataskipping.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.index.dataskipping.util.ReflectionHelper

/**
 * A [[BloomFilterEncoder]] implementation that avoids copying arrays.
 */
object FastBloomFilterEncoder extends BloomFilterEncoder with ReflectionHelper {
  override val dataType: StructType = StructType(
      StructField("bitCount", LongType, nullable = false) ::
      StructField("data", StringType, nullable = false) ::
      StructField("numHashFunctions", LongType, nullable = false) :: Nil)
    // Must be consistent with read/write below (in other words,
    // consistent with physical layout of object field in OpenSearch)

  override def encode(bf: BloomFilter): InternalRow = {
    val bloomFilterImplClass = bf.getClass
    val bits = get(bloomFilterImplClass, "bits", bf)
    val bitArrayClass = bits.getClass
    InternalRow(
      getLong(bitArrayClass, "bitCount", bits),
      UTF8String.fromString(
        ArrayData.toArrayData(get(bitArrayClass, "data", bits))
          .toLongArray().mkString(",")),
      getLong(bloomFilterImplClass, "numHashFunctions", bf))
  }

  override def decode(value: Any): BloomFilter = {
    val struct = value.asInstanceOf[InternalRow]
    val bitCount = struct.getLong(0)
    val data = struct.getString(1).split(',').map(_.toLong)
    val numHashFunctions = struct.getLong(2)

    val bf = BloomFilter.create(1)
    val bloomFilterImplClass = bf.getClass
    val bits = get(bloomFilterImplClass, "bits", bf)
    val bitArrayClass = bits.getClass
    setLong(bitArrayClass, "bitCount", bits, bitCount)
    set(bitArrayClass, "data", bits, data)
    setInt(bloomFilterImplClass, "numHashFunctions", bf, numHashFunctions.toInt)
    bf
  }
}
