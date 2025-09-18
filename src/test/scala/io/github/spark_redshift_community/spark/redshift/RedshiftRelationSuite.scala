/*
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package io.github.spark_redshift_community.spark.redshift.test

import io.github.spark_redshift_community.spark.redshift.RedshiftRelation
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

class RedshiftRelationSuite extends AnyFunSuite {
  def generateRandomMetadata(): Metadata = {
    Metadata.fromJson(s"""{"${UUID.randomUUID()}":"${UUID.randomUUID()}"}""")
  }

  def generateRandomBoolean(): Boolean = {
    Math.random() > 0.5
  }

  def generateRandomStructField(dt: DataType): StructField = {
    StructField(UUID.randomUUID().toString,
      dt, nullable = generateRandomBoolean(), generateRandomMetadata())
  }


  val basicTypes: Seq[DataType] = Seq(ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType(1, 0),
    DecimalType(1, 1),
    DecimalType(2, 1),
    StringType,
    BinaryType,
    BooleanType,
    TimestampType)

  val basicArrayTypes: Seq[DataType] = basicTypes.map(ArrayType(_))
  val basicMapTypes: Seq[DataType] = basicTypes.map(dt => MapType(dt, dt))

  val allBasicTypes: Seq[DataType] = basicTypes ++ basicArrayTypes ++ basicMapTypes

  val singleFieldFlatSchemas: Seq[() => StructType] = allBasicTypes.
    map(dt => () => { StructType(generateRandomStructField(dt)::Nil) })
  val singleFieldNestedSchemas: Seq[() => StructType] = singleFieldFlatSchemas.
    map(schemaGenerator => () => { StructType(generateRandomStructField(schemaGenerator())::Nil)})

  val twoFieldFlatSchemas: Seq[() => StructType] = allBasicTypes.zip(allBasicTypes).
    map({case (dt1, dt2) =>
      () => StructType(generateRandomStructField(dt1)::generateRandomStructField(dt2)::Nil)})
  val twoFieldNestedSchemas: Seq[() => StructType] = twoFieldFlatSchemas.
    map(schemaGenerator => () => {
      StructType(generateRandomStructField(schemaGenerator())::
        generateRandomStructField(schemaGenerator())::Nil) })

  val allSizeOneSchemaGenerators: Seq[() => StructType] = singleFieldFlatSchemas ++
    singleFieldNestedSchemas

  val allSizeTwoSchemaGenerators: Seq[() => StructType] = twoFieldFlatSchemas ++
    twoFieldNestedSchemas

  test("schemaTypesMatch is true when types match") {
    allSizeOneSchemaGenerators.zip(allSizeOneSchemaGenerators).foreach {
      case (schemaGenerator1, schemaGenerator2) =>
        assert(RedshiftRelation.schemaTypesMatch(schemaGenerator1(), schemaGenerator2()))
    }

    allSizeTwoSchemaGenerators.zip(allSizeTwoSchemaGenerators).foreach {
      case (schemaGenerator1, schemaGenerator2) =>
        assert(RedshiftRelation.schemaTypesMatch(schemaGenerator1(), schemaGenerator2()))
    }
  }

  test("schemaTypesMatch is false when types do not match") {
    allSizeOneSchemaGenerators.zipWithIndex.foreach {
      case (schemaGenerator1, index1) => allSizeOneSchemaGenerators.zipWithIndex.foreach {
        case (schemaGenerator2, index2) =>
          if (index1 != index2) {
            val schema1 = schemaGenerator1()
            val schema2 = schemaGenerator2()
            assert(!RedshiftRelation.schemaTypesMatch(schema1, schema2),
              s"$schema1 should not match $schema2")
          }
      }
    }

    allSizeTwoSchemaGenerators.zipWithIndex.foreach {
      case (schemaGenerator1, index1) => allSizeTwoSchemaGenerators.zipWithIndex.foreach {
        case (schemaGenerator2, index2) =>
          if (index1 != index2) {
            val schema1 = schemaGenerator1()
            val schema2 = schemaGenerator2()
            assert(!RedshiftRelation.schemaTypesMatch(schema1, schema2),
              s"$schema1 should not match $schema2")
          }
      }
    }
  }

  test("schemaTypesMatch throws IllegalStateException when schema lengths do not match") {
    allSizeOneSchemaGenerators.foreach {
      schemaGenerator1 => allSizeTwoSchemaGenerators.foreach {
        schemaGenerator2 =>
          val exception = intercept[IllegalStateException] {
            RedshiftRelation.schemaTypesMatch(schemaGenerator1(), schemaGenerator2())
          }
          assert(exception.getMessage == "Schema types do not match in length")
      }
    }
  }
}
