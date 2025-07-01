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
package io.github.spark_redshift_community.spark.redshift.pushdown.deoptimize.test

import io.github.spark_redshift_community.spark.redshift.pushdown.deoptimize.ReadSidePadding
import io.github.spark_redshift_community.spark.redshift.pushdown.deoptimize.UndoCharTypePadding
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.types.{CharType, DataType, IntegerType, Metadata, StringType, VarcharType}

class UndoCharTypePaddingSuite extends AnyFunSuite {

  private val a = 'a.string
  private val output = Seq(a)
  private val localRelation = LocalRelation(output)
  private val length = 10
  private val charRefMatchLength = ref(CharType(length))
  private val charRefMismatchLength = ref(CharType(length - 1))
  private val varcharRef = ref(VarcharType(length))

  test("ReadSidePadding matcher matches all conditions and extracts ref") {
    readSidePadding() match {
      case ReadSidePadding(matchedRef) =>
        assert(matchedRef == charRefMatchLength)

      case _ =>
        fail("Should match!")
    }
  }

  test("ReadSidePadding matcher requires correct class type") {
    readSidePadding(clazz = classOf[Object]) match {
      case ReadSidePadding(_) =>
        fail("Should not match any class!")

      case _ =>
    }
  }

  test("ReadSidePadding matcher requires correct function name") {
    readSidePadding(functionName = "random123456") match {
      case ReadSidePadding(_) =>
        fail("Should only match function name: readSidePadding")

      case _ =>
    }
  }

  test("ReadSidePadding matcher requires correct expression type") {
    readSidePadding(args = Seq(Literal(1))) match {
      case ReadSidePadding(_) =>
        fail("Should not match wrong args!")

      case _ =>
    }
  }

  test("ReadSidePadding matcher requires String data type") {
    readSidePadding(dataType = IntegerType) match {
      case ReadSidePadding(_) =>
        fail("Should only match data type: StringType")

      case _ =>
    }
  }

  test("ReadSidePadding matcher requires Char input type") {
    readSidePadding(args = Seq(varcharRef, Literal(length))) match {
      case ReadSidePadding(_) =>
        fail("Should only match input type: char")

      case _ =>
    }
  }

  test("ReadSidePadding matcher requires correct length for char input type") {
    readSidePadding(args = Seq(charRefMismatchLength, Literal(length))) match {
      case ReadSidePadding(_) =>
        fail(s"Should only match input type: char($length)")

      case _ =>
    }
  }

  test("Undo readSidePadding with alias in Project") {
    val plan = Project(Seq(Alias(readSidePadding(), "a")()), localRelation)
    val result = UndoCharTypePadding(plan)
    checkCanonicalized(result, Project(Seq(Alias(charRefMatchLength, "a")()), localRelation))
  }

  test("Undo readSidePadding in IsNotNull condition of Filter") {
    val plan = Filter(IsNotNull(readSidePadding()), localRelation)
    val result = UndoCharTypePadding(plan)
    checkCanonicalized(result, Filter(IsNotNull(charRefMatchLength), localRelation))
  }

  private def ref(dataType: DataType): AttributeReference = {
    val typeString = dataType match {
      case CharType(length) =>
        s"char($length)"
      case VarcharType(length) =>
        s"varchar($length)"
      case _ =>
        throw new IllegalArgumentException()
    }
    AttributeReference(
      name = "a",
      dataType = StringType,
      metadata = Metadata.fromJson(
        s"""
           |{
           | "__CHAR_VARCHAR_TYPE_STRING" : "$typeString"
           |}
           |""".stripMargin))()
  }


  private def readSidePadding(
      clazz: Class[_] = classOf[CharVarcharCodegenUtils],
      dataType: DataType = StringType,
      functionName: String = "readSidePadding",
      args: Seq[Expression] = Seq(charRefMatchLength, Literal(length))): StaticInvoke = {
    StaticInvoke(clazz, dataType, functionName, args)
  }

  private def checkCanonicalized(
      actualPlan: LogicalPlan,
      expectedPlan: LogicalPlan): Unit = {
    assert(actualPlan.canonicalized == expectedPlan.canonicalized)
  }

}
