package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import io.github.spark_redshift_community.spark.redshift.{RedshiftFailMessage, RedshiftPushdownUnsupportedException}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{Decimal, DecimalType, DoubleType, StringType}
import org.apache.spark.unsafe.types.UTF8String

private[querygeneration] object MiscStatementCommon {
  final def setToExpr(set: Set[Any]): Seq[Expression] = {
    set.map {
      case d: Decimal => Literal(d, DecimalType(d.precision, d.scale))
      case s @ (_: String | _: UTF8String) => Literal(s, StringType)
      case d: Double => Literal(d, DoubleType)
      case e: Expression => e
      case default =>
        // This exception will not break the connector. It will be caught in
        // QueryBuilder.treeRoot.
        throw new RedshiftPushdownUnsupportedException(
          RedshiftFailMessage.FAIL_PUSHDOWN_SET_TO_EXPR,
          s"${default.getClass.getSimpleName} @ MiscStatement.setToExpr",
          "Class " + default.getClass.getName + " is not supported in the 'in()' expression",
          false
        )
    }.toSeq
  }
}
