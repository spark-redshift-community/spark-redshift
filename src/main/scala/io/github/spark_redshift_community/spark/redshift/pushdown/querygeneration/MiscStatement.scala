package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import io.github.spark_redshift_community.spark.redshift.{RedshiftFailMessage, RedshiftPushdownUnsupportedException}
import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, EmptyRedshiftSQLStatement, IntVariable, RedshiftSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, CaseWhen, Cast, Coalesce, Descending, Expression, If, In, InSet, Literal, MakeDecimal, ScalarSubquery, SortOrder, UnscaledValue}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Extractors for everything else. */
private[querygeneration] object MiscStatement {

  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case Alias(child: Expression, name: String) =>
        blockStatement(convertStatement(child, fields), name)

      // Spark 3.2 introduces below new parameter.
      //   override val ansiEnabled: Boolean = SQLConf.get.ansiEnabled
      // So support to pushdown, if ansiEnabled is false.
      // https://github.com/apache/spark/commit/6f51e37eb52f21b50c8d7b15c68bf9969fee3567

      // To support spark 3.1 as below
      // case Cast(child, t, _) =>
      case Cast(child, t, _, ansiEnabled) if !ansiEnabled =>
        getCastType(t) match {
          case Some(cast) =>
            // For known unsupported data conversion, raise exception to break the
            // pushdown process.
            (child.dataType, t) match {
              case (_: DateType | _: TimestampType,
              _: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType) => {
                // This exception will not break the connector. It will be caught in
                // QueryBuilder.treeRoot.
                throw new RedshiftPushdownUnsupportedException(
                  RedshiftFailMessage.FAIL_PUSHDOWN_UNSUPPORTED_CONVERSION,
                  s"Don't support to convert ${child.dataType} column to $t type",
                  "",
                  false)
              }
              case _ =>
            }

            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
          case _ => convertStatement(child, fields)
        }
      case If(child, trueValue, falseValue) =>
        ConstantString("IFF") +
          blockStatement(
            convertStatements(fields, child, trueValue, falseValue)
          )

      case In(child, list) =>
        blockStatement(
          convertStatement(child, fields) + "IN" +
            blockStatement(convertStatements(fields, list: _*))
        )

      case InSet(child, hset) =>
        convertStatement(In(child, setToExpr(hset)), fields)

      case MakeDecimal(child, precision, scale, _) =>
        ConstantString("CAST") + blockStatement(
          blockStatement(convertStatement(child, fields) + "/ POW(10," +
                          IntVariable(Some(scale)) + ")"
          ) + " AS DECIMAL(" + IntVariable(Some(precision)) + "," +
            IntVariable(Some(scale)) + ")"
        )

      case SortOrder(child, Ascending, _, _) =>
        blockStatement(convertStatement(child, fields)) + "ASC"
      case SortOrder(child, Descending, _, _) =>
        blockStatement(convertStatement(child, fields)) + "DESC"
      
      // Spark 3.2 introduces below new field
      //   joinCond: Seq[Expression] = Seq.empty
      // So support to pushdown, if joinCond is empty.
      // https://github.com/apache/spark/commit/806da9d6fae403f88aac42213a58923cf6c2cb05
      // To support spark 3.1
      //      case ScalarSubquery(subquery, _, _) =>
      case ScalarSubquery(subquery, _, _, joinCond) if joinCond.isEmpty =>
        blockStatement(new QueryBuilder(subquery).statement)

      case UnscaledValue(child) =>
        child.dataType match {
          case d: DecimalType =>
            blockStatement(
              convertStatement(child, fields) + "* POW(10," + IntVariable(
                Some(d.scale)
              ) + ")"
            )
          case _ => null
        }

      case CaseWhen(branches, elseValue) =>
        ConstantString("CASE") +
          mkStatement(branches.map(conditionValue => {
            ConstantString("WHEN") + convertStatement(conditionValue._1, fields) +
              ConstantString("THEN") + convertStatement(conditionValue._2, fields)
          }
          ), " ") + { elseValue match {
          case Some(value) => ConstantString("ELSE") + convertStatement(value, fields)
          case None => EmptyRedshiftSQLStatement()
        }} + ConstantString("END")

      case Coalesce(columns) =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(
            mkStatement(
              columns.map(convertStatement(_, fields)),
              ", "
            )
          )

      case _ => null
    })
  }

  private final def setToExpr(set: Set[Any]): Seq[Expression] = {
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

  /** Attempts a best effort conversion from a SparkType
    * to a Redshift type to be used in a Cast.
    */
  private[querygeneration] final def getCastType(t: DataType): Option[String] =
    Option(t match {
      case StringType => "VARCHAR"
      case BinaryType => "VARBINARY"
      case DateType => "DATE"
      case TimestampType => "TIMESTAMP"
      case d: DecimalType =>
        "DECIMAL(" + d.precision + ", " + d.scale + ")"
      case IntegerType => "INTEGER"
      case LongType => "BIGINT"
      case FloatType => "FLOAT4"
      case DoubleType => "FLOAT8"
      case ShortType => "SMALLINT"
      case BooleanType => "BOOLEAN"
      case _ => null
    })

}
