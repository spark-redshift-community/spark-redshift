package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, RedshiftSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Abs, Acos, Asin, Atan, Attribute, Ceil, CheckOverflow, Cos, Exp, Expression, Floor, Greatest, Least, Log10, Pi, Pow, PromotePrecision, Round, Sin, Sqrt, Tan, UnaryMinus}

import scala.language.postfixOps

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object NumericStatement {

  /** Used mainly by QueryGeneration.convertExpression. This matches
    * a tuple of (Expression, Seq[Attribute]) representing the expression to
    * be matched and the fields that define the valid fields in the current expression
    * scope, respectively.
    *
    * @param expAttr A pair-tuple representing the expression to be matched and the
    *                attribute fields.
    * @return An option containing the translated SQL, if there is a match, or None if there
    *         is no match.
    */
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case _: Abs | _: Acos | _: Cos | _: Tan | _: Atan |
          _: Floor | _: Sin | _: Asin | _: Sqrt | _: Ceil |
          _: Sqrt | _: Greatest | _: Least | _: Exp =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case _: Log10 =>
        ConstantString("LOG") +
          blockStatement(convertStatements(fields, expr.children: _*))

      // From spark 3.1, UnaryMinus() has 2 parameters.
      case UnaryMinus(child, _) =>
        ConstantString("-") +
          blockStatement(convertStatement(child, fields))

      case Pow(left, right) =>
        ConstantString("POWER") +
          blockStatement(
            convertStatement(left, fields) + "," + convertStatement(
              right,
              fields
            )
          )

      case PromotePrecision(child) => convertStatement(child, fields)

      case CheckOverflow(child, t, _) =>
        MiscStatement.getCastType(t) match {
          case Some(cast) =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
          case _ => convertStatement(child, fields)
        }

      // Spark has resolved PI() as 3.141592653589793
      // Suppose connector can't see Pi().
      case Pi() => ConstantString("PI()") !

      case Round(child, scale) =>
        ConstantString("ROUND") + blockStatement(
          convertStatements(fields, child, scale)
        )

      case _ => null
    })
  }
}
