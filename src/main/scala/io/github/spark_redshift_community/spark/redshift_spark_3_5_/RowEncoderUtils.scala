package io.github.spark_redshift_community.spark.redshift

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType

private[redshift] object RowEncoderUtils {
  def expressionEncoderForSchema(schema: StructType): ExpressionEncoder[Row] = {
    ExpressionEncoder(RowEncoder.encoderFor(schema))
  }
}
