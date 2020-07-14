package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake._
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  ScalaUDF
}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF

/**
  * This class is used to catch unsupported statement and raise an exception
  * to stop the push-down to snowflake.
  */
private[querygeneration] object UnsupportedStatement {

  final val EXCEPTION_MESSAGE = "pushdown failed"

  /** Used mainly by QueryGeneration.convertStatement. This matches
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
             ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1

    // This exception is not a real issue. It will be caught in
    // QueryBuilder.treeRoot and a telemetry message will be sent if
    // there are any snowflake tables in the query. It can't be done here
    // because it is not clear whether there are any snowflake tables here.
    throw new SnowflakePushdownUnsupportedException(
      EXCEPTION_MESSAGE,
      expr.prettyName,
      expr.sql,
      isKnownUnsupportedOperation(expr))
  }

  // Determine whether the unsupported operation is known or not.
  private def isKnownUnsupportedOperation(expr: Expression): Boolean = {
    // PythonUDF is not supported on spark 2.3
    // The pushdown for UDF is known unsupported
    (expr.isInstanceOf[ScalaUDF]
      || expr.isInstanceOf[ScalaUDAF])
  }
}
