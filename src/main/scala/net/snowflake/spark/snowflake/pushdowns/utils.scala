package net.snowflake.spark.snowflake.pushdowns

class SnowflakePushdownException(message: String) extends Exception(message)

case class QueryAlias(prefix: String, height: Int=0) {
  override def toString: String = s"${prefix}_$height"

  /**
    * Returns a query alias one level deeper than this alias.
    */
  def child: QueryAlias = this.copy(height=height + 1)

  /**
    * Forks this QueryAlias into two aliases, suitable
    * for using in two parallel Query trees.
    */
  def fork: (QueryAlias, QueryAlias) = (
    QueryAlias(prefix=s"${prefix}_$height"),
    QueryAlias(prefix=s"${prefix}_${height + 1}")
    )
}

object StringBuilderImplicits {
  implicit class IndentingStringBuilder(val sb: StringBuilder) {
    def indent(depth: Int): StringBuilder = sb.append("  " * depth)
  }
}
