package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.sql.catalyst.expressions._

object SimpleBinaryExpression {

  def unapply(expr: BinaryExpression): Option[(String, Seq[Expression])] = expr match {
    case
      ShiftRightUnsigned(_, _) |
      StartsWith(_, _) |
      // TimeSub(_, _) |
      // Levenshtein(_, _) |
      StringInstr(_, _) |
      // TruncDate(_, _) |
      // Decode(_, _) |
      // NextDay(_, _) |
      // GetMapValue(_, _) |
      // UnixTimestamp(_, _) |
      // ArrayContains(_, _) |
      // ShiftLeft(_, _) |
      // StringRepeat(_, _) |
      // DateSub(_, _) |
      // GetJsonObject(_, _) |
      // ShiftRight(_, _) |
      // FromUTCTimestamp(_, _) |
      // StringSplit(_, _) |
      Contains(_, _) |
      // SortArray(_, _) |
      RLike(_, _) |
      // NaNvl(_, _) |
      // FormatNumber(_, _) |
      // Like(_, _) |
      // AddMonths(_, _) |
      // GetArrayItem(_, _) |
      // DateFormatClass(_, _) |
      // FromUnixTime(_, _) |
      // MonthsBetween(_, _) |
      // DateDiff(_, _) |
      EndsWith(_, _) |
      // FindInSet(_, _) |
      // Hypot(_, _) |
      // Pow(_, _) |
      Atan2(_, _) |
      // TimeAdd(_, _) |
      // Encode(_, _) |
      // CombineSets(_, _) |
      // ToUTCTimestamp(_, _) |
      // Round(_, _) |
      // DateAdd(_, _) |
      Sha2(_, _) => Some(expr.prettyName.toUpperCase, Seq(expr.left, expr.right))
    case _ => None
  }
}

