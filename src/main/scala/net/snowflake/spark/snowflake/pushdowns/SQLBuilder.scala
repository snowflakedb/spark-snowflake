package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, expressions}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer

/**
  * Convenience methods for creating a SQLBuilder
  */
object SQLBuilder {
  /**
    * Create a SQLBuilder from a SQL expression string along with any associated params.
    * @param sql The SQL expression to initialize the SQLBuilder with
    * @param params Any params for the provided expression
    */
  def fromStatic(sql: String, params: Seq[Any]=Nil): SQLBuilder =
  new SQLBuilder().raw(sql).addParams(params)

  def withAlias(alias: QueryAlias, inner: SQLBuilder => Unit): SQLBuilder =
    new SQLBuilder().aliasedBlock(alias, inner)

  def withFields(fields: Seq[NamedExpression]): SQLBuilder =
    new SQLBuilder(fields)

  /**
    * We need this destructor since the BinaryOperator object is private to Spark
    * TODO: Remove when Spark makes it public (maybe 1.6)
    */
  object BinaryOperator {
    def unapply(e: expressions.BinaryOperator): Option[(Expression, Expression)] = Some((e.left, e.right))
  }
}

case class LazySQL(block: SQLBuilder => SQLBuilder) extends LeafExpression with Unevaluable {
  override def nullable: Boolean = false
  override def dataType: DataType = null
}

/**
  * SQLBuilder is a mutable object for efficiently building up complex SQL expressions.
  * Internally it uses StringBuilder and a ListBuffer to efficiently support many small writes.
  * All methods return `this` for chaining.
  */
class SQLBuilder(fields: Seq[NamedExpression]=Nil) {
  var sql: StringBuilder = new StringBuilder
  var params: ListBuffer[Any] = ListBuffer.empty[Any]

  /**
    * Appends another SQLBuilder to this one.
    * @param other The other SQLBuilder
    * @return This SQLBuilder
    */
  def appendBuilder(other: SQLBuilder): SQLBuilder =
  raw(other.sql).addParams(other.params)

  /**
    * Appends another SQLBuilder to this one.
    * @param other The other SQLBuilder
    * @param ifNone A string to append to this SQLBuilder if other is None
    * @return This SQLBuilder
    */
  def maybeAppendBuilder(other: Option[SQLBuilder], ifNone: String=""): SQLBuilder = other match {
    case Some(o) => raw(" ").raw(o.sql).addParams(o.params)
    case None => raw(ifNone)
  }

  /**
    * Adds a named expression to the builder.
    * @note The expression is fully qualified if possible (ex. `foo`.`bar`)
    */
  def attr(a: Attribute): SQLBuilder = {
    NamedExpression
    fields.find(e => e.exprId == a.exprId) match {
      case Some(resolved) =>
        qualifiedAttr(resolved.qualifier, resolved.name)
      case None =>
        qualifiedAttr(a.qualifier, a.name)
    }
    this
  }

  /**
    * A String attribute optionally qualified by another string.
    */
  def qualifiedAttr(qualifier: Option[String], name: String): SQLBuilder = {
    qualifier.map(q => identifier(q).raw("."))
    identifier(name)
  }

  /**
    * Escapes and appends a single identifier to the SQLBuilder.
    */
  def identifier(a: String): SQLBuilder = { sql.append("\"").append(a).append("\""); this }

  /**
    * Adds a raw string to the expression, no escaping occurs.
    */
  def raw(s: String): SQLBuilder = { sql.append(s); this }

  /**
    * @see SQLBuilder#raw(String)
    */
  def raw(s: StringBuilder): SQLBuilder = { sql.append(s); this }

  /**
    * Wraps the provided lambda in parentheses.
    * @param inner A lambda which will be executed between adding parenthesis to the expression
    */
  def block(inner: => Unit): SQLBuilder = { raw("("); inner; raw(")"); this }

  /**
    * Wraps the provided lambda with a non-qualified alias.
    * @param alias The alias to give the inner expression
    * @param inner A lambda which will be wrapped with ( ... ) AS alias
    */
  def aliasedBlock(alias: String, inner: SQLBuilder => Unit): SQLBuilder =
  block({ inner(this) }).raw(" AS ").identifier(alias)

  /**
    * @see SQLBuilder#aliasedBlock(String, SQLBuilder => Unit)
    */
  def aliasedBlock(alias: QueryAlias, inner: SQLBuilder => Unit): SQLBuilder =
  aliasedBlock(alias.toString, inner)

  /**
    * Adds the provided param to the internal params list.
    */
  def param(p: Any): SQLBuilder = { params += p; this }

  /**
    * @see SQLBuilder#param(Any)
    * @note Use this variant if the parameter is one of the Catalyst Types
    */
  def param(p: Any, t: DataType): SQLBuilder = {
    val paramValue = p match {
      case l: Literal => l.value
      case _ => p
    }
    params += CatalystTypeConverters.convertToScala(paramValue, t)
    this
  }

  /**
    * @see SQLBuilder#param(Any)
    * @note Handles converting the literal from its Catalyst Type to a Scala Type
    */
  def param(l: Literal): SQLBuilder = {
    params += CatalystTypeConverters.convertToScala(l.value, l.dataType)
    this
  }

  /**
    * Add a list of params directly to the internal params list.
    * Optimized form of calling SQLBuilder#param(Any) for each element of newParams.
    */
  def addParams(newParams: Seq[Any]): SQLBuilder = { params ++= newParams; this }

  /**
    * Adds a list of elements to the SQL expression, as a comma-delimited list.
    * Handles updating the expression as well as adding
    * each param to the internal params list.
    *
    * @param newParams A list of objects to add to the expression
    * @param t The Catalyst type for all of the objects
    */
  def paramList(newParams: Seq[Any], t: DataType): SQLBuilder = {
    block {
      for (j <- newParams.indices) {
        if (j != 0) {
          raw(", ?").param(newParams(j), t)
        }
        else {
          raw("?").param(newParams(j), t)
        }
      }
    }
  }

  def sqlFunction(fnName: String, children: Expression*): SQLBuilder = {
    raw(fnName).block { addExpressions(children, ", ") }
  }

  /**
    * Adds a list of expressions to this SQLBuilder, joins each expression with the provided conjunction.
    * @param expressions A list of [[org.apache.spark.sql.catalyst.expressions.Expression]]s
    * @param conjunction A string to join the resulting SQL Expressions with
    */
  def addExpressions(expressions: Seq[Expression], conjunction: String): SQLBuilder = {
    for (i <- expressions.indices) {
      if (i != 0) {
        raw(conjunction)
      }
      addExpression(expressions(i))
    }
    this
  }

  def maybeAddExpressions(expressions: Seq[Expression], conjunction: String): Option[SQLBuilder] = {
    if (expressions.nonEmpty) {
      Some(addExpressions(expressions, conjunction))
    } else {
      None
    }
  }

  /**
    * Adds a Catalyst [[org.apache.spark.sql.catalyst.expressions.Expression]] to this SQLBuilder
    * @param expression The [[org.apache.spark.sql.catalyst.expressions.Expression]] to add
    */
  def addExpression(expression: Expression): SQLBuilder = {
    expression match {
      case a: Attribute => attr(a)
      case l: Literal => raw(l.toString()).param(l)
      case Alias(child: Expression, name: String) =>
        block { addExpression(child) }.raw(" AS ").identifier(name)

      case Cast(child, t) => t match {
        case TimestampType | DateType => block {
          raw("UNIX_TIMESTAMP")
          block { addExpression(child) }
          raw(" * 1000")
        }
        case _ => TypeConversions.DataFrameTypeToSnowflakeCastType(t) match {
          case None => addExpression(child)
          case Some(t) => {
            raw("CAST").block {
              addExpression(child)
                .raw(" AS ")
                .raw(t)
            }
          }
        }
      }

      case LazySQL(block) => block(this)

      case StartsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        attr(a).raw(" LIKE ?").param(s"${v.toString}%")
      case EndsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        attr(a).raw(" LIKE ?").param(s"%${v.toString}")
      case Contains(a: Attribute, Literal(v: UTF8String, StringType)) =>
        attr(a).raw(" LIKE ?").param(s"%${v.toString}%")

      case InSet(a: Attribute, set) => attr(a).raw(" IN ").paramList(set.toSeq, a.dataType)

      case In(a: Attribute, list) if list.forall(_.isInstanceOf[Literal]) =>
        attr(a).raw(" IN ").paramList(list, a.dataType)

      case If(predicate, trueVal, falseVal) => sqlFunction("IF", predicate, trueVal, falseVal)

      /*
    case CaseWhen(branches) => {
      raw("CASE ")
      branches.sliding(2, 2).foreach({
        case Seq(cond, expr) =>
          raw("WHEN ").addExpression(cond).raw(" THEN ").addExpression(expr).raw(" ")
        case Seq(elseExpr) =>
          raw("ELSE ").addExpression(elseExpr)
      })
    }
    case CaseKeyWhen(key, branches) => {
      raw("CASE ").addExpression(key).raw(" ")
      branches.sliding(2, 2).foreach({
        case Seq(cond, expr) =>
          raw("WHEN ").addExpression(cond).raw(" THEN ").addExpression(expr).raw(" ")
        case Seq(elseExpr) =>
          raw("ELSE ").addExpression(elseExpr)
      })
    }
*/
      case Concat(children) => sqlFunction("CONCAT", children:_*)
      case ConcatWs(children) if children.length > 1 => sqlFunction("CONCAT_WS", children:_*)

      case Least(children) => sqlFunction("LEAST", children:_*)
      case Greatest(children) => sqlFunction("GREATEST", children:_*)

      case Coalesce(children) => sqlFunction("COALESCE", children:_*)

      case StringLocate(substr, str, start) => {
        val actualStart = If(
          // spark compat: if the start position is 0, replace with 1
          EqualTo(start, Literal(0)),
          Literal(1),
          start
        )
        addExpression(
          If(
            // spark compat: if the start position is null, we need to return 0
            IsNull(start),
            Literal(0),
            If(
              // spark compat: if the substring is the empty string, always return 1
              EqualTo(substr, Literal("")),
              Literal(1),
              LazySQL(_.sqlFunction("LOCATE", substr, str, actualStart))
            )
          )
        )
      }
      case Substring(str, pos, len) => {
        val actualPos = If(
          // spark compat: if the start position is 0, replace with 1
          EqualTo(pos, Literal(0)),
          Literal(1),
          pos
        )
        sqlFunction("SUBSTR", str, actualPos, len)
      }
      case StringRPad(str, len, pad) =>
        addExpression(
          If(
            EqualTo(pad, Literal("")),
            Substring(str, Literal(0), len),
            LazySQL(_.sqlFunction("RPAD", str, len, pad))
          )
        )
      case StringLPad(str, len, pad) =>
        addExpression(
          If(
            EqualTo(pad, Literal("")),
            Substring(str, Literal(0), len),
            LazySQL(_.sqlFunction("LPAD", str, len, pad))
          )
        )

      case SubstringIndex(str, delim, count) => sqlFunction("SUBSTRING_INDEX", str, delim, count)

      // AGGREGATE EXPRESSIONS
      /*
            case CountDistinct(children) => raw("COUNT(DISTINCT ").addExpressions(children, ", ").raw(")")
            case SumDistinct(child) => raw("SUM(DISTINCT ").addExpression(child).raw(")")


            case ApproxCountDistinct(child, relativeSD) if relativeSD >= 0.01 =>
              raw("APPROX_COUNT_DISTINCT").block { addExpression(child) }
      */
      case SortOrder(child: Expression, Ascending) => block { addExpression(child) }.raw(" ASC")
      case SortOrder(child: Expression, Descending) => block { addExpression(child) }.raw(" DESC")

      // UNARY EXPRESSIONS
      // These are special cases - see SimpleUnaryExpression

      case Rand(seed) => sqlFunction("RAND", Literal(seed))

      case ToDate(child) => sqlFunction("DATE", child)
      case BitwiseNot(child) => sqlFunction("~", child)
      case UnaryMinus(child) => sqlFunction("-", child)

      case Log2(child) => sqlFunction("LOG", Literal(2), child) // scalastyle:ignore
      case Log10(child) => sqlFunction("LOG", Literal(10), child) // scalastyle:ignore

      case Signum(child) => sqlFunction("SIGN", child)
      case ToRadians(child) => sqlFunction("RADIANS", child)
      case ToDegrees(child) => sqlFunction("DEGREES", child)

      case Not(child) => sqlFunction("NOT", child)

      case IsNull(child) => block { addExpression(child) }.raw(" IS NULL")
      case IsNotNull(child) => block { addExpression(child) }.raw(" IS NOT NULL")

      case Length(child) => sqlFunction("CHAR_LENGTH", child)

      // TODO: handle this and the no-argument case
      // case UnixTimestamp(child) => sqlFunction("UNIX_TIMESTAMP", child)

      // BINARY OPERATORS

      case Logarithm(left, right) => sqlFunction("LOG", left, right) // scalastyle:ignore
      case MaxOf(left, right) => sqlFunction("GREATEST", left, right)
      case MinOf(left, right) => sqlFunction("LEAST", left, right)

      case Like(left, right) => addExpression(left).raw(" LIKE ").addExpression(right)

      // a pmod b := (a % b + b) % b
      case Pmod(left, right) =>
        block {
          block {
            addExpressions(Seq(left, right), " % ")
            raw(" + ")
            addExpression(right)
          }.raw(" % ").addExpression(right)
        }

      // GENERIC HANDLERS

      // All Spark BinaryOperator symbols work in SQL expressions, except
      // for the three handled above.
      case op @ SQLBuilder.BinaryOperator(left: Expression, right: Expression) =>
        block { addExpression(left).raw(s" ${op.symbol} ").addExpression(right) }

      case SimpleUnaryExpression(name, child) => sqlFunction(name, child)
      case Aggregates(name, child) => sqlFunction(name, child)
      case SimpleBinaryExpression(name, children) => sqlFunction(name, children:_*)
    }
    this
  }
}
