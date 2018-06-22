package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.telemetry.Telemetry
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.slf4j.LoggerFactory
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

object SnowflakeTelemetry {

  private var logs: List[(ObjectNode, Long)] = Nil //data and timestamp
  private val logger = LoggerFactory.getLogger(getClass)
  private val mapper = new ObjectMapper()

  //For test only
  private[snowflake] var output: ObjectNode = _
  private var debugMode: Boolean = false

  private[snowflake] def enableDebugMode(): Unit = debugMode = true

  def addLog(log: (ObjectNode, Long)): Unit = {
    //for test only
    if (debugMode) {
      println("---------->>>Telemetry Output<<<----------")
      println(log._1.toString)
      println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    }
    this.synchronized{
      output = log._1
      logs = log :: logs
    }

  }

  def send(telemetry: Telemetry):Unit = {
    var tmp: List[(ObjectNode, Long)] = _
    this.synchronized{
      tmp = logs
      logs = Nil
    }
    tmp.foreach{
      case(log, timestamp) => {
        if (debugMode)
          println(s"Send ----------> timestamp:$timestamp log:${log.toString}")
        telemetry.addLogToBatch(log,timestamp)
      }
    }
    if(!telemetry.sendBatch()) logger.error("Telemetry is failed")
  }


  /**
    * Generate json string for giving spark plan tree, if only the tree is complete (root is ReturnAnswer) Snowflake plan
    */
  def planToJson(plan: LogicalPlan): Option[ObjectNode] =
    plan.nodeName match {
      case "ReturnAnswer" => {
        val (isSFPlan, json) = planTree(plan)
        if (isSFPlan) Some(json) else None
      }
      case _ => None
    }

  /**
    * convert a plan tree to json
    */
  private def planTree(plan: LogicalPlan): (Boolean, ObjectNode) = {
    val result = mapper.createObjectNode()
    var action = plan.nodeName
    var isSFPlan = false
    val argsString = plan.argString
    val argsNode = mapper.createObjectNode()
    val children = mapper.createArrayNode()

    plan match {
      case LogicalRelation(sfRelation: SnowflakeRelation, _, _, _) =>
        isSFPlan = true
        action = "SnowflakeRelation"
        val schema = mapper.createArrayNode()
        sfRelation.schema.fields.map(_.dataType.typeName).foreach(schema.add)
        argsNode.set("schema", schema)

      case Filter(condition, _) =>
        argsNode.set("conditions", expToJson(condition))

      case Project(fields, _) =>
        argsNode.set("fields",expressionsToJson(fields))

      case Join(_, _, joinType, Some(condition)) =>
        argsNode.put("type",joinType.toString)
        argsNode.set("conditions",expToJson(condition))

      case Aggregate(groups, fields, _) =>
        argsNode.set("field",expressionsToJson(fields))
        argsNode.set("group",expressionsToJson(groups))

      case Limit(condition, _) =>
        argsNode.set("condition",expToJson(condition))

      case LocalLimit(condition, _) =>
        argsNode.set("condition", expToJson(condition))

      case Sort(orders, isGlobal, _) =>
        argsNode.put("global",isGlobal)
        argsNode.set("order", expressionsToJson(orders))

      case Window(namedExpressions, _, _, _) =>
        argsNode.set("expression", expressionsToJson(namedExpressions))

      case Union(_) =>
      case Expand(_, _, _) =>
      case _ =>
    }

    plan.children.foreach(x=>{
      val (isSF, js) = planTree(x)
      if(isSF) isSFPlan = true
      children.add(js)
    })

    result.put("action",action)
    if(argsNode.toString=="{}"){
      result.put("args", argsString)
    } else{
      result.set("args", argsNode)
    }
    result.set("children", children)
    (isSFPlan, result)

  }

  /**
    * Expression to Json array
    */
  private def expressionsToJson(name: Seq[Expression]): ArrayNode = {
    val result = mapper.createArrayNode()
    name.map(expToJson).foreach(result.add)
    result
  }


  /**
    * Expression to Json object
    */
  private def expToJson(exp: Expression): ObjectNode = {
    val result = mapper.createObjectNode()
    if(exp.children.isEmpty){
      result.put("source", exp.nodeName)
      result.put("type", exp.dataType.typeName)
    }
    else{
      result.put("operator", exp.nodeName)
      val parameters = mapper.createArrayNode()
      sortArgs(exp.nodeName,exp.children.map(expToJson)).foreach(parameters.add)
      result.set("parameters",parameters)
    }
    result
  }

  //Since order of arguments in some spark expression is random,
  //sort them to provide fixed result for testing.
  private def sortArgs(operator: String, args: Seq[ObjectNode]): Seq[ObjectNode] =
    operator match {
      case "And"|"Or" => args.sortBy(_.toString)
      case _ => args
    }
}
