package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.{SnowflakeRelation, SnowflakeSQLStatement}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.LeafExecNode

import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

/**
 * Snowflake Scan Plan for pushing query fragment to snowflake endpoint
 *
 * @param projection   projected columns
 * @param snowflakeSQL SQL query that is pushed to snowflake for evaluation
 * @param relation     Snowflake datasource
 */
case class SnowflakeScanExec(projection: Seq[Attribute],
                             snowflakeSQL: SnowflakeSQLStatement,
                             relation: SnowflakeRelation) extends LeafExecNode {
  // result holder
  @transient implicit private var data: Future[PushDownResult] = _
  @transient implicit private val service: ExecutorService = Executors.newCachedThreadPool()

  override protected def doPrepare(): Unit = {
    logInfo(s"Preparing query to push down - $snowflakeSQL")

    val work = new Callable[PushDownResult]() {
      override def call(): PushDownResult = {
        val result = {
          try {
            val data = relation.buildScanFromSQL[InternalRow](snowflakeSQL, Some(schema))
            PushDownResult(data = Some(data))
          } catch {
            case e: Exception =>
              logError("Failure in query execution", e)
              PushDownResult(failure = Some(e))
          }
        }
        result
      }
    }
    data = service.submit(work)
    logInfo("submitted query asynchronously")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (data.get().failure.nonEmpty) {
      // raise original exception
      throw data.get().failure.get
    }

    data.get().data.get.mapPartitions { iter =>
      val project = UnsafeProjection.create(schema)
      iter.map(project)
    }
  }

  override def cleanupResources(): Unit = {
    logDebug(s"shutting down service to clean up resources")
    service.shutdown()
  }

  override def output: Seq[Attribute] = projection
}

/**
 * Result holder
 *
 * @param data    RDD that holds the data
 * @param failure failure information if we unable to push down
 */
private case class PushDownResult(data: Option[RDD[InternalRow]] = None,
                                  failure: Option[Exception] = None)
  extends Serializable