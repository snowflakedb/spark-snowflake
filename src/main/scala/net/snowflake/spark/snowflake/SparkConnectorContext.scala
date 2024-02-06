/*
 * Copyright 2015-2020 Snowflake Computing
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.snowflake.spark.snowflake

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.slf4j.LoggerFactory

import scala.collection.mutable

private[snowflake] case class RunningQuery (conn: ServerConnection, queryID: String)

object SparkConnectorContext {
  // The map to track running queries for spark application.
  // The key is the application ID, the value is the set of running queries.
  private val runningQueries = mutable.Map[String, mutable.Set[RunningQuery]]()

  // save all closed applications' ID, and skip Spark's retries after application closed.
  private[snowflake] val closedApplicationIDs = mutable.HashSet.empty[String]

  private[snowflake] def getRunningQueries = runningQueries

  // Register spark listener to cancel any running queries if application fails.
  // Only one listener is registered for one spark application
  private[snowflake] def registerSparkListenerIfNotYet(sparkContext: SparkContext): Unit =
    withSyncAndDoNotThrowException {
      val appId = sparkContext.applicationId
      if (!runningQueries.keySet.contains(appId)) {
        logger.info("Spark connector register listener for: " + appId)
        runningQueries.put(appId, mutable.Set.empty)
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            // add application ID to the block list.
            // when Spark retries these closed applications,
            // Spark connector will skip those queries.
            closedApplicationIDs.add(appId)
            try {
              cancelRunningQueries(appId)
              // Close all cached connections
              ServerConnection.closeAllCachedConnections
            } finally {
              super.onApplicationEnd(applicationEnd)
            }
          }
        })
      }
    }

  // Currently, this function is called when the spark application is END,
  // so, the map entry for this application is removed after all the running queries
  // are canceled.
  private[snowflake] def cancelRunningQueries(appId: String): Unit =
    withSyncAndDoNotThrowException {
      val queries = runningQueries.get(appId)
      if (queries.nonEmpty) {
        queries.get.foreach(rq => try {
          if (!rq.conn.isClosed) {
            val statement = rq.conn.createStatement()
            val sessionID = rq.conn.getSessionID
            logger.warn(s"Canceling query ${rq.queryID} for session: $sessionID")
            statement.execute(s"select SYSTEM$$CANCEL_QUERY('${rq.queryID}')")
            statement.close()
          }
        } catch {
          case th: Throwable =>
            logger.warn("Fail to cancel running queries: ", th)
        })
        logger.warn(s"Finish cancelling all queries for $appId")
        runningQueries.remove(appId)
      } else {
        logger.info(s"No running query for: $appId")
      }
    }

  private[snowflake] def addRunningQuery(sparkContext: SparkContext,
                                         conn: ServerConnection,
                                         queryID: String): Unit =
    withSyncAndDoNotThrowException {
      registerSparkListenerIfNotYet(sparkContext)
      val appId = sparkContext.applicationId
      val sessionID = conn.getSessionID
      logger.info(s"Add running query for $appId session: $sessionID queryId: $queryID")
      val queries = runningQueries.get(appId)
      queries.foreach(_.add(RunningQuery(conn, queryID)))
    }

  private[snowflake] def removeRunningQuery(sparkContext: SparkContext,
                                            conn: ServerConnection,
                                            queryID: String): Unit =
    withSyncAndDoNotThrowException {
      val appId = sparkContext.applicationId
      val sessionID = conn.getSessionID
      logger.info(s"Remove running query for $appId session: $sessionID queryId: $queryID")
      val queries = runningQueries.get(appId)
      queries.foreach(_.remove(RunningQuery(conn, queryID)))
    }

  private[snowflake] val logger = LoggerFactory.getLogger(getClass)

  private var isConfigLogged = false

  private val locker = new Object

  // The system configuration is logged once.
  private[snowflake] def recordConfig(): Unit = {
    withSyncAndDoNotThrowException {
      if (!isConfigLogged) {
        isConfigLogged = true
        logger.info(s"Spark Connector system config: " +
          s"${SnowflakeTelemetry.getClientConfig().toPrettyString}")
      }
    }
  }

  private def withSyncAndDoNotThrowException(block: => Unit): Unit =
    try {
      locker.synchronized {
        block
      }
    } catch {
      case th: Throwable =>
        logger.warn("Hit un-caught exception: " + th.getMessage)
    }

}
