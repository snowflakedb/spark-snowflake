/*
 * Copyright 2015-2016 Snowflake Computing
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.snowflake.spark.snowflake.benchmarks

import net.snowflake.spark.snowflake.Utils._

import scala.collection.mutable

class TPCHSuite extends PerformanceSuite {

  override var requiredParams = {
    val map = new mutable.LinkedHashMap[String, String]
    map.put("TPCHSuite", "")
    map
  }
  override var acceptedArguments = {
    val map = new mutable.LinkedHashMap[String, Set[String]]
    map.put("TPCHSuite", Set("*"))
    map
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (runTests) {
      val lineitem = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "LINEITEM")
        .option("sfSchema", "TPCH_SF1")
        .load()

      lineitem.createOrReplaceTempView("LINEITEM")

      val orders = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "ORDERS")
        .option("sfSchema", "TPCH_SF1")
        .load()

      orders.createOrReplaceTempView("ORDERS")

      val partsupp = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "PARTSUPP")
        .option("sfSchema", "TPCH_SF1")
        .load()

      partsupp.createOrReplaceTempView("PARTSUPP")

      val part = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "PART")
        .option("sfSchema", "TPCH_SF1")
        .load()

      part.createOrReplaceTempView("PART")

      val supplier = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "SUPPLIER")
        .option("sfSchema", "TPCH_SF1")
        .load()

      supplier.createOrReplaceTempView("SUPPLIER")

      val customer = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "CUSTOMER")
        .option("sfSchema", "TPCH_SF1")
        .load()

      customer.createOrReplaceTempView("CUSTOMER")

      val nation = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "NATION")
        .option("sfSchema", "TPCH_SF1")
        .load()

      nation.createOrReplaceTempView("NATION")

      val region = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "REGION")
        .option("sfSchema", "TPCH_SF1")
        .load()

      region.createOrReplaceTempView("REGION")
    }
  }

  test("TPCH-Q01") {
    testQuery(s"""select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        lineitem
where
        l_shipdate <= cast(date_add('1998-12-01', -90) as varchar)
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus""",
              "TPCH-Q01")
  }

  test("TPCH-Q02") {
    testQuery(s"""select
       s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
from
       part,
       supplier,
       partsupp,
       nation,
       region
where
       p_partkey = ps_partkey
   and s_suppkey = ps_suppkey
   and p_size = 15
   and p_type like '%%BRASS'
   and s_nationkey = n_nationkey
   and n_regionkey = r_regionkey
   and r_name = 'EUROPE'
   and ps_supplycost = (
    select
           min(ps_supplycost)
      from
           partsupp,
           supplier,
           nation,
           region
     where
           p_partkey = ps_partkey
       and s_suppkey = ps_suppkey
       and s_nationkey = n_nationkey
       and n_regionkey = r_regionkey
       and r_name = 'EUROPE'
       )
order by
       s_acctbal desc,
       n_name,
       s_name,
       p_partkey
limit 100""",
              "TPCH-Q02")
  }

  test("TPCH-Q03") {
    testQuery(s"""select
       l_orderkey,
       sum(l_extendedprice*(1-l_discount)) as revenue,
       o_orderdate,
       o_shippriority
  from
       customer,
       orders,
       lineitem
where
       c_mktsegment = 'BUILDING'
   and c_custkey = o_custkey
   and l_orderkey = o_orderkey
   and o_orderdate < '1995-03-15'
   and l_shipdate > '1995-03-15'
 group by
       l_orderkey,
       o_orderdate,
       o_shippriority
 order by
       2 desc,
       o_orderdate
limit 10""",
              "TPCH-Q03")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    try {} finally {
      super.afterAll()
    }
  }

}
