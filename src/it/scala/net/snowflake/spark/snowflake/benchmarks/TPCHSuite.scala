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
        l_shipdate <= '1998-09-02'
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

  // TODO: Fix because Snowflake does not support LEFT SEMI JOINs (which exist translates to in Spark)
  /*
  test("TPCH-Q04") {
    testQuery(s"""select
       o_orderpriority,
       count(*) as order_count
  from
       orders
 where
       o_orderdate >= '1993-07-01'
   and o_orderdate < cast(date_add('1993-07-01', 92) as varchar)
   and exists (
        select
   *
          from
               lineitem
         where
               l_orderkey = o_orderkey
           and l_commitdate < l_receiptdate
              )
 group by
       o_orderpriority
 order by
      o_orderpriority""",
              "TPCH-Q04")
  }
   */

  test("TPCH-Q05") {
    testQuery(s"""select
      n_name,
      sum(l_extendedprice * (1 - l_discount)) as revenue
 from
      customer,
      orders,
      lineitem,
      supplier,
      nation,
      region
where
      c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and o_orderdate >= '1994-01-01'
  and o_orderdate < '1995-01-01'
group by
      n_name
order by
      2 desc""",
              "TPCH-Q05")
  }

  test("TPCH-Q06") {
    testQuery(s"""select
  sum(l_extendedprice*l_discount) as revenue
from
  lineitem
where
  l_shipdate >= '1994-01-01'
  and l_shipdate < '1995-01-01'
  and l_discount >= 0.05 and l_discount <= 0.07
  and l_quantity < 24""",
              "TPCH-Q06")
  }

  /* Fails because of extract function

  test("TPCH-Q07") {
    testQuery(s"""select
      supp_nation,
      cust_nation,
      l_year, sum(v1) as revenue
 from (
   select
          n1.n_name as supp_nation,
          n2.n_name as cust_nation,
          extract(year from l_shipdate) as l_year,
          l_extendedprice * (1 - l_discount) as v1
     from
          supplier,
          lineitem,
          orders,
          customer,
          nation n1,
          nation n2
    where
          s_suppkey = l_suppkey
      and o_orderkey = l_orderkey
      and c_custkey = o_custkey
      and s_nationkey = n1.n_nationkey
      and c_nationkey = n2.n_nationkey
      and (
             (n1.n_name = 'FRANCE'  and n2.n_name = 'GERMANY')
          or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
          )
          and l_shipdate between '1995-01-01' and '1996-12-31'
      ) shipping
group by
      supp_nation,
      cust_nation,
      l_year
order by
      supp_nation,
      cust_nation,
      l_year""", "TPCH-Q07")
  }



  test("TPCH-Q08") {
    testQuery(s"""select
      o_year,
      sum(case
          when nation = 'BRAZIL'
          then v1
          else 0
          end) / sum(v1) as mkt_share
 from (
   select
          extract(year from o_orderdate) as o_year,
          l_extendedprice * (1-l_discount) as v1,
          n2.n_name as nation
     from
          part,
          supplier,
          lineitem,
          orders,
          customer,
          nation n1,
          nation n2,
          region
    where
          p_partkey = l_partkey
      and s_suppkey = l_suppkey
      and l_orderkey = o_orderkey
      and o_custkey = c_custkey
      and c_nationkey = n1.n_nationkey
      and n1.n_regionkey = r_regionkey
      and r_name = 'AMERICA'
      and s_nationkey = n2.n_nationkey
      and o_orderdate between '1995-01-01' and '1996-12-31'
      and p_type = 'ECONOMY ANODIZED STEEL'
     ) all_nations
group by
      o_year
order by
      o_year""", "TPCH-Q08")
  }

  test("TPCH-Q09") {
    testQuery(s"""select
      nation,
      o_year,
      sum(amount) as sum_profit
 from (
   select
          n_name as nation,
          extract(year from o_orderdate) as o_year,
          l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
     from
          part,
          supplier,
          lineitem,
          partsupp,
          orders,
          nation
    where
          s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%blue%'
      ) profit
group by
      nation,
      o_year
order by
      nation,
      o_year desc""", "TPCH-Q09")
  }

*/

  test("TPCH-Q10") {
    testQuery(s"""select
      c_custkey,
      c_name,
      sum(l_extendedprice * (1 - l_discount)) as revenue,
      c_acctbal,
      n_name,
      c_address,
      c_phone,
      c_comment
 from
      customer,
      orders,
      lineitem,
      nation
where
      c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= '1993-10-01'
  and o_orderdate < '1994-01-01'
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
      c_custkey,
      c_name,
      c_acctbal,
      c_phone,
      n_name,
      c_address,
      c_comment
order by
      3 desc
limit 20""",
              "TPCH-Q10")
  }

  // Having clause not yet supported for pushdown, but query can work
  test("TPCH-Q11") {
    testQuery(s"""select ps_partkey,
      sum(ps_supplycost * ps_availqty) as value
    from
      partsupp,
      supplier,
      nation
    where
      ps_suppkey = s_suppkey
 and s_nationkey = n_nationkey
 and n_name = 'GERMANY'
    group by
     ps_partkey
   having
     sum(ps_supplycost * ps_availqty) > (
   select
          sum(ps_supplycost * ps_availqty) * 0.0001
     from
          partsupp,
          supplier,
          nation
    where
          ps_suppkey = s_suppkey
      and s_nationkey = n_nationkey
      and n_name = 'GERMANY'
    )
    order by
     2 desc
limit 20""",
              "TPCH-Q11")
  }

  test("TPCH-Q12") {
    testQuery(s"""select
        l_shipmode,
        sum(case
            when o_orderpriority ='1-URGENT'
            or o_orderpriority ='2-HIGH'
            then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
            else 0
        end) as low_line_count
  from
       orders,
       lineitem
 where
       o_orderkey = l_orderkey
   and l_shipmode in ('MAIL', 'SHIP')
   and l_commitdate < l_receiptdate
   and l_shipdate < l_commitdate
   and l_receiptdate >= '1994-01-01'
   and l_receiptdate < '1995-01-01'
 group by
       l_shipmode
 order by
       l_shipmode""",
              "TPCH-Q12")
  }

  test("TPCH-Q13") {
    testQuery(s"""select
      c_count,
      count(*) as custdist
 from (
   select
          c_custkey,
          count(o_orderkey) as c_count
     from
          customer
     left outer join
          orders
       on
          c_custkey = o_custkey
          and o_comment not like '%%special%%requests%%'
          group by
          c_custkey
      ) c_orders
group by
      c_count
order by
      2 desc,
      c_count desc""",
              "TPCH-Q13")
  }

  test("TPCH-Q14") {
    testQuery(s"""select
        100.00 * sum(case
        when p_type like 'PROMO%%'
        then l_extendedprice*(1-l_discount)
        else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
   from
        lineitem,
        part
  where l_partkey = p_partkey
    and l_shipdate >= '1995-09-01'
    and l_shipdate < '1995-10-01'""",
              "TPCH-Q14")
  }

// Converts to scalar-subquery, does not push down
  test("TPCH-Q15") {

    val partsupp = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", "PARTSUPP")
      .option("sfSchema", "TPCH_SF1")
      .load()

    val revenue = sparkSession.sql(s"""select
      l_suppkey as supplier_no,
      sum(l_extendedprice * (1 - l_discount)) as total_revenue
from
      lineitem
where
      l_shipdate >= '1998-12-01'
      and l_shipdate <  '1999-03-01'
group by l_suppkey""")

    revenue.createOrReplaceTempView("revenue")

    testQuery(s"""select
      s_suppkey,
      s_name,
      s_address,
      s_phone,
      total_revenue
 from
      supplier,
      revenue
where
      s_suppkey = supplier_no
      and total_revenue = (
       select
              max(total_revenue)
         from
              revenue
      )
order by
      s_suppkey""", "TPCH-Q15")
  }

  // Anti-join failure, will not pushdown. Analysis sometimes fails.
/*
  test("TPCH-Q16") {
    testQuery(s"""select
      p_brand,
      p_type,
      p_size,
      count(distinct ps_suppkey) as supplier_cnt
 from
      partsupp,
      part
where
      p_partkey = ps_partkey
  and p_brand <> 'Brand#45'
  and p_type not like 'MEDIUM POLISHED%%'
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  and ps_suppkey not in (
   select
          s_suppkey
     from
          supplier
    where
          s_comment like '%%Customer%%Complaints%%'
      )
group by
      p_brand,
      p_type,
      p_size
order by
      4 desc,
      p_brand,
      p_type,
      p_size
limit 20""", "TPCH-Q16")
  } */


  test("TPCH-Q17") {
    testQuery(s"""select
      sum(l_extendedprice) / 7.0 as avg_yearly
 from
      lineitem,
      part
where
      p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
   select
          0.2 * avg(l_quantity)
     from
          lineitem
    where
          l_partkey = p_partkey
     )""",
              "TPCH-Q17")
  }

  // Left-Semi join does not pushdown
  test("TPCH-Q18") {
    testQuery(s"""select
      c_name,
      c_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice,
      sum(l_quantity) sum_quantity
 from
      customer,
      orders,
      lineitem
where
      o_orderkey in (
          select
                 l_orderkey
            from
                 lineitem
           group by
                 l_orderkey
          having
                 sum(l_quantity) > 300
       )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
      c_name,
      c_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice
order by
      o_totalprice desc,
      o_orderdate
limit 100""",
              "TPCH-Q18")

  }

  test("TPCH-Q19") {
    testQuery(s"""select
      sum(l_extendedprice * (1 - l_discount) ) as revenue
 from
      lineitem,
      part
where
    (
      p_partkey = l_partkey
  and p_brand = 'Brand#12'
  and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
  and l_quantity >= 1 and l_quantity <= 1 + 10
  and p_size between 1 and 5
  and l_shipmode in ('AIR', 'AIR REG')
  and l_shipinstruct = 'DELIVER IN PERSON'
    )
   or
    (
      p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
  and l_quantity >= 10 and l_quantity <= 10 + 10
  and p_size between 1 and 10
  and l_shipmode in ('AIR', 'AIR REG')
  and l_shipinstruct = 'DELIVER IN PERSON'
    )
   or
    (
      p_partkey = l_partkey
  and p_brand = 'Brand#34'
  and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
  and l_quantity >= 20 and l_quantity <= 20 + 10
  and p_size between 1 and 15
  and l_shipmode in ('AIR', 'AIR REG')
  and l_shipinstruct = 'DELIVER IN PERSON'
    )""",
              "TPCH-Q19")
  }

  // Left-Semi
  test("TPCH-Q20") {
    testQuery(s"""select
      s_name,
      s_address
 from
      supplier,
      nation
where
      s_suppkey in (
           select
                  ps_suppkey
            from
                  partsupp
           where
                  ps_partkey in (
                      select
                             p_partkey
                        from
                             part
                       where
                             p_name like 'forest%%'
                 )
  and ps_availqty >(
           select
                  0.5 * sum(l_quantity)
             from
                  lineitem
            where
                  l_partkey = ps_partkey
              and l_suppkey = ps_suppkey
              and l_shipdate >= '1994-01-01'
              and l_shipdate < '1995-01-01'
                   )
      )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
      s_name
limit 30""",
              "TPCH-Q20")
  }

  // Left-Semi
  test("TPCH-Q21") {
    testQuery(s"""select
      s_name,
      count(*) as numwait
 from
      supplier,
      lineitem l1,
      orders,
      nation
where
      s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
       select
              *
         from
              lineitem l2
        where
              l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
      )
  and not exists (
       select
              *
         from
              lineitem l3
        where
              l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          and l3.l_receiptdate > l3.l_commitdate
      )
  and s_nationkey = n_nationkey
  and n_name = 'SAUDI ARABIA'
group by
      s_name
order by
      2 desc,
      s_name
limit 100""",
              "TPCH-Q21")
  }

  // Scalar subquery
  test("TPCH-Q22") {
    testQuery(s"""select
      cntrycode,
      count(*) as numcust,
      sum(c_acctbal) as totacctbal
 from (
   select
          substr(c_phone, 1, 2) as cntrycode,
          c_acctbal
     from
          customer
    where
          substr(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
      and c_acctbal > (
           select
                  avg(c_acctbal)
             from
                  customer
            where
                  c_acctbal > 0.00
              and substr(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
          )
      and not exists (
           select
                  *
             from
                  orders
            where
                  o_custkey = c_custkey
          )
      ) custsale
group by
      cntrycode
order by
      cntrycode""",
              "TPCH-Q22")
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
