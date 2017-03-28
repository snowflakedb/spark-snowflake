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
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class TPCDSSuite extends PerformanceSuite {

  protected final var s3RootDir: String = ""

  protected final val tables: Seq[String] = Seq("call_center",
                                                "catalog_page",
                                                "catalog_returns",
                                                "catalog_sales",
                                                "customer",
                                                "customer_address",
                                                "customer_demographics",
                                                "date_dim",
                                                "dbgen_version",
                                                "household_demographics",
                                                "income_band",
                                                "inventory",
                                                "item",
                                                "promotion",
                                                "reason",
                                                "ship_mode",
                                                "store",
                                                "store_returns",
                                                "store_sales",
                                                "time_dim",
                                                "warehouse",
                                                "web_page",
                                                "web_returns",
                                                "web_sales",
                                                "web_site")

  override var requiredParams = {
    val map = new mutable.LinkedHashMap[String, String]
    map.put("TPCDSSuite", "")
    map
  }

  override var acceptedArguments = {
    val map = new mutable.LinkedHashMap[String, Set[String]]
    map.put("TPCDSSuite", Set("*"))
    map
  }

  override protected var dataSources: mutable.LinkedHashMap[
    String,
    Map[String, DataFrame]] =
    new mutable.LinkedHashMap[String, Map[String, DataFrame]]

  private def registerDF(tableName: String): Unit = {

    val sf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName.toUpperCase)
      .load()

    val sf_stage =
      if (internalStage)
        sparkSession.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(connectorOptionsNoTable.filterNot(x => x._1 == "tempDir"))
          .option("dbtable", tableName.toUpperCase)
          .load()
      else sf

    val parquet =
      if (s3Parquet)
        sparkSession.read
          .schema(sf.schema)
          .parquet(s3RootDir + s"/$tableName/parquet")
      else sf

    val csv =
      if (s3CSV)
        sparkSession.read
          .schema(sf.schema)
          .option("delimiter", "|")
          .csv(s3RootDir + s"/$tableName/csv")
      else sf

    val jdbc =
      if (jdbcSource)
        sparkSession.read
          .schema(sf.schema)
          .jdbc(jdbcURL, tableName, jdbcProperties)
      else sf

    dataSources.put(tableName.toUpperCase,
                    Map("parquet"         -> parquet,
                        "csv"             -> csv,
                        "snowflake"       -> sf,
                        "snowflake-stage" -> sf_stage,
                        "jdbc"            -> jdbc))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (runTests) {
      if (s3Parquet || s3CSV)
        s3RootDir = getConfigValue("s3SourceFilesRoot")

      tables.foreach(registerDF)
    }
  }

  test("TPCDS-Q01") {
    testQuery(s"""with customer_total_return as
      (select sr_customer_sk as ctr_customer_sk,
      sr_store_sk as ctr_store_sk
      ,sum(SR_FEE) as ctr_total_return
      from store_returns
      ,date_dim
      where sr_returned_date_sk = d_date_sk
      and d_year =2002
      group by sr_customer_sk
      ,sr_store_sk)
      select  c_customer_id
      from customer_total_return ctr1
      ,store
      ,customer
      where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
      from customer_total_return ctr2
      where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
      and s_store_sk = ctr1.ctr_store_sk
      and s_state = 'TN'
      and ctr1.ctr_customer_sk = c_customer_sk
      order by c_customer_id
      limit 100""",
              "TPCDS-Q01")
  }

  test("TPCDS-Q02") {
    testQuery(s"""with wscs as
 (select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales) x
        union all
       (select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales)),
 wswscs as
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs
     ,date_dim
 where d_date_sk = sold_date_sk
 group by d_week_seq)
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2)
       ,round(mon_sales1/mon_sales2,2)
       ,round(tue_sales1/tue_sales2,2)
       ,round(wed_sales1/wed_sales2,2)
       ,round(thu_sales1/thu_sales2,2)
       ,round(fri_sales1/fri_sales2,2)
       ,round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,tue_sales tue_sales1
        ,wed_sales wed_sales1
        ,thu_sales thu_sales1
        ,fri_sales fri_sales1
        ,sat_sales sat_sales1
  from wswscs,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 2001) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2
        ,mon_sales mon_sales2
        ,tue_sales tue_sales2
        ,wed_sales wed_sales2
        ,thu_sales thu_sales2
        ,fri_sales fri_sales2
        ,sat_sales sat_sales2
  from wswscs
      ,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 2001+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1""",
              "TPCDS-Q02")
  }

  test("TPCDS-Q03") {
    testQuery(s"""select  dt.d_year
       ,item.i_brand_id brand_id
       ,item.i_brand brand
       ,sum(ss_ext_sales_price) sum_agg
 from  date_dim dt
      ,store_sales
      ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = 128
   and dt.d_moy=11
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 limit 100
""",
              "TPCDS-Q03")
  }

  test("TPCDS-Q017") {
    testQuery(s"""select  i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as_store_returns_quantitycount
       ,avg(sr_return_quantity) as_store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as_store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales
     ,store_returns
     ,catalog_sales
     ,date_dim d1
     ,date_dim d2
     ,date_dim d3
     ,store
     ,item
 where d1.d_quarter_name = '2000Q1'
   and d1.d_date_sk = ss_sold_date_sk
   and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk
   and ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
   and sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
   and sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
   and cs_sold_date_sk = d3.d_date_sk
   and d3.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
 group by i_item_id
         ,i_item_desc
         ,s_state
 order by i_item_id
         ,i_item_desc
         ,s_state
limit 100""",
              "TPCDS-Q17")
  }

  test("TPCDS-Q21") {
    testQuery(s"""select  *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when (cast(d_date as date) < cast ('1998-04-08' as date))
	                then inv_quantity_on_hand
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as date) >= cast ('1998-04-08' as date))
                      then inv_quantity_on_hand
                      else 0 end) as inv_after
   from inventory
       ,warehouse
       ,item
       ,date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and d_date between date_add(to_date('1998-04-08'), -30)
     	 	      and date_add(to_date('1998-04-08'), 30)
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0
             then inv_after / inv_before
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 limit 100""",
              "TPCDS-Q21")
  }

  test("TPCDS-Q26") {
    testQuery(s"""select  i_item_id,
        avg(cs_quantity) agg1,
        avg(cs_list_price) agg2,
        avg(cs_coupon_amt) agg3,
        avg(cs_sales_price) agg4
 from catalog_sales, customer_demographics, date_dim, item, promotion
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd_demo_sk and
       cs_promo_sk = p_promo_sk and
       cd_gender = 'F' and
       cd_marital_status = 'U' and
       cd_education_status = '4 yr Degree' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 1999
 group by i_item_id
 order by i_item_id
 limit 100""",
              "TPCDS-Q26")
  }

  test("TPCDS-Q32") {
    testQuery(s"""select  sum(cs_ext_discount_amt) as excess
from
   catalog_sales
   ,item
   ,date_dim
where
i_manufact_id = 977
and i_item_sk = cs_item_sk
and d_date between to_date('2000-01-27') and
    date_add(to_date('2000-01-27'),90)
and d_date_sk = cs_sold_date_sk
and cs_ext_discount_amt
     > (
         select
            1.3 * avg(cs_ext_discount_amt)
         from
            catalog_sales
           ,date_dim
         where
              cs_item_sk = i_item_sk
          and d_date between to_date('2000-01-27') and
	      	     	     date_add(to_date('2000-01-27'), 90)
          and d_date_sk = cs_sold_date_sk
      )
limit 100""",
              "TPCDS-Q32")
  }

  test("TPCDS-Q42") {
    testQuery(s"""select  dt.d_year
 	,item.i_category_id
 	,item.i_category
 	,sum(ss_ext_sales_price)
 from 	date_dim dt
 	,store_sales
 	,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
 	and store_sales.ss_item_sk = item.i_item_sk
 	and item.i_manager_id = 1
 	and dt.d_moy=9
 	and dt.d_year=1998
 group by 	dt.d_year
 		,item.i_category_id
 		,item.i_category
 order by       sum(ss_ext_sales_price) desc,dt.d_year
 		,item.i_category_id
 		,item.i_category
limit 100""",
              "TPCDS-Q42")
  }

  test("TPCDS-Q58") {
    testQuery(s"""with ss_items as
 (select i_item_id item_id
        ,sum(ss_ext_sales_price) ss_item_rev
 from store_sales
     ,item
     ,date_dim
 where ss_item_sk = i_item_sk
   and d_date in (select d_date
                  from date_dim
                  where d_week_seq = (select d_week_seq
                                      from date_dim
                                      where d_date = '2000-01-03'))
   and ss_sold_date_sk   = d_date_sk
 group by i_item_id),
 cs_items as
 (select i_item_id item_id
        ,sum(cs_ext_sales_price) cs_item_rev
  from catalog_sales
      ,item
      ,date_dim
 where cs_item_sk = i_item_sk
  and  d_date in (select d_date
                  from date_dim
                  where d_week_seq = (select d_week_seq
                                      from date_dim
                                      where d_date = '2000-01-03'))
  and  cs_sold_date_sk = d_date_sk
 group by i_item_id),
 ws_items as
 (select i_item_id item_id
        ,sum(ws_ext_sales_price) ws_item_rev
  from web_sales
      ,item
      ,date_dim
 where ws_item_sk = i_item_sk
  and  d_date in (select d_date
                  from date_dim
                  where d_week_seq =(select d_week_seq
                                     from date_dim
                                      where d_date = '2000-01-03'))
  and ws_sold_date_sk   = d_date_sk
 group by i_item_id)
  select  ss_items.item_id
       ,ss_item_rev
       ,ss_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ss_dev
       ,cs_item_rev
       ,cs_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 cs_dev
       ,ws_item_rev
       ,ws_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ws_dev
       ,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
 from ss_items,cs_items,ws_items
 where ss_items.item_id=cs_items.item_id
   and ss_items.item_id=ws_items.item_id
   and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
   and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
   and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
   and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
 order by item_id
         ,ss_item_rev
 limit 100""",
              "TPCDS-Q58")
  }

  test("TPCDS-Q83") {
    testQuery(s"""with sr_items as
 (select i_item_id item_id,
        sum(sr_return_quantity) sr_item_qty
 from store_returns,
      item,
      date_dim
 where sr_item_sk = i_item_sk
 and   d_date    in
	(select d_date
	from date_dim
	where d_week_seq in
		(select d_week_seq
		from date_dim
	  where d_date in ('1998-01-02','1998-10-15','1998-11-10')))
 and   sr_returned_date_sk   = d_date_sk
 group by i_item_id),
 cr_items as
 (select i_item_id item_id,
        sum(cr_return_quantity) cr_item_qty
 from catalog_returns,
      item,
      date_dim
 where cr_item_sk = i_item_sk
 and   d_date    in
	(select d_date
	from date_dim
	where d_week_seq in
		(select d_week_seq
		from date_dim
	  where d_date in ('1998-01-02','1998-10-15','1998-11-10')))
 and   cr_returned_date_sk   = d_date_sk
 group by i_item_id),
 wr_items as
 (select i_item_id item_id,
        sum(wr_return_quantity) wr_item_qty
 from web_returns,
      item,
      date_dim
 where wr_item_sk = i_item_sk
 and   d_date    in
	(select d_date
	from date_dim
	where d_week_seq in
		(select d_week_seq
		from date_dim
		where d_date in ('1998-01-02','1998-10-15','1998-11-10')))
 and   wr_returned_date_sk   = d_date_sk
 group by i_item_id)
  select  sr_items.item_id
       ,sr_item_qty
       ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev
       ,cr_item_qty
       ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev
       ,wr_item_qty
       ,wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev
       ,(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
 from sr_items
     ,cr_items
     ,wr_items
 where sr_items.item_id=cr_items.item_id
   and sr_items.item_id=wr_items.item_id
 order by sr_items.item_id
         ,sr_item_qty
 limit 100""",
              "TPCDS-Q05")
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
