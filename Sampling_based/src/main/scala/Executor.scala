package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import java.time._
import java.time.format._
import java.nio.file.{Paths, Files}

object Executor {

  def calcDate(date: String, dateType: String, interval: Long, minus: Boolean): String = {
    val toDate = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val initialDate = LocalDate.parse(date, toDate)
    var returnDate = ""

    if (minus) {
      dateType match {
        case "day"   => returnDate = initialDate.minusDays(interval).toString()
        case "month" => returnDate = initialDate.minusMonths(interval).toString()
        case "year"  => returnDate = initialDate.minusYears(interval).toString()
      }
    } else {
      dateType match {
        case "day"   => returnDate = initialDate.plusDays(interval).toString()
        case "month" => returnDate = initialDate.plusMonths(interval).toString()
        case "year"  => returnDate = initialDate.plusYears(interval).toString()
      }
    }

    println("date: " + date + " ; final date: " + returnDate)
    returnDate
  }

  def get_sample(desc: Description, query: String, session: SparkSession): DataFrame= {
    if (Files.exists(Paths.get(desc.samplePath + query))){
      session.read.parquet(desc.samplePath + query)
    }
    else {
      println("There is no sample for query ", query)
      null
    }
  }

  def execute_Q1(desc: Description, session: SparkSession, params: List[Any]) = {
    //select
    //	l_returnflag,
    //	l_linestatus,
    //	sum(l_quantity) as sum_qty,
    //	sum(l_extendedprice) as sum_base_price,
    //	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    //	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    //	avg(l_quantity) as avg_qty,
    //	avg(l_extendedprice) as avg_price,
    //	avg(l_discount) as avg_disc,
    //	count(*) as count_order
    //from
    //	lineitem
    //where
    //	l_shipdate <= date '1998-12-01' - interval ':1' day (3)
    //group by
    //	l_returnflag,
    //	l_linestatus
    //order by
    //	l_returnflag,
    //	l_linestatus;
    import session.implicits._

    val sample = get_sample(desc, "Q1", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      println("There is a sample available for this query. The result on full dataset:")

      var t0 = System.currentTimeMillis()
      val gt = execute(lineitem); gt.show()
      var t1 = System.currentTimeMillis()
      println("Execution time on full data: " + (t1 - t0) + "ms")

      val precise_c = lineitem.groupBy("l_returnflag", "l_linestatus").count()
      println("Result on the sampled dataset: ")
      t0 = System.currentTimeMillis()
      var estimate = execute(sample); //estimate.show()

      val sample_c = sample.groupBy("l_returnflag", "l_linestatus").count()
      val fraction = sample_c.join(precise_c, Seq("l_returnflag", "l_linestatus")).withColumn("fraction", sample_c("count")/precise_c("count"))

      println("Revised estimation")
      estimate = fraction.join(estimate,  Seq("l_returnflag", "l_linestatus"))
        .withColumn("sum_qty", estimate("sum_qty")/fraction("fraction") )
          .withColumn("sum_base_price", estimate("sum_base_price")/fraction("fraction"))
          .withColumn("sum_disc_price", estimate("sum_disc_price")/fraction("fraction"))
          .withColumn("sum_charge", estimate("sum_charge")/fraction("fraction"))
          .withColumn("count_order", estimate("count_order")/fraction("fraction")).drop("fraction")
      estimate.show()

      t1 = System.currentTimeMillis()
      println("Execution time on sampled data: " + (t1 - t0) + "ms")

      val errors = gt.join(estimate, Seq("l_returnflag", "l_linestatus"))
        .withColumn("sum_qty_error", (estimate("sum_qty") - gt("sum_qty"))/gt("sum_qty")*100)
        .withColumn("sum_base_price_error", (estimate("sum_base_price") - gt("sum_base_price"))/gt("sum_base_price")*100)
        .withColumn("sum_disc_price_error", (estimate("sum_disc_price") - gt("sum_disc_price"))/gt("sum_disc_price")*100)
        .withColumn("sum_charge_error", (estimate("sum_charge") - gt("sum_charge"))/gt("sum_charge")*100)
        .withColumn("avg_qty_error", (estimate("avg_qty") - gt("avg_qty"))/gt("avg_qty")*100)
        .withColumn("avg_price_error", (estimate("avg_price") - gt("avg_price"))/gt("avg_price")*100)
        .withColumn("avg_disc_error", (estimate("avg_disc") - gt("avg_disc"))/gt("avg_disc")*100)
        .withColumn("count_order_error", (estimate("count_order") - gt("count_order"))/gt("count_order")*100)
        .select("l_returnflag", "l_linestatus", "sum_qty_error", "sum_base_price_error", "sum_disc_price_error", "sum_charge_error", "avg_qty_error", "avg_price_error", "avg_disc_error", "count_order_error")

      errors.select(errors.columns.map(mean(_)): _*).show()

  }

    def execute(df: DataFrame) = {
      val multiply = udf { (x: Double) => x * proportion }
      val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
      val increase = udf { (x: Double, y: Double) => x * (1 + y) }

      // Calculate true date
      val dateInterval: Long = params(0).toString().toLong
      val date: String = calcDate("1998-12-01", "day", dateInterval, true)

      val where_ = df.filter(col("l_shipdate") <= date)
      val grBy_ = where_.groupBy("l_returnflag", "l_linestatus")

      val result = grBy_.agg(
        sum("l_quantity").as("sum_qty"),
        sum("l_extendedprice").as("sum_base_price"),
        sum(decrease( col("l_extendedprice"), col("l_discount"))).as("sum_disc_price"),
        sum(increase(decrease(col("l_extendedprice"), col("l_discount")), col("l_tax"))).as("sum_charge"),
        avg("l_quantity").as("avg_qty"),
        avg("l_extendedprice").as("avg_price"),
        avg("l_discount").as("avg_disc"),
        count("l_quantity").as("count_order"))

      result.sort("l_returnflag", "l_linestatus")
    }

  }

  def execute_Q3(desc: Description, session: SparkSession, params: List[Any]) = {
    // select
    //	l_orderkey,
    //	sum(l_extendedprice * (1 - l_discount)) as revenue,
    //	o_orderdate,
    //	o_shippriority
    //from
    //	customer,
    //	orders,
    //	lineitem
    //where
    //	c_mktsegment = ':1'
    //	and c_custkey = o_custkey
    //	and l_orderkey = o_orderkey
    //	and o_orderdate < date ':2'
    //	and l_shipdate > date ':2'
    //group by
    //	l_orderkey,
    //	o_orderdate,
    //	o_shippriority
    //order by
    //	revenue desc,
    //	o_orderdate;

    assert(params.size == 2)
    // https://github.com/electrum/tpch-dbgen/blob/master/queries/3.sql
    // using:
    // params(0) as :1
    // params(1) as :2
    import session.implicits._

    val sample = get_sample(desc, "Q3", session)
    var lineitem: DataFrame = desc.lineitem

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      println("Using sample")
      println("Original Data")
      execute(lineitem).show(20)
      println("Sample")
      execute(sample).show(20)
    }

    def execute(df: DataFrame): DataFrame = {
      val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

      val customFilter = desc.customer.filter($"c_mktsegment" === params(0))
      val orderFilter = desc.orders.filter($"o_orderdate" < params(1))
      val lineitemFilter = df.filter($"l_shipdate" > params(1))

      val where_ = customFilter.join(orderFilter, $"c_custkey" === orderFilter("o_custkey"))
        .select($"o_orderkey", $"o_orderdate", $"o_shippriority")
        .join(lineitemFilter, $"o_orderkey" === lineitemFilter("l_orderkey"))

      val select_ = where_.select(
        $"l_orderkey",
        decrease($"l_extendedprice", $"l_discount").as("part_revenue"),
        $"o_orderdate", $"o_shippriority")

      val grBy_ = select_.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
      val result = grBy_.agg(sum($"part_revenue").as("revenue"))

      result.sort($"revenue".desc, $"o_orderdate").limit(10)
    }


  }

  def execute_Q5(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._
    val sample = get_sample(desc, "Q5", session)
    var lineitem: DataFrame = desc.lineitem

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      println("Using sample")
      println("Original Data")
      execute(lineitem).show(20)
      println("Sample")
      execute(sample).show(20)
    }


    def execute(df: DataFrame): DataFrame = {
      val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

      val rname: String = params(0).toString()
      val firstDate: String = params(1).toString()
      val secondDate = calcDate(firstDate, "year", 1, true)

      val orderFilter = desc.orders.filter($"o_orderdate" < firstDate && $"o_orderdate" >= secondDate)
      val regionFilter = desc.region.filter($"r_name" === rname)

      val where_ = regionFilter.join(desc.nation, $"r_regionkey" === desc.nation("n_regionkey"))
        .join(desc.supplier, $"n_nationkey" === desc.supplier("s_nationkey"))
        .join(df, $"s_suppkey" === df("l_suppkey"))
        .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")

      val where2_ = where_.join(orderFilter, $"l_orderkey" === orderFilter("o_orderkey"))
        .join(desc.customer, $"o_custkey" === desc.customer("c_custkey") && $"s_nationkey" === desc.customer("c_nationkey"))

      val select_ = where2_.select($"n_name", decrease($"l_extendedprice", $"l_discount").as("part_revenue"))

      val grBy_ = select_.groupBy($"n_name")

      grBy_.agg(sum($"part_revenue").as("revenue")).sort($"revenue".desc)
    }


  }

  def execute_Q6(desc: Description, session: SparkSession, params: List[Any]) = {

    //select
    //	sum(l_extendedprice * l_discount) as revenue
    //from
    //	lineitem
    //where
    //	l_shipdate >= date ':1'
    //	and l_shipdate < date ':1' + interval '1' year
    //	and l_discount between :2 - 0.01 and :2 + 0.01
    //	and l_quantity < :3;

    import session.implicits._
    val sample = get_sample(desc, "Q6", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      println("There is a sample available for this query. The result on full dataset:")

      var t0 = System.currentTimeMillis()
      val gt = execute(lineitem); gt.show()
      var t1 = System.currentTimeMillis()
      println("Execution time on full data: " + (t1 - t0) + "ms")

      println("Result on the sampled dataset: ")
      proportion = desc.lineitem.count().toDouble / sample.count().toDouble
      t0 = System.currentTimeMillis()
      var estimate = execute(sample); estimate.show()
      t1 = System.currentTimeMillis()
      println("Execution time on sampled data: " + (t1 - t0) + "ms")

      println(execute(lineitem).collectAsList())
      println(estimate.collectAsList())

    }

    def execute(df: DataFrame): DataFrame = {
      val multiply = udf { (x: Double) => x * proportion }

      val firstDate: String = params(0).toString()
      val secondDate: String = calcDate(firstDate, "year", 1, false)

      val givenDiscount: Double = Math.round(params(1).toString().toDouble * 100) / 100.0
      val firstDiscount = Math.round((givenDiscount - 0.01) * 100) / 100.0
      val secondDiscount = Math.round((givenDiscount + 0.01) * 100) / 100.0

      val givenQuantity: Int = params(2).toString.toInt

      df.where(
        $"l_shipdate" >= firstDate && $"l_shipdate" < secondDate &&
          $"l_discount" >= firstDiscount && $"l_discount" <= secondDiscount &&
          $"l_quantity" < givenQuantity)
        .agg(multiply(sum($"l_extendedprice" * $"l_discount")).as("revenue"))
    }

  }

  def execute_Q7(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._
    val sample = get_sample(desc, "Q7", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0
    val order = desc.orders

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      proportion = desc.lineitem.count().toDouble / sample.count().toDouble

      var t0 = System.currentTimeMillis()
      val gt = execute(lineitem); gt.show()
      var t1 = System.currentTimeMillis()
      println("Execution time on full data: " + (t1 - t0) + "ms")

      println("Result on the sampled dataset: ")
      t0 = System.currentTimeMillis()
      var estimate = execute(sample); estimate.show()
      t1 = System.currentTimeMillis()
      println("Execution time on sampled data: " + (t1 - t0) + "ms")
    }

    def execute(frame: DataFrame) : DataFrame = {
      val lineitem = frame
      val order = desc.orders
      val partsupp = desc.partsupp
      val nation = desc.nation
      val supplier = desc.supplier
      val customer = desc.customer

      val getYear = udf { (x: String) => x.substring(0, 4) }
      val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

      val n1name: String = params(0).toString()
      val n2name: String = params(1).toString()

      val nationFilter = nation.filter($"n_name" === n1name || $"n_name" === n2name)

      val lineitemFilter = lineitem.filter($"l_shipdate" >= "1995-01-01" && $"l_shipdate" <= "1996-12-31")

      val supNation = nationFilter
        .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
        .join(lineitemFilter, $"s_suppkey" === lineitemFilter("l_suppkey"))
        .select($"n_name".as("supp_nation"), $"l_orderkey", $"l_extendedprice", $"l_discount", $"l_shipdate")

      val custNation = nationFilter
        .join(customer, $"n_nationkey" === customer("c_nationkey"))
        .join(order, $"c_custkey" === order("o_custkey"))
        .select($"n_name".as("cust_nation"), $"o_orderkey")

      val nationFilterJoin = custNation.join(supNation, $"o_orderkey" === supNation("l_orderkey"))
        .filter(($"supp_nation" === n1name && $"cust_nation" === n2name) ||
          ($"supp_nation" === n2name && $"cust_nation" === n1name))

      val select_ = nationFilterJoin.select(
        $"supp_nation",
        $"cust_nation",
        getYear($"l_shipdate").as("l_year"),
        decrease($"l_extendedprice", $"l_discount").as("volume"))

      val grBy_ = select_.groupBy($"supp_nation", $"cust_nation", $"l_year")
        .agg(sum($"volume").as("revenue"))

      grBy_.sort($"supp_nation", $"cust_nation", $"l_year")
    }

  }

  def execute_Q9(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._
    val sample = get_sample(desc, "Q9", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0
    val order = desc.orders

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      proportion = desc.lineitem.count().toDouble / sample.count().toDouble
      println("Using sample")
      println("Original Data")
      execute(lineitem).show(20)
      println("Sample")
      execute(sample).show(20)
    }


    def execute(frame: DataFrame): DataFrame = {
      val lineitem = frame
      val order = desc.orders
      val partsupp = desc.partsupp
      val nation = desc.nation
      val supplier = desc.supplier
      val part = desc.part

      val extractYear = udf { (x: String) => x.substring(0, 4) }
      val expr = udf { (x: Double, y: Double, v: Double, w: Double) => x * (1 - y) - (v * w) }

      val p_name: String = params(0).toString()

      val partFilter = part.filter($"p_name".contains(p_name))

      val subQuery = partFilter
        .join(lineitem, $"p_partkey" === lineitem("l_partkey"))
        .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
        .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey") && $"l_partkey" === partsupp("ps_partkey"))
        .join(order, $"l_orderkey" === order("o_orderkey"))
        .join(nation, $"s_nationkey" === nation("n_nationkey"))
        .select(
          ($"n_name").as("nation"),
          extractYear($"o_orderdate").as("o_year"),
          expr($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity").as("amount"))

      val query = subQuery.as("profit").select(
        $"nation",
        $"o_year",
        $"amount")
        .groupBy($"nation", $"o_year").agg(sum($"amount").as("sum_profit"))

      query.sort($"nation", $"o_year".desc)
    }


  }

  def execute_Q10(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._
    val sample = get_sample(desc, "Q10", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0
    val order = desc.orders

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      proportion = desc.lineitem.count().toDouble / sample.count().toDouble

      var t0 = System.currentTimeMillis()
      val gt = execute(lineitem); gt.show()
      var t1 = System.currentTimeMillis()
      println("Execution time on full data: " + (t1 - t0) + "ms")

      println("Result on the sampled dataset: ")
      t0 = System.currentTimeMillis()
      var estimate = execute(sample); estimate.show()
      t1 = System.currentTimeMillis()
      println("Execution time on sampled data: " + (t1 - t0) + "ms")
    }

    def execute(frame: DataFrame): DataFrame = {
      val firstDate: String = params(0).toString()
      val secondDate = calcDate(firstDate, "month", 3, false)

      val order = desc.orders
      val customer = desc.customer
      val nation = desc.nation

      val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
      val multiply = udf { (x: Double) => x * proportion }

      val lineitemFilter = frame.filter($"l_returnflag" === "R")
      val orderFilter = order.filter($"o_orderdate" >= firstDate && $"o_orderdate" < secondDate)

      val where_ = orderFilter.join(customer, $"o_custkey" === customer("c_custkey"))
        .join(lineitemFilter, $"o_orderkey" === lineitemFilter("l_orderkey"))
        .join(nation, $"c_nationkey" === nation("n_nationkey"))

      val select_ = where_.select(
        $"c_custkey",
        $"c_name",
        decrease($"l_extendedprice", $"l_discount").as("part_revenue"),
        $"c_acctbal",
        $"n_name",
        $"c_address",
        $"c_phone",
        $"c_comment")

      val grBy_ = select_.groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
        .agg(multiply(sum($"part_revenue")).as("revenue"))

      grBy_.sort($"revenue".desc).limit(20)
    }


  }

  def execute_Q11(desc: Description, session: SparkSession, params: List[Any]) = {
    //select
    //	ps_partkey,
    //	sum(ps_supplycost * ps_availqty) as value
    //from
    //	partsupp,
    //	supplier,
    //	nation
    //where
    //	ps_suppkey = s_suppkey
    //	and s_nationkey = n_nationkey
    //	and n_name = ':1'
    //group by
    //	ps_partkey having
    //		sum(ps_supplycost * ps_availqty) > (
    //			select
    //				sum(ps_supplycost * ps_availqty) * :2
    //			from
    //				partsupp,
    //				supplier,
    //				nation
    //			where
    //				ps_suppkey = s_suppkey
    //				and s_nationkey = n_nationkey
    //				and n_name = ':1'
    //		)
    //order by
    //	value desc;
    import session.implicits._
    val sample = get_sample(desc, "Q11", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0
    val order = desc.orders

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      proportion = desc.lineitem.count().toDouble / sample.count().toDouble
      println("Using sample")
      println("Original Data")
      execute(lineitem).show(20)
      println("Sample")
      execute(sample).show(20)
    }

    def execute(frame: DataFrame):DataFrame = {
      val nation = desc.nation
      val supplier = desc.supplier
      val partsupp = desc.partsupp

      val n_name: String = params(0).toString()
      val multParam: Double = params(1).toString().toDouble

      val mul = udf { (x: Double, y: Int) => x * y }
      val mul2 = udf { (x: Double) => x * multParam }

      val nationFilter = nation.filter($"n_name" === n_name)

      val baseQuery = nationFilter.join(supplier, $"n_nationkey" === supplier("s_nationkey"))
        .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
        .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("part_value"))

      val having_ = baseQuery.agg(sum("part_value").as("having_value"))

      val query = baseQuery.groupBy($"ps_partkey").agg(sum("part_value").as("query_value"))
        .join(having_, $"query_value" > mul2($"having_value"))

      query.sort($"query_value".desc).select($"ps_partkey", ($"query_value").as("value"))
    }
  }

  def execute_Q12(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._
    val sample = get_sample(desc, "Q12", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0
    val order = desc.orders

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      proportion = desc.lineitem.count().toDouble / sample.count().toDouble
      println("Using sample")
      println("Original Data")
      execute(lineitem).show(20)
      println("Sample")
      execute(sample).show(20)
    }


    def execute(frame: DataFrame): DataFrame = {
      val shipmode1: String = params(0).toString()
      val shipmode2: String = params(1).toString()

      val firstDate: String = params(2).toString()
      val secondDate: String = calcDate(firstDate, "year", 1, false)

      val mul = udf { (x: Double, y: Double) => x * y }
      val highPriority = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
      val lowPriority = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

      val lineitemFilter = frame.filter((
        $"l_shipmode" === shipmode1 || $"l_shipmode" === shipmode2) &&
        $"l_commitdate" < $"l_receiptdate" &&
        $"l_shipdate" < $"l_commitdate" &&
        $"l_receiptdate" >= firstDate && $"l_receiptdate" < secondDate)

      val where_ = lineitemFilter.join(order, $"l_orderkey" === order("o_orderkey"))

      val select_ = where_.select($"l_shipmode", $"o_orderpriority")
      val grBy_ = select_.groupBy($"l_shipmode").agg(
        sum(highPriority($"o_orderpriority")).as("high_line_count"),
        sum(lowPriority($"o_orderpriority")).as("low_line_count"))

      grBy_.sort($"l_shipmode")
    }

  }

  def execute_Q17(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    val sample = get_sample(desc, "Q17", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      proportion = desc.lineitem.count().toDouble / sample.count().toDouble

      var t0 = System.currentTimeMillis()
      val gt = execute(lineitem); gt.show()
      var t1 = System.currentTimeMillis()
      println("Execution time on full data: " + (t1 - t0) + "ms")

      println("Result on the sampled dataset: ")
      t0 = System.currentTimeMillis()
      var estimate = execute(sample); estimate.show()
      t1 = System.currentTimeMillis()
      println("Execution time on sampled data: " + (t1 - t0) + "ms")
    }

    def execute(frame: DataFrame) : DataFrame =  {
      val lineitem = frame
      val part = desc.part

      val p_brand: String = params(0).toString()
      val p_container: String = params(1).toString()

      val mul = udf { (x: Double) => x * 0.2 }

      val partFilter = part.filter($"p_brand" === p_brand && $"p_container" === p_container).select($"p_partkey")

      val baseQuery = partFilter.join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")

      val subQuery = baseQuery.groupBy("p_partkey")
        .agg(mul(avg($"l_quantity")).as("sub_quantity"))
        .select($"p_partkey".as("sub_key"), $"sub_quantity")

      val result = baseQuery.join(subQuery, $"sub_key" === baseQuery("p_partkey"))
        .filter($"l_quantity" < $"sub_quantity")
        .agg((sum($"l_extendedprice") / 7.0).as("avg_yearly"))
      result
    }



  }

  def execute_Q18(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    val sample = get_sample(desc, "Q18", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      proportion = desc.lineitem.count().toDouble / sample.count().toDouble
      println("Using sample")
      println("Original Data")
      execute(lineitem).show(20)
      println("Sample")
      execute(sample).show(20)
    }

    def execute(frame: DataFrame) : DataFrame = {
      val lineitem = frame
      val order = desc.orders
      val customer = desc.customer

      val sum_quantity: Double = params(0).toString().toDouble

      val subQuery = lineitem.groupBy("l_orderkey")
        .agg(sum($"l_quantity").as("sum_quantity"))
        .filter($"sum_quantity" > sum_quantity)

      val where_ = subQuery.select($"l_orderkey".as("orderkey"), $"sum_quantity")
        .join(order, order("o_orderkey") === $"orderkey")
        .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
        .join(customer, $"o_custkey" === customer("c_custkey"))

      val select_ = where_.select($"c_name", $"c_custkey",
        $"o_orderkey", $"o_orderdate",
        $"o_totalprice", $"l_quantity")

      val grBy_ = select_.groupBy($"c_name", $"c_custkey", $"o_orderkey",
        $"o_orderdate", $"o_totalprice").agg(sum("l_quantity"))

      grBy_.sort($"o_totalprice".desc, $"o_orderdate").limit(100)
    }

  }

  def execute_Q19(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    val sample = get_sample(desc, "Q19", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      proportion = desc.lineitem.count().toDouble / sample.count().toDouble

      var t0 = System.currentTimeMillis()
      val gt = execute(lineitem); gt.show()
      var t1 = System.currentTimeMillis()
      println("Execution time on full data: " + (t1 - t0) + "ms")

      println("Result on the sampled dataset: ")
      t0 = System.currentTimeMillis()
      var estimate = execute(sample); estimate.show()
      t1 = System.currentTimeMillis()
      println("Execution time on sampled data: " + (t1 - t0) + "ms")
    }


    def execute(df: DataFrame): DataFrame = {
      val lineitem: DataFrame = df
      val part = desc.part

      val f1 = udf { (x: String) => x.matches("SM CASE|SM BOX|SM PACK|SM PKG") }
      val f2 = udf { (x: String) => x.matches("MED BAG|MED BOX|MED PKG|MED PACK") }
      val f3 = udf { (x: String) => x.matches("LG CASE|LG BOX|LG PACK|LG PKG") }

      val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

      val p_brand1: String = params(0).toString()
      val l_quantity11: Int = params(3).toString().toInt
      val l_quantity12: Int = l_quantity11 + 10

      val p_brand2: String = params(1).toString()
      val l_quantity21: Int = params(4).toString().toInt
      val l_quantity22: Int = l_quantity21 + 10

      val p_brand3: String = params(2).toString()
      val l_quantity31: Int = params(5).toString().toInt
      val l_quantity32: Int = l_quantity31 + 10

      val baseQuery = part.join(lineitem, $"l_partkey" === $"p_partkey")
        .filter(($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG") &&
          $"l_shipinstruct" === "DELIVER IN PERSON")

      val filter1 = ($"p_brand" === p_brand1) && f1($"p_container") &&
        ($"l_quantity" >= l_quantity11) && ($"l_quantity" <= l_quantity12) &&
        ($"p_size" >= 1 && $"p_size" <= 5)
      val filter2 = ($"p_brand" === p_brand2) && f2($"p_container") &&
        ($"l_quantity" >= l_quantity21) && ($"l_quantity" <= l_quantity22) &&
        ($"p_size" >= 1 && $"p_size" <= 10)
      val filter3 = ($"p_brand" === p_brand3) && f3($"p_container") &&
        ($"l_quantity" >= l_quantity31) && ($"l_quantity" <= l_quantity32) &&
        ($"p_size" >= 1 && $"p_size" <= 15)

      val filters = baseQuery.filter(filter1 || filter2 || filter3)

      val select_ = filters.select(decrease($"l_extendedprice", $"l_discount").as("part_revenue"))

      select_.agg(sum($"part_revenue").as("revenue"))
    }

  }

  def execute_Q20(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    val sample = get_sample(desc, "Q20", session)
    var lineitem: DataFrame = desc.lineitem
    var proportion: Double = 1.0

    if (sample == null) {
      println("No sample available for this query. Using the whole TPC-H dataset. ")
      lineitem = desc.lineitem
      execute(lineitem).show(20)
    } else {
      proportion = desc.lineitem.count().toDouble / sample.count().toDouble

      var t0 = System.currentTimeMillis()
      val gt = execute(lineitem); gt.show()
      var t1 = System.currentTimeMillis()
      println("Execution time on full data: " + (t1 - t0) + "ms")

      println("Result on the sampled dataset: ")
      t0 = System.currentTimeMillis()
      var estimate = execute(sample); estimate.show()
      t1 = System.currentTimeMillis()
      println("Execution time on sampled data: " + (t1 - t0) + "ms")
    }

    def execute(df: DataFrame): DataFrame = {
      val lineitem = df
      val nation = desc.nation
      val partsupp = desc.partsupp
      val supplier = desc.supplier
      val part = desc.part

      val p_name: String = params(0).toString()
      val firstDate: String = params(1).toString()
      val secondDate: String = calcDate(firstDate, "year", 1, false)
      val n_name: String = params(2).toString()

      val pnameFilter = udf { (x: String) => x.startsWith(p_name) }
      val nationFilter = nation.filter($"n_name" === n_name)

      val ps_availqtySubQuery = lineitem.filter($"l_shipdate" >= firstDate && $"l_shipdate" < secondDate)
        .groupBy($"l_partkey", $"l_suppkey")
        .agg((sum($"l_quantity") * 0.5).as("sum_quantity"))

      val ps_partkeySubQuery = part.filter(pnameFilter($"p_name"))
        .select($"p_partkey").distinct

      val s_suppkeySubQuery = ps_partkeySubQuery.join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
        .join(ps_availqtySubQuery, $"ps_suppkey" === ps_availqtySubQuery("l_suppkey") &&
          $"ps_partkey" === ps_availqtySubQuery("l_partkey"))
        .filter($"ps_availqty" > $"sum_quantity")
        .select($"ps_suppkey").distinct

      val query = supplier.select($"s_suppkey", $"s_name", $"s_nationkey", $"s_address")
        .join(nationFilter, $"s_nationkey" === nationFilter("n_nationkey"))

      s_suppkeySubQuery.join(query, $"ps_suppkey" === query("s_suppkey"))
        .select($"s_name", $"s_address")
        .sort($"s_name")
    }




  }
}
