package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.HashPartitioner
//import scala.util.control.Breaks._
import org.apache.spark.sql.functions._
//import scala.math._
//import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}

object Sampler {
  def sample(session: SparkSession, lineitem: DataFrame, e: Double, ci: Double): List[DataFrame] = {
    // 1. Deal with confidence interval ci
    val schema = lineitem.schema.map(x => x.name)
    val aggColumn = "l_extendedprice"
    val z_table = List((0.90, 1.645), (0.91, 1.70), (0.92, 1.75), (0.93, 1.81), (0.94, 1.88),
      (0.95, 1.96), (0.96, 2.05), (0.97, 2.17), (0.98, 2.33), (0.99, 2.575)) // (nth, percentile) of standard Gaussian
    var z = 2.575
    var i = 0
    var done = false
    // Enquire the z-table
    while (i < z_table.size && !done) {
      if (ci <= z_table(i)._1) { // find the closest upper bound of ci
        z = z_table(i)._2
        done = true
      }
      i += 1
    }
    print("z value: ", z, "corresponding to confidence interval:", ci)

    // Total query column sets appear in TPC-H queries
    val totalQcs = List(
//      List(List("l_returnflag", "l_linestatus", "l_shipdate"), List("Q1")), //List(8, 9, 10) Q1
//      List(List("l_orderkey", "l_shipdate"), List("Q3")), //List(0, 10) Q3
//      List(List("l_orderkey", "l_suppkey"), List("Q5")), //List(0, 2) Q5
//      List(List("l_quantity", "l_discount", "l_shipdate"), List("Q6")), //List(4, 6, 10) Q6
//      List(List("l_orderkey", "l_suppkey", "l_shipdate"), List("Q7")), //List(0, 2, 10) Q7
//      List(List("l_orderkey", "l_partkey", "l_suppkey"), List("Q9")), //List(0, 1, 2) Q9
//      List(List("l_orderkey", "l_returnflag"), List("Q10")), //List(0, 8) Q10
//      List(List("l_orderkey", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipmode"), List("Q11")), //List(0, 10, 11, 12, 14) Q11
//      List(List("l_partkey", "l_quantity"), List("Q12")), //List(1, 4) Q12
//      List(List("l_orderkey", "l_quantity"), List("Q17")), //List(0, 4) Q17
//      List(List("l_partkey", "l_quantity", "l_shipinstruct", "l_shipmode"), List("Q18")), //List(1, 4, 13, 14) Q18
//      List(List("l_partkey", "l_suppkey", "l_shipdate"), List("Q19")) //List(1, 2, 10) Q19, Q20 shares the same QCS

      List(List("l_returnflag", "l_linestatus", "l_shipdate"), List("Q1"))//, //Q1
//      List(List("l_orderkey", "l_shipdate"), List("Q3")), //List(0, 10) Q3
//      List(List("l_orderkey", "l_suppkey"), List("Q5")), //List(0, 2) Q5
//      List(List("l_quantity", "l_discount", "l_shipdate"), List("Q6")), //List(4, 6, 10) Q6
//      List(List("l_shipdate"), List("Q7")), //Q7
//      List(List("l_orderkey", "l_partkey", "l_suppkey"), List("Q9")), //List(0, 1, 2) Q9
//      List(List("l_returnflag"), List("Q10")), //Q10
//      List(List("l_orderkey", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipmode"), List("Q11")), //List(0, 10, 11, 12, 14) Q11
//      List(List("l_partkey", "l_quantity"), List("Q12")), //List(1, 4) Q12
//      List(List("l_quantity"), List("Q17")), //Q17
//      List(List("l_orderkey", "l_quantity"), List("Q18")), //Q18
//      List(List("l_quantity", "l_shipinstruct", "l_shipmode"), List("Q19")), //Q19
//      List(List("l_shipdate"), List("Q20")) //Q20. Q7 and Q20 shares the same ones
    )

    println("total QCS size: ", totalQcs.size)
    val totalQcsIndex = totalQcs.map(qcs => qcs(0).map(q => schema.indexOf(q)))
    val attrIndex = schema.indexOf(aggColumn)

    // use datatype to infer the storage space of one tuple,  with default size, assume that one row = 172 bytes
    val rowBytes = lineitem.schema.map(x => x.dataType.defaultSize).reduce((a, b) => a + b)

    // 2. Deal with the error bound e
    // calculate absolute error according to relative error and sum of l_extendedprice
    val sumValue = lineitem.agg(functions.sum(aggColumn)).first.get(0).asInstanceOf[java.math.BigDecimal].doubleValue()
    val errorBound = sumValue * e


    // 3. Sample one by one
    val stratifiedSampleList = List[DataFrame]()
    i = 0
    while (i < totalQcs.size){

      // 3.1. Get the stratum size and variance for this specific qcs. dfAgg: a dataframe where the rows are the strata, the count, the variance
      val dfAgg = lineitem.groupBy(totalQcs(i)(0).head, totalQcs(i)(0).tail: _*)
        .agg(functions.count(aggColumn), functions.var_pop(aggColumn))
        .select("count(" + aggColumn + ")", "var_pop(" + aggColumn + ")")

      // 3.2. Calculate sample size for this specific qcs with the help of magic K
      val magicK_samplesize = magicKSearch(dfAgg, aggColumn, z, errorBound)
      val magicK = magicK_samplesize._1
      val sampleSize = magicK_samplesize._2 //the total size after stratified sampling
      println("currently processing query ", totalQcs(i)(1)(0),  ", of which the magic K is ", magicK, " and the number of rows in the sample: ", sampleSize)
      println("sample size (MB): ", sampleSize * rowBytes / 1048576)

      if ((sampleSize * rowBytes / 1048576) > 500) {
        println("this sample is too large and hence skipped ")
      }
      else{
        // 3.3. Do the actual stratified sample
        // 3.3.1. do scalable simple random sampling
        val qcsWithKey = lineitem.rdd.map(row => (totalQcsIndex(i).map(x => row(x)).mkString("_"), row)).groupByKey
        //              (key, compactBuffer) => (BufferSize, BufferContent)              (Int, Iterable[Row]), magic K
        val stratifiedSample = qcsWithKey.map(x => (x._2.size, x._2)).flatMap(x => ScaSRS(x, magicK))
        println("stratified samples are created!")
//        val stratifiedSampleSize = stratifiedSample.count(); println("real stratified sample size (tuples): ", stratifiedSampleSize)
//        val stratifiedStrataSize = qcsWithKey.count(); println("# groups:", qcsWithKey.getNumPartitions, stratifiedStrataSize)

        // 3.3.2. Equip the sample with DataFrame head and save this sample
        val sampleDF = session.createDataFrame(stratifiedSample, lineitem.schema)
        sampleDF.write.mode(SaveMode.Overwrite).parquet("./Samples/"+totalQcs(i)(1)(0))

//        stratifiedSampleList = stratifiedSampleList :+ sampleDF
//        qcsIndicator = qcsIndicator :+ (totalQcs(i)(1)(0), totalQcs(i)(0), (stratifiedSampleSize * rowBytes / 1048576.0),  stratifiedSampleSize.toDouble, stratifiedStrataSize.toDouble, e, ci)

      }

      i += 1
    }

    print("\n number of sampled qcs: ", stratifiedSampleList.size)
    import session.implicits._
    stratifiedSampleList
  }


  def magicKSearch(dfAgg: DataFrame, aggColumn: String, z: Double, errorBound: Double): (Double, Double) = {
    var minStrataSize = 2.0
    var maxStrataSize = dfAgg.agg(functions.max("count(" + aggColumn + ")")).first.getLong(0).toDouble
    val rddNewAgg = dfAgg.rdd.map(row => Row(row.getLong(0).toDouble, row.getDouble(1))) // count, variance
    var magicK = 0.0; var sampleSize = 0.0

    // use bisection to find the best (minimum) K
    while (minStrataSize + 1 < maxStrataSize) {
      var mid = scala.math.floor((minStrataSize + maxStrataSize) / 2)
      println("maxStrataSize: ", maxStrataSize, "minStrataSize: ", minStrataSize, "midStrataSize: ", mid)

      val v_by_n = rddNewAgg.map(row => cal_v_by_n(row, mid)).reduce(_+_)
      val estimateError = scala.math.sqrt(v_by_n) * z
      println("estimated error with bisection: ", estimateError, "  user defined error bound", errorBound)
      if (estimateError <= errorBound) {
        maxStrataSize = mid
      } else {
        minStrataSize = mid
      }
    }

    if(scala.math.sqrt(rddNewAgg.map(row => cal_v_by_n(row, minStrataSize)).reduce(_+_)) * z <= errorBound) {
      magicK = minStrataSize
    }
    else if(scala.math.sqrt(rddNewAgg.map(row => cal_v_by_n(row, maxStrataSize)).reduce(_+_)) * z <= errorBound) {
      magicK = maxStrataSize
    }
    else{ // the required K can't be found
//      magicK = null
      throw new IllegalArgumentException("The required K can't be found")
    }
    sampleSize = rddNewAgg.map(row => calSize(row, magicK)).reduce(_+_)
    return (magicK, sampleSize) // magic K, the total size after stratified sampling
  }


  def cal_v_by_n(row: Row, K: Double): Double = {
    var stratumSampleSize = K // mid of strata size
    val stratumSize = row.getDouble(0)
    if (stratumSize < K) stratumSampleSize = stratumSize
    val error = scala.math.pow(stratumSize, 2) * row.getDouble(1) / stratumSampleSize
    return error
  }

  def calSize(row: Row, K: Double): Double = {
    if (row.getDouble(0) < K) // if this stratum's size is less than the magic K
      row.getDouble(0) // no sample is required but itself will be used
    else
      K // a sample of size K needs to be performed on this stratum
  }

  def ScaSRS(size_stratum: (Int, Iterable[Row]), K: Double): Iterable[Row] = {
    val sigma = 0.00005
    val stratumSize = size_stratum._1
    val stratum = size_stratum._2.toIterator
    val r = scala.util.Random

    if (stratumSize < K) stratum.toIterable
    else {
      val p = K.toDouble / stratumSize.toDouble
      val gamma1 = -1.0 * (scala.math.log(sigma) / stratumSize)
      val gamma2 = -1.0 * (2 * scala.math.log(sigma) / 3 / stratumSize)
      val q1 = scala.math.min(1, p + gamma1 + scala.math.sqrt(gamma1 * gamma1 + 2 * gamma1 * p))
      val q2 = scala.math.max(0, p + gamma2 - scala.math.sqrt(gamma2 * gamma2 + 3 * gamma2 * p))

      var l = 0
      var waitlist = List[(Double, Row)]()
      var res = List[Row]() //selected rows

      while (stratum.hasNext) {
        val nextRow = stratum.next
        val Xj = r.nextDouble
        if (Xj < q2) {
          res = res :+ nextRow
          l += 1
        }
        else if (Xj < q1) {
          waitlist = waitlist :+ (Xj, nextRow) //associate the row with the generated random number
        }
      }

      //select the smallest pn-l items from waitlist
      waitlist = waitlist.sortBy(_._1)
      if (K.toInt - l > 0) res = res ++ waitlist.take(K.toInt - l).map(_._2)
      res
    }
  }
}
