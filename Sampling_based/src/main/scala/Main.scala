package sampling

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.io._
import java.nio.file.{Paths, Files}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val onCluster = false
    val firstTime = false

    var sparkConf: SparkConf = null
    var TPCH_PATH = ""
    var SAMPLE_PATH = ""

    if (onCluster) {
      sparkConf = new SparkConf().setAppName("SamplingBasedAQP")
      TPCH_PATH = "/scratch/futong/tpc/tpch_parquet_sf10/"
      SAMPLE_PATH = "/scratch/futong/Samples/"
    } else {
      sparkConf = new SparkConf().setAppName("SamplingBasedAQP").setMaster("local[16]").set("spark.driver.memory", "8G")
      TPCH_PATH = "/Users/futongliu/Downloads/SemesterProject/tpch_parquet_sf1/"
      SAMPLE_PATH = "/Users/futongliu/Downloads/SemesterProject/Sampling_based/Samples/"
    }

    val sc = SparkContext.getOrCreate(sparkConf)
    val session = SparkSession.builder().getOrCreate()

    val lineitem = session.read.parquet(TPCH_PATH + "lineitem.parquet")
    val customer = session.read.load(TPCH_PATH + "customer.parquet")
    val nation = session.read.load(TPCH_PATH + "nation.parquet")
    val order = session.read.load(TPCH_PATH + "order.parquet")
    val part = session.read.load(TPCH_PATH + "part.parquet")
    val partsupp = session.read.load(TPCH_PATH + "partsupp.parquet")
    val region = session.read.load(TPCH_PATH + "region.parquet")
    val supplier = session.read.load(TPCH_PATH + "supplier.parquet")

    var desc_ = new Description
    desc_.e = 0.05
    desc_.ci = 0.95
    desc_.samplePath = SAMPLE_PATH

    desc_.lineitem = lineitem
    desc_.customer = customer
    desc_.nation = nation
    desc_.customer = customer
    desc_.part = part
    desc_.orders = order
    desc_.partsupp = partsupp
    desc_.region = region
    desc_.supplier = supplier

    if (firstTime){
      val stratifiedSampleList = Sampler.sample(session, desc_.lineitem, desc_.e, desc_.ci)
    }

    else{
      println("Q1")
      Executor.execute_Q1(desc_, session, List(90))


      println("Q3")
      Executor.execute_Q3(desc_, session, List("BUILDING", "1995-03-15"))

      println("Q5")
      Executor.execute_Q5(desc_, session, List("ASIA", "1995-01-01"))

      println("Q6")
      Executor.execute_Q6(desc_, session, List("1994-01-01", 0.06, 24))

      println("Q7")
      Executor.execute_Q7(desc_, session, List("FRANCE", "GERMANY"))

      println("Q9")
      Executor.execute_Q9(desc_, session, List("green"))


      println("Q10")
      Executor.execute_Q10(desc_, session, List("1993-10-01"))


      println("Q11")
      Executor.execute_Q11(desc_, session, List("GERMANY", 0.0001))


      println("Q12")
      Executor.execute_Q12(desc_, session, List("MAIL", "SHIP", "1994-01-01"))


      println("Q17")
      Executor.execute_Q17(desc_, session, List("Brand#23", "MED BOX"))


      println("Q18")
      Executor.execute_Q18(desc_, session, List(300))


      println("Q19")
      Executor.execute_Q19(desc_, session, List("Brand#12", "Brand#23", "Brand#34", 1, 10, 20))


      println("Q20")
      Executor.execute_Q20(desc_, session, List("forest", "1994-01-01", "CANADA"))

    }


//    desc_.samples = sampling._1
//    desc_.sampleDescription = sampling._2

    // Execute first query
//    println("Q1")
//    val sampleResult1 = Executor.execute_Q1(desc_, session, List(90)) // to "1998-09-02"
////    sampleResult1.show(sampleResult1.count().toInt, false)
//    sampleResult1.show()

    //    println("Q3")
    //    val sampleResult3 = Executor.execute_Q3(desc_, session, List("BUILDING", "1995-03-15"))
    //    sampleResult3.show(sampleResult3.count.toInt, false)
    //
    //    println("Q5")
    //    val sampleResult5 = Executor.execute_Q5(desc_, session, List("ASIA", "1995-01-01"))
    //    sampleResult5.show(sampleResult5.count.toInt, false)
    //
    //    println("Q6")
    //    val sampleResult6 = Executor.execute_Q6(desc_, session, List("1994-01-01", 0.06, 24))
    //    sampleResult6.show(sampleResult6.count.toInt, false)
    //
    //    println("Q7")
    //    val sampleResult7 = Executor.execute_Q7(desc_, session, List("FRANCE", "GERMANY"))
    //    sampleResult7.show(sampleResult7.count.toInt, false)
    //
    //    println("Q9")
    //    val sampleResult9 = Executor.execute_Q9(desc_, session, List("green"))
    //    sampleResult9.show(sampleResult9.count.toInt, false)
    //
    //    println("Q10")
    //    val sampleResult10 = Executor.execute_Q10(desc_, session, List("1993-10-01"))
    //    sampleResult10.show(sampleResult10.count.toInt, false)
    //
    //    println("Q11")
    //    val sampleResult11 = Executor.execute_Q11(desc_, session, List("GERMANY", 0.0001))
    //    sampleResult11.show(sampleResult11.count.toInt, false)
    //
    //    println("Q12")
    //    val sampleResult12 = Executor.execute_Q12(desc_, session, List("MAIL", "SHIP", "1994-01-01"))
    //    sampleResult12.show(sampleResult12.count.toInt, false)
    //
    //    println("Q17")
    //    val sampleResult17 = Executor.execute_Q17(desc_, session, List("Brand#23", "MED BOX"))
    //    sampleResult17.show(sampleResult17.count.toInt, false)
    //
    //    println("Q18")
    //    val sampleResult18 = Executor.execute_Q18(desc_, session, List(300))
    //    sampleResult18.show(sampleResult18.count.toInt, false)
    //
    //    println("Q19")
    //    val sampleResult19 = Executor.execute_Q19(desc_, session, List("Brand#12", "Brand#23", "Brand#34", 1, 10, 20))
    //    sampleResult19.show(sampleResult19.count.toInt, false)
    //
    //    println("Q20")
    //    val sampleResult20 = Executor.execute_Q20(desc_, session, List("forest", "1994-01-01", "CANADA"))
    //    sampleResult20.show(sampleResult20.count.toInt, false)

  }

}