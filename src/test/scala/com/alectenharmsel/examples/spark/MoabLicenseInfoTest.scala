package com.alectenharmsel.examples.spark

import java.io.IOException
import java.util.ArrayList
import java.util.List
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.scalatest._

class MoabLicenseInfoTest extends FlatSpec with Matchers {

  val data = Context.sc.parallelize(
    Array[String](
      "05/11 22:58:25  MNodeUpdateResExpression(nyx5624,FALSE,TRUE)",
      "07/06 00:04:37  INFO:     job 10627132 violates active HARD MAXMEM limit of 512000 for acct heyo: Filesystem closed",
      "03/31 00:15:25  INFO:     Node 'nyx4009' status: state='Busy' rsvlist='screms.2325,9938104,9938105,9911261,9911265,9911278,9911279,9911282,9911283,9934851,9911285,9934846,9934847' joblist='9911261,9911265,9911278,9911279,9911282,9911283,9911285,9934846,9934847,9934851,9938104,9938105'",
      "03/31 00:15:18  INFO:     current iteration load for total: 77006",
      "03/31 00:32:39  INFO:     adding class 'cac' to node 'nyx5567'",
      "05/11 22:58:25  INFO:     License cfd_solv_ser        0 of   6 available  (Idle: 33.3%  Active: 66.67%)",
      "05/11 22:59:25  INFO:     License cfd_solv_ser        0 of   6 available  (Idle: 33.3%  Active: 66.67%)",
      "05/11 23:58:25  INFO:     License cfd_solv_ser        1 of   7 available  (Idle: 33.3%  Active: 66.67%)",
      "05/11 23:59:25  INFO:     License cfd_solv_ser        0 of   7 available  (Idle: 33.3%  Active: 66.67%)"
    )
  )
  val res = MoabLicenseInfo.run(data).collect()

  it should "return a single result" in {
    res.size should be (1)
  }

  it should "have 1 available" in {
    res(0)._3 should be (1)
  }

  it should "have 26 total" in {
    res(0)._4 should be (26)
  }

  it should "have a proper date" in {
    res(0)._1 should be ("05/11")
  }

  it should "have a proper license name" in {
    res(0)._2 should be ("cfd_solv_ser")
  }
}
