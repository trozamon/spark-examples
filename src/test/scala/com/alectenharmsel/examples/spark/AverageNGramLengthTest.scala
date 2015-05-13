package com.alectenharmsel.examples.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.scalatest._

class AverageNGramLengthTest extends FlatSpec with Matchers {

  val data = Context.sc.parallelize(
    Array[String](
      "four\t2000\t1\t1",
      "five\t2000\t1\t1",
      "six\t2001\t1\t1",
      "two\t2001\t1\t1"
    )
  )
  val res = AverageNGramLength.run(data).collect()

  it should "return two results" in {
    res.size should be (2)
  }

  it should "calculate 4.0 for 2000" in {
    res(0)._2 should be (4.0)
  }

  it should "calculate 3.0 for 2001" in {
    res(1)._2 should be (3.0)
  }
}
