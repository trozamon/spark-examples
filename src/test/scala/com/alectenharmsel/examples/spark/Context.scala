package com.alectenharmsel.examples.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Context {

  val conf = new SparkConf().setMaster("local").setAppName("test")
  val sc = new SparkContext(conf)

}
