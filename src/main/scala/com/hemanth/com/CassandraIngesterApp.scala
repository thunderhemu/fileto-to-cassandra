package com.hemanth.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CassandraIngesterApp extends App {

  val conf = new SparkConf().setAppName("code-challenge").setMaster("local")
  val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  new CassandraIngester(spark).process(args)
   spark.stop()

}
