package com.hemanth.com

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j._

class CassandraIngester(spark : SparkSession) {

  val logger = LogManager.getRootLogger
  logger.setLevel(Level.INFO)
  val util = new Util(spark)
  def process(args: Array[String]) : Unit = {
     if (args.length != 4)
       throw new IllegalArgumentException("Invalid number of arguments")
    val inputFile = args(0)
    val format = args(1)
    val cassandraTableName = args(2)
    val cassandraKeySpace = args(3)

    validation(inputFile,format,cassandraTableName)
    val rawDf = readFileTODf(inputFile,format)
    writeToHbase(rawDf.selectExpr( "col1", "cast(col2 as string) as col2","cast(col3 as string) as col3"),cassandraTableName,cassandraKeySpace)
  }

  def validation(inputFile : String , format : String, hbaseTable : String) : Unit = {
    require(util.fileExists(inputFile) , "Invalid file path, kindly provide valid file")
    require(format != null , "Invalid file format , kindly provide valid file format")
    require( hbaseTable != null , "Invalid hbase table, hbase table can't be null")
  }

  def readFileTODf(inputFile : String,format : String)  = format.toLowerCase match {
    case "orc" => spark.read.orc(inputFile)
    case "parquet" => spark.read.parquet(inputFile)
    case "csv"   => spark.read.format("csv").option("header", "true").load(inputFile)
    case "tsv"  => spark.read.option("sep", "\t").option("header", "true").csv(inputFile)
    case _ => throw new IllegalArgumentException("Invalid file format, this format is not supported at the moment")
  }

  def writeToHbase(df : DataFrame, cassandraTableName : String,keySpace : String) = df.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->keySpace,"table"->cassandraTableName))
      .mode(SaveMode.Append)
      .save()


}
