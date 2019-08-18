package com.hemanth.com.tests

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.hemanth.com.{CassandraIngester, Util}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
class CassandraIngesterTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers with Eventually
       with SparkTemplate with EmbeddedCassandra {

  override def clearCache(): Unit = CassandraConnector.evictCache()

  val conf = new SparkConf().setAppName("code-challenge-test").setMaster("local[*]")
  val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  private val tableName = "testhbase"

  //Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  val connector = CassandraConnector(defaultConf)




  override def beforeAll(): Unit = {
    super.beforeAll()
    connector.withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
      session.execute("CREATE TABLE test.long_tweets(username text, col2 text, col3 text, PRIMARY KEY(username));")
     }
    }

  override def afterAll() = {
    spark.stop
  }



  test("file-exists - Invalid case"){
    val file = "src/test/resources/file.txt"
    assert(! new Util(spark).fileExists(file))
  }

  test("file-exists - valid case"){
    val file = "src/test/resources/input/file.txt"
    assert( new Util(spark).fileExists(file))
  }

  test("readFileTODf --> csv "){
    val df = new CassandraIngester(spark).readFileTODf("src/test/resources/input/test1.csv","csv")
    df.show(false)
    df.printSchema()
    assert(df.count() == 4)
  }

  test("readFileTODf --> tsv "){
    val df = new CassandraIngester(spark).readFileTODf("src/test/resources/input/test2.csv","tsv")
    df.show(false)
    df.printSchema()
    assert(df.count() == 4)
  }

  test("readFileTODf --> parquet "){
    val inputdf = spark.read.format("csv").option("header", "true").load("src/test/resources/input/test1.csv")
    inputdf.write.mode("overwrite").parquet("src/test/resources/input/test3")
    val df = new CassandraIngester(spark).readFileTODf("src/test/resources/input/test3","parquet")
    df.show(false)
    df.printSchema()
    assert(df.count() == 4)
  }

  test("readFileTODf --> orc "){
    val inputdf = spark.read.format("csv").option("header", "true").load("src/test/resources/input/test1.csv")
    inputdf.write.mode("overwrite").orc("src/test/resources/input/test4")
    val df = new CassandraIngester(spark).readFileTODf("src/test/resources/input/test4","orc")
    df.show(false)
    df.printSchema()
    assert(df.count() == 4)
  }

  test("invalid number arguments"){
    val thrown = intercept[Exception] {
      new CassandraIngester(spark).process(Array("one"))
    }
    assert(thrown.getMessage == "Invalid number of arguments")
  }

  test("invalid input file"){
    val thrown = intercept[Exception] {
      new CassandraIngester(spark).process(Array("src/test/input/invalidfile.csv","csv","tb1"))
    }
    assert(thrown.getMessage == "requirement failed: Invalid file path, kindly provide valid file")
  }

  test("invalid file format - null case"){
    val thrown = intercept[Exception] {
      new CassandraIngester(spark).process(Array("src/test/resources/input/invalidformat.txt",null,"tb1"))
    }
    assert(thrown.getMessage == "requirement failed: Invalid file format , kindly provide valid file format")
  }

  test("invalid table - null case"){
    val thrown = intercept[Exception] {
      new CassandraIngester(spark).process(Array("src/test/resources/input/invalidformat.txt","csv",null))
    }
    assert(thrown.getMessage == "requirement failed: Invalid hbase table, hbase table can't be null")
  }

  test("invalid file format "){
    val thrown = intercept[Exception] {
      new CassandraIngester(spark).process(Array("src/test/resources/input/invalidformat.txt","text","tb1"))
    }
    assert(thrown.getMessage == "Invalid file format, this format is not supported at the moment")
  }


}
