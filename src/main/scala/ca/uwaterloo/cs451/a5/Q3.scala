package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q3 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q3Conf(argv)

    log.info("Input: " + args.input())

    val date = args.date()
    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val Lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val Part = sc.textFile(args.input() + "/part.tbl")
      val Supplier = sc.textFile(args.input() + "/supplier.tbl")

      val partkeys = Part
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(1))
        })
        .collectAsMap()
      val partkeysHMap = sc.broadcast(partkeys)

      val suppkeys = Supplier
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(1))
        })
        .collectAsMap()
      val suppkeysHMap = sc.broadcast(suppkeys)

      Lineitem
        .filter(line => {
          val tokens = line.split('|')
          tokens(10).contains(date)
        })
        .map(line => {
          val tokens = line.split('|')
          (tokens(0).toLong, (tokens(1), tokens(2)))
        })
        .sortByKey()
        .take(20)
        .map(p => (p._1, partkeysHMap.value(p._2._1), suppkeysHMap.value(p._2._2)))
        .foreach(println)

    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd
      val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
      val supplierRDD = supplierDF.rdd

      val partkeys = partRDD
        .map(line => (line(0), line(1)))
        .collectAsMap()
      val partkeysHMap = sc.broadcast(partkeys)

      val suppkeys = supplierRDD
        .map(line => (line(0), line(1)))
        .collectAsMap()
      val suppkeysHMap = sc.broadcast(suppkeys)

      lineitemRDD
        .filter(line => line(10).toString.contains(date))
        .map(line => (line(0).toString.toLong, (line(1), line(2))))
        .sortByKey()
        .take(20)
        .map(p => (p._1, partkeysHMap.value(p._2._1), suppkeysHMap.value(p._2._2)))
        .foreach(println)
    }
  }
}
