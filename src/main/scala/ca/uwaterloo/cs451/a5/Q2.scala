package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q2 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q2Conf(argv)

    log.info("Input: " + args.input())

    val date = args.date()
    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val Lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val Order = sc.textFile(args.input() + "/orders.tbl")
      val Orderkey = Lineitem
        .filter(line => {
          val tokens = line.split('|')
          tokens(10).contains(date)
        })
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), 1)
        })

      val clerks = Order
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(6))
        })

      Orderkey.cogroup(clerks)
        .filter(p => p._2._1.iterator.hasNext)
        .map(p => (p._2._2.iterator.next(), p._1.toLong))
        .sortBy(_._2)
        .take(20)
        .foreach(println)
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd

      val orderkeys = lineitemRDD
        .filter(line => line(10).toString.contains(date))
        .map(line => (line(0), 1))

      val clerks = ordersRDD
        .map(line => (line(0), line(6)))

      orderkeys.cogroup(clerks)
        .filter(p => p._2._1.iterator.hasNext)
        .map(p => (p._2._2.iterator.next(), p._1.toString.toLong))
        .sortBy(_._2)
        .take(20)
        .foreach(println)
    }
  }
}
