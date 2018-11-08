package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q4Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q4 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q4Conf(argv)

    log.info("Input: " + args.input())

    val date = args.date()
    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val orders = sc.textFile(args.input() + "/orders.tbl")
      val customer = sc.textFile(args.input() + "/customer.tbl")
      val nation = sc.textFile(args.input() + "/nation.tbl")

      val custkeys = customer
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(3))
        })
        .collectAsMap()
      val custkeysHMap = sc.broadcast(custkeys)

      val nationkeys = nation
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(1))
        })
        .collectAsMap()
      val nationkeysHMap = sc.broadcast(nationkeys)

      val l_orderkeys = lineitem
        .filter(line => {
          val tokens = line.split('|')
          tokens(10).contains(date)
        })
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), 1)
        })
        .reduceByKey(_ + _)

      val o_orderkeys = orders
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(1))
        })


      l_orderkeys.cogroup(o_orderkeys)
        .filter(p => p._2._1.iterator.hasNext)
        .map(p => (custkeysHMap.value(p._2._2.iterator.next()), p._2._1.iterator.next()))
        .reduceByKey(_ + _)
        .map(p => (p._1.toInt, (nationkeysHMap.value(p._1), p._2)))
        .sortByKey()
        .collect()
        .foreach(p => println("(" + p._1 + "," + p._2._1 + "," + p._2._2 + ")"))

    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd

      val custkeys = customerRDD
        .map(line => (line(0), line(3)))
        .collectAsMap()
      val custkeysHMap = sc.broadcast(custkeys)

      val nationkeys = nationRDD
        .map(line => (line(0), line(1)))
        .collectAsMap()
      val nationkeysHMap = sc.broadcast(nationkeys)

      val l_orderkeys = lineitemRDD
        .filter(line => line(10).toString.contains(date))
        .map(line => (line(0), 1))
        .reduceByKey(_ + _)

      val o_orderkeys = ordersRDD
        .map(line => (line(0), line(1)))

      l_orderkeys.cogroup(o_orderkeys)
        .filter(p => p._2._1.iterator.hasNext)
        .map(p => (custkeysHMap.value(p._2._2.iterator.next()), p._2._1.iterator.next()))
        .reduceByKey(_ + _)
        .map(p => (p._1.toString.toInt, (nationkeysHMap.value(p._1), p._2)))
        .sortByKey()
        .collect()
        .foreach(p => println("(" + p._1 + "," + p._2._1 + "," + p._2._2 + ")"))
    }
  }
}