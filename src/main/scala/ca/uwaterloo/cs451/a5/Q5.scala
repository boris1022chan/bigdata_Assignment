
package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q5 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q5Conf(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val orders = sc.textFile(args.input() + "/orders.tbl")
      val customer = sc.textFile(args.input() + "/customer.tbl")
      val nation = sc.textFile(args.input() + "/nation.tbl")

      val nationkeys = nation
        .map(line => {
          val tokens = line.split('|')
          (tokens(1), tokens(0).toInt)
        })
        .collectAsMap()
      val nationkeysHMap = sc.broadcast(nationkeys)
      val nationkey_ca = nationkeysHMap.value("CANADA")
      val nationkey_us = nationkeysHMap.value("UNITED STATES")

      val custkeys = customer
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(3).toInt)
        })
        .collectAsMap()
      val custkeysHMap = sc.broadcast(custkeys)

      val l_orderkeys = lineitem
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(10))
        })

      val o_orderkeys = orders
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(1))
        })
        .filter(p => {
          val nationkey = custkeysHMap.value(p._2)
          nationkey == nationkey_ca || nationkey == nationkey_us
        })

      l_orderkeys.cogroup(o_orderkeys)
        .filter(p => p._2._2.iterator.hasNext)
        .flatMap(p => {
          val custkey = p._2._2.iterator.next()
          p._2._1.map(shipdate => ((custkeysHMap.value(custkey), shipdate.slice(0,7)), 1))
        })
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()
     //   .foreach(p => println(p._2))
        .foreach(p => println("(" + p._1._1 + "," + p._1._2 + "," + p._2 + ")"))
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

      val nationkeys = nationRDD
        .map(line => (line(1), line(0).toString.toInt))
        .collectAsMap()
      val nationkeysHMap = sc.broadcast(nationkeys)
      val nationkey_ca = nationkeysHMap.value("CANADA")
      val nationkey_us = nationkeysHMap.value("UNITED STATES")

      val custkeys = customerRDD
        .map(line => (line(0), line(3).toString.toInt))
        .collectAsMap()
      val custkeysHMap = sc.broadcast(custkeys)

      val l_orderkeys = lineitemRDD
        .map(line => (line(0), line(10)))

      val o_orderkeys = ordersRDD
        .map(line => (line(0), line(1)))
        .filter(p => {
          val nationkey = custkeysHMap.value(p._2)
          nationkey == nationkey_ca || nationkey == nationkey_us
        })

      l_orderkeys.cogroup(o_orderkeys)
        .filter(p => p._2._2.iterator.hasNext)
        .flatMap(p => {
          val custkey = p._2._2.iterator.next()
          p._2._1.map(shipdate => ((custkeysHMap.value(custkey), shipdate.toString.substring(0,7)), 1))
        })
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()
        .foreach(p => println("(" + p._1._1 + "," + p._1._2 + "," + p._2 + ")"))
    }
  }
}
