package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._

class ConfApply(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input directory", required = true)
  val output = opt[String](descr = "output directory", required = true)
  val model = opt[String](descr = "model input directory", required = true)
  verify()
}

object ApplySpamClassifier extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfApply(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    val textFile = sc.textFile(args.input())
    val model = sc.textFile(args.model())

    val temp = model.map(line => {
      val tokens = line.split("\\(|,|\\)")
      val feature = tokens(1).toInt
      val weight = tokens(2).toDouble
      (feature, weight)
    }).collectAsMap()
    val w = sc.broadcast(temp)

    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.value.contains(f)) score += w.value(f))
      score
    }

    val output = textFile.map(line => {
      val tokens = line.split(" ")
      val docid = tokens(0)
      val isSpam = tokens(1)

      val length = tokens.length - 2
      val features = new Array[Int](length)
      for (i <- 0 until length ) {
        features(i) = tokens(i+2).toInt
      }
      val score = spamminess(features)
      var ResultLabel = "ham"
      if (score > 0) {ResultLabel = "spam"}

      (docid, isSpam, score, ResultLabel)
    })
    output.saveAsTextFile(args.output())
  }
}
