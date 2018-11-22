package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._

class ConfEnsemble(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input directory", required = true)
  val output = opt[String](descr = "output directory", required = true)
  val model = opt[String](descr = "model input directory", required = true)
  val method = opt[String](descr = "ensemble technique", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfEnsemble(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val model_x = sc.textFile(args.model() + "/part-00000")
    val model_y = sc.textFile(args.model() + "/part-00001")
    val model_z = sc.textFile(args.model() + "/part-00002")

    // get w
    val temp_x = model_x.map(line => {
      val tokens = line.split("\\(|,|\\)")
      val feature = tokens(1).toInt
      val weight = tokens(2).toDouble
      (feature, weight)
    }).collectAsMap()
    val w_x = sc.broadcast(temp_x)

    val temp_y = model_y.map(line => {
      val tokens = line.split("\\(|,|\\)")
      val feature = tokens(1).toInt
      val weight = tokens(2).toDouble
      (feature, weight)
    }).collectAsMap()
    val w_y = sc.broadcast(temp_y)

    val temp_z = model_z.map(line => {
      val tokens = line.split("\\(|,|\\)")
      val feature = tokens(1).toInt
      val weight = tokens(2).toDouble
      (feature, weight)
    }).collectAsMap()
    val w_z = sc.broadcast(temp_z)

    val method = args.method()
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d

      var score_x = 0d
      features.foreach(f => if (w_x.value.contains(f)) score_x += w_x.value(f))

      var score_y = 0d
      features.foreach(f => if (w_y.value.contains(f)) score_y += w_y.value(f))

      var score_z = 0d
      features.foreach(f => if (w_z.value.contains(f)) score_z += w_z.value(f))

      if (method == "average") {
        score = (score_x + score_y + score_z) / 3.0
      } else if (method == "vote") {
        var spam = 0d
        var ham = 0d

        if (score_x > 0) spam += 1 else ham += 1
        if (score_y > 0) spam += 1 else ham += 1
        if (score_z > 0) spam += 1 else ham += 1

        score = spam - ham
      }

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
      var prediction = "ham"
      if (score > 0) {
        prediction = "spam"
      }

      (docid, isSpam, score, prediction)
    })
    output.saveAsTextFile(args.output())
  }
}