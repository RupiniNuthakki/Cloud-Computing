import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Multiply {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf", "false")
    conf.set("spark.eventLog.enabled", "false")

    // read in the input matrices
    val matrixM = sc.textFile(args(0)).map(line => {
        val Array(i, j, v) = line.split(",")
        (i.toInt, (j.toInt, v.toDouble))
    })
    val matrixN = sc.textFile(args(1)).map(line => {
        val Array(i, j, v) = line.split(",")
        ((j.toInt, i.toInt), v.toDouble)
    })

    // perform matrix multiplication
    val matrixMN = matrixM.join(matrixN.map { case ((j, i), v) => (j, (i, v)) })
        .map { case (_, ((i, v1: Double), (_, v2: Double))) => ((i, _: Int), v1 * v2) }
        .reduceByKey(_ + _)
        .map { case ((i, j), v) => Seq((i, j, v)) }
        .groupBy(_._1)
        .mapValues(_.map { case (_, j, v) => (j, v) }.toList.sortBy(_._1).map(_._2).mkString(","))


    // save the output matrix
    matrixMN.saveAsTextFile(args(2))

    sc.stop()
  }
}
