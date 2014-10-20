/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/home/venkat/Downloads/spark-1.1.0/data.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numThis = logData.filter(line => line.contains("This")).count()
    val numProgram = logData.filter(line => line.contains("program")).count()
    val numFirst = logData.filter(line => line.contains("first")).count()
    println("Lines with This: %s, Lines with program: %s, Lines with first: %s".format(numThis, numProgram, numFirst))
  }
}

