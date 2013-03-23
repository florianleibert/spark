package spark.streaming.examples

import java.io.File
import spark.streaming._

object CheckpointTest {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: CheckpointTest <master> <checkpointDir>")
      System.exit(1)
    }

    val master = args(0)
    val checkpointDir = args(1)

    System.setProperty("spark.cleaner.ttl", "60000")

    // Create the context
    if (new File(checkpointDir).exists()) {
      println("Loading checkpoint: " + checkpointDir)
      new StreamingContext(checkpointDir).start()
    } else {
      println("Creating new SparkContext with checkpoint dir " + checkpointDir)
      val ssc = new StreamingContext(args(0), "CheckpointTest", Seconds(2),
        System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
      ssc.checkpoint(checkpointDir)

      // Create the FileInputDStream on the directory and use the
      // stream to count words in new files created
      val lines = ssc.constantStream(Array("hi"))
      val nums = lines.map { s =>
        println("Computing a line!")
        System.currentTimeMillis() / 1000
      }.persist()
      .checkpoint(Seconds(4))
      val grouped = nums.window(Seconds(10))
      grouped.print()
      ssc.start()
    }
  }
}
