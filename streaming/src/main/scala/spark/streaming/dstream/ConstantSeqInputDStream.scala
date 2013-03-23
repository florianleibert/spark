package spark.streaming.dstream

import spark.streaming.{Time, StreamingContext}
import spark.RDD

/**
 * An input stream that always returns the same collection on each timestep. Useful for testing.
 */
class ConstantSeqInputDStream[T: ClassManifest](ssc_ : StreamingContext, data: Seq[T])
  extends InputDStream[T](ssc_) {

  override def start() {}

  override def stop() {}

  override def compute(validTime: Time): Option[RDD[T]] = {
    Some(context.sparkContext.parallelize(data))
  }
}
