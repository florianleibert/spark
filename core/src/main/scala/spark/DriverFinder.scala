package spark

import collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import java.net.URI
import java.io.{FileInputStream, InputStreamReader, BufferedReader}

trait DriverChangeListener {
  def onMasterChanged(newDriverUrl: String)
}

class DriverFinder(driverUrl: String) extends Logging {
  private val listeners = new ArrayBuffer[DriverChangeListener]

  def addListener(listener: DriverChangeListener) {
    listeners += listener
  }

  def removeListener(listener: DriverChangeListener) {
    listeners -= listener
  }

  // For now, we check for a master change using a file in a shared filesystem (e.g. HDFS)
  private val driverFile = System.getProperty("spark.driver.file", "/tmp/spark-master")
  private var currentDriver = driverUrl

  new Thread {
    setDaemon(true)

    override def run() {
      while (true) {
        val in = new BufferedReader(new InputStreamReader(new FileInputStream(driverFile)))
        // TODO: Make sure that the file is complete
        val newDriver = in.readLine()
        if (newDriver != currentDriver) {
          logInfo("New driver found: " + newDriver + "(old was " + currentDriver + ")")
          currentDriver = newDriver
          listeners.foreach(_.onMasterChanged(newDriver))
        }
        Thread.sleep(500)
      }
    }
  }.start()
}
