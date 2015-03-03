import java.io.{PrintWriter, _}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.joda.time.DateTime

object EventGenerator extends StateMachineConstant {

  
  // Number of state machines to open 
  val STATE_MACHINES = Conf.getLong("stateMachines")
  // Number of files to play once loaded 
  val PLAY_FILES = Conf.getInt("filesNum")
  // Number of lines to generate per file (default)
  val EVENTS_PER_FILE = Conf.getLong("eventsPerFile")
  // Maximum distance between two events of the same client within the sequence
  val TIMEOUT = Conf.getLong("timeout")
  
  // Directory for the generated files
  val TMP_DIR = Conf.getString("tmpDir")
  
  val FILENAME_TEMPLATE = Conf.getString("filenameTemplate")

  val HDFS = FileSystem.get(URI.create(HDFS_URI), new Configuration())
  
  val MESSAGE = """{ "BrowserGUID" : "1efcef34d09867bafa0f39bc03e58e87", "ContactId" : "%2$d", "Action" : "%1$s", "Duration" : 0, "GoalNo" : 17,"""+
    """"GoalTitle" : "Cdiscount.com - N°1 du e-commerce - Tous vos achats à prix discount", "GoalType" : "page", "PageTypeId" : 1,"""+
    """"ProcessedPPA" : [ "ReferrerProcessActions_ReferrerWidget_FromReferrer" ], "Referrer" : """+
    """{ "Url" : "https://order.preprod-cdiscount.com/OrderProcessCustomer.mvc/Login", "_id" : "5dd6c316f58647cf47892b9736789c23", "Queries" : [] },"""+
    """"SessionGUID" : "f8b837029bee4d6ab39559cfd757195e", "SessionGoalTypeId" : 1, "TimeCreated" : %3$d, "TrackerGoalId" : "f9aa44aae85e22c7be730252ac533a67","""+
    """"_id" : "53a17ffeff10f159588b4572", "rawPost" : "", "rawURL" : "http://www.cdiscount.com/Produit3.html" }""" + "\n" 


  def main(args: Array[String]) {
    val cmd = if (args.isEmpty) "help" else args.head
    
    val HDFS = FileSystem.get(URI.create(HDFS_URI), new Configuration())
    HDFS.mkdirs(new Path(HDFS_FULL_URI)) // Ensure directories exists
    
    cmd match {
      case "ls" =>
        list()
      case "clean" =>
        clean()
      case "generate" =>
        generate()
      case "stream" =>
        stream()
      case "reset" =>
        resetNames()
      case _ =>
        println("""Commands are :
ls    List files on HDFS directory
clean  Remove files from HDFS directory
generate  Geneerate event files
stream  Start streaming files
reset  Reset file status to stream again
help  this screen
""")
    }
  }
  
  def list() {
	  for {
		  hdfsFileStatuses <- Option(HDFS.listStatus(new Path(URI.create(HDFS_FULL_URI))))
		  hdfsFileStatus <- hdfsFileStatuses
	  } println("%s %d %d".format(hdfsFileStatus.getPath, hdfsFileStatus.getLen, hdfsFileStatus.getReplication))
  }

  def resetNames() {
    for (x <- HDFS.listStatus(new Path(URI.create(HDFS_FULL_URI)), ScalaPathFilter(p => !p.getName.startsWith(".")))) {
      val p = x.getPath
      HDFS.rename(p, new Path(p.getParent, "." + p.getName))
      log("Reseting %s" format p)
    }
  }
  
  
  def stream() {
    for (x <- HDFS.listStatus(new Path(URI.create(HDFS_FULL_URI)), ScalaPathFilter(p => p.getName.startsWith(".")))) {
      val p = x.getPath
      val n = new Path(p.getParent, p.getName.substring(1))
      HDFS.rename(p, n)
      log("Activating: %s" format n)

      Thread sleep SAMPLING_INTERVAL
    }
  }
  
  case class ScalaPathFilter(fun: Path => Boolean) extends PathFilter {
    def accept(path: Path): Boolean = {
      fun(path)
    }
  }
  
  def clean() {
    for {
      hdfsFileStatuses <- Option(HDFS.listStatus(new Path(URI.create(HDFS_FULL_URI))))
      hdfsFileStatus <- hdfsFileStatuses
    } {
    	log("Removing: %s" format hdfsFileStatus.getPath)
      HDFS.delete(hdfsFileStatus.getPath, false)
    }
  }
  
  def generate() {
    /** Generate data files and store them in HDFS **/
    
    // First generate enough data
    val filesBeforeLoad = ceilDiv(STATE_MACHINES, EVENTS_PER_FILE)
    for (i <- 0L until filesBeforeLoad) {
      fileWrite(i, w => for(c <- 0L until EVENTS_PER_FILE)
        w.write(formatEvent(EventA, i * EVENTS_PER_FILE + c, c)))
    }
    //Then generate some noise
    
     for (i <- filesBeforeLoad until (filesBeforeLoad + PLAY_FILES)) {
      fileWrite(i, w => for(c <- 0L until (EVENTS_PER_FILE / 2)) {
        w.write(formatEvent(EventA, i * EVENTS_PER_FILE + c, 2 * c))
        w.write(formatEvent(EventB, i * EVENTS_PER_FILE + c, 2 * c + 1))
      })
    }
  }
  
  def ceilDiv(x: Long, y: Long): Long = {
    (x - 1) / y + 1 
  } 
  
  def fileWrite(n: Long, f: Writer => Unit) = {
	  val filename = s"$TMP_DIR/$FILENAME_TEMPLATE".format(n)
    val writer = new PrintWriter(filename)
    log(s"Creating $filename")
    f(writer)
	  writer.close()
	  HDFS.moveFromLocalFile(new Path(filename), new Path(URI.create(HDFS_FULL_URI)))
    log(s"Moved $filename => $HDFS_FULL_URI")
  } 
  
  def log(msg: String):Unit = {
	  val t0 = DateTime.now
    println(f"[$t0%s] $msg%s")
  }

  def formatEvent(event: String, client: Long, timestamp: Long): String = {
    MESSAGE.format(event, client, timestamp)
  }
}
