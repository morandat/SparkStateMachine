import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.FDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object SparkLS extends StateMachineConstant {

  val conf = new SparkConf()
  conf.set("spark.executor.memory", "2g") // <--- mouaif
  val ssc = new StreamingContext(conf, Milliseconds(SAMPLING_INTERVAL))


  @transient private var path_ : Path = null
  @transient private var fs_ : FileSystem = null

  private def directoryPath: Path = {
    if (path_ == null) path_ = new Path(HDFS_FULL_URI)
    path_
  }

  private def fs: FileSystem = {
    if (fs_ == null) fs_ = directoryPath.getFileSystem(ssc.sparkContext.hadoopConfiguration)
    fs_
  }

  def main(args: Array[String]): Unit = {
    ssc.checkpoint(s"$HDFS_URI/Spark/Checkpoints")

    print(s"Streaming from: $HDFS_FULL_URI\n")

    val newFiles = fs.listStatus(directoryPath).map(_.getPath.toString)

    newFiles.foreach(x => print("%s\n".format(x)))

    val files = new FDStream[LongWritable, Text, TextInputFormat](ssc, HDFS_FULL_URI)
    val data =  files.map(_._2.toString)

    val events = data.flatMap(_.split("\n"))

    events.count().print()

    ssc.start()
    ssc.awaitTermination()
  }

  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")
}