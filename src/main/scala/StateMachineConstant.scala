import com.typesafe.config._

trait StateMachineConstant {

  // Events vocabulary
  val EventA = "a"
  val EventB = "b"
  
  // States
  val StateA = 0
  val StateB = 1
  val initialState = StateA;

  
  // Default timeout
  val DefaultTimeout = 1000
  
  val Conf = ConfigFactory.load()

  val HDFS_URI = Conf.getString("hdfsURI")
  val HDFS_PATH = Conf.getString("hdfsPath")
  val HDFS_FULL_URI = s"$HDFS_URI$HDFS_PATH"

  val REDIS_HOST = Conf.getString("redisHost")
  val REDIS_PORT = Conf.getInt("redisPort")

  val SAMPLING_INTERVAL = Conf.getInt("samplingInterval")

}