import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.redis.RedisClient
import com.typesafe.config.Config
import java.net.URI
import java.io.BufferedReader
import scala.collection.mutable._
import org.apache.hadoop.io.SequenceFile
import org.joda.time.DateTime
import org.joda.time.LocalTime
import org.joda.time.Instant
import org.joda.time.Interval

/**
 * @author morandat
 */
object SingleStateMachine extends StateMachineConstant {
  val HDFS_CONFIGURATION = new Configuration()
	val HDFS = FileSystem.get(URI.create(HDFS_URI), HDFS_CONFIGURATION)

  case class Event(ContactId: String, Action: String, TimeCreated: Int) // FIXME TimeCreated should be more complex
  
  case class State(state: Int, timeout: Int) {
    def hasTimeouted: Boolean = { timeout == 0 }
    def timeElapsed: State = { State(state, timeout - 1) } // TODO throw an exception if it has timeouted ?
  }

  
  def main(args: Array[String]): Unit = {
		val stateMachines = HashMap[String, State]()

    RedisKeys.flushAll()
    
    for (x <- HDFS.listStatus(new Path(URI.create(HDFS_FULL_URI)), ScalaPathFilter(p => p.getName.startsWith(".")))) {
      print(s"Reading file: $x ->")
      val start = Instant.now()
      val hdl = HDFS.open(x.getPath)
      val map = HashMap[String, ArrayBuffer[Event]]()
      val buffer = new Array[Byte](x.getLen toInt);  
      hdl.readFully(buffer)
      hdl.close()

      val str = new String(buffer)
      for (l <- str.split("\n")) {
        val key = parseJSON(l)
        map.getOrElseUpdate(key._1, new ArrayBuffer[Event]()).append(key._2);
      }
      
      for (events <- map.values) {
        val id = events.head.ContactId
        val finalState = events.
        sortWith(_.TimeCreated < _.TimeCreated).
        foldLeft(stateMachines.get(id)) { (s, e) => nextState(e.Action, s) }
        finalState match {
        case Some(v @ _) =>
          stateMachines.put(id, v)
        case None =>
          stateMachines.remove(id)
        }
      }

      for (machine <- stateMachines) {
        val id = machine._1
        val state = machine._2
        if (state.hasTimeouted) {
          val tstate = timeoutState(state)
          if (tstate.isDefined)
        	  stateMachines.put(id, tstate.get)
          else
            stateMachines.remove(id)
        } else
          stateMachines.put(id, state.timeElapsed)
      }
      val stop = Instant.now()
      print("%s\n" format(new Interval(start, stop).toDuration()))
    }
  }
  
  def parseJSON(event: String): (String, Event) = {
    implicit val formats = DefaultFormats
    val e = parse(event).extract[Event]
    (e.ContactId, e)
  }
  
  def nextState(action: String, state: Option[State]): Option[State] = {
    import RedisKeys._

    val s = state.getOrElse(State(initialState, DefaultTimeout))
    s.state match {
    case StateA =>
    if (action == EventA) { // Initial + a => B
      Some(State(StateB, DefaultTimeout))
    } else { // Initial + ~a => Err 
      NotStartedState.incr()
      None
    }
    case StateB =>
    if (action == EventB) // B + b => OK
      FinalOKState.incr()
      else // B + !b => Err
        FinalErrorState.incr()
        None
    case _ => // Other State => Err (should not arrive by construction)
    NoSuchState.incr()
    None
    }
  }

  def timeoutState(state: State): Option[State] = {
      RedisKeys.Timeout.incr()
      None
  }

  case class ScalaPathFilter(fun: Path => Boolean) extends PathFilter {
    def accept(path: Path): Boolean = {
      fun(path)
    }
  }
  
  object RedisKeys {
    private val client: RedisClient = new RedisClient(REDIS_HOST, REDIS_PORT)

    def flushAll() = {
    	client.flushall // I Don't really like this,
    	//      keys.foreach { _.flush() } // I would prefer this one
    }

    sealed trait RedisKey {
    	def incr() = {
    		client.incr(toString)
    	}

    	def flush() = {
    		client.set(toString, 0)
    	}
    }

    case object FinalErrorState extends RedisKey
    case object FinalOKState extends RedisKey
    case object Timeout extends RedisKey
    case object NoSuchState extends RedisKey
    case object NotStartedState extends RedisKey

    val keys: Set[RedisKey] = Set(FinalErrorState, FinalOKState, Timeout, NoSuchState, NotStartedState)
  }
}