/**
 * Created by wbraik on 21/07/14.
 */

import com.redis.RedisClient
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.{DStream, FDStream, InputDStream}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchStarted, StreamingListenerBatchSubmitted}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.Queue

object SparkStateMachine extends StateMachineConstant {

  case class Event(ContactId: String, Action: String, TimeCreated: Int) // FIXME TimeCreated should be more complex
  
  val conf = new SparkConf()
	conf.set("spark.executor.memory", "2g") // <--- mouaif
	val ssc = new StreamingContext(conf, Milliseconds(SAMPLING_INTERVAL))

  def main(args: Array[String]): Unit = {
//    ssc.addStreamingListener(new BatchMonitor)
    ssc.checkpoint(s"$HDFS_URI/Spark/Checkpoints")
    RedisKeys.flushAll(); // FIXME Move-me

//  val events = new TextFileSeqAsStream(fileSequence(), ssc)
    val events = textFileStream(ssc, HDFS_FULL_URI)
      .flatMap(_.split("\n"))
      .map(parseJSON _)

    val states = events.updateStateByKey(updateFunction _)

    states.count().print()

    ssc.start()
    ssc.awaitTermination()
  }

  def textFileStream(ssc: StreamingContext, path: String): DStream[String] = {
    new FDStream[LongWritable, Text, TextInputFormat](ssc, path).map(_._2.toString)
  }

  def parseJSON(event: String): (String, Event) = {
    implicit val formats = DefaultFormats // for jackson

    val e = parse(event).extract[Event]
    (e.ContactId, e)
  }

  def updateFunction(newEvents: Seq[Event], previousState: Option[State]): Option[State] = {
    if (!newEvents.isEmpty) {
      return newEvents.
        sortWith((e1, e2) => e1.TimeCreated < e2.TimeCreated).
        foldLeft(previousState) { (s, e) => nextState(e.Action, s) }
    } else if (previousState.isEmpty){ // This case should never arise, maybe let's just fall back on the following state.get exception
    	RedisKeys.NoSuchState.incr()
      return None
    } else {
      val state = previousState.get
      if (state.hasTimeouted) {
        timeoutState(state)
      } else { // Client hasn't timed out yet
        Some(state.timeElapsed)
      }
    }
  }
  
	def nextState(action: String, state: Option[State]): Option[State] = {
		import SparkStateMachine.RedisKeys._

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
  
   case class State(state: Int, timeout: Int) {
    def hasTimeouted: Boolean = { timeout == 0 }
    def timeElapsed: State = { State(state, timeout - 1) } // TODO throw an exception if it has timeouted ?
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

class ClockStream(
    var tick: Long,
    ssc: StreamingContext)
extends InputDStream[Long](ssc) {

  override def start() { }
  override def stop() { }
  
	override def compute(validTime: Time): Option[RDD[Long]] = {
    tick = tick + 1
    Some(ssc.sparkContext.makeRDD(Seq(tick)))
  }
}

class BatchMonitor extends StreamingListener {
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    print("Submitted: %d %d\n" format(batchSubmitted.batchInfo.processingStartTime.getOrElse(-1), batchSubmitted.batchInfo.processingEndTime.getOrElse(-1)))
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    print("Started: %d %d\n" format(batchStarted.batchInfo.processingStartTime.getOrElse(-1), batchStarted.batchInfo.processingEndTime.getOrElse(-1)))
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    print("Completed: %d %d'\n" format(batchCompleted.batchInfo.processingStartTime.getOrElse(-1), batchCompleted.batchInfo.processingEndTime.getOrElse(-1)))
  }
}

// Currently unused
class TextFileSeqAsStream(
    val files: Queue[String],
    ssc: StreamingContext)
extends InputDStream[(LongWritable, Text)](ssc) {
	override def start() { }

  override def stop() { }

  override def compute(validTime: Time): Option[RDD[(LongWritable, Text)]] = {
    try {
    	val f = files.dequeue
    	val rdd = ssc.sparkContext.newAPIHadoopFile[LongWritable, Text, TextInputFormat](f)

    	if (rdd.partitions.size == 0) {
    		logError("File " + f + " has no data in it. Spark Streaming can only ingest " +
    				"files that have been \"moved\" to the directory assigned to the file stream. " +
    				"Refer to the streaming programming guide for more details.")
    	}
    	Some(rdd)
    } catch {
      case e: NoSuchElementException =>
        ssc.stop(false, true)
        None
    }
  }
}