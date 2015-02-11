/**
 * Created by wbraik on 21/07/14.
 */

import com.redis.RedisClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStateMachine {

  // Initial state
  val StateA = "A"
  // Transition state
  val StateB = "B"
  // Default timeout
  val DefaultTimeout = 1000
  // Windows corresponding to the batch duration
  // Events
  val EventA = "a"
  val EventB = "b"
  // Redis keys
  val RedisErr = "ERR"
  val RedisOk = "OK"
  val RedisTo = "TO"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("hdfs://zeus:8020/Spark/Checkpoints")

    val events = ssc.textFileStream("hdfs://zeus:8020/Events")
      .flatMap(_.split(", "))
      .map(e => (e.substring(1), e.substring(0, 1)))

    val states = events.updateStateByKey[(String, Int)](updateFunction _)

    states.count().print()
    //states.print()
    states.saveAsTextFiles("hdfs://zeus:8020/Spark/Tmp/state")

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(newEvents: Seq[String], previousState: Option[(String, Int)]): Option[(String, Int)] = {

    var state = previousState.getOrElse((StateA, DefaultTimeout))

    if (!newEvents.isEmpty) {
      // Events found for the client, reset timeout
      for (i <- 0 until newEvents.length) {
        // Parse client's new events and compute its new state
        val event = newEvents(i)

        state._1 match {
          case StateA =>
            if (event == EventA) {
              // A + a -> B
              state = (StateB, DefaultTimeout)
            } else {
              // A + b -> C, ERROR
              state = (StateA, DefaultTimeout)
              RedisManager.store(RedisErr)
            }
          case StateB =>
            if (event == EventB) {
              // B + b -> D, final state reached
              state = (StateA, DefaultTimeout)
              RedisManager.store(RedisOk)
            } else {
              // B + a -> C, ERROR
              state = (StateA, DefaultTimeout)
              RedisManager.store(RedisErr)
            }
        }
      }
      Some(state)
    } else {
      // No events found for this client
      if (state._1 == StateA) {
        // Client is hanging on Initial state, delete it
        None
      } else if (state._2 - 1 == 0) {
        // Client timed out, delete it
        RedisManager.store(RedisTo)
        None
      } else {
        // Client hasn't timed out yet
        Some((state._1, state._2 - 1))
      }
    }
  }
}

object RedisManager {

  val redisCli: RedisClient = new RedisClient("zeus", 6379)
  redisCli.flushall

  def store(key: String): Unit = {
    //redisCli.incr(key)
  }
}