/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.core
import akka.actor.Actor
import akka.actor.ActorRef
import monitor.Monitor.traceMonitorActor
import monitor.NotProcessedMessages
import monitor.AllDeliveredMessagesAreProcessed
import akka.actor.LocalActorRef
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer

/**
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */
object TestExecutionManager {

  private var actorsWithMessages = new HashSet[ActorRef]

  /**
   * Starts the monitor actor
   */
  def startTest {
    monitor.Monitor.startMonitoring()
  }

  /**
   * Checks for the stability of the system.
   * The system is stable if:
   * 1) the mailbox of the actors in the test are empty
   * 2) the monitor actor has received the confirmation of processing all the delivered messages
   * In fact, if condition 1 holds, the actor might be busy with processing the last message. The
   * second condition takes care of this situation
   *
   * It tries to check the stability of the system by giving maxTry. the default is 10 times
   * and each time it sleeps 100 milliseconds before trying again.
   *
   * @return false if the system did not get stable with that maxTry
   *
   *
   */
  def checkForStability(maxTry: Int = 10): Boolean =
    {
      var triedCheck = 0
      var isStable = false

      while (triedCheck < maxTry && !isStable) {
        Thread.sleep(100)
        triedCheck += 1

        isStable = true
        actorsWithMessages.clear()

        for (a ← Actor.registry.local) {

          if (a.isRunning && !a.asInstanceOf[LocalActorRef].dispatcher.mailboxIsEmpty(a.asInstanceOf[LocalActorRef])) {
            log(a + " has message")
            if (triedCheck == maxTry)
              actorsWithMessages.add(a)
            isStable = false
          }
        }

        /*
         * If no message is in the mailbox of the actors check to see if they have finished with processing 
         * all of their messages. In that case, ask from monitor actor to see if the messages that 
         * have been delivered are processed.
         */
        if (isStable) {
          if (!(traceMonitorActor ? AllDeliveredMessagesAreProcessed).get.asInstanceOf[Boolean]) {
            isStable = false
            if (triedCheck == maxTry) {
              //record the messages that are delivred but not processed yet
              val notProcessedMessages = (traceMonitorActor ? NotProcessedMessages).mapTo[ArrayBuffer[RealMessageInvocation]].get
            }
            log("not all delivered messages are processed yet")

          }
        }
      }
      if (!isStable) println("no stable")
      isStable

    }

  /**
   * Stops the test by stopping all the test actors and the monitor actor
   */
  def stopTest {
    log("stability for stopping the test")
    val stable = checkForStability()

    /*
     *  remove the actors that has not finished processing their messages
     *  from registry to prepare the registry for the next test. 
     *  These actors might be in an infinite loop and cannot be stopped 
     */
    for (actor ← actorsWithMessages) {
      log("unregister" + actor)
      Actor.registry.unregister(actor)
      log(actor + " has not finished processing its messages")
    }

    // stop other actors
    Actor.registry.local.shutdownAll

  }

  private var debug = false
  private def log(s: String) = if (debug) println(s)
}