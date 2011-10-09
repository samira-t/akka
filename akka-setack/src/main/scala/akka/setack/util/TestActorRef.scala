/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.util
import akka.actor.LocalActorRef
import akka.actor.Actor
import akka.japi.Creator
import akka.actor.ActorInitializationException
import akka.util.ReflectiveAccess
import akka.event.EventHandler
import java.lang.reflect.InvocationTargetException
import akka.actor.ActorRefProvider
import akka.actor.Props
import com.eaio.uuid.UUID
import akka.actor.Props
import akka.actor.ActorRefProvider
import akka.actor.Address
import akka.actor.ActorRef
import akka.actor.Deployer
import akka.actor.DeploymentConfig._
import akka.setack.core.dispatcher.TestDispatcher
import akka.setack.core.TestActorRef

/**
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */

/**
 * This actor is an actor whose instances can be matched with
 * any other actors.
 * It is a wild card for matching
 */
class AnyActor extends Actor {
  def receive = {
    case _ ⇒
  }
}

/**
 * Local ActorRef provider.
 */
/*class TestActorRefProvider extends ActorRefProvider {

  def actorOf(props: Props, address: String): Option[TestActorRef] = actorOf(props, address, false)

  def findActorRef(address: String): Option[ActorRef] = Actor.registry.local.actorFor(address)

  private[akka] def actorOf(props: Props, address: String, systemService: Boolean): Option[TestActorRef] = {
    Address.validate(address)

    Actor.registry.actorFor(address) match { // check if the actor for the address is already in the registry
      case ref @ Some(r) ⇒ Some(r.asInstanceOf[TestActorRef]) // it is -> return it

      case None ⇒
        Some(new TestActorRef(props, address, systemService)) // it is not -> create it

        // if 'Props.deployId' is not specified then use 'address' as 'deployId'
        val deployId = props.deployId match {
          case Props.`defaultDeployId` | null ⇒ address
          case other                          ⇒ other
        }

        Deployer.lookupDeploymentFor(deployId) match { // see if the deployment already exists, if so use it, if not create actor

          case Some(Deploy(_, _, router, _, Local)) ⇒
            // FIXME create RoutedActorRef if 'router' is specified
            Some(new TestActorRef(props, address, systemService)) // create a local actor

          case deploy ⇒ None // non-local actor
        }
    }
  }
}*/

/**
 * The factory methods in Actor object should be replaced with the factory methods in this
 * object. It returns a TestActorRef as the reference instead of ActroRef
 */
object TestActorRef {

  //  val provider = new TestActorRefProvider

  /**
   * an instance of AnyActor for using as a wild card
   */
  val anyActorRef = actorOf[AnyActor]

  /**
   *  Creates an ActorRef out of the Actor with type T.
   * <pre>
   *   import Actor._
   *   val actor = actorOf[MyActor]
   *   actor ! message
   *   actor.stop()
   * </pre>
   * You can create and start the actor in one statement like this:
   * <pre>
   *   val actor = actorOf[MyActor]
   * </pre>
   */
  def actorOf[T <: Actor: Manifest](address: String): TestActorRef =
    actorOf(manifest[T].erasure.asInstanceOf[Class[_ <: Actor]], address)

  /**
   * Creates an ActorRef out of the Actor with type T.
   * Uses generated address.
   * <pre>
   *   import Actor._
   *   val actor = actorOf[MyActor]
   *   actor ! message
   *   actor.stop
   * </pre>
   * You can create and start the actor in one statement like this:
   * <pre>
   *   val actor = actorOf[MyActor]
   * </pre>
   */
  def actorOf[T <: Actor: Manifest]: TestActorRef =
    actorOf(manifest[T].erasure.asInstanceOf[Class[_ <: Actor]], new UUID().toString)

  /**
   * Creates an ActorRef out of the Actor of the specified Class.
   * Uses generated address.
   * <pre>
   *   import Actor._
   *   val actor = actorOf(classOf[MyActor])
   *   actor ! message
   *   actor.stop()
   * </pre>
   * You can create and start the actor in one statement like this:
   * <pre>
   *   val actor = actorOf(classOf[MyActor])
   * </pre>
   */
  def actorOf[T <: Actor](clazz: Class[T]): TestActorRef = actorOf(clazz, new UUID().toString)

  /**
   * Creates an ActorRef out of the Actor of the specified Class.
   * <pre>
   *   import Actor._
   *   val actor = actorOf(classOf[MyActor])
   *   actor ! message
   *   actor.stop
   * </pre>
   * You can create and start the actor in one statement like this:
   * <pre>
   *   val actor = actorOf(classOf[MyActor])
   * </pre>
   */
  def actorOf[T <: Actor](clazz: Class[T], address: String): TestActorRef = actorOf(Props(clazz), address)

  /**
   * Creates an ActorRef out of the Actor. Allows you to pass in a factory function
   * that creates the Actor. Please note that this function can be invoked multiple
   * times if for example the Actor is supervised and needs to be restarted.
   * Uses generated address.
   * <p/>
   * <pre>
   *   import Actor._
   *   val actor = actorOf(new MyActor)
   *   actor ! message
   *   actor.stop()
   * </pre>
   * You can create and start the actor in one statement like this:
   * <pre>
   *   val actor = actorOf(new MyActor)
   * </pre>
   */
  def actorOf[T <: Actor](factory: ⇒ T): TestActorRef = actorOf(factory, new UUID().toString)

  /**
   * Creates an ActorRef out of the Actor. Allows you to pass in a factory function
   * that creates the Actor. Please note that this function can be invoked multiple
   * times if for example the Actor is supervised and needs to be restarted.
   * <p/>
   * This function should <b>NOT</b> be used for remote actors.
   * <pre>
   *   import Actor._
   *   val actor = actorOf(new MyActor)
   *   actor ! message
   *   actor.stop
   * </pre>
   * You can create and start the actor in one statement like this:
   * <pre>
   *   val actor = actorOf(new MyActor)
   * </pre>
   */
  def actorOf[T <: Actor](creator: ⇒ T, address: String): TestActorRef = actorOf(Props(creator), address)

  /**
   * Creates an ActorRef out of the Actor. Allows you to pass in a factory (Creator<Actor>)
   * that creates the Actor. Please note that this function can be invoked multiple
   * times if for example the Actor is supervised and needs to be restarted.
   * Uses generated address.
   * <p/>
   * JAVA API
   */
  def actorOf[T <: Actor](creator: Creator[T]): TestActorRef = actorOf(Props(creator), new UUID().toString)

  /**
   * Creates an ActorRef out of the Actor. Allows you to pass in a factory (Creator<Actor>)
   * that creates the Actor. Please note that this function can be invoked multiple
   * times if for example the Actor is supervised and needs to be restarted.
   * <p/>
   * This function should <b>NOT</b> be used for remote actors.
   * JAVA API
   */
  def actorOf[T <: Actor](creator: Creator[T], address: String): TestActorRef = actorOf(Props(creator), address)

  /**
   * Creates an ActorRef out of the Actor.
   * <p/>
   * <pre>
   *   FIXME document
   * </pre>
   */
  def actorOf(props: Props): TestActorRef = actorOf(props, new UUID().toString)

  /**
   * Creates an ActorRef out of the Actor.
   * <p/>
   * <pre>
   *   FIXME document
   * </pre>
   */
  def actorOf(props: Props, address: String): TestActorRef =
    //provider.actorOf(props.withDispatcher(TestDispatcher), address).get
    new TestActorRef(props.withDispatcher(TestDispatcher), address)
}