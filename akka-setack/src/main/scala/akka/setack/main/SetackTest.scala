package akka.setack
import core.TestExecutionManager
import core.monitor.Monitor
import util.TestMessageUtil
import core.TestMessageInvocation
import core.TestActorRef
import util.TestActorRefFactory
import util.TestExecutionUtil
import core.TestMessageInvocationSequence
import akka.actor.Actor
import akka.japi.Creator
import akka.actor.Props
import akka.actor.UntypedChannel

trait SetackTest {

  val monitor = new Monitor()
  val testExecutionManager = new TestExecutionManager(monitor)
  val testMessageUtil = new TestMessageUtil(monitor)
  val testActorRefFactory = new TestActorRefFactory(monitor)
  val testExecutionUtil = new TestExecutionUtil(testExecutionManager)

  def superBeforeEach() {
    testExecutionManager.startTest
  }

  def superAfterEach() {
    testExecutionManager.stopTest
  }
  /**
   * Waits for the system to gets stable and then executes the body which is usually a set of
   * assertion statements.
   */
  def whenStable(body: ⇒ Unit)(implicit tryCount: Int = TestConfig.maxTryForStability): Boolean = {
    val isStable = testExecutionManager.checkForStability(tryCount)
    body
    isStable
  }

  /*
   * testMessageUtil API calls
   */
  def testMessage(sender: UntypedChannel, receiver: UntypedChannel, message: Any) =
    testMessageUtil.testMessage(sender, receiver, message)

  def testMessagePattern(sender: UntypedChannel, receiver: UntypedChannel, messagePattern: PartialFunction[Any, Any]) =
    testMessageUtil.testMessagePattern(sender, receiver, messagePattern)

  /**
   * Checks if the message is delivered or not by asking from trace monitor actor.
   */
  def isDelivered(testMessage: TestMessageInvocation) = testMessageUtil.isDelivered(testMessage)

  /**
   * @return the number of the test messages delivered.
   */
  def deliveryCount(testMessage: TestMessageInvocation) = testMessageUtil.deliveryCount(testMessage)

  /**
   * Checks if the message is processed by asking from trace monitor actor.
   */
  def isProcessed(testMessage: TestMessageInvocation) = testMessageUtil.isProcessed(testMessage)

  /**
   * @return the number of the test messages processed.
   */
  def processingCount(testMessage: TestMessageInvocation) = testMessageUtil.processingCount(testMessage)

  /*
 * testExecutionUtil API call
 */
  /**
   * API for constraining the schedule of test execution and removing some non-determinism by specifying
   * a set of partial orders between the messages. The receivers of the messages in each partial order should
   * be the same (an instance of TestActorRef)
   */
  def setSchedule(partialOrders: TestMessageInvocationSequence*) = testExecutionUtil.setSchedule(partialOrders.toSet)

  /*
   * testActorRefFactory API calls
   */
  def actorOf[T <: Actor: Manifest](address: String) = testActorRefFactory.actorOf[T](address)

  def actorOf[T <: Actor: Manifest] = testActorRefFactory.actorOf[T]

  def actorOf[T <: Actor](clazz: Class[T]) = testActorRefFactory.actorOf[T](clazz)

  def actorOf[T <: Actor](clazz: Class[T], address: String) = testActorRefFactory.actorOf[T](clazz, address)

  def actorOf[T <: Actor](factory: ⇒ T) = testActorRefFactory.actorOf[T](factory)

  def actorOf[T <: Actor](creator: ⇒ T, address: String) = testActorRefFactory.actorOf[T](creator, address)

  def actorOf[T <: Actor](creator: Creator[T]) = testActorRefFactory.actorOf[T](creator)

  def actorOf[T <: Actor](creator: Creator[T], address: String) = testActorRefFactory.actorOf[T](creator, address)

  def actorOf(props: Props): TestActorRef = testActorRefFactory.actorOf(props)

  def actorOf(props: Props, address: String) = testActorRefFactory.actorOf(props, address)

}