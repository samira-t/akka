package akka.setack
import akka.setack.core.TestExecutionManager

trait SetackTest {

  def superBeforeEach() {
    TestExecutionManager.startTest
  }

  def superAfterEach() {
    TestExecutionManager.stopTest
  }

}