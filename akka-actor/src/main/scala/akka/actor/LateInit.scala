package akka.actor

/**
 * Add-on for FSM to defer initialization of the state machine until
 * ActorRef.start() time. This is useful in case of a state timeout on the
 * initial state, when that timeout shall not start upon Actor creation, or
 * when implementing FSMs in an inheritance tree.
 *
 * Two things are mandatory:
 *  - DelayedInit processing starts with the first class (as opposed to trait)
 *    which inherits from it, hence LateInit must be mixed in at that level;
 *    otherwise there will be an initialization order failure.
 *  - The <code>preStart</code> method needs to be invoked, therefore make sure
 *    to call <code>super.preStart</code> if you are overriding it within your
 *    class. Also, in contrast to a normal actor, overriding
 *    <code>preStart</code> needs the <code>override</code> modifier in this
 *    case.
 */
trait LateInit { this : FSM[_, _] =>

  private var initcode : List[() => Unit] = Nil

  abstract override def delayedInit(body : => Unit) {
    if (initcode eq null) {
      sys.error("LateInit needs to be mixed into the first class inheriting from FSM")
    }
    initcode ::= body _
  }

  def preStart {
    initcode.reverse foreach (_.apply)
    initialize
  }

}

// vim: set ts=2 sw=2 et:
