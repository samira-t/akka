package akka.stm

import org.multiverse.api._
import exceptions._
import functions.LongFunction
import lifecycle.{TransactionEvent, TransactionListener}
import org.multiverse.stms.gamma.transactions.{GammaTransactionFactoryBuilder, GammaTransactionFactory, GammaTransaction, GammaTransactionPool}
import org.multiverse.stms.gamma.transactionalobjects._
import java.util.concurrent.TimeUnit
import org.multiverse.stms.gamma.{GammaStmUtils, GammaStm}
import org.multiverse.MultiverseConstants

/**
 *
 */
final class Ref[E] {

    import AkkaStm._

    private val ref = new GammaRef[E](GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm]);
    val lock: AkkaLock = new AkkaLockImpl(ref)
    val view = new RefView[E] {

        override def get = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicGet() else ref.get(tx)
        }

        override def getOrElse(f: => E) = {
            val result = ref.get
            if (result == null) f else result
        }

        override def opt() = Option(get)

        override def isNull = get == null

        override def swap(newValue: E) = {
            val tx = getThreadLocalTx
            if (tx == null) ref.atomicGetAndSet(newValue) else ref.getAndSet(tx, newValue)
        }

        override def set(newValue: E) = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicSet(newValue) else ref.set(tx, newValue)
        }

        override def alter(f: (E) => E) = {
            val tx = getThreadLocalTx
            if (tx eq null) atom.alter(f) else Ref.this.alter(f)(tx)
        }

        override def toString = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicToString else ref.toString(tx)
        }
    }
    val atom = new RefAtom[E] {

        override def get = ref.atomicGet

        override def getOrElse(f: => E) = {
            val result = ref.atomicGet
            if (result == null) f else result
        }

        override def weakGet: E = ref.atomicWeakGet

        override def opt = Option(ref.atomicGet)

        override def isNull: Boolean = ref.atomicIsNull

        override def swap(newValue: E) = ref.atomicGetAndSet(newValue)

        override def set(newValue: E) = ref.atomicSet(newValue)

        override def alter(f: (E) => E) = ref.atomicAlterAndGet(new org.multiverse.api.functions.Function[E] {
            override def call(value: E) = f(value)
        })

        override def compareAndSet(expectedValue: E, newValue: E) = ref.atomicCompareAndSet(expectedValue, newValue)

        override def toString = ref.atomicToString
    }

    def value: E = ref.get()

    def value_=(newValue: E): Unit = ref.set(newValue)

    def getOrElse(defaultValue: E, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): E = {
        val result = ref.getAndLock(tx, lockMode)
        if (result == null) defaultValue else result
    }

    def opt(lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Option[E] = {
        val result = ref.getAndLock(tx, lockMode)
        if (result == null) None else new Some[E](result)
    }

    def get(lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): E =
        ref.getAndLock(tx, lockMode)

    def set(newValue: E, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): E =
        ref.setAndLock(tx, newValue, lockMode)

    def swap(newValue: E, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): E =
        ref.getAndSetAndLock(tx, newValue, lockMode)

    def alter(f: (E) => E, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): E = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], lockMode.asInt())
        val result = f(tranlocal.ref_value.asInstanceOf[E])
        tranlocal.ref_value = result
        result
    }

    def ensure(implicit tx: Transaction = getThreadLocalTx): Unit = ref.ensure(tx)

    def isNull(implicit tx: Transaction = getThreadLocalTx): Boolean = ref.isNull(tx)

    def commute(f: (E) => E)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        ref.commute(tx, new org.multiverse.api.functions.Function[E] {
            override def call(v: E) = f(v)
        })
    }

    def awaitNotNullAndGet(implicit tx: Transaction = getThreadLocalTx): E = ref.awaitNotNullAndGet(tx)

    def awaitNull(implicit tx: Transaction = getThreadLocalTx): Unit = ref.awaitNull(tx)

    def toDebugString(): String = ref.toDebugString

    def toString(implicit tx: Transaction = getThreadLocalTx): String = ref.toString(tx)

    def await(f: (E) => Boolean, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx) = {
        if (!f(ref.getAndLock(lockMode))) AkkaStm.retry()
    }
}

final class IntRef(value: Int = 0) {

    import AkkaStm._

    private val ref = new GammaIntRef(GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm], value);
    val lock: AkkaLock = new AkkaLockImpl(ref)

    val view = new View[Int] {

        override def get = {
            implicit val tx = getThreadLocalTx
            if (tx eq null) ref.atomicGet() else IntRef.this.get()()
        }

        override def swap(newValue: Int) = {
            implicit val tx = getThreadLocalTx
            if (tx eq null) ref.atomicGetAndSet(newValue) else IntRef.this.swap(newValue)
        }

        override def set(newValue: Int) = {
            implicit val tx = getThreadLocalTx
            if (tx eq null) ref.atomicSet(newValue) else IntRef.this.set(newValue)
        }

        override def alter(f: (Int) => Int) = {
            val tx = getThreadLocalTx
            if (tx eq null) atom.alter(f) else IntRef.this.alter(f)
        }

        override def toString = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicToString else ref.toString(tx)
        }
    }

    val atom = new NumberAtom[Int] {

        override def get = ref.atomicGet

        override def weakGet = ref.atomicWeakGet

        override def swap(newValue: Int) = ref.atomicGetAndSet(newValue)

        override def set(newValue: Int) = ref.atomicSet(newValue)

        override def alter(f: (Int) => Int) = ref.atomicAlterAndGet(new org.multiverse.api.functions.IntFunction {
            override def call(value: Int) = f(value)
        })

        override def compareAndSet(expectedValue: Int, newValue: Int) = ref.atomicCompareAndSet(expectedValue, newValue)

        override def atomicInc(amount: Int = 1) = ref.atomicIncrementAndGet(amount)

        override def atomicDec(amount: Int = 1) = ref.atomicIncrementAndGet(-1 * amount)

        override def toString = ref.atomicToString
    }

    def *=(rhs: Int)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], MultiverseConstants.LOCKMODE_NONE)
        val result: Int = tranlocal.long_value.asInstanceOf[Int] * rhs
        tranlocal.long_value = result.asInstanceOf[Long]
    }

    def +=(rhs: Int)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], MultiverseConstants.LOCKMODE_NONE)
        val result: Int = tranlocal.long_value.asInstanceOf[Int] + rhs
        tranlocal.long_value = result.asInstanceOf[Long]
    }

    def -=(rhs: Int)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], MultiverseConstants.LOCKMODE_NONE)
        val result: Int = tranlocal.long_value.asInstanceOf[Int] - rhs
        tranlocal.long_value = result.asInstanceOf[Long]
    }

    def get(lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Int =
        ref.getAndLock(tx, lockMode)

    def set(newValue: Int, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Int =
        ref.setAndLock(tx, newValue, lockMode)

    def swap(newValue: Int, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Int =
        ref.getAndSetAndLock(tx, newValue, lockMode)

    def incAndGet(amount: Int = 1)(implicit tx: Transaction = getThreadLocalTx): Int =
        ref.incrementAndGet(tx, amount)

    def getAndInc(amount: Int = 1)(implicit tx: Transaction = getThreadLocalTx): Int =
        ref.getAndIncrement(tx, amount)

    def inc(amount: Int = 1)(implicit tx: Transaction = getThreadLocalTx): Unit =
        ref.increment(tx, amount)

    def dec(amount: Int = 1)(implicit tx: Transaction = getThreadLocalTx): Unit =
        ref.decrement(tx, amount)

    def alter(f: (Int) => Int, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Int = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], lockMode.asInt())
        val result = f(tranlocal.long_value.asInstanceOf[Int])
        tranlocal.long_value = result.asInstanceOf[Long]
        result
    }

    def commute(f: (Int) => Int)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        ref.commute(tx, new org.multiverse.api.functions.IntFunction {
            override def call(v: Int) = f(v)
        })
    }

    def ensure(implicit tx: Transaction = getThreadLocalTx): Unit = ref.ensure(tx)

    def toDebugString(): String = ref.toDebugString

    def toString(implicit tx: Transaction = getThreadLocalTx): String = ref.toString(tx)

    //def await(value: Int)(implicit tx: Transaction = getThreadLocalTx) = ref.await(tx, value)

    def await(f: (Int) => Boolean, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx) =
        if (!f(ref.getAndLock(tx, lockMode))) tx.retry()
}

final class DoubleRef(value: Double = 0) {

    import AkkaStm._
    import GammaStmUtils._

    private val ref = new GammaDoubleRef(GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm], value);
    val lock: AkkaLock = new AkkaLockImpl(ref)

    val view = new View[Double] {

        override def get() = {
            implicit val tx = getThreadLocalTx
            if (tx eq null) ref.atomicGet() else DoubleRef.this.get()
        }

        override def swap(newValue: Double) = {
            implicit val tx = getThreadLocalTx
            if (tx eq null) ref.atomicGetAndSet(newValue) else DoubleRef.this.swap(newValue)
        }

        override def set(newValue: Double) = {
            implicit val tx = getThreadLocalTx
            if (tx eq null) ref.atomicSet(newValue) else DoubleRef.this.set(newValue)
        }

        def alter(f: (Double) => Double): Double = {
            implicit val tx = getThreadLocalTx
            if (tx eq null) atom.alter(f) else DoubleRef.this.alter(f)
        }

        override def toString = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicToString else ref.toString(tx)
        }
    }

    val atom = new NumberAtom[Double] {

        override def get = ref.atomicGet

        override def weakGet = ref.atomicWeakGet

        override def swap(newValue: Double) = ref.atomicGetAndSet(newValue)

        override def set(newValue: Double) = ref.atomicSet(newValue)

        override def alter(f: (Double) => Double) = ref.atomicAlterAndGet(new org.multiverse.api.functions.DoubleFunction {
            override def call(value: Double) = f(value)
        })

        override def compareAndSet(expectedValue: Double, newValue: Double) = ref.atomicCompareAndSet(expectedValue, newValue)

        override def atomicInc(amount: Double = 1) = ref.atomicIncrementAndGet(amount)

        override def atomicDec(amount: Double = 1) = ref.atomicIncrementAndGet(-1 * amount)

        override def toString = ref.atomicToString
    }

    def *=(rhs: Double)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], MultiverseConstants.LOCKMODE_NONE)
        val result: Double = longAsDouble(tranlocal.long_value) * rhs
        tranlocal.long_value = doubleAsLong(result)
    }


    def +=(rhs: Int)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], MultiverseConstants.LOCKMODE_NONE)
        val result: Double = longAsDouble(tranlocal.long_value) + rhs
        tranlocal.long_value = doubleAsLong(result)
    }

    def -=(rhs: Int)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], MultiverseConstants.LOCKMODE_NONE)
        val result: Double = longAsDouble(tranlocal.long_value) - rhs
        tranlocal.long_value = doubleAsLong(result)
    }

    def get(lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Double =
        ref.getAndLock(tx, lockMode)

    def set(newValue: Double, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Double =
        ref.setAndLock(tx, newValue, lockMode)

    def swap(newValue: Double, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Double =
        ref.getAndSetAndLock(tx, newValue, lockMode)

    def inc(amount: Double = 1)(implicit tx: Transaction = getThreadLocalTx): Unit =
        ref.incrementAndGet(tx, amount)

    def getAndInc(amount: Double = 1)(implicit tx: Transaction = getThreadLocalTx): Double =
        ref.getAndIncrement(tx, amount)

    def dec(amount: Double = 1)(implicit tx: Transaction = getThreadLocalTx): Unit =
        ref.incrementAndGet(tx, -amount)

    def alter(f: (Double) => Double, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Double = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], lockMode.asInt())
        val result = f(GammaStmUtils.longAsDouble(tranlocal.long_value))
        tranlocal.long_value = GammaStmUtils.doubleAsLong(result)
        result
    }

    def commute(f: (Double) => Double)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        ref.commute(tx, new org.multiverse.api.functions.DoubleFunction {
            override def call(v: Double) = f(v)
        })
    }

    //def await(value: Double)(implicit tx: Transaction = AkkaStm.getThreadLocalTx) =
    //    ref.await(tx, value)

    def ensure(implicit tx: Transaction = getThreadLocalTx): Unit = ref.ensure(tx)

    def toDebugString(): String = ref.toDebugString

    def toString(implicit tx: Transaction = getThreadLocalTx): String = ref.toString(tx)

    def await(f: (Double) => Boolean, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx) = {
        if (!f(ref.getAndLock(tx, lockMode))) {
            tx.retry()
        }
    }
}

final class BooleanRef(value: Boolean = false) {

    import AkkaStm._

    private val ref = new GammaBooleanRef(GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm], value);
    val lock: AkkaLock = new AkkaLockImpl(ref)

    val view = new View[Boolean] {

        override def get = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicGet() else ref.get(tx)
        }

        override def swap(newValue: Boolean) = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicGetAndSet(newValue) else ref.getAndSet(tx, newValue)
        }

        override def set(newValue: Boolean) = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicSet(newValue) else ref.set(tx, newValue)
        }

        override def alter(f: (Boolean) => Boolean) = {
            implicit val tx = getThreadLocalTx
            if (tx eq null) atom.alter(f) else BooleanRef.this.alter(f)
        }

        override def toString = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicToString else ref.toString(tx)
        }
    }

    val atom = new Atom[Boolean] {

        override def get = ref.atomicGet

        override def weakGet = ref.atomicWeakGet

        override def swap(newValue: Boolean) = ref.atomicGetAndSet(newValue)

        override def set(newValue: Boolean) = ref.atomicSet(newValue)

        override def alter(f: (Boolean) => Boolean) = ref.atomicAlterAndGet(new org.multiverse.api.functions.BooleanFunction {
            override def call(value: Boolean) = f(value)
        })

        override def compareAndSet(expectedValue: Boolean, newValue: Boolean) = ref.atomicCompareAndSet(expectedValue, newValue)

        override def toString = ref.atomicToString
    }

    def get(lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Boolean =
        ref.getAndLock(tx, lockMode)

    def set(newValue: Boolean, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Boolean =
        ref.setAndLock(tx, newValue, lockMode)

    def swap(newValue: Boolean, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Boolean =
        ref.getAndSetAndLock(tx, newValue, lockMode)

    def alter(f: (Boolean) => Boolean, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Boolean = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], lockMode.asInt())
        val result = f(GammaStmUtils.longAsBoolean(tranlocal.long_value))
        tranlocal.long_value = GammaStmUtils.booleanAsLong(result)
        result
    }

    def commute(f: (Boolean) => Boolean)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        ref.commute(tx, new org.multiverse.api.functions.BooleanFunction {
            override def call(v: Boolean) = f(v)
        })
    }

    def await(value: Boolean)(implicit tx: Transaction = getThreadLocalTx) =
        ref.await(tx, value)

    def ensure(implicit tx: Transaction = getThreadLocalTx): Unit = ref.ensure(tx)

    def toDebugString(): String = ref.toDebugString

    def toString(implicit tx: Transaction = getThreadLocalTx): String = ref.toString(tx)
}

final class LongRef(value: Long = 0) {

    import AkkaStm._

    private val ref = new GammaLongRef(GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm], value);
    val lock: AkkaLock = new AkkaLockImpl(ref)

    val view = new View[Long] {

        override def get = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicGet() else ref.get(tx)
        }

        override def swap(newValue: Long) = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicGetAndSet(newValue) else ref.getAndSet(tx, newValue)
        }

        override def set(newValue: Long) = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicSet(newValue) else ref.set(tx, newValue)
        }

        def alter(f: (Long) => Long) = {
            implicit val tx = getThreadLocalTx
            if (tx eq null) atom.alter(f) else LongRef.this.alter(f)
        }

        override def toString = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicToString else ref.toString(tx)
        }
    }

    val atom = new NumberAtom[Long] {

        override def get = ref.atomicGet

        override def weakGet = ref.atomicWeakGet

        override def swap(newValue: Long) = ref.atomicGetAndSet(newValue)

        override def set(newValue: Long) = ref.atomicSet(newValue)

        override def alter(f: (Long) => Long) = ref.atomicAlterAndGet(new LongFunction {
            def call(a: Long) = f(a)
        })

        override def compareAndSet(expectedValue: Long, newValue: Long) = ref.atomicCompareAndSet(expectedValue, newValue)

        override def atomicInc(amount: Long = 1) = ref.atomicIncrementAndGet(amount)

        override def atomicDec(amount: Long = 1): Long = ref.atomicIncrementAndGet(-1 * amount)

        override def toString = ref.atomicToString
    }

    def *=(rhs: Long)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], MultiverseConstants.LOCKMODE_NONE)
        tranlocal.long_value = tranlocal.long_value * rhs
    }


    def +=(rhs: Long)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], MultiverseConstants.LOCKMODE_NONE)
        tranlocal.long_value = tranlocal.long_value + rhs
    }

    def -=(rhs: Long)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], MultiverseConstants.LOCKMODE_NONE)
        tranlocal.long_value = tranlocal.long_value - rhs
    }

    def get(lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Long =
        ref.getAndLock(tx, lockMode)

    def set(newValue: Long, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Long =
        ref.setAndLock(tx, newValue, lockMode)

    def swap(newValue: Long, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Long =
        ref.getAndSetAndLock(tx, newValue, lockMode)

    def inc(amount: Long = 1)(implicit tx: Transaction = getThreadLocalTx): Unit =
        ref.increment(tx, amount)

    def dec(amount: Long = 1)(implicit tx: Transaction = getThreadLocalTx): Unit =
        ref.decrement(tx, amount)

    def incAndGet(amount: Long = 1)(implicit tx: Transaction = getThreadLocalTx): Long =
        ref.incrementAndGet(tx, amount)

    def getAndInc(amount: Long = 1)(implicit tx: Transaction = getThreadLocalTx): Long =
        ref.getAndIncrement(tx, amount)

    def alter(f: (Long) => Long, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx): Long = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], lockMode.asInt())
        val result = f(tranlocal.long_value)
        tranlocal.long_value = result
        result
    }

    def commute(f: (Long) => Long)(implicit tx: Transaction = getThreadLocalTx): Unit = {
        ref.commute(tx, new org.multiverse.api.functions.LongFunction {
            override def call(v: Long) = f(v)
        })
    }

    //def await(value: Long)(implicit tx: Transaction = AkkaStm.getThreadLocalTx()) = ref.await(tx, value)

    def await(f: (Long) => Boolean, lockMode: LockMode = LockMode.None)(implicit tx: Transaction = getThreadLocalTx) = {
        if (!f(ref.getAndLock(tx, lockMode))) {
            tx.retry()
        }
    }

    def ensure(implicit tx: Transaction = getThreadLocalTx): Unit = ref.ensure(tx)

    def toDebugString(): String = ref.toDebugString

    def toString(implicit tx: Transaction = getThreadLocalTx): String = ref.toString(tx)
}

class LeanTxExecutor(txFactory: GammaTransactionFactory) extends TxExecutor {
    val config = txFactory.getConfiguration
    val backoffPolicy = config.getBackoffPolicy

    def apply[@specialized E](block: (Transaction) => E): E = {
        val txContainer = ThreadLocalTransaction.getThreadLocalTransactionContainer();

        var tx = txContainer.tx.asInstanceOf[GammaTransaction]
        if (tx ne null) return block(tx)

        var pool = txContainer.txPool.asInstanceOf[GammaTransactionPool]
        if (pool eq null) {
            pool = new GammaTransactionPool
            txContainer.txPool = pool
        }

        tx = txFactory.newTransaction(pool)
        txContainer.tx = tx
        var cause: Throwable = null
        var abort = true
        try {
            do {
                try {
                    cause = null
                    val result = block(tx)
                    tx.commit
                    abort = false
                    return result
                } catch {
                    case e: RetryError => {
                        cause = e
                        tx.awaitUpdate()
                    }
                    case e: SpeculativeConfigurationError => {
                        cause = e
                        abort = false
                        val oldTx = tx
                        tx = txFactory.upgradeAfterSpeculativeFailure(tx, pool)
                        pool.put(oldTx)
                        txContainer.tx = tx
                    }
                    case e: ReadWriteConflict => {
                        cause = e
                        backoffPolicy.delayUninterruptible(tx.getAttempt)
                    }
                }
            } while (tx.softReset)
        } finally {
            if (abort) tx.abort()
            pool.put(tx)
            txContainer.tx = null
        }

        throw new TooManyRetriesException(
            format("[%s] TxExecutor has reached maximum number of %s retries", config.getFamilyName, config.getMaxRetries), cause)
    }
}

class FatTxExecutor(txConfigurer: TxExecutorConfigurer) extends TxExecutor {
    val txFactory = txConfigurer.builder.newTransactionFactory
    val config = txFactory.getConfiguration
    val backoffPolicy = config.getBackoffPolicy

    def apply[@specialized E](block: (Transaction) => E): E = {
        val txContainer = ThreadLocalTransaction.getThreadLocalTransactionContainer();
        var pool = txContainer.txPool.asInstanceOf[GammaTransactionPool]
        if (pool eq null) {
            pool = new GammaTransactionPool
            txContainer.txPool = pool
        }

        var tx = txContainer.tx.asInstanceOf[GammaTransaction]

        config.propagationLevel match {
            case PropagationLevel.Mandatory =>
                if (tx ne null) block(tx)
                else throw new TransactionMandatoryException(
                    format("[%s] No transaction is found for TxExecutor with 'Mandatory' propagation level",
                        config.familyName))
            case PropagationLevel.Never =>
                if (tx eq null) block(null)
                else throw new TransactionNotAllowedException(
                    format("[%s] No transaction is allowed for TxExecutor with propagation level 'Never'" +
                        ", but transaction '%s' was found", config.familyName, tx.getConfiguration.getFamilyName))
            case PropagationLevel.Requires =>
                if (tx ne null) block(tx)
                else {
                    tx = txFactory.newTransaction(pool)
                    txContainer.tx = tx
                    try {
                        doApply(tx, pool, block)
                    } finally {
                        txContainer.tx = null;
                        pool.put(tx)
                    }
                }
            case PropagationLevel.RequiresNew =>
                val suspendedTx = tx
                tx = txFactory.newTransaction(pool)
                txContainer.tx = tx
                try {
                    doApply(tx, pool, block)
                } finally {
                    txContainer.tx = suspendedTx
                    pool.put(tx)
                }
            case PropagationLevel.Supports =>
                block(tx)
            case _ =>
                throw new IllegalStateException
        }
    }

    private def doApply[@specialized E](initialTx: GammaTransaction, pool: GammaTransactionPool, block: (Transaction) => E): E = {
        var tx = initialTx
        var cause: Throwable = null
        var abort = true
        try {
            do {
                try {
                    cause = null
                    val result = block(tx)
                    tx.commit
                    abort = false
                    return result
                } catch {
                    case e: RetryError => {
                        cause = e
                        tx.awaitUpdate()
                    }
                    case e: SpeculativeConfigurationError => {
                        cause = e
                        abort = false
                        val oldTx = tx
                        tx = txFactory.upgradeAfterSpeculativeFailure(tx, pool)
                        pool.put(oldTx)
                    }
                    case e: ReadWriteConflict => {
                        cause = e
                        backoffPolicy.delayUninterruptible(tx.getAttempt)
                    }
                }
            } while (tx.softReset)
        } finally {
            if (abort) tx.abort()
        }

        throw new TooManyRetriesException(
            format("[%s] TxExecutor has reached maximum number of %s retries", config.getFamilyName, config.getMaxRetries), cause)
    }
}

/**
 * The TxExecutor is responsible for executing transactional blocks using some transaction. It is responsible
 * for the ACI behavior of the STM and will automatically retry a transaction when it runs into problems like
 * a read/write conflict. This will repeat until the operation completes successfully, throws a Throwable or
 * till the maximum number of retries has been reached. The underlying STM implementation makes use of
 * {@link ControlFlowError} to regulate control flow. It is very important that these errors are not caught
 * unless you really know what you are doing.
 *
 * TxExecutors are 'expensive' to create, so therefor should be reused. It is best to create the TxExecutors in
 * the beginning and store them in a static field (they are threadsafe) and reuse them for all transactions for
 * a specific operation.
 *
 * The underlying STM implementation: Multiverse, uses a speculative mechanism (if enabled) where transactions
 * learn based on previous executions so to select the best performing/scalable settings (not all settings can
 * be inferred, but quite a lot internal settings can). This can lead to some unexpected transaction failures
 * in the beginning because a {@link SpeculativeConfigurationError} is thrown. This error this is caught by the TxExecutor
 * and the transaction is retried. Once the TxExecutor had learned, it will not make the same mistake again.
 * This is another reason to reuse the TxExecutor, since else this knowledge would be lost every time a transaction
 * using a new TxExecutor is executed.
 *
 * Transactions are pooled to improve performance, so references to transactions should not be maintained. The same
 * transaction instance could be reused for completely unrelated transactional operations.
 *
 * In Multiverse the TxExecutor is called the AtomicBlock, so see that for more details.
 * todo: link to Multiverse documentation.
 */
trait TxExecutor {

    def apply[@specialized E](block: (Transaction) => E): E;
}

/**
 * A view on a Ref is the set of operations where each operation either executed on an existing transaction,
 * and if one is missing the operation is executed atomically. So operations are safe to call within a transaction,
 * but also if one is missing.
 *
 * The difference between a {@link View} and an {@link Atom} is that a view is able to lift on an existing transaction
 * if one is available, and an Atom will never lift on an existing transaction.
 */
trait View[E] {

    def get(): E

    def set(newValue: E): E

    def swap(newValue: E): E

    def alter(f: (E) => E): E
}

/**
 * A {@link View} tailored for the Ref that stores objects instead of primitives.
 */
trait RefView[E] extends View[E] {

    def getOrElse(defaultValue: E)

    def opt(): Option[E]

    def isNull(): Boolean
}

/**
 * An Atom of a Ref is the set of operations where each operation is executed atomically no matter if currently
 * a transaction is running.
 */
trait Atom[E] {

    def get(): E

    def set(newValue: E): E

    def swap(newValue: E): E

    def alter(f: (E) => E): E

    def weakGet(): E

    def compareAndSet(expectedValue: E, newValue: E): Boolean
}

/**
 * An {@link Atom} tailored for a {@link Ref} that is able to store object references.
 */
trait RefAtom[E] extends Atom[E] {

    def getOrElse(defaultValue: E)

    def opt(): Option[E]

    def isNull(): Boolean
}

/**
 * An {@link Atom} tailored for storing primitive numbers.
 */
trait NumberAtom[E] extends Atom[E] {

    def atomicInc(amount: E): E

    def atomicDec(amount: E): E
}

/**
 * The AkkaLock provides access to the pessimistic behavior of the Ref.
 */
trait AkkaLock {

    import AkkaStm._

    def atomicGetLockMode(): LockMode

    def getLockMode(implicit tx: Transaction = getThreadLocalTx): LockMode

    def acquire(desiredLockMode: LockMode)(implicit tx: Transaction = getThreadLocalTx): Unit
}

class AkkaLockImpl(protected val lock: Lock) extends AkkaLock {

    import AkkaStm._

    override def atomicGetLockMode() = lock.atomicGetLockMode

    override def getLockMode(implicit tx: Transaction = getThreadLocalTx) = lock.getLockMode(tx)

    override def acquire(desiredLockMode: LockMode)(implicit tx: Transaction = getThreadLocalTx) = lock.acquire(tx, desiredLockMode)
}

class TxExecutorConfigurer(val builder: GammaTransactionFactoryBuilder) {
    /**
     * Sets if the {@link org.multiverse.api.exceptions.ControlFlowError} is reused. Normally you don't want to reuse them
     * because they can be expensive to create (especially the stacktrace) and they could be created very often. But for
     * debugging purposes it can be quite annoying because you want to see the stacktrace.
     *
     * @param reused true if ControlFlowErrors should be reused.
     * @return the updated TransactionFactoryBuilder.
     * @see TxExecutorConfigurer#isControlFlowErrorsReused()
     */
    def withControlFlowErrorsReused(reused: Boolean) = {
        if (builder.getConfiguration.isControlFlowErrorsReused) this
        else new TxExecutorConfigurer(builder.setControlFlowErrorsReused(reused))
    }

    /**
     * Sets the {@link Transaction} familyname. If an {@link AtomicBlock} is used inside a method, a useful familyname could
     * be the full name of the class and the method.
     * <p/>
     * The transaction familyName is useful debugging purposes, but has not other meaning.
     *
     * @param familyName the familyName of the transaction.
     * @return the updated TransactionFactoryBuilder
     * @throws NullPointerException if familyName is null.
     * @see TransactionConfiguration#getFamilyName()
     */
    def withFamilyName(familyName: String) = {
        if (familyName eq builder.getConfiguration.getFamilyName) this
        else new TxExecutorConfigurer(builder.setFamilyName(familyName))
    }

    /**
     * Sets the {@link org.multiverse.api.PropagationLevel} used. With the PropagationLevel you have control
     * on how the transaction deals with transaction nesting. The default is {@link PropagationLevel#Requires}
     * which automatically starts a transaction is one is missing, or lifts on a transaction if available.
     *
     * @param propagationLevel the new PropagationLevel
     * @return the updated TransactionFactoryBuilder
     * @throws NullPointerException if propagationLevel is null.
     * @see TransactionConfiguration#getPropagationLevel()
     * @see PropagationLevel
     */
   def withPropagationLevel(propagationLevel: PropagationLevel) = {
        if (propagationLevel eq builder.getConfiguration.getPropagationLevel) this
        else new TxExecutorConfigurer(builder.setPropagationLevel(propagationLevel))
    }

    /**
     * Sets the {@link Transaction} {@link LockMode} for all reads. If a LockMode is set higher than {@link LockMode#None}, this transaction
     * will locks all reads (and writes since a read is needed for a write) and the transaction automatically becomes
     * serialized.
     *
     * @param lockMode the LockMode to set.
     * @return the updated TransactionFactoryBuilder.
     * @throws NullPointerException if lockMode is null.
     * @see TransactionConfiguration#getReadLockMode()
     * @see LockMode
     */
    def withReadLockMode(lockMode: LockMode) = {
        if (lockMode eq builder.getConfiguration.getReadLockMode) this
        else new TxExecutorConfigurer(builder.setReadLockMode(lockMode))
    }

    /**
     * Sets the {@link Transaction{} {@link LockMode} for all writes. For a write, always a read needs to be done, so if the read LockMode is
     *
     * <p>Freshly constructed objects that are not committed, automatically are locked with {@link LockMode#Exclusive}.
     *
     * <p>If the write LockMode is set after the read LockMode and the write LockMode is lower than the read LockMode,
     * an {@code IllegalTransactionFactoryException} will be thrown when a {@link TransactionFactory} is created.
     *
     * <p>If the write LockMode is set before the read LockMode and the write LockMode is lower than the read LockMode,
     * the write LockMode automatically is upgraded to that of the read LockMode. This makes setting the readLock
     * mode less of a nuisance.
     *
     * @param lockMode the LockMode to set.
     * @return the updated TransactionFactoryBuilder.
     * @throws NullPointerException if lockMode is null.
     * @see TransactionConfiguration#getWriteLockMode()
     * @see LockMode
     */
    def withWriteLockMode(lockMode: LockMode) = {
        if (lockMode eq builder.getConfiguration.getWriteLockMode) this
        else new TxExecutorConfigurer(builder.setWriteLockMode(lockMode))
    }

    def withPermanentListener(listener: TransactionListener) =
        new TxExecutorConfigurer(builder.addPermanentListener(listener))

    /**
     * Sets the {@link Transaction} {@link TraceLevel}. With tracing it is possible to see what is happening inside a transaction.
     *
     * @param traceLevel the new traceLevel.
     * @return the updated TransactionFactoryBuilder.
     * @throws NullPointerException if traceLevel is null.
     * @see TransactionConfiguration#getTraceLevel()
     * @see TraceLevel
     */
    def withTraceLevel(traceLevel: TraceLevel) = {
        if (traceLevel eq builder.getConfiguration.traceLevel) this
        else new TxExecutorConfigurer(builder.setTraceLevel(traceLevel))
    }

    /**
     * Sets the timeout (the maximum time a {@link Transaction} is allowed to block. Long.MAX_VALUE indicates that an
     * unbound timeout should be used (so no timeout).
     *
     * @param timeoutNs the timeout specified in nano seconds
     * @return the updated TransactionFactoryBuilder
     * @see TransactionConfiguration#getTimeoutNs()
     * @see Transaction#getRemainingTimeoutNs()
     */
    def withRetryTimeoutNs(timeout: Long, unit: TimeUnit = TimeUnit.SECONDS) = {
        val timeoutNs = unit.toNanos(timeout)
        if (timeoutNs == builder.getConfiguration.getTimeoutNs) this
        else new TxExecutorConfigurer(builder.setTimeoutNs(timeoutNs))
    }

    /**
     * Sets if the {@link Transaction} can be interrupted while doing blocking operations.
     *
     * @param interruptible if the transaction can be interrupted while doing blocking operations.
     * @return the updated TransactionFactoryBuilder
     * @see TransactionConfiguration#isInterruptible()
     */
   def withInterruptible(interruptible: Boolean) = {
        if (interruptible == builder.getConfiguration.isInterruptible) this
        else new TxExecutorConfigurer(builder.setInterruptible(interruptible))
    }

    /**
     * Sets the {@link Transaction} {@link BackoffPolicy}. Policy is used to backoff when a transaction conflicts with another
     * {@link Transaction}. See the {@link BackoffPolicy} for more information.
     *
     * @param backoffPolicy the backoff policy to use.
     * @return the updated TransactionFactoryBuilder
     * @throws NullPointerException if backoffPolicy is null.
     * @see TransactionConfiguration#getBackoffPolicy()
     */
   def withBackoffPolicy(backoffPolicy: BackoffPolicy) = {
        if (backoffPolicy eq builder.getConfiguration.getBackoffPolicy) this
        else new TxExecutorConfigurer(builder.setBackoffPolicy(backoffPolicy))
    }

    /**
     * Sets if the {@link Transaction} dirty check is enabled. Dirty check is that something only needs to be written,
     * if there really is a change (else it will be interpreted as a read). If it is disabled, it will always write, and
     * this could prevent the aba isolation anomaly, but causes more conflicts so more contention. In most cases enabling
     * it is the best option.
     *
     * @param dirtyCheckEnabled true if dirty check should be executed, false otherwise.
     * @return the updated TransactionFactoryBuilder.
     * @see TransactionConfiguration#isDirtyCheckEnabled()
     */
    def withDirtyCheck(dirtyCheckEnabled: Boolean) = {
        if (dirtyCheckEnabled == builder.getConfiguration.isDirtyCheckEnabled) this
        else new TxExecutorConfigurer(builder.setDirtyCheckEnabled(dirtyCheckEnabled))
    }

    /**
     * Sets the maximum number of spins that are allowed when a {@link Transaction} can't be read/written/locked
     * because it is locked by another transaction.
     *
     * <p>Setting the value to a very high value, could lead to more an increased chance of a live locking.
     *
     * @param spinCount the maximum number of spins
     * @return the updated TransactionFactoryBuilder.
     * @throws IllegalArgumentException if spinCount smaller than 0.
     * @see TransactionConfiguration#getSpinCount()
     */
    def withSpinCount(spinCount: Int) = {
        if (spinCount == builder.getConfiguration.getSpinCount) this
        else new TxExecutorConfigurer(builder.setSpinCount(spinCount))
    }

    /**
     * Sets the readonly property on a {@link Transaction}. If a transaction is configured as readonly, no write operations
     * (also no construction of new transactional objects making use of that transaction) is allowed
     *
     * @param readonly true if the transaction should be readonly, false otherwise.
     * @return the updated TransactionFactoryBuilder
     * @see TransactionConfiguration#isReadonly()
     */
     def withReadonly(readonly: Boolean) = {
        if (readonly == builder.getConfiguration.isReadonly) this
        else new TxExecutorConfigurer(builder.setReadonly(readonly))
    }

    /**
     * Sets if the {@link Transaction} should automatically track all reads that have been done. This is needed for blocking
     * operations, but also for other features like writeskew detection.
     *
     * <p>Tracking reads puts more pressure on the transaction since it needs to store all reads, but it reduces the chance
     * of read conflicts, since once read from main memory, it can be retrieved from the transaction.
     *
     * The transaction is free to track reads even though this property is disabled.
     *
     * @param enabled true if read tracking enabled, false otherwise.
     * @return the updated TransactionFactoryBuilder
     * @see TransactionConfiguration#isReadTrackingEnabled()
     */
    def withReadTrackingEnabled(enabled: Boolean) = {
        if (enabled == builder.getConfiguration.isReadTrackingEnabled) this
        else new TxExecutorConfigurer(builder.setReadTrackingEnabled(enabled))
    }

    /**
     * With the speculative configuration enabled, the {@link Stm} is allowed to determine optimal settings for
     * a {@link Transaction}.
     *
     * <p>Some behavior like readonly or the need for tracking reads can be determined runtime. The system can start with
     * a readonly non readtracking transaction and upgrade to an update or a read tracking once a write or retry
     * happens.
     *
     * <p>It depends on the {@link Stm} implementation on which properties it is going to speculate.
     *
     * <p>Enabling it can cause a few unexpected 'retries' of transactions, but it can seriously improve performance.
     *
     * @param speculative indicates if speculative configuration should be enabled.
     * @return the updated TransactionFactoryBuilder
     * @see TransactionConfiguration#isSpeculative()
     */
    def withSpeculation(speculative: Boolean) = {
        if (speculative == builder.getConfiguration.isSpeculative) this
        else new TxExecutorConfigurer(builder.setSpeculative(speculative))
    }

    /**
     * Sets the the maximum count a {@link Transaction} can be retried. The default is 1000. Setting it to a very low value
     * could mean that a transaction can't complete. Setting it to a very high value could lead to live-locking.
     *
     * <p>If the speculative configuration mechanism is enabled ({@link #setSpeculative(boolean)}), a few retries
     * are done in the beginning to figure out the best settings.
     *
     * @param maxRetries the maximum number of times a transaction can be tried.
     * @return the updated TransactionFactoryBuilder
     * @throws IllegalArgumentException if maxRetries smaller than 0.
     * @see TransactionConfiguration#getMaxRetries()
     */
    def withMaxRetries(maxRetries: Int) = {
        if (maxRetries == builder.getConfiguration.getMaxRetries) this
        else new TxExecutorConfigurer(builder.setMaxRetries(maxRetries))
    }

    /**
     * Sets the {@link IsolationLevel} on the {@link Transaction}.
     *
     * <p>The {@link Transaction} is free to upgraded to a higher {@link IsolationLevel}. This is essentially the same
     * behavior you get when Oracle is used, where a read uncommitted is upgraded to a read committed and a repeatable
     * read is upgraded to the Oracle version of serialized (so with the writeskew problem still there).
     *
     * @param isolationLevel the new IsolationLevel
     * @return the updated TransactionFactoryBuilder
     * @throws NullPointerException if isolationLevel is null.
     * @see TransactionConfiguration#getIsolationLevel()
     * @see IsolationLevel
     */
    def withIsolationLevel(isolationLevel: IsolationLevel) = {
        if (isolationLevel eq builder.getConfiguration.getIsolationLevel) this
        else new TxExecutorConfigurer(builder.setIsolationLevel(isolationLevel))
    }

    /**
     * Sets if the {@link Transaction} is allowed to do an explicit retry (needed for a blocking operation). One use case
     * for disallowing it, it when the transaction is used inside an actor, and you don't want that inside the logic
     * executed by the agent a blocking operations is done (e.g. taking an item of a blocking queue).
     *
     * @param blockingAllowed true if explicit retry is allowed, false otherwise.
     * @return the updated TransactionFactoryBuilder
     */
    def withBlockingAllowed(blockingAllowed: Boolean) = {
        if (blockingAllowed == builder.getConfiguration.isBlockingAllowed) this
        else new TxExecutorConfigurer(builder.setBlockingAllowed(blockingAllowed))
    }

    def newTxExecutor() = {
        if (isLean) new LeanTxExecutor(builder.newTransactionFactory)
        else new FatTxExecutor(this)
    }

    def isLean: Boolean = (builder.getConfiguration.propagationLevel eq PropagationLevel.Requires) || (builder.getConfiguration.traceLevel eq TraceLevel.None)
}

/**
 * A factory responsible for creating transactional references.
 */
trait RefFactory {

    /**
     * Creates a committed {@link IntRef}.
     */
    def newIntRef(value: Int = 0)

    /**
     * Creates a committed {@link LongRef}.
     */
    def newLongRef(value: Long = 0)

    /**
     * Creates a committed {@link BooleanRef}.
     */
    def newBooleanRef(value: Boolean = false)

    /**
     * Creates a committed {@link DoubleRef}.
     */
    def newDoubleRef(value: Double = 0)

    /**
     * Creates a committed {@link Ref}.
     */
    def newRef[E](): Ref[E]
}

object AkkaStm extends RefFactory {
    val gammaStm = GlobalStmInstance.getGlobalStmInstance.asInstanceOf[GammaStm]
    val defaultTxExecutor = new LeanTxExecutor(
        gammaStm.newTransactionFactoryBuilder().newTransactionFactory())

    def getThreadLocalTx(): Transaction = ThreadLocalTransaction.getThreadLocalTransaction()

    def apply[@specialized E](block: (Transaction) => E): E = defaultTxExecutor.apply(block)

    //compared to the original approach, this approach prevents wrapping the f in a runnable and that inside an transactionlistener.
    //now the f is wrapped
    def deferred(f: Unit => Unit): Unit = {
        getThreadLocalTx match {
            case null => throw new TransactionMandatoryException
            case tx: GammaTransaction => tx.register(new TransactionListener {
                def notify(tx: Transaction, e: TransactionEvent) = if (e == TransactionEvent.PostCommit) f
            })
        }
    }

    def compensating(f: Unit => Unit): Unit = {
        getThreadLocalTx match {
            case null => throw new TransactionMandatoryException
            case tx: GammaTransaction => tx.register(new TransactionListener {
                def notify(tx: Transaction, e: TransactionEvent) = if (e == TransactionEvent.PostAbort) f
            })
        }
    }

    def retry() = StmUtils.retry

    def newTxExecutorConfigurer() = new TxExecutorConfigurer(gammaStm.newTransactionFactoryBuilder)

    def newIntRef(value: Int = 0) = new IntRef(value)

    def newLongRef(value: Long = 0) = new LongRef(value)

    def newBooleanRef(value: Boolean = false) = new BooleanRef(value)

    def newDoubleRef(value: Double = 0) = new DoubleRef(value)

    def newRef[E]() = new Ref[E]()
}