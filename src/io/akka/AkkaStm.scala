package io.akka

import org.multiverse.api._
import exceptions._
import functions.LongFunction
import lifecycle.{TransactionEvent, TransactionListener}
import org.multiverse.stms.gamma.transactions.{GammaTransactionFactoryBuilder, GammaTransactionFactory, GammaTransaction, GammaTransactionPool}
import org.multiverse.stms.gamma.transactionalobjects._
import javax.transaction.TransactionRequiredException
import java.util.concurrent.TimeUnit
import org.multiverse.stms.gamma.{GammaStmUtils, GammaStm}
import org.multiverse.MultiverseConstants

final class Ref[E] {

    import AkkaStm._

    private val ref = new GammaRef[E](GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm]);
    val lock: AkkaLock = new AkkaLockImpl(ref)
    val view = new RefView[E] {

        override def get = {
            val tx = getThreadLocalTx
            if (tx eq null) ref.atomicGet() else ref.get(tx)
        }

        override def getOrElse(defaultValue: E) = {
            val result = get()
            if (result == null) defaultValue else result
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

        override def getOrElse(defaultValue: E) = {
            val result = ref.atomicGet
            if (result == null) defaultValue else result
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

trait TxExecutor {

    def apply[@specialized E](block: (Transaction) => E): E;
}

trait View[E] {

    def get(): E

    def set(newValue: E): E

    def swap(newValue: E): E

    def alter(f: (E) => E): E
}

trait RefView[E] extends View[E] {

    def getOrElse(defaultValue: E)

    def opt(): Option[E]

    def isNull(): Boolean
}

trait Atom[E] {

    def get(): E

    def set(newValue: E): E

    def swap(newValue: E): E

    def alter(f: (E) => E): E

    def weakGet(): E

    def compareAndSet(expectedValue: E, newValue: E): Boolean
}

trait RefAtom[E] extends Atom[E] {

    def getOrElse(defaultValue: E)

    def opt(): Option[E]

    def isNull(): Boolean
}

trait NumberAtom[E] extends Atom[E] {

    def atomicInc(amount: E): E

    def atomicDec(amount: E): E
}

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

    def withControlFlowErrorsReused(reused: Boolean) = {
        if (builder.getConfiguration.isControlFlowErrorsReused) this
        else new TxExecutorConfigurer(builder.setControlFlowErrorsReused(reused))
    }

    def withFamilyName(familyName: String) = {
        if (familyName eq builder.getConfiguration.getFamilyName) this
        else new TxExecutorConfigurer(builder.setFamilyName(familyName))
    }

    def withPropagationLevel(propagationLevel: PropagationLevel) = {
        if (propagationLevel eq builder.getConfiguration.getPropagationLevel) this
        else new TxExecutorConfigurer(builder.setPropagationLevel(propagationLevel))
    }

    def withReadLockMode(lockMode: LockMode) = {
        if (lockMode eq builder.getConfiguration.getReadLockMode) this
        else new TxExecutorConfigurer(builder.setReadLockMode(lockMode))
    }

    def withWriteLockMode(lockMode: LockMode) = {
        if (lockMode eq builder.getConfiguration.getWriteLockMode) this
        else new TxExecutorConfigurer(builder.setWriteLockMode(lockMode))
    }

    def withPermanentListener(listener: TransactionListener) =
        new TxExecutorConfigurer(builder.addPermanentListener(listener))

    def withTraceLevel(traceLevel: TraceLevel) = {
        if (traceLevel eq builder.getConfiguration.traceLevel) this
        else new TxExecutorConfigurer(builder.setTraceLevel(traceLevel))
    }

    def withRetryTimeoutNs(timeout: Long, unit: TimeUnit = TimeUnit.SECONDS) = {
        val timeoutNs = unit.toNanos(timeout)
        if (timeoutNs == builder.getConfiguration.getTimeoutNs) this
        else new TxExecutorConfigurer(builder.setTimeoutNs(timeoutNs))
    }

    def withInterruptible(interruptible: Boolean) = {
        if (interruptible == builder.getConfiguration.isInterruptible) this
        else new TxExecutorConfigurer(builder.setInterruptible(interruptible))
    }

    def withBackoffPolicy(backoffPolicy: BackoffPolicy) = {
        if (backoffPolicy eq builder.getConfiguration.getBackoffPolicy) this
        else new TxExecutorConfigurer(builder.setBackoffPolicy(backoffPolicy))
    }

    def withDirtyCheck(dirtyCheckEnabled: Boolean) = {
        if (dirtyCheckEnabled == builder.getConfiguration.isDirtyCheckEnabled) this
        else new TxExecutorConfigurer(builder.setDirtyCheckEnabled(dirtyCheckEnabled))
    }

    def withSpinCount(spinCount: Int) = {
        if (spinCount == builder.getConfiguration.getSpinCount) this
        else new TxExecutorConfigurer(builder.setSpinCount(spinCount))
    }

    def withReadonly(readonly: Boolean) = {
        if (readonly == builder.getConfiguration.isReadonly) this
        else new TxExecutorConfigurer(builder.setReadonly(readonly))
    }

    def withReadTrackingEnabled(enabled: Boolean) = {
        if (enabled == builder.getConfiguration.isReadTrackingEnabled) this
        else new TxExecutorConfigurer(builder.setReadTrackingEnabled(enabled))
    }

    def withSpeculation(speculative: Boolean) = {
        if (speculative == builder.getConfiguration.isSpeculative) this
        else new TxExecutorConfigurer(builder.setSpeculative(speculative))
    }

    def withMaxRetries(maxRetries: Int) = {
        if (maxRetries == builder.getConfiguration.getMaxRetries) this
        else new TxExecutorConfigurer(builder.setMaxRetries(maxRetries))
    }

    def withIsolationLevel(isolationLevel: IsolationLevel) = {
        if (isolationLevel eq builder.getConfiguration.getIsolationLevel) this
        else new TxExecutorConfigurer(builder.setIsolationLevel(isolationLevel))
    }

    def withBlockingAllowed(blockingAllowed: Boolean) = {
        if (blockingAllowed == builder.getConfiguration.isBlockingAllowed) this
        else new TxExecutorConfigurer(builder.setBlockingAllowed(blockingAllowed))
    }

    def withJtaEnabled() = {
        //just an example how the permanent listeners can be added to add your own logic that will always be executed.
        //no need to register the individual listeners anymore (also makes it faster).
        val listener = new TransactionListener() {
            def notify(tx: Transaction, e: TransactionEvent) = {
                e match {
                    case TransactionEvent.PreStart => println("prestart")
                    case TransactionEvent.PostStart => println("poststart")
                    case TransactionEvent.PrePrepare => println("preprepare")
                    case TransactionEvent.PostAbort => println("postabort")
                    case TransactionEvent.PostCommit => println("postcommit")
                }
            }
        }
        new TxExecutorConfigurer(builder.addPermanentListener(listener).setSpeculative(false))
    }

    def newTxExecutor() = {
        if (isLean) new LeanTxExecutor(builder.newTransactionFactory)
        else new FatTxExecutor(this)
    }

    def isLean: Boolean = (builder.getConfiguration.propagationLevel eq PropagationLevel.Requires) || (builder.getConfiguration.traceLevel == TraceLevel.None)
}

trait RefFactory {

    def newIntRef(value: Int = 0)

    def newLongRef(value: Long = 0)

    def newBooleanRef(value: Boolean = false)

    def newDoubleRef(value: Double = 0)

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
            case null => throw new TransactionRequiredException
            case tx: GammaTransaction => tx.register(new TransactionListener {
                def notify(tx: Transaction, e: TransactionEvent) = if (e == TransactionEvent.PostCommit) f()
            })
        }
    }

    def compensating(f: Unit => Unit): Unit = {
        getThreadLocalTx match {
            case null => throw new TransactionRequiredException
            case tx: GammaTransaction => tx.register(new TransactionListener {
                def notify(tx: Transaction, e: TransactionEvent) = if (e == TransactionEvent.PostAbort) f()
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