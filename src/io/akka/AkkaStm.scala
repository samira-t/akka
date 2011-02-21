package io.akka

import org.multiverse.api._
import exceptions._
import lifecycle.TransactionListener
import org.multiverse.stms.gamma.transactions.{GammaTransactionFactoryBuilder, GammaTransactionFactory, GammaTransaction, GammaTransactionPool}
import org.multiverse.stms.gamma.{GammaStmUtils, GammaStm}
import org.multiverse.stms.gamma.transactionalobjects._

final class Ref[E] {

    private val ref = new GammaRef[E](GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm]);
    val lock: AkkaLock = new AkkaLockImpl(ref)
    val view = new RefView[E] {
        def swap(newValue: E) = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicGetAndSet(newValue) else Ref.this.swap(newValue, tx)
        }

        def set(newValue: E) = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicSet(newValue) else Ref.this.set(newValue, tx)
        }

        def get(): E = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicGet() else Ref.this.get(tx)
        }

        def alter(f: (E) => E): E = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) throw new TodoException() else Ref.this.alter(f, tx)
        }

        def getOrElse(defaultValue: E) = {
            val result = get()
            if (result == null) defaultValue else result
        }

        def opt(): Option[E] = {
            val result = get()
            if (result == null) None else new Some[E](result)
        }

        def isNull(): Boolean = get() ==null
    }
    val atom = new RefAtom[E] {
        def swap(newValue: E) = ref.atomicGetAndSet(newValue)

        def set(newValue: E) = ref.atomicSet(newValue)

        def get(): E = ref.atomicGet

        def alter(f: (E) => E): E = throw new TodoException()

        def weakGet(): E = ref.atomicWeakGet

        def compareAndSet(expectedValue: E, newValue: E): Boolean = ref.atomicCompareAndSet(expectedValue, newValue)

        def getOrElse(defaultValue: E) = {
            val result = ref.atomicGet
            if (result == null) defaultValue else result
        }

        def opt(): Option[E] = {
            val result = ref.atomicGet
            if (result == null) None else new Some[E](result)
        }

        def isNull(): Boolean = ref.atomicIsNull
    }

    def value: E = ref.get()

    def value_=(newValue: E): Unit = ref.set(newValue)

    def getOrElse(defaultValue: E, lockMode: LockMode = LockMode.None): E = {
        val result = ref.getAndLock(lockMode)
        if (result == null) defaultValue else result
    }

    def getOrElse(defaultValue: E, tx: Transaction, lockMode: LockMode = LockMode.None): E = {
        val result = ref.getAndLock(tx, lockMode)
        if (result == null) defaultValue else result
    }

    def opt(lockMode: LockMode = LockMode.None): Option[E] = {
        val result = ref.getAndLock(lockMode)
        if (result == null) None else new Some[E](result)
    }

    def opt(tx: Transaction, lockMode: LockMode = LockMode.None): Option[E] = {
        val result = ref.getAndLock(tx, lockMode)
        if (result == null) None else new Some[E](result)
    }

    def get(lockMode: LockMode = LockMode.None): E = ref.getAndLock(lockMode)

    def get(tx: Transaction, lockMode: LockMode = LockMode.None): E = ref.getAndLock(tx, lockMode)

    def set(newValue: E, lockMode: LockMode = LockMode.None): E = ref.setAndLock(newValue, lockMode)

    def set(newValue: E, tx: Transaction, lockMode: LockMode = LockMode.None): E = ref.setAndLock(tx, newValue, lockMode)

    def swap(newValue: E, tx: Transaction, lockMode: LockMode = LockMode.None): E = ref.getAndSetAndLock(tx, newValue, lockMode)

    def swap(newValue: E, lockMode: LockMode = LockMode.None): E = ref.getAndSetAndLock(newValue, lockMode)

    def alter(f: (E) => E, lockMode: LockMode = LockMode.None): E = alter(f, AkkaStm.getThreadLocalTransaction, lockMode)

    def alter(f: (E) => E, tx: Transaction, lockMode: LockMode = LockMode.None): E = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], lockMode.asInt())
        val result = f(tranlocal.ref_value.asInstanceOf[E])
        tranlocal.ref_value = result
        result
    }

    def isNull(): Boolean = ref.isNull()

    def isNull(tx: Transaction): Boolean = ref.isNull(tx)

    def commute(f: (E) => E): Unit = throw new TodoException()

    def commute(f: (E) => E, tx: Transaction): Unit = throw new TodoException()

    def awaitNotNullAndGet(): E = ref.awaitNotNullAndGet

    def awaitNotNullAndGet(tx: Transaction): E = ref.awaitNotNullAndGet(tx)

    def await(f: (E) => Boolean, lockMode: LockMode = LockMode.None) = {
        if (!f(ref.getAndLock(lockMode))) {
            AkkaStm.retry()
        }
    }

    def await(f: (E) => Boolean, tx: Transaction, lockMode: LockMode = LockMode.None) = {
        if (!f(ref.getAndLock(tx, lockMode))) {
            tx.retry()
        }
    }
}

final class IntRef(value: Int = 0) {

    private val ref = new GammaIntRef(GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm], value);
    val lock: AkkaLock = new AkkaLockImpl(ref)

    val view = new View[Int] {
        def swap(newValue: Int) = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicGetAndSet(newValue) else IntRef.this.swap(newValue, tx)
        }

        def set(newValue: Int) = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicSet(newValue) else IntRef.this.set(newValue, tx)
        }

        def get(): Int = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicGet() else IntRef.this.get(tx)
        }

        def alter(f: (Int) => Int): Int = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) throw new TodoException() else IntRef.this.alter(f, tx)
        }
    }

    val atom = new NumberAtom[Int] {
        def swap(newValue: Int) = ref.atomicGetAndSet(newValue)

        def set(newValue: Int) = ref.atomicSet(newValue)

        def get(): Int = ref.atomicGet

        def alter(f: (Int) => Int): Int = throw new TodoException()

        def weakGet(): Int = ref.atomicWeakGet

        def compareAndSet(expectedValue: Int, newValue: Int): Boolean = ref.atomicCompareAndSet(expectedValue, newValue)

        def atomicInc(amount: Int = 1): Int = ref.atomicIncrementAndGet(amount)

        def atomicDec(amount: Int = 1): Int = ref.atomicIncrementAndGet(-1 * amount)
    }

    def get(lockMode: LockMode = LockMode.None): Int = ref.getAndLock(lockMode)

    def get(tx: Transaction, lockMode: LockMode = LockMode.None): Int = ref.getAndLock(tx, lockMode)

    def set(newValue: Int, lockMode: LockMode = LockMode.None): Int = ref.setAndLock(newValue, lockMode)

    def set(newValue: Int, tx: Transaction, lockMode: LockMode = LockMode.None): Int = ref.setAndLock(tx, newValue, lockMode)

    def swap(newValue: Int, tx: Transaction, lockMode: LockMode = LockMode.None): Int = ref.getAndSetAndLock(tx, newValue, lockMode)

    def swap(newValue: Int, lockMode: LockMode = LockMode.None): Int = ref.getAndSetAndLock(newValue, lockMode)

    def incAndGet(amount: Int = 1): Int = ref.incrementAndGet(amount)

    def incAndGet(amount: Int = 1, tx: Transaction): Int = ref.incrementAndGet(tx, amount)

    def getAndInc(amount: Int = 1): Int = ref.getAndIncrement(amount)

    def getAndInc(amount: Int = 1, tx: Transaction): Int = ref.getAndIncrement(tx, amount)

    def inc(amount: Int = 1): Unit = ref.increment(amount)

    def inc(amount: Int = 1, tx: Transaction): Unit = ref.increment(tx, amount)

    def dec(amount: Int = 1): Unit = ref.decrement(amount)

    def dec(amount: Int = 1, tx: Transaction): Unit = ref.decrement(tx, amount)

    def alter(f: (Int) => Int, lockMode: LockMode = LockMode.None): Int =
        alter(f, AkkaStm.getThreadLocalTransaction(), lockMode)

    def alter(f: (Int) => Int, tx: Transaction, lockMode: LockMode = LockMode.None): Int = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], lockMode.asInt())
        val result = f(tranlocal.long_value.asInstanceOf[Int])
        tranlocal.long_value = result.asInstanceOf[Long]
        result
    }

    def commute(f: (Int) => Int): Unit = throw new TodoException()

    def commute(f: (Int) => Int, tx: Transaction): Unit = throw new TodoException()

    def await(value: Int) = ref.await(value)

    def await(value: Int, tx: Transaction) = ref.await(tx, value)

    def await(f: (Int) => Boolean, lockMode: LockMode = LockMode.None) = {
        if (!f(ref.getAndLock(lockMode))) {
            AkkaStm.retry()
        }
    }

    def await(f: (Int) => Boolean, tx: Transaction, lockMode: LockMode = LockMode.None) = {
        if (!f(ref.getAndLock(tx, lockMode))) {
            tx.retry()
        }
    }
}

final class DoubleRef(value: Double = 0) {

    private val ref = new GammaDoubleRef(GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm], value);
    val lock: AkkaLock = new AkkaLockImpl(ref)

    val view = new View[Double] {
        def swap(newValue: Double) = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicGetAndSet(newValue) else DoubleRef.this.swap(newValue, tx)
        }

        def set(newValue: Double) = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicSet(newValue) else DoubleRef.this.set(newValue, tx)
        }

        def get() = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicGet() else DoubleRef.this.get(tx)
        }

        def alter(f: (Double) => Double): Double = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) throw new TodoException() else DoubleRef.this.alter(f, tx)
        }
    }

    val atom = new NumberAtom[Double] {
        def swap(newValue: Double) = ref.atomicGetAndSet(newValue)

        def set(newValue: Double) = ref.atomicSet(newValue)

        def get(): Double = ref.atomicGet

        def alter(f: (Double) => Double): Double = throw new TodoException()

        def weakGet(): Double = ref.atomicWeakGet

        def compareAndSet(expectedValue: Double, newValue: Double): Boolean = ref.atomicCompareAndSet(expectedValue, newValue)

        def atomicInc(amount: Double = 1): Double = ref.atomicIncrementAndGet(amount)

        def atomicDec(amount: Double = 1): Double = ref.atomicIncrementAndGet(-1 * amount)
    }

    def get(lockMode: LockMode = LockMode.None): Double = ref.getAndLock(lockMode)

    def get(tx: Transaction, lockMode: LockMode = LockMode.None): Double = ref.getAndLock(tx, lockMode)

    def set(newValue: Double, lockMode: LockMode = LockMode.None): Double = ref.setAndLock(newValue, lockMode)

    def set(newValue: Double, tx: Transaction, lockMode: LockMode = LockMode.None): Double = ref.setAndLock(tx, newValue, lockMode)

    def swap(newValue: Double, tx: Transaction, lockMode: LockMode = LockMode.None): Double = ref.getAndSetAndLock(tx, newValue, lockMode)

    def swap(newValue: Double, lockMode: LockMode = LockMode.None): Double = ref.getAndSetAndLock(newValue, lockMode)

    def inc(amount: Double = 1): Unit = ref.incrementAndGet(amount)

    def inc(amount: Double = 1, tx: Transaction): Unit = ref.incrementAndGet(tx, amount)

    def incAndGet(amount: Double = 1): Double = ref.incrementAndGet(amount)

    def incAndGet(amount: Double = 1, tx: Transaction): Double = ref.incrementAndGet(tx, amount)

    def getAndInc(amount: Double = 1): Double = ref.getAndIncrement(amount)

    def getAndInc(amount: Double = 1, tx: Transaction): Double = ref.getAndIncrement(tx, amount)

    def dec(amount: Double = 1): Unit = ref.incrementAndGet(-amount)

    def dec(amount: Double = 1, tx: Transaction): Unit = ref.incrementAndGet(tx, -amount)

    def alter(f: (Double) => Double, lockMode: LockMode = LockMode.None): Double =
        alter(f, AkkaStm.getThreadLocalTransaction(), lockMode)

    def alter(f: (Double) => Double, tx: Transaction, lockMode: LockMode = LockMode.None): Double = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], lockMode.asInt())
        val result = f(GammaStmUtils.longAsDouble(tranlocal.long_value))
        tranlocal.long_value = GammaStmUtils.doubleAsLong(result)
        result
    }

    def commute(f: (Double) => Double): Unit = throw new TodoException()

    def commute(f: (Double) => Double, tx: Transaction): Unit = throw new TodoException()

    def await(value: Double) = ref.await(value)

    def await(value: Double, tx: Transaction) = ref.await(tx, value)

    def await(f: (Double) => Boolean, lockMode: LockMode = LockMode.None) = {
        if (!f(ref.getAndLock(lockMode))) {
            AkkaStm.retry()
        }
    }

    def await(f: (Double) => Boolean, tx: Transaction, lockMode: LockMode = LockMode.None) = {
        if (!f(ref.getAndLock(tx, lockMode))) {
            tx.retry()
        }
    }
}

final class BooleanRef(value: Boolean = false) {

    private val ref = new GammaBooleanRef(GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm], value);
    val lock: AkkaLock = new AkkaLockImpl(ref)

    val view = new View[Boolean] {
        def swap(newValue: Boolean) = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicGetAndSet(newValue) else BooleanRef.this.swap(newValue, tx)
        }

        def set(newValue: Boolean) = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicSet(newValue) else BooleanRef.this.set(newValue, tx)
        }

        def get() = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicGet() else BooleanRef.this.get(tx)
        }

        def alter(f: (Boolean) => Boolean): Boolean = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) throw new TodoException() else BooleanRef.this.alter(f, tx)
        }
    }

    val atom = new Atom[Boolean] {
        def swap(newValue: Boolean) = ref.atomicGetAndSet(newValue)

        def set(newValue: Boolean) = ref.atomicSet(newValue)

        def get(): Boolean = ref.atomicGet

        def alter(f: (Boolean) => Boolean): Boolean = throw new TodoException()

        def weakGet(): Boolean = ref.atomicWeakGet

        def compareAndSet(expectedValue: Boolean, newValue: Boolean): Boolean = ref.atomicCompareAndSet(expectedValue, newValue)
    }

    def get(lockMode: LockMode = LockMode.None): Boolean = ref.getAndLock(lockMode)

    def get(tx: Transaction, lockMode: LockMode = LockMode.None): Boolean = ref.getAndLock(tx, lockMode)

    def set(newValue: Boolean, lockMode: LockMode = LockMode.None): Boolean = ref.setAndLock(newValue, lockMode)

    def set(newValue: Boolean, tx: Transaction, lockMode: LockMode = LockMode.None): Boolean = ref.setAndLock(tx, newValue, lockMode)

    def swap(newValue: Boolean, tx: Transaction, lockMode: LockMode = LockMode.None): Boolean = ref.getAndSetAndLock(tx, newValue, lockMode)

    def swap(newValue: Boolean, lockMode: LockMode = LockMode.None): Boolean = ref.getAndSetAndLock(newValue, lockMode)

    def alter(f: (Boolean) => Boolean, lockMode: LockMode = LockMode.None): Boolean =
        alter(f, AkkaStm.getThreadLocalTransaction(), lockMode)

    def alter(f: (Boolean) => Boolean, tx: Transaction, lockMode: LockMode = LockMode.None): Boolean = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], lockMode.asInt())
        val result = f(GammaStmUtils.longAsBoolean(tranlocal.long_value))
        tranlocal.long_value = GammaStmUtils.booleanAsLong(result)
        result
    }

    def commute(f: (Boolean) => Boolean): Unit = throw new TodoException()

    def commute(f: (Boolean) => Boolean, tx: Transaction): Unit = throw new TodoException()

    def await(value: Boolean) = ref.await(value)

    def await(value: Boolean, tx: Transaction) = ref.await(tx, value)
}


final class LongRef(value: Long = 0) {

    private val ref = new GammaLongRef(GlobalStmInstance.getGlobalStmInstance().asInstanceOf[GammaStm], value);
    val lock: AkkaLock = new AkkaLockImpl(ref)

    val view = new View[Long] {
        def swap(newValue: Long) = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicGetAndSet(newValue) else LongRef.this.swap(newValue, tx)
        }

        def set(newValue: Long) = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicSet(newValue) else LongRef.this.set(newValue, tx)
        }

        def get() = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) ref.atomicGet() else LongRef.this.get(tx)
        }

        def alter(f: (Long) => Long): Long = {
            val tx = AkkaStm.getThreadLocalTransaction
            if (tx == null) throw new TodoException() else LongRef.this.alter(f, tx)
        }
    }

    val atom = new NumberAtom[Long] {
        def swap(newValue: Long) = ref.atomicGetAndSet(newValue)

        def set(newValue: Long) = ref.atomicSet(newValue)

        def get(): Long = ref.atomicGet

        def alter(f: (Long) => Long): Long = throw new TodoException()

        def weakGet(): Long = ref.atomicWeakGet

        def compareAndSet(expectedValue: Long, newValue: Long): Boolean = ref.atomicCompareAndSet(expectedValue, newValue)

        def atomicInc(amount: Long = 1): Long = ref.atomicIncrementAndGet(amount)

        def atomicDec(amount: Long = 1): Long = ref.atomicIncrementAndGet(-1 * amount)
    }

    def get(tx: Transaction, lockMode: LockMode = LockMode.None): Long = ref.getAndLock(tx, lockMode)

    def get(lockMode: LockMode = LockMode.None): Long = ref.getAndLock(lockMode)

    def set(newValue: Long, lockMode: LockMode = LockMode.None): Long = ref.setAndLock(newValue, lockMode)

    def set(newValue: Long, tx: Transaction, lockMode: LockMode = LockMode.None): Long = ref.setAndLock(tx, newValue, lockMode)

    def swap(newValue: Long, lockMode: LockMode = LockMode.None): Long = ref.getAndSetAndLock(newValue, lockMode)

    def swap(newValue: Long, tx: Transaction, lockMode: LockMode = LockMode.None): Long = ref.getAndSetAndLock(tx, newValue, lockMode)

    def inc(amount: Long = 1): Unit = ref.increment(amount)

    def inc(amount: Long = 1, tx: Transaction): Unit = ref.increment(tx, amount)

    def dec(amount: Long = 1): Unit = ref.decrement(amount)

    def dec(amount: Long = 1, tx: Transaction): Unit = ref.decrement(tx, amount)

    def incAndGet(amount: Long = 1): Long = ref.incrementAndGet(amount)

    def incAndGet(amount: Long = 1, tx: Transaction): Long = ref.incrementAndGet(tx, amount)

    def getAndInc(amount: Long = 1): Long = ref.getAndIncrement(amount)

    def getAndInc(amount: Long = 1, tx: Transaction): Long = ref.getAndIncrement(tx, amount)

    def alter(f: (Long) => Long, lockMode: LockMode = LockMode.None): Long = alter(f, AkkaStm.getThreadLocalTransaction(), lockMode)

    def alter(f: (Long) => Long, tx: Transaction, lockMode: LockMode = LockMode.None): Long = {
        val tranlocal = ref.openForWrite(tx.asInstanceOf[GammaTransaction], lockMode.asInt())
        val result = f(tranlocal.long_value)
        tranlocal.long_value = result
        result
    }

    def commute(f: (Long) => Long): Unit = throw new TodoException()

    def commute(f: (Long) => Long, tx: Transaction): Unit = throw new TodoException()

    def await(value: Long) = ref.await(value)

    def await(value: Long, tx: Transaction) = ref.await(tx, value)

    def await(f: (Long) => Boolean, lockMode: LockMode = LockMode.None) = {
        if (!f(ref.getAndLock(lockMode))) {
            AkkaStm.retry()
        }
    }

    def await(f: (Long) => Boolean, tx: Transaction, lockMode: LockMode = LockMode.None) = {
        if (!f(ref.getAndLock(tx, lockMode))) {
            tx.retry()
        }
    }
}

class LeanTxExecutor(transactionFactory: GammaTransactionFactory) extends TxExecutor {
    val config = transactionFactory.getConfiguration
    val backoffPolicy = config.getBackoffPolicy

    def apply[@specialized E](block: (Transaction) => E): E = {
        val transactionContainer = ThreadLocalTransaction.getThreadLocalTransactionContainer();
        var pool: GammaTransactionPool = transactionContainer.txPool.asInstanceOf[GammaTransactionPool]
        if (pool == null) {
            pool = new GammaTransactionPool
            transactionContainer.txPool = pool
        }

        var tx = transactionContainer.tx.asInstanceOf[GammaTransaction]
        if (tx != null) {
            return block(tx)
        }

        tx = transactionFactory.newTransaction(pool)
        transactionContainer.tx = tx
        var cause: Throwable = null
        var abort: Boolean = true
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
                        tx.awaitUpdate
                    }
                    case e: SpeculativeConfigurationError => {
                        cause = e
                        abort = false
                        val old: GammaTransaction = tx
                        tx = transactionFactory.upgradeAfterSpeculativeFailure(tx, pool)
                        pool.put(old)
                        transactionContainer.tx = tx
                    }
                    case e: ReadWriteConflict => {
                        cause = e
                        backoffPolicy.delayUninterruptible(tx.getAttempt)
                    }
                }
            } while (tx.softReset)
        } finally {
            if (abort) {
                tx.abort
            }
            pool.put(tx)
            transactionContainer.tx = null
        }

        throw new TooManyRetriesException(
            format("[%s] Maximum number of %s retries has been reached",
                config.getFamilyName, config.getMaxRetries), cause)
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

    def atomicInc(amount: E ): E

    def atomicDec(amount: E ): E
}

trait AkkaLock {

    def atomicGetLockMode(): LockMode

    def getLockMode(tx: Transaction): LockMode

    def acquire(tx: Transaction, desiredLockMode: LockMode): Unit
}

class AkkaLockImpl(val lock: Lock) extends AkkaLock {

    def atomicGetLockMode(): LockMode = lock.atomicGetLockMode

    def getLockMode(tx: Transaction): LockMode = lock.getLockMode(tx)

    def acquire(tx: Transaction, desiredLockMode: LockMode): Unit = lock.acquire(tx, desiredLockMode)
}

class TxExecutorConfigurer(val builder: GammaTransactionFactoryBuilder) {

    def setControlFlowErrorsReused(reused: Boolean) = new TxExecutorConfigurer(builder.setControlFlowErrorsReused(reused))

    def setFamilyName(familyName: String) = new TxExecutorConfigurer(builder.setFamilyName(familyName))

    def setPropagationLevel(propagationLevel: PropagationLevel) = new TxExecutorConfigurer(builder.setPropagationLevel(propagationLevel))

    def setReadLockMode(lockMode: LockMode) = new TxExecutorConfigurer(builder.setReadLockMode(lockMode))

    def setWriteLockMode(lockMode: LockMode) = new TxExecutorConfigurer(builder.setWriteLockMode(lockMode))

    def addPermanentListener(listener: TransactionListener) = new TxExecutorConfigurer(builder.addPermanentListener(listener))

    def setTraceLevel(traceLevel: TraceLevel) = new TxExecutorConfigurer(builder.setTraceLevel(traceLevel))

    def setTimeoutNs(timeoutNs: Long) = new TxExecutorConfigurer(builder.setTimeoutNs(timeoutNs))

    def setInterruptible(interruptible: Boolean) = new TxExecutorConfigurer(builder.setInterruptible(interruptible))

    def setBackoffPolicy(backoffPolicy: BackoffPolicy) = new TxExecutorConfigurer(builder.setBackoffPolicy(backoffPolicy))

    def setDirtyCheckEnabled(dirtyCheckEnabled: Boolean) = new TxExecutorConfigurer(builder.setDirtyCheckEnabled(dirtyCheckEnabled))

    def setSpinCount(spinCount: Int) = new TxExecutorConfigurer(builder.setSpinCount(spinCount))

    def setReadonly(readonly: Boolean) = new TxExecutorConfigurer(builder.setReadonly(readonly))

    def setReadTrackingEnabled(enabled: Boolean) = new TxExecutorConfigurer(builder.setReadTrackingEnabled(enabled))

    def setSpeculative(speculative: Boolean) = new TxExecutorConfigurer(builder.setSpeculative(speculative))

    def setMaxRetries(maxRetries: Int) = new TxExecutorConfigurer(builder.setMaxRetries(maxRetries))

    def setIsolationLevel(isolationLevel: IsolationLevel) = new TxExecutorConfigurer(builder.setIsolationLevel(isolationLevel))

    def setBlockingAllowed(blockingAllowed: Boolean) = new TxExecutorConfigurer(builder.setBlockingAllowed(blockingAllowed))

    def newTxExecutor() = {
        if (builder.getConfiguration.propagationLevel == PropagationLevel.Requires)
            new LeanTxExecutor(builder.newTransactionFactory)
        else
            throw new TodoException()
    }
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
        gammaStm.newTransactionFactoryBuilder()
            .newTransactionFactory())

    def getThreadLocalTransaction(): Transaction = ThreadLocalTransaction.getThreadLocalTransaction()

    def apply[@specialized E](block: (Transaction) => E): E = {
        defaultTxExecutor.apply(block)
    }

    def isControlFlow(x: Throwable): Boolean = x.isInstanceOf[ControlFlowError]

    def retry() = StmUtils.retry

    def newTxExecutorConfigurer() = new TxExecutorConfigurer(gammaStm.newTransactionFactoryBuilder)

    def newIntRef(value: Int = 0) = new IntRef(value)

    def newLongRef(value: Long = 0) = new LongRef(value)

    def newBooleanRef(value: Boolean = false) = new BooleanRef(value)

    def newDoubleRef(value: Double = 0) = new DoubleRef(value)

    def newRef[E](): Ref[E] = new Ref[E]()
}
