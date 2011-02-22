package io.akka

import org.multiverse.api.Transaction

import AkkaStm._

object Example {
    val atomicBlock = newTxExecutorConfigurer.withReadonly(true).newTxExecutor

    def main(args: Array[String]) {
        test1()
        test2()
        test3()
    }

    def test3() = {
        val ref = new Ref[String]

        atomicBlock.apply((tx: Transaction) => ref.set(ref.get() + "foo"))

        println(ref.atom.get())
    }

    def test1() = {
        val ref = new Ref[String]
        apply((tx: Transaction) =>
            ref.set(ref.get() + "foo")
        //    ref.value = ref.value + "foo"
        )
        println(ref.atom.get())
    }

    def test2() = {
        val ref = new IntRef
        apply((tx: Transaction) =>
            ref.set(ref.get() + 1)

        )
        println(ref.atom.get())
    }
}