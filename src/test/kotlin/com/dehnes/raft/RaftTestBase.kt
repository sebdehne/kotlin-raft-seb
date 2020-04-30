package com.dehnes.raft

import com.dehnes.raft.persistence.InMemoryRaftStatePersistence
import mu.KLogger
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

const val node1 = "node1"
const val node2 = "node2"
const val node3 = "node3"
const val node4 = "node4"
const val node5 = "node5"
const val node6 = "node6"
const val node7 = "node7"
const val node8 = "node8"
const val node9 = "node9"

abstract class RaftTestBase {

    @Volatile
    var executionService: ExecutorService? = null
    private val logger = KotlinLogging.logger {}
    private val errors = LinkedBlockingQueue<Throwable>()

    @Volatile
    private lateinit var cluster: MutableMap<String, NodeWithInAndOutFilter>
    val fsms = mutableMapOf<String, MutableList<Any>>()

    @BeforeEach
    internal fun setUp() {
        UUID.randomUUID().toString() // warm-up
        executionService = Executors.newCachedThreadPool()
        cluster = mutableMapOf()
    }

    @AfterEach
    internal fun tearDown() {
        waitForErrors(10, TimeUnit.MILLISECONDS)

        // shutdown all remaining nodes
        cluster.forEach { (nodeName, nodeInfo) ->
            filterInbound(nodeName) { true }
            nodeInfo.raftNode.sendMsg(ShutdownNode("id", "client", nodeName))
        }
        allowExecutionAndWaitUntilAllFiltersConsumed()

        errors.clear()
        executionService!!.shutdown()
        fsms.clear()
    }

    private val clusterMessagingService = object : RaftMessagingService {
        override fun sendRequest(request: RaftRequest) {
            sendMessage(request)
        }

        override fun reply(response: Response) {
            sendMessage(response)
        }

        private fun sendMessage(message: Message) {

            val nodeWithInAndOutFilter = cluster[message.from]!!
            nodeWithInAndOutFilter.allowance.poll(1, TimeUnit.SECONDS)
                    ?: error("Timeout while waiting for allowance")
            val outFilter = nodeWithInAndOutFilter.outFilter

            val canProceed = synchronized(outFilter) {
                val indexMatched = outFilter.indexOfFirst { f -> f(message) != null }
                if (indexMatched < 0) {
                    error("No outbound handling found for $message")
                }
                outFilter.removeAt(indexMatched)(message)!!
            }

            if (!canProceed) {
                return
            }

            cluster[message.to]!!.raftNode.sendMsg(message)
        }
    }

    private fun handleInboundFilter(nodeName: String) = { message: Any ->
        val nodeWithInAndOutFilter = cluster[nodeName]!!
        nodeWithInAndOutFilter.allowance.poll(5, TimeUnit.SECONDS)
                ?: error("Timeout while waiting for allowance")
        val inFilter = nodeWithInAndOutFilter.inFilter

        synchronized(inFilter) {
            val indexMatched = inFilter.indexOfFirst { f -> f(message) != null }
            if (indexMatched < 0) {
                error("No inbound handling found for $message")
            }
            inFilter.removeAt(indexMatched)(message)!!
        }
    }

    private fun getFsmFor(nodeName: String): RaftStatemachine = object : RaftStatemachine {
        init {
            synchronized(fsms) {
                fsms[nodeName] = mutableListOf()
            }
        }

        override fun applyToFsm(value: Any) {
            synchronized(fsms) {
                fsms[nodeName]!!.add(value)
            }
        }

        override fun isRetry(previousValue: Any, nextValue: Any) = false
    }

    fun addCluster(config: Map<String, Int>) {
        val clusterConfig = (config.keys.toList() + cluster.keys.toList()).distinct()
        config.forEach { e ->
            cluster[e.key] = NodeWithInAndOutFilter(RaftNode(
                    e.key,
                    executionService!!,
                    { t, _ ->
                        logger.error("", t)
                        errors.offer(t)
                    },
                    clusterMessagingService,
                    getFsmFor(e.key),
                    InMemoryRaftStatePersistence(clusterConfig),
                    20,
                    100,
                    handleInboundFilter(e.key),
                    e.value.toLong()
            ).also { it.start() })
        }
    }

    fun removeNode(nodeName: String) {
        cluster.remove(nodeName)
        fsms.remove(nodeName)
    }

    fun allowTimers(nodeName: String, vararg timerTypes: RaftBase.TimerType) {
        filterInbound(nodeName) { any ->
            if (any is RaftBase.TimerFired) {
                (any.timerType in timerTypes).logIfFalse(logger, "timer ${any.timerType} not in $timerTypes")
            } else
                null
        }
    }

    inline fun <reified Req : Message> drop(from: String, to: String) {
        filterOutbound(from) { any ->
            if (any is Req && any.from == from && any.to == to) false else null
        }
    }

    inline fun <reified Msg : Message> allow1Way(from: String, to: String) {
        filterOutbound(from) { any ->
            if (any is Msg && any.from == from && any.to == to) {
                true
            } else
                null
        }
        filterInbound(to) { any ->
            if (any is Msg && any.from == from && any.to == to) {
                true
            } else
                null
        }
    }

    inline fun <reified Req : Message, reified Resp : Message> allow2Way(from: String, to: String) {
        filterOutbound(from) { any ->
            if (any is Req && any.from == from && any.to == to) {
                true
            } else
                null
        }
        filterInbound(to) { any ->
            if (any is Req && any.from == from && any.to == to) {
                true
            } else
                null
        }
        filterOutbound(to) { any ->
            if (any is Resp && any.from == to && any.to == from) {
                true
            } else
                null
        }
        filterInbound(from) { any ->
            if (any is Resp && any.from == to && any.to == from) {
                true
            } else
                null
        }
    }

    fun filterInbound(nodeName: String, messageFilter: MessageFilter) {
        val inFilter = cluster[nodeName]!!.inFilter
        synchronized(inFilter) {
            inFilter.add(messageFilter)
        }
    }

    fun filterOutbound(nodeName: String, messageFilter: MessageFilter) {
        val outFilter = cluster[nodeName]!!.outFilter
        synchronized(outFilter) {
            outFilter.add(messageFilter)
        }
    }

    fun allowExecutionAndWaitUntilAllFiltersConsumed() {
        cluster.forEach { (_, nodeWithInAndOutFilter) ->
            synchronized(nodeWithInAndOutFilter.inFilter) {
                nodeWithInAndOutFilter.inFilter.forEach { _ -> nodeWithInAndOutFilter.allowance.offer(true) }
            }
            synchronized(nodeWithInAndOutFilter.outFilter) {
                nodeWithInAndOutFilter.outFilter.forEach { _ -> nodeWithInAndOutFilter.allowance.offer(true) }
            }
        }

        val timeoutAt = System.currentTimeMillis() + 5000
        while (cluster.any { e -> e.value.allowance.isNotEmpty() }) {
            Thread.sleep(5)
            if (System.currentTimeMillis() > timeoutAt) {
                error("There are still filters which are unused")
            }
        }

        waitForErrors(10, TimeUnit.MILLISECONDS)
    }

    fun sendMsg(nodeName: String, any: Any) {
        cluster[nodeName]!!.raftNode.sendMsg(any)
    }

    fun getState(nodeName: String) = cluster[nodeName]!!.raftNode.getStateForTest()

    fun waitForErrors(timeout: Long, timeUnit: TimeUnit) {
        val throwable = errors.poll(timeout, timeUnit)
        if (throwable != null) {
            throw RuntimeException(throwable)
        }
    }
}

data class NodeWithInAndOutFilter(
        val raftNode: RaftNode,
        val allowance: LinkedBlockingQueue<Boolean> = LinkedBlockingQueue(),
        val inFilter: MutableList<MessageFilter> = mutableListOf(),
        val outFilter: MutableList<MessageFilter> = mutableListOf()
)

typealias MessageFilter = (any: Any) -> Boolean?

fun Boolean.logIfFalse(kLogger: KLogger, msg: String): Boolean {
    if (!this) {
        kLogger.info(msg)
    }
    return this
}