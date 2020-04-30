package com.dehnes.raft

import com.dehnes.raft.persistence.RaftStatePersistence
import mu.KLogger
import org.slf4j.MDC
import java.util.concurrent.*
import java.util.concurrent.locks.ReentrantLock

abstract class RaftBase(
        val nodeName: String,
        private val executorService: ExecutorService,
        private val onException: (t: Throwable, node: RaftBase) -> Unit,
        private val raftMessagingService: RaftMessagingService,
        protected val raftStatePersistence: RaftStatePersistence,
        private val inboundMessageFilter: (any: Any) -> Boolean
) {

    abstract fun logger(): KLogger

    private val timer = Executors.newSingleThreadScheduledExecutor { runnable ->
        val thread = Thread(runnable, "timer-for-$nodeName")
        thread.isDaemon = true
        thread.uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { t, throwable ->
            logger().warn("exception in timer $t", throwable)
            onException(throwable, this)
        }
        thread
    }
    private val msgQueue = ConcurrentLinkedQueue<Any>()

    private val lock = ReentrantLock()
    private val awaitingResponses = mutableMapOf<String, MutableMap<String, AwaitingResponsesState>>() // guarded by lock
    private val timers: MutableMap<TimerType, ScheduledFuture<*>> = mutableMapOf() // guarded by lock

    protected var state = RaftState.following

    @Volatile
    private var isShutdown = false

    fun sendMsg(msg: Any?) {
        if (isShutdown) {
            error("Node is shutdown")
        }
        if (msg != null) {
            msgQueue.offer(msg)
        }
        tryHandleNextMsg()
    }

    protected fun stopTimer(timerType: TimerType) {
        timers.remove(timerType)?.cancel(false)
    }

    protected fun scheduleTimer(timerType: TimerType, firesInMilliseconds: Long) {
        stopTimer(timerType)
        timers[timerType] = timer.schedule(
                {
                    logException {
                        sendMsg(TimerFired(timerType))
                    }
                },
                firesInMilliseconds,
                TimeUnit.MILLISECONDS
        )
    }

    protected fun forgetSentRequests() = awaitingResponses.clear()

    protected fun sendMessage(request: RaftRequest, state: Any? = null) {
        logger().debug { "Sending request $request" }
        if (request.to !in awaitingResponses) {
            awaitingResponses[request.to] = mutableMapOf()
        }
        awaitingResponses[request.to]!![request.id] = AwaitingResponsesState(state, request.id)
        raftMessagingService.sendRequest(request)
    }

    protected fun sendMessage(response: Response) {
        logger().debug { "Sending response $response" }
        raftMessagingService.reply(response)
    }

    private fun tryHandleNextMsg() {
        executorService.submit {
            logException {
                val gotLock = runLocked(null, null) {
                    handleNextMsgLocked()
                }
                if (gotLock) {
                    if (msgQueue.peek() != null) {
                        tryHandleNextMsg()
                    }
                }
            }
        }
    }

    internal fun runLocked(timeout: Long?, timeUnit: TimeUnit?, task: () -> Unit): Boolean {
        val lockMethod = if (timeout != null && timeUnit != null) {
            { lock.tryLock(timeout, timeUnit) }
        } else {
            { lock.tryLock() }
        }

        val gotLock = lockMethod()
        if (gotLock) {
            try {
                task()
            } finally {
                this.lock.unlock()
            }
        }
        return gotLock
    }

    private fun handleNextMsgLocked() {
        while (msgQueue.peek() != null && !isShutdown) {

            /*
             * This is just for testing. Makes it possible
             * to simulate package loss or pause
             */
            if (!inboundMessageFilter(msgQueue.peek()!!)) {
                continue
            }

            when (val msg = msgQueue.poll()!!) {
                is ShutdownNode -> stop()
                is RaftResponse -> {
                    val state = awaitingResponses[msg.from]?.remove(msg.id)
                    if (state != null && msg.id == state.msgId) {
                        handleResponse(msg, state.state)
                    } else {
                        logger().debug { "Not handling response due not expecting it $msg" }
                    }
                }
                is RaftRequest -> handleRequest(msg)
                is ClientCommandRequest -> handleClientCmd(msg)
                is Request -> handleOtherRequest(msg)
                is TimerFired -> handleTimer(msg.timerType)
                else -> logger().warn { "Ignoring unsupported $msg" }
            }
        }
    }

    abstract fun handleRequest(request: RaftRequest)
    abstract fun handleOtherRequest(request: Request)
    abstract fun handleResponse(response: RaftResponse, state: Any?)
    abstract fun handleTimer(timerType: TimerType)
    abstract fun handleClientCmd(clientCommandRequest: ClientCommandRequest)

    private fun logException(task: () -> Unit) {
        val backup = MDC.getCopyOfContextMap()
        MDC.put("RAFT_NODE", nodeName)
        MDC.put("RAFT_TERM", raftStatePersistence.getCurrentTerm().toString())
        MDC.put("RAFT_STATE", state.name)
        try {
            task()
        } catch (e: Exception) {
            onException(e, this)
        } finally {
            MDC.clear()
            MDC.setContextMap(backup)
        }
    }

    private fun stop() {
        timers.values.forEach {
            it.cancel(false)
        }
        isShutdown = true
        msgQueue.clear()
        awaitingResponses.clear()
        logger().info { "Shut down node $nodeName" }
    }

    data class AwaitingResponsesState(
            val state: Any?,
            val msgId: String
    )

    enum class TimerType {
        heartbeat,
        election
    }

    class TimerFired(
            val timerType: TimerType
    ) {
        override fun toString(): String {
            return "TimerFired(timerType=$timerType)"
        }
    }

}

enum class RaftState {
    following,
    candidate,
    leader
}

