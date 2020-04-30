package com.dehnes.raft

import com.dehnes.raft.LogCompareResult.localIsMoreUp2Date
import com.dehnes.raft.RaftState.*
import com.dehnes.raft.persistence.RaftStatePersistence
import com.dehnes.raft.persistence.RaftStatePersistence.LogEntry
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.math.min

class RaftNode(
        nodeName: String,
        executorService: ExecutorService,
        onException: (t: Throwable, node: RaftBase) -> Unit,
        raftMessagingService: RaftMessagingService,
        private val raftStatemachine: RaftStatemachine,
        raftStatePersistence: RaftStatePersistence,
        estimatedBroadcastTimeInMs: Long,
        private val sendMaxEntries: Int,
        canProceed: (any: Any) -> Boolean,
        electionTimeout: Long? = null
) : RaftBase(
        nodeName,
        executorService,
        onException,
        raftMessagingService,
        raftStatePersistence,
        canProceed
) {

    private val logger = KotlinLogging.logger {}

    private val electionTimeout = electionTimeout
            ?: (estimatedBroadcastTimeInMs * 10) + ThreadLocalRandom.current().nextLong(estimatedBroadcastTimeInMs * 10)
    private val heartbeatTimeout = this.electionTimeout / 2

    // volatile state
    private var commitIndex = -1L
    private var lastApplied = -1L

    private var pendingConfigChange: List<String>? = null

    // volatile leader state
    private val nextIndex = mutableMapOf<String, Long>()
    private val matchIndex = mutableMapOf<String, Long>()

    private var currentlyHandlingClientCommand: ClientCommandRequest? = null

    override fun logger() = logger

    fun start() {
        logger().info { "Started with electionTimeout=$electionTimeout" }
        resetElectionTimer()
    }

    override fun handleRequest(request: RaftRequest) {
        logger().debug { "Got request $request" }
        commonHandle()

        if (request.from !in cluster()) {
            logger().debug { "Ignoring request from non-cluster member $request" }
        } else if (request.to != nodeName) {
            logger().debug { "Ignoring request not meant for me $request" }
        } else {
            if (request.term > raftStatePersistence.getCurrentTerm()) {
                becomeFollower(request.term, "Switching to follower because higher term detected")
            }

            when (state) {
                following -> {
                    when (request) {
                        is VoteRequest -> {
                            if (request.term < raftStatePersistence.getCurrentTerm()) {
                                sendMessage(request.response(raftStatePersistence.getCurrentTerm(), false))
                            } else {
                                val voteGranted = if (raftStatePersistence.getVotedFor() == null && compareLog(raftStatePersistence, request) != localIsMoreUp2Date) {
                                    raftStatePersistence.setVotedFor(request.from)
                                    resetElectionTimer()
                                    true
                                } else {
                                    false
                                }
                                sendMessage(request.response(raftStatePersistence.getCurrentTerm(), voteGranted))
                            }
                        }
                        is AppendEntriesRequest -> {
                            sendMessage(request.response(
                                    raftStatePersistence.getCurrentTerm(),
                                    handleLog(
                                            raftStatePersistence,
                                            request,
                                            commitIndex,
                                            logger()) {
                                        commitIndex = it
                                    }
                            ))
                            resetElectionTimer()
                            commonHandle()
                        }
                    }
                }
                candidate -> {
                    when (request) {
                        is AppendEntriesRequest -> {
                            if (request.term < raftStatePersistence.getCurrentTerm()) {
                                sendMessage(request.response(raftStatePersistence.getCurrentTerm(), false))
                            } else {
                                becomeFollower(request.term, "Giving up election, becoming a follower")
                                sendMessage(request.response(
                                        raftStatePersistence.getCurrentTerm(),
                                        handleLog(
                                                raftStatePersistence,
                                                request,
                                                commitIndex,
                                                logger()
                                        ) { commitIndex = it })
                                )
                            }
                        }
                        is VoteRequest -> {
                            sendMessage(request.response(raftStatePersistence.getCurrentTerm(), false))
                        }
                    }
                }
                leader -> {
                    if (request is AppendEntriesRequest) {
                        if (request.term >= raftStatePersistence.getCurrentTerm()) {
                            becomeFollower(request.term, "Received AppendEntriesRequest from ${request.from}")
                        }
                    }
                }
            }
        }
    }

    override fun handleOtherRequest(request: Request) {
        when (request) {
            is GetStateSnapshotRequest -> {
                sendMessage(
                        GetStateSnapshotResponse(
                                request.id,
                                nodeName,
                                request.from,
                                raftStatePersistence.getCurrentTerm(),
                                state,
                                commitIndex,
                                lastApplied,
                                raftStatePersistence.getLogSize()
                        )
                )
            }
            else -> logger().warn { "Dont know what to do with $request" }
        }
    }

    override fun handleResponse(response: RaftResponse, sendState: Any?) {
        logger().debug { "Got response $response" }
        commonHandle()

        if (response.term > raftStatePersistence.getCurrentTerm()) {
            becomeFollower(response.term)
        } else {
            when (state) {
                candidate -> {
                    val voteResponse = response as VoteResponse
                    if (voteResponse.term == raftStatePersistence.getCurrentTerm() && voteResponse.voteGranted) {
                        raftStatePersistence.appendGotVote(voteResponse.from)

                        if (hasMajorityOfVotes(raftStatePersistence.getGotVotes(), otherNodes())) {
                            becomeLeader()
                        }
                    }
                }
                leader -> {
                    val appendEntriesResponse = response as AppendEntriesResponse
                    val appendEntriesRequestState = sendState as AppendEntriesRequestState
                    val follower = appendEntriesResponse.from
                    if (!appendEntriesResponse.success) {
                        nextIndex[follower] = nextIndex[follower]!! - 1
                        assert(nextIndex[follower]!! >= 0)
                        sendAPE(follower, false)
                    } else {
                        nextIndex[follower] = appendEntriesRequestState.offsetIndex + appendEntriesRequestState.entriesSent
                        if (appendEntriesRequestState.entriesSent > 0) {
                            matchIndex[follower] = appendEntriesRequestState.offsetIndex + appendEntriesRequestState.entriesSent - 1
                        }
                        commitIfPossible()
                        commonHandle()
                    }
                }
                following -> {
                }
            }
        }
    }

    override fun handleTimer(timerType: TimerType) {
        logger().debug { "Timer $timerType fires" }
        commonHandle()

        if (timerType == TimerType.election && (state == following || state == candidate)) {
            if (isPartOfCluster()) {
                becomeCandidate()
            } else {
                becomeFollower(raftStatePersistence.getCurrentTerm())
            }
        } else if (state == leader && timerType == TimerType.heartbeat) {
            replicateToNodes().forEach { n -> sendAPE(n, false) }
            resetHeartbeatTimer()
        } else {
            logger().debug { "Ignored $timerType" }
        }
    }

    override fun handleClientCmd(clientCommandRequest: ClientCommandRequest) {
        if (currentlyHandlingClientCommand != null) {
            logger().info { "currently handling another request, ignoring $clientCommandRequest" }
            return
        }
        if (state != leader) {
            logger().info { "not the leader - ignoring $clientCommandRequest" }
            return
        }
        logger().debug { "handleClientCmd $clientCommandRequest" }
        commonHandle()

        val value = clientCommandRequest.value
        val alreadyExistsInLog = raftStatePersistence.getLogSize() > 0 && raftStatemachine.isRetry(
                raftStatePersistence.getLogAt(raftStatePersistence.getLogSize() - 1).entry,
                value)
        val isConfigUpdateAndAlreadyInTransition = value is ClusterConfigurationUpdate && raftStatePersistence.getClusterConfiguration().isInTransition()
        if (alreadyExistsInLog || isConfigUpdateAndAlreadyInTransition) {
            logger().info {
                "ignoring retry $clientCommandRequest " +
                        "alreadyExistsInLog=$alreadyExistsInLog " +
                        "isConfigUpdateAndAlreadyInTransition=$isConfigUpdateAndAlreadyInTransition"
            }
        } else {
            if (value is ClusterConfigurationUpdate) {
                if (pendingConfigChange != null) {
                    logger().warn { "Ignoring ClusterConfigurationUpdate because of already ongoing change (pre-phase)" }
                } else {
                    pendingConfigChange = value.newConfiguration
                    logger().info { "New config received, started pre-phase " + value.newConfiguration }
                    initFollowerState(false)
                }
            } else {
                raftStatePersistence.appendLog(LogEntry(raftStatePersistence.getCurrentTerm(), value))
            }
            replicateToNodes().forEach { n -> sendAPE(n, false) }
            resetHeartbeatTimer()
        }
    }

    fun getStateForTest(): GetStateSnapshotResponse {
        var result: GetStateSnapshotResponse? = null
        runLocked(1, TimeUnit.DAYS) {
            result = GetStateSnapshotResponse(
                    "",
                    nodeName,
                    "",
                    raftStatePersistence.getCurrentTerm(),
                    state,
                    commitIndex,
                    lastApplied,
                    raftStatePersistence.getLogSize()
            )
        }
        return result!!
    }

    private fun commitIfPossible() {
        val candidateIndex = findMajorityMatchIndex(matchIndex)
        if (candidateIndex > commitIndex && raftStatePersistence.getLogAt(candidateIndex).term == raftStatePersistence.getCurrentTerm()) {
            commitIndex = candidateIndex
        }
    }

    private fun commonHandle() {
        val appliedSomething = applyCommited()

        val clientCommandRequest = currentlyHandlingClientCommand
        if (appliedSomething && clientCommandRequest != null) {
            sendMessage(ClientCommandResponse(
                    clientCommandRequest.id,
                    nodeName,
                    clientCommandRequest.from
            ))
            currentlyHandlingClientCommand = null
        }

        if ((state == leader || state == candidate) && !isPartOfCluster()) {
            becomeFollower(
                    raftStatePersistence.getCurrentTerm(),
                    "Stepping down because I'm not part of cluster configuration"
            )
        }

        // is config change pre-phase done?
        if (pendingConfigChange != null) {
            val newNodes = newNodesInPrePhase()
            // check if all new nodes have caught up
            if (newNodes.all { n -> matchIndex[n]!! >= raftStatePersistence.getLogSize() - 1 }) {
                val newCluster = pendingConfigChange!!
                val clusterConfiguration = raftStatePersistence
                        .getClusterConfiguration()
                        .startTransition(newCluster)
                raftStatePersistence.appendLog(LogEntry(
                        raftStatePersistence.getCurrentTerm(),
                        clusterConfiguration))
                logger().info { "Finished waiting all new nodes to catch up. Started transition to new config " + raftStatePersistence.getClusterConfiguration() }
                pendingConfigChange = null
                replicateToNodes().forEach { n -> sendAPE(n, false) }
                resetHeartbeatTimer()
            }
        }
    }

    private fun applyCommited(): Boolean {
        var appliedSomething = false
        while (commitIndex > lastApplied) {
            val nextIndex = lastApplied + 1
            val nextLogEntryValue = raftStatePersistence.getLogAt(nextIndex).entry

            if (nextLogEntryValue is RaftStatePersistence.ClusterConfiguration) {
                //  It is now safe for the leader to create a log entry describing Cnew and replicate it to the cluster.
                if (state == leader && nextLogEntryValue.isInTransition()) {
                    val clusterConfiguration = raftStatePersistence
                            .getClusterConfiguration()
                            .endTransition()
                    logger().info { "Completing config transitioning by applied new config $clusterConfiguration" }
                    raftStatePersistence.appendLog(
                            LogEntry(
                                    raftStatePersistence.getCurrentTerm(),
                                    clusterConfiguration
                            )
                    )
                    initFollowerState(false)
                    replicateToNodes().forEach { n -> sendAPE(n, false) }
                }
            } else {
                raftStatemachine.applyToFsm(nextLogEntryValue)
            }
            lastApplied = nextIndex
            appliedSomething = true
        }
        return appliedSomething
    }

    private fun becomeLeader() {
        logger().info { "Becoming leader" }
        resetHeartbeatTimer()
        forgetSentRequests()
        stopTimer(TimerType.election)
        state = leader
        raftStatePersistence.clearGotVotes()
        raftStatePersistence.clearVotedFor()
        initFollowerState(true)
        replicateToNodes().forEach { n ->
            sendAPE(n, true)
        }
    }

    private fun becomeFollower(newTerm: Long, msg: String = "About to switch to follower") {
        logger().info { msg }
        pendingConfigChange = null
        raftStatePersistence.setCurrentTerm(newTerm)
        state = following
        raftStatePersistence.clearVotedFor()
        raftStatePersistence.clearGotVotes()
        forgetSentRequests()
        resetElectionTimer()
        stopTimer(TimerType.heartbeat)
    }

    private fun becomeCandidate() {
        logger().info { "About to switch to candidate" }
        forgetSentRequests()
        state = candidate
        raftStatePersistence.setCurrentTerm(raftStatePersistence.getCurrentTerm() + 1)
        raftStatePersistence.clearGotVotes()
        raftStatePersistence.appendGotVote(nodeName)
        resetElectionTimer()

        otherNodes().forEach { n ->
            sendMessage(VoteRequest(
                    UUID.randomUUID().toString(),
                    nodeName,
                    n,
                    raftStatePersistence.getCurrentTerm(),
                    raftStatePersistence.getLogSize() - 1,
                    if (raftStatePersistence.getLogSize() > 0) raftStatePersistence.getLogAt(raftStatePersistence.getLogSize() - 1).term else 0
            ))
        }
    }

    private fun sendAPE(to: String, sendEmpty: Boolean) {
        val nextIndex = nextIndex[to]!!

        val entriesToSend = raftStatePersistence.logSubList(
                nextIndex,
                if (sendEmpty || raftStatePersistence.getLogSize() == 0L) {
                    nextIndex
                } else {
                    min(raftStatePersistence.getLogSize(), nextIndex + sendMaxEntries)
                }
        )

        val prevLogIndex = nextIndex - 1

        val request = AppendEntriesRequest(
                UUID.randomUUID().toString(),
                nodeName,
                to,
                raftStatePersistence.getCurrentTerm(),
                prevLogIndex,
                if (prevLogIndex >= 0) raftStatePersistence.getLogAt(prevLogIndex).term else 0,
                entriesToSend.map { it.entry },
                commitIndex
        )

        sendMessage(request, AppendEntriesRequestState(
                entriesToSend.size.toLong(),
                nextIndex
        ))
    }

    private fun initFollowerState(reset: Boolean) {
        val otherNodesAndNewNodes = replicateToNodes()
        if (reset) {
            nextIndex.clear()
            matchIndex.clear()
        }
        otherNodesAndNewNodes.forEach { n ->
            if (n !in nextIndex) {
                nextIndex[n] = raftStatePersistence.getLogSize()
            }
            if (n !in matchIndex) {
                matchIndex[n] = -1
            }
        }
        nextIndex.keys.filter { n -> n !in otherNodesAndNewNodes }.forEach { n -> nextIndex.remove(n) }
        matchIndex.keys.filter { n -> n !in otherNodesAndNewNodes }.forEach { n -> matchIndex.remove(n) }
    }

    private fun resetElectionTimer() {
        scheduleTimer(TimerType.election, electionTimeout)
    }

    private fun resetHeartbeatTimer() {
        scheduleTimer(TimerType.heartbeat, heartbeatTimeout)
    }

    private data class AppendEntriesRequestState(
            val entriesSent: Long,
            val offsetIndex: Long
    )

    private fun isPartOfCluster() = nodeName in cluster()
    private fun cluster() = raftStatePersistence.getClusterConfiguration().cluster
    private fun otherNodes() = cluster().filter { n -> n != nodeName }
    private fun replicateToNodes(): List<String> {
        val pendingConfigChange = this.pendingConfigChange ?: emptyList()
        return (otherNodes() + pendingConfigChange).distinct()
    }

    private fun newNodesInPrePhase(): List<String> {
        val otherNodes = otherNodes()
        return (this.pendingConfigChange ?: emptyList()).filter { n -> n !in otherNodes }
    }
}

interface RaftStatemachine {
    fun applyToFsm(value: Any)
    fun isRetry(previousValue: Any, nextValue: Any): Boolean
}