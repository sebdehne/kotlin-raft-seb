package com.dehnes.raft

import com.dehnes.raft.persistence.InMemoryRaftStatePersistence
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.Executors
import kotlin.system.exitProcess

class CommunicationGroup(
        val groupId: Int,
        val config: Map<String, RaftNode>
) {
    var currentTerm = -1L
    var currentLeader: String? = null

    fun hasNode(nodeName: String) = nodeName in config

    fun handleMessageFromNode(message: Message) {
        if (message.to == "client") {
            println("CommunicationGroup[$groupId]: $message")
        } else {
            synchronized(this) {
                if (message is AppendEntriesRequest) {
                    if (message.term > currentTerm) {
                        currentLeader = message.from
                        currentTerm = message.term
                    }
                }
            }
            config[message.to]?.sendMsg(message)
        }
    }

    fun sendMessageToLeader(getMessage: (dst: String) -> Message) {
        config[currentLeader]?.let { it.sendMsg(getMessage(it.nodeName)) }
    }

    fun sendMessageAll(getMessage: (dst: String) -> Message) {
        config.forEach { e -> e.value.sendMsg(getMessage(e.key)) }
    }

    override fun toString(): String {
        return "CommunicationGroup(groupId=$groupId, currentTerm=$currentTerm, currentLeader=$currentLeader group=" + config.keys + ")"
    }
}

fun main() {
    UUID.randomUUID().toString() // warm-up takes 150ms
    val executorService = Executors.newCachedThreadPool()
    val logger = KotlinLogging.logger {}

    val onException: (Throwable, RaftBase) -> Unit = { t, node ->
        logger.error("Error at node: ${node.nodeName}", t)
        t.printStackTrace()
        exitProcess(1)
    }

    var groups = emptyList<CommunicationGroup>()

    val clusterMessagingService = object : RaftMessagingService {
        override fun sendRequest(request: RaftRequest) {
            sendMessage(request)
        }

        override fun reply(response: Response) {
            sendMessage(response)
        }

        private fun sendMessage(message: Message) {
            groups.firstOrNull { c -> c.hasNode(message.from) }?.handleMessageFromNode(message)
        }
    }

    val fsms = mutableMapOf<String, MutableList<Any>>()
    val getFsmFor = { nodeName: String ->
        object : RaftStatemachine {
            override fun applyToFsm(value: Any) {
                synchronized(fsms) {
                    if (nodeName !in fsms) {
                        fsms[nodeName] = mutableListOf()
                    }
                    fsms[nodeName]!!.add(value)
                }
            }

            override fun isRetry(previousValue: Any, nextValue: Any) = false
        }
    }
    val newNode = { nodeName: String, initialCluster: List<String> ->
        val raftNode = RaftNode(
                nodeName,
                executorService,
                onException,
                clusterMessagingService,
                getFsmFor(nodeName),
                InMemoryRaftStatePersistence(initialCluster),
                20,
                100,
                { true }
        )
        raftNode.start()
        raftNode
    }

    println("Welcome, type 'help'.")
    while (true) {
        try {
            val command = readLine()?.trim() ?: ""
            when {
                command == "help" -> println("""
                    nodes - lists all created nodes
                    groups 1,2,3 4,5,6 - sets up which nodes can communicate (new nodes are created if not existing and left-out nodes are killed)
                    config <groupId> 1,2,3 - sends new config 1,2,3 to the leader of group with groupId 
                    add <groupId> <value> - sends a new value to the Raft group with groupId 
                    status N - prints the cluster status and FSM content for cluster N
                    state N - asks all nodes to report their state for group N
                    fsm N - print fsm for node N
                    exit - exit
                """.trimIndent())
                command == "nodes" -> println(groups.flatMap { it.config.keys })
                command.startsWith("groups ") -> {
                    val oldGroups = groups
                    val parts = command.split(" ")
                    val result = mutableListOf<CommunicationGroup>()
                    for (i in 1 until parts.size) {
                        val nodeNames = parts[i].split(",").map { "node$it" }

                        val clusterMap = nodeNames.map { nodeName ->
                            // find existing node or create new one
                            oldGroups.flatMap { it.config.values }.firstOrNull { n -> n.nodeName == nodeName }
                                    ?: newNode(nodeName, nodeNames)
                        }.map { n -> n.nodeName to n }.toMap()
                        result.add(CommunicationGroup(i - 1, clusterMap))
                    }
                    // kill all left-out nodes
                    oldGroups.flatMap { it.config.values }
                            .filter { n -> result.none { g -> n.nodeName in g.config } }
                            .forEach { n ->
                                n.sendMsg(ShutdownNode(
                                        UUID.randomUUID().toString(),
                                        "client",
                                        n.nodeName
                                ))
                                fsms.remove(n.nodeName)
                            }

                    groups = result.toList()
                    println("Updated groups: $groups")
                }
                command.startsWith("add ") -> {
                    val parts = command.split(" ")
                    groups[parts[1].toInt()].sendMessageToLeader { dst ->
                        ClientCommandRequest(
                                UUID.randomUUID().toString(),
                                "client",
                                dst,
                                parts[2]
                        )
                    }
                }
                command.startsWith("status ") -> {
                    val parts = command.split(" ")
                    println(groups[parts[1].toInt()])
                }
                command.startsWith("state ") -> {
                    val parts = command.split(" ")
                    groups[parts[1].toInt()].sendMessageAll { dst ->
                        GetStateSnapshotRequest(
                                UUID.randomUUID().toString(),
                                "client",
                                dst
                        )
                    }
                }
                command.startsWith("fsm ") -> {
                    val parts = command.split(" ")
                    println(fsms[parts[1]])
                }
                command.startsWith("config ") -> {
                    val parts = command.split(" ")
                    val targetGroup = parts[1].toInt()
                    val nodes = parts[2].split(",").map { "node$it" }
                    groups[targetGroup].sendMessageToLeader { dst ->
                        ClientCommandRequest(
                                UUID.randomUUID().toString(),
                                "client",
                                dst,
                                ClusterConfigurationUpdate(nodes)
                        )
                    }
                }
                command == "exit" -> exitProcess(0)
                else -> {
                    println("Dont know command: '$command'")
                }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}

