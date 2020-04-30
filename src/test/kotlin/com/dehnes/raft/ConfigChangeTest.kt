package com.dehnes.raft

import com.dehnes.raft.RaftBase.TimerType
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals

class ConfigChangeTest: RaftTestBase() {

    @Test
    fun configChangeBasic() {

        addCluster(mapOf(
                node1 to 350,
                node2 to 450,
                node3 to 550
        ))

        // allow the timer to fire for node1
        allowTimers(node1, TimerType.election)
        // allow the VoteRequest to be handled
        allow2Way<VoteRequest, VoteResponse>(node1, node2)
        allow2Way<VoteRequest, VoteResponse>(node1, node3)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)

        allowExecutionAndWaitUntilAllFiltersConsumed()

        // send a value to the cluster
        sendMsg(node1, ClientCommandRequest(
                "id",
                "client",
                node1,
                "Hello World!"
        ))
        filterInbound(node1) {any -> any is ClientCommandRequest }
        // allow AppendEntriesRequest to be handled
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)

        // wait here until the value is replicated to all
        allowExecutionAndWaitUntilAllFiltersConsumed()
        assertEquals("Hello World!", fsms[node1]!![0])

        // new config: node2,node3,node4
        addCluster(mapOf(
                node4 to 400
        ))
        // allow it to "start"
        allowTimers(node4, TimerType.election)
        allow1Way<VoteRequest>(node4, node1)
        allow1Way<VoteRequest>(node4, node2)
        allow1Way<VoteRequest>(node4, node3)

        // and let the others operate normally in the mean time
        allowTimers(node1, TimerType.heartbeat)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)
        allowTimers(node1, TimerType.heartbeat)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)
        allowTimers(node1, TimerType.heartbeat)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)

        // wait here until the value is commited at all nodes
        allowExecutionAndWaitUntilAllFiltersConsumed()

        // now make the cluster aware of the new config
        sendMsg(node1, ClientCommandRequest(
                UUID.randomUUID().toString(),
                "client",
                node1,
                ClusterConfigurationUpdate(listOf(node2, node3, node4))
        ))
        filterInbound(node1) {any -> any is ClientCommandRequest }
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node4) // empty
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node4) // with 1 logentry
        // pre-phase is now finished, allow the new joint-config to be replicated

        allowExecutionAndWaitUntilAllFiltersConsumed()

        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node4)
        // now the joint-config is replicated, all the final new config to be replicated now
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node4)

        // wait here until the value is commited at all nodes
        allowExecutionAndWaitUntilAllFiltersConsumed()

        // allow new election round
        allowTimers(node4, TimerType.election)
        allow2Way<VoteRequest, VoteResponse>(node4, node2)
        allow2Way<VoteRequest, VoteResponse>(node4, node3)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node4, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node4, node3)

        allowExecutionAndWaitUntilAllFiltersConsumed()

        assertEquals(RaftState.following, getState(node2).state)
        assertEquals(RaftState.following, getState(node3).state)
        assertEquals(RaftState.leader, getState(node4).state)

        assertEquals(1, fsms[node2]!!.size)
        assertEquals(1, fsms[node3]!!.size)
        assertEquals(1, fsms[node4]!!.size)


    }


}