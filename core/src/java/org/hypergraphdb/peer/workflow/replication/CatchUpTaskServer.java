package org.hypergraphdb.peer.workflow.replication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.log.LogEntry;
import org.hypergraphdb.peer.log.Timestamp;
import org.hypergraphdb.peer.workflow.TaskActivity;
import org.hypergraphdb.peer.workflow.TaskFactory;
import org.hypergraphdb.query.HGAtomPredicate;
import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.HGDBOntology.*;

/**
 * @author ciprian.costa
 * 
 * This task manages a catchup request from another peer. It will determine the
 * required Remember tasks and execute them.
 */
public class CatchUpTaskServer extends TaskActivity<CatchUpTaskServer.State>
{
    protected enum State
    {
        Started, Done
    }
    
    public CatchUpTaskServer(HyperGraphPeer thisPeer, UUID taskId)
    {
        super(thisPeer, taskId, State.Started, State.Done);
    }

    @Override
    public void handleMessage(Object msg)
    {
        Timestamp lastTimestamp = (Timestamp) getPart(msg, CONTENT,
                                                      SLOT_LAST_VERSION);
        HGAtomPredicate interest = (HGAtomPredicate) getPart(msg, CONTENT,
                                                             SLOT_INTEREST);

        System.out.println("Catch up request from " + getPart(msg, REPLY_TO)
                           + " starting from " + lastTimestamp
                           + " with interest " + interest);

        ArrayList<LogEntry> entries = getThisPeer().getLog().getLogEntries(lastTimestamp,
                                                                           interest);

        Collections.sort(entries);
        for (LogEntry entry : entries)
        {
            System.out.println("Should catch up with: " + entry.getTimestamp());

            Object sendToPeer = getPart(msg, REPLY_TO);
            entry.setLastTimestamp(getPeerInterface().getPeerNetwork().getPeerId(sendToPeer),
                                   lastTimestamp);

            RememberTaskClient rememberTask = new RememberTaskClient(getThisPeer(),
                                                                     entry,
                                                                     sendToPeer,
                                                                     getThisPeer().getLog());
            rememberTask.run();
            // System.out.println("sent catch up for: " +
            // rememberTask.getResult());

            lastTimestamp = entry.getTimestamp();
        }

        setState(State.Done);
    };

    public static class CatchUpTaskServerFactory implements TaskFactory
    {
        public TaskActivity<?> newTask(HyperGraphPeer peer, UUID taskId, Object msg)
        {
            return new CatchUpTaskServer(peer, taskId);
        }
    }
}