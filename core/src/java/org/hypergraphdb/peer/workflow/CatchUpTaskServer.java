package org.hypergraphdb.peer.workflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.log.LogEntry;
import org.hypergraphdb.peer.log.Timestamp;
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
        Started, Working, Done
    }
    private Object msg;

    public CatchUpTaskServer(HyperGraphPeer thisPeer, Object msg)
    {
        super(thisPeer, (UUID) getPart(msg, SEND_TASK_ID), State.Started,
              State.Done);
        this.msg = msg;
    }

    @Override
    protected void startTask()
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
        public CatchUpTaskServerFactory()
        {
        }

        public TaskActivity<?> newTask(HyperGraphPeer peer, Object msg)
        {
            return new CatchUpTaskServer(peer, msg);
        }

    }
}
