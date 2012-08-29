/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.replication;


import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import mjson.Json;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.log.LogEntry;
import org.hypergraphdb.peer.log.Timestamp;
import org.hypergraphdb.peer.workflow.TaskActivity;
import org.hypergraphdb.peer.workflow.TaskFactory;
import org.hypergraphdb.query.HGAtomPredicate;
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
    public void handleMessage(Json msg)
    {
        Timestamp lastTimestamp = new Timestamp(msg.at(Messages.CONTENT).at(SLOT_LAST_VERSION).asInteger());
        HGAtomPredicate interest = Messages.fromJson(msg.at(Messages.CONTENT).at(SLOT_INTEREST));;
        System.out.println("Catch up request from " + msg.at(Messages.REPLY_TO)
                           + " starting from " + lastTimestamp
                           + " with interest " + interest);

        ArrayList<LogEntry> entries = getThisPeer().getLog().getLogEntries(lastTimestamp,
                                                                           interest);

        Collections.sort(entries);
        for (LogEntry entry : entries)
        {
            System.out.println("Should catch up with: " + entry.getTimestamp());

            Object sendToPeer = Messages.getSender(msg);
            entry.setLastTimestamp(getPeerInterface().getThisPeer().getIdentity(sendToPeer),
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