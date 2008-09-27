package org.hypergraphdb.peer.workflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.InterestEvaluator;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.log.LogEntry;
import org.hypergraphdb.peer.log.Timestamp;
import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.peer.workflow.RememberTaskServer.State;
import org.hypergraphdb.query.HGAtomPredicate;
import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.HGDBOntology.*;


/**
 * @author ciprian.costa
 *
 * This task manages a catchup request from another peer. It will determine the required Remember tasks and execute them.
 */
public class CatchUpTaskServer extends TaskActivity<CatchUpTaskServer.State>
{
	protected enum State {Started, Working, Done}

	private HyperGraphPeer peer;
	private Object msg;

	public CatchUpTaskServer(PeerInterface peerInterface, HyperGraphPeer peer, Object msg)
	{
		super(peerInterface, (UUID)getPart(msg, SEND_TASK_ID), State.Started, State.Done);
		this.peer = peer;
		this.msg = msg;
	}

	@Override
	protected void startTask()
	{
		Timestamp lastTimestamp = (Timestamp) getPart(msg, CONTENT, SLOT_LAST_VERSION);
		HGAtomPredicate interest = (HGAtomPredicate) getPart(msg, CONTENT, SLOT_INTEREST);
		
		System.out.println("Catch up request from " + getPart(msg, REPLY_TO) + " starting from " + lastTimestamp + " with interest " + interest);
		
		ArrayList<LogEntry> entries = peer.getLog().getLogEntries(lastTimestamp, interest);

		Collections.sort(entries);
		for(LogEntry entry : entries)
		{
			System.out.println("Should catch up with: " + entry.getTimestamp());
		
			Object sendToPeer = getPart(msg, REPLY_TO);
			entry.setLastTimestamp(getPeerInterface().getPeerNetwork().getPeerId(sendToPeer), lastTimestamp);
		
			RememberTaskClient rememberTask = new RememberTaskClient(getPeerInterface(), entry, sendToPeer, peer.getLog());
			rememberTask.run();
			//System.out.println("sent catch up for: " + rememberTask.getResult());

			lastTimestamp = entry.getTimestamp();
		}
		
		setState(State.Done);
	};

	public static class CatchUpTaskServerFactory implements TaskFactory
	{
		private HyperGraphPeer peer;
		public CatchUpTaskServerFactory(HyperGraphPeer peer)
		{
			this.peer = peer;
		}
		public TaskActivity<?> newTask(PeerInterface peerInterface, Object msg)
		{
			return new CatchUpTaskServer(peerInterface, peer, msg);
		}
		
	}
}
