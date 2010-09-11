/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.log;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.Iterator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.StorageService;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.AtomPartCondition;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.ComparisonOperator;
import org.hypergraphdb.query.HGAtomPredicate;
/**
 * @author Cipri Costa
 *
 * Manages all log operations. Ensures serialization of events
 */
public class Log
{
	public static final HGPersistentHandle LATEST_VERSION_HANDLE = null;
		// HGHandleFactory.makeHandle("136b5d67-7b0c-41f4-a0e0-105f2c42622e");

	private HyperGraph logDb;
	private HashMap<HGPeerIdentity, Peer> peers = new HashMap<HGPeerIdentity, Peer>();
	private HashMap<Object, HGHandle> peerHandles = new HashMap<Object, HGHandle>();
	private PeerInterface peerInterface;
	private HashMap<Object, HashMap<Timestamp, Timestamp>> peerQueues = new HashMap<Object, HashMap<Timestamp,Timestamp>>();
	private Timestamp timestamp;
	
	public Log(HyperGraph logDb, PeerInterface peerInterface)
	{
		this.logDb = logDb;
		this.peerInterface = peerInterface;
		
		//initialize with the latest version
		byte[] data = logDb.getStore().getData(LATEST_VERSION_HANDLE);
		if (data == null)
		{
			System.out.println("LATEST_VERSION_HANDLE not found");
			timestamp = new Timestamp();
			HGPersistentHandle handle = logDb.getPersistentHandle(logDb.add(timestamp));
			logDb.getStore().store(LATEST_VERSION_HANDLE, handle.toByteArray());
		}
		else
		{
			HGHandle handle = logDb.getHandleFactory().makeHandle(data);
			timestamp = logDb.get(handle);
			
			System.out.println("LATEST_VERSION_HANDLE : " + timestamp);
		}
		timestamp.moveNext();

	}

	public LogEntry createLogEntry(HGPersistentHandle handle, Object value, StorageService.Operation operation)
	{
		LogEntry entry = new LogEntry(value, logDb, handle, operation);
		
		return entry;
	}
	
	/**
	 * 
	 * Adds an event to the log.
	 * @param value
	 * @param peerFilter
	 * @return
	 */
	public LogEntry addEntry(LogEntry entry, Iterator<Object> targets)
	{		
		//ensure only one at a time is logged
		synchronized(timestamp)
		{
			Timestamp entryTimestamp = timestamp.moveNext();
			
			entry.setTimestamp(entryTimestamp);
			HGHandle timestampHandle = logDb.add(entryTimestamp);
			
			logDb.getStore().store(LATEST_VERSION_HANDLE, (logDb.getPersistentHandle(timestampHandle)).toByteArray());
			HGHandle opHandle = logDb.add(entry.operation);
			logDb.add(new HGPlainLink(timestampHandle, entry.getLogEntryHandle(), opHandle));
			
			//get timestamp, save, 
			while (targets.hasNext())
			{
				Object target = targets.next();
	
				HGPeerIdentity targetId = peerInterface.getThisPeer().getIdentity(target);
				Peer peer = getPeer(targetId);
				
				//make connection with peer
				HGPlainLink link = new HGPlainLink(peerHandles.get(targetId), entry.getLogEntryHandle());
				logDb.add(link);
				entry.setLastTimestamp(targetId, peer.getTimestamp());
				
				peer.setTimestamp(entryTimestamp);
				logDb.replace(peerHandles.get(targetId), peer);
				
				System.out.println(entry.getLastTimestamp(targetId));
			}
		}
		return entry;
	}

	public void purge()
	{
		
	}
	
	public void confirmFromPeer(HGPeerIdentity targetId, Timestamp timestamp)
	{
		// record the peer received the message - this will be used for purging
		Peer peer = getPeer(targetId);
		
		synchronized(timestamp)
		{
			Timestamp oldTimestamp = peer.getLastConfirmedTimestamp();
		
			if (oldTimestamp.compareTo(timestamp) < 0)
			{
				peer.setLastConfirmedTimestamp(timestamp);
				logDb.replace(peerHandles.get(targetId), peer);
			}
		}
	}
	
	private Peer getPeer(HGPeerIdentity targetId)
	{
		Peer peer = peers.get(targetId);
		if (peer == null)
		{
			//try to find the peer
			HGSearchResult<HGHandle> peerSearchResult = logDb.find(new And(new AtomTypeCondition(Peer.class), new AtomPartCondition(new String[]{"peerId"}, targetId)));
			HGHandle peerHandle;
			if (peerSearchResult.hasNext())
			{
				peerHandle = peerSearchResult.next();
				peer = logDb.get(peerHandle);
			}else{
				peer = new Peer(targetId);
				peerHandle = logDb.add(peer);
			}
			peers.put(targetId, peer);
			peerHandles.put(targetId, peerHandle);						
		}

		return peer;
	}
	
	public Timestamp getLastFrom(Object peer)
	{
		return getPeer(peerInterface.getThisPeer().getIdentity(peer)).getLastFrom();
	}
	
	/**
	 * serializes messages from each peer. initializes catchup phase if necessary.
	 * @param current_version 
	 * @param last_version 
	 */
	public boolean registerRequest(HGPeerIdentity peerId, Timestamp last_version, Timestamp current_version)
	{
		//TODO - add a timeout and return false;
		
		Peer peer = getPeer(peerId);
		
		//if other versions should be written before ...
		if (peer.getLastFrom().compareTo(last_version) != 0 && false)
		{
			
			try
			{
				System.out.println("Log: expecting " + last_version + " and found " + peer.getLastFrom() + ". Waiting...");
				getPeerQueue(peerId, true).put(last_version, current_version);
				synchronized (current_version)
				{
					current_version.wait();					
				}
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return true;
		}
		
		//check if versions are ok
		//if not wait for a certain period for other requests to arrive
		//if not then initialize catchup phase (could be done later)
		
		return true;
	}
	
	public void finishRequest(HGPeerIdentity peerId, Timestamp last_version, Timestamp current_version)
	{
		synchronized(timestamp)
		{
			Peer peer = getPeer(peerId);
			
			peer.setLastFrom(current_version);
			logDb.replace(peerHandles.get(peerId), peer);
		
			HashMap<Timestamp, Timestamp> queue = getPeerQueue(peerId, false);
			if (queue != null)
			{
				if (queue.containsKey(last_version))
				{
					queue.get(last_version).notify();
					queue.remove(last_version);
				}
			}
		}
	}
	
	private HashMap<Timestamp, Timestamp> getPeerQueue(Object peerId, boolean createNew)
	{
		HashMap<Timestamp, Timestamp> queue = peerQueues.get(peerId);
		
		if ((queue == null) && createNew)
		{
			queue = new HashMap<Timestamp, Timestamp>();
			peerQueues.put(peerId, queue);
		}
		
		return queue;
	}

	public ArrayList<LogEntry> getLogEntries(Timestamp startingFrom, HGAtomPredicate interest)
	{
		//find all timestamps greater then a value
		ArrayList<LogEntry> result = new ArrayList<LogEntry>();
		HGSearchResult<HGHandle> timestamps = logDb.find(hg.and(hg.type(Timestamp.class), hg.value(startingFrom, ComparisonOperator.LT)));
		
		while(timestamps.hasNext())
		{
			HGHandle handle = timestamps.next();
			
			for(HGHandle linkHandle : logDb.getIncidenceSet(handle))
			{
				HGPlainLink link = logDb.get(linkHandle);
				if (link.getArity() > 1)
				{
					if (interest.satisfies(logDb, link.getTargetAt(1)))
					{
						Timestamp ts = logDb.get(handle);
						LogEntry entry = new LogEntry(link.getTargetAt(1), logDb, ts);
						entry.setOperation((StorageService.Operation)logDb.get(link.getTargetAt(2)));
						
						result.add(entry);
					}
				}
			}
		}

		// TODO Auto-generated method stub
		return result;
	}
}
