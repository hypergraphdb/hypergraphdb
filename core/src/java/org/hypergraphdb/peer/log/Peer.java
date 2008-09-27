package org.hypergraphdb.peer.log;


/**
 * @author ciprian.costa
 * Simple bean that stores all the required information about a peer.
 */
public class Peer
{
	private Object peerId;
	private Timestamp timestamp = new Timestamp();
	private Timestamp lastConfirmedTimestamp =  new Timestamp();
	private Timestamp lastFrom = new Timestamp();
	
	public Peer()
	{
		
	}
	public Peer(Object peerId)
	{
		this.peerId = peerId;
	}

	public Object getPeerId()
	{
		return peerId;
	}

	public void setPeerId(Object peerId)
	{
		this.peerId = peerId;
	}
	public Timestamp getTimestamp()
	{
		return timestamp;
	}
	public void setTimestamp(Timestamp timestamp)
	{
		this.timestamp = timestamp;
	}
	public Timestamp getLastConfirmedTimestamp()
	{
		return lastConfirmedTimestamp;
	}
	public void setLastConfirmedTimestamp(Timestamp lastConfirmedTimestamp)
	{
		this.lastConfirmedTimestamp = lastConfirmedTimestamp;
	}
	public Timestamp getLastFrom()
	{
		return lastFrom;
	}
	public void setLastFrom(Timestamp lastFrom)
	{
		this.lastFrom = lastFrom;
	}
	
	public String toString()
	{
		return "Peer: " + peerId + "; timestamp: " + timestamp + "; last confirmed: " + lastConfirmedTimestamp + "; last from: " + lastFrom;
	}
	
}
