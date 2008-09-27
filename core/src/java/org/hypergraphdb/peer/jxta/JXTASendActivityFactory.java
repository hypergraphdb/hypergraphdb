package org.hypergraphdb.peer.jxta;

import net.jxta.document.Advertisement;
import net.jxta.peergroup.PeerGroup;
import net.jxta.protocol.PipeAdvertisement;

import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;

/**
 * @author Cipri Costa
 *
 * Simple factory for JXTASendActivities
 */
public class JXTASendActivityFactory implements PeerRelatedActivityFactory
{
	private PeerGroup peerGroup;
	private Advertisement pipeAdv;
	
	public JXTASendActivityFactory(PeerGroup peerGroup, PipeAdvertisement pipeAdv)
	{
		this.peerGroup = peerGroup;
		this.pipeAdv = pipeAdv;
	}
	
	@Override
	public PeerRelatedActivity createActivity()
	{
		return new JXTASendActivity(peerGroup, pipeAdv);
	}

}
