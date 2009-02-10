package org.hypergraphdb.peer.jxta;

import java.util.Set;

import net.jxta.document.Advertisement;

import org.hypergraphdb.peer.PeerFilter;

/**
 * @author Cipri Costa
 *
 * Object that finds all the known peers that match with some description. For the time being,
 * all it does is check the published name in the pipe advertisement and compare it with the 
 * description that is assumed to be a string.
 */
public class JXTAPeerFilter extends PeerFilter
{
	private Set<Advertisement> advs;
	
	public JXTAPeerFilter(Set<Advertisement> advs)
	{
		this.advs = advs;
	}

	@Override
	public void filterTargets()
	{
		synchronized (advs)
		{
			for(Advertisement adv : advs)
			{
				if (getEvaluator().shouldSend(adv))
				{
					matchFound(adv);
				}
			}
		}		
	}	
	
}
