package org.hypergraphdb.peer.replication;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.workflow.Activity;
import org.hypergraphdb.query.HGAtomPredicate;
import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.HGDBOntology.*;

/**
 * @author ciprian.costa The task is used both to get the interests of all peers
 *         and to respond to publish interest messages.
 * 
 */
public class GetInterestsTask extends Activity
{
    // TODO: temporary
    private AtomicInteger count = new AtomicInteger();

    public GetInterestsTask(HyperGraphPeer thisPeer)
    {
        super(thisPeer);
    }

    public GetInterestsTask(HyperGraphPeer thisPeer, UUID taskId)
    {
        super(thisPeer, taskId);
    }

    public void initiate()
    {
        PeerRelatedActivityFactory activityFactory = getPeerInterface().newSendActivityFactory();

        PeerFilter peerFilter = getPeerInterface().newFilterActivity(null);

        peerFilter.filterTargets();
        Iterator<Object> it = peerFilter.iterator();
        count.set(1);
        while (it.hasNext())
        {
            count.incrementAndGet();
            Object target = it.next();
            sendMessage(activityFactory, target);
        }
        if (count.decrementAndGet() == 0)
        {
            getState().setCompleted();
        }
    }
    
    private void sendMessage(PeerRelatedActivityFactory activityFactory,
                             Object target)
    {
        Message msg = createMessage(Performative.Request, ATOM_INTEREST, getId());
        PeerRelatedActivity activity = (PeerRelatedActivity) activityFactory.createActivity();
        activity.setTarget(target);
        activity.setMessage(msg);

        try
        {
            getThisPeer().getExecutorService().submit(activity);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }
    
    public void handleMessage(Message msg)
    {
        HGPeerIdentity other = getThisPeer().getIdentity(getPart(msg, Messages.REPLY_TO));
        Replication.get(getThisPeer()).getOthersInterests().put(other,(HGAtomPredicate) getPart(msg,
                                                                                                Messages.CONTENT)); 
        if (count.decrementAndGet() == 0)
        {
            getState().setCompleted();
        }
    }
}