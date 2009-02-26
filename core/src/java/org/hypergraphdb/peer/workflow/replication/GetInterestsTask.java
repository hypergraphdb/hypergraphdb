package org.hypergraphdb.peer.workflow.replication;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.peer.workflow.TaskActivity;
import org.hypergraphdb.peer.workflow.TaskFactory;
import org.hypergraphdb.query.HGAtomPredicate;
import static org.hypergraphdb.peer.Structs.*;
import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.HGDBOntology.*;

/**
 * @author ciprian.costa The task is used both to get the interests of all peers
 *         and to respond to publish interest messages.
 * 
 */
public class GetInterestsTask extends TaskActivity<GetInterestsTask.State>
{
    protected enum State
    {
        Started, Done
    }

    // TODO: temporary
    private AtomicInteger count = new AtomicInteger();

    public GetInterestsTask(HyperGraphPeer thisPeer)
    {
        super(thisPeer, State.Started, State.Done);
    }

    public GetInterestsTask(HyperGraphPeer thisPeer, UUID taskId)
    {
        super(thisPeer, taskId, State.Started, State.Done);
    }

    protected void initiate()
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
            setState(State.Done);
        }
    }
    
    private void sendMessage(PeerRelatedActivityFactory activityFactory,
                             Object target)
    {
        Object msg = createMessage(Performative.Request, ATOM_INTEREST,
                                   getTaskId());
        combine(msg, struct(RECEIVED_TASK_ID, getTaskId()));

        PeerRelatedActivity activity = (PeerRelatedActivity) activityFactory.createActivity();
        activity.setTarget(target);
        activity.setMessage(msg);

        try
        {
            getPeerInterface().execute(activity);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }
    
    public void handleMessage(Object msg)
    {
        getPeerInterface().getPeerNetwork().setAtomInterests(getPart(msg,
                                                                     REPLY_TO),
                                                             (HGAtomPredicate) getPart(msg,
                                                                                       CONTENT));

        if (count.decrementAndGet() == 0)
        {
            setState(State.Done);
        }
    }

    public static class GetInterestsFactory implements TaskFactory
    {
        public TaskActivity<?> newTask(HyperGraphPeer peerInterface, UUID taskId, Object msg)
        {
            return new GetInterestsTask(peerInterface, taskId);
        }
    }
}