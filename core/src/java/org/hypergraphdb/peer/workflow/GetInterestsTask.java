package org.hypergraphdb.peer.workflow;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.protocol.Performative;
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

    private Object msg = null;

    // TODO: temporary
    private AtomicInteger count = new AtomicInteger();

    public GetInterestsTask(PeerInterface peerInterface)
    {
        super(peerInterface, State.Started, State.Done);
    }

    public GetInterestsTask(PeerInterface peerInterface, Object msg)
    {
        super(peerInterface, (UUID) getPart(msg, SEND_TASK_ID), State.Started,
              State.Done);

        this.msg = msg;
    }

    protected void startTask()
    {
        if (msg != null)
        {
            // task was created because someone else is publishing
            getPeerInterface().getPeerNetwork().setAtomInterests(getPart(msg,
                                                                         REPLY_TO),
                                                                 (HGAtomPredicate) getPart(msg,
                                                                                           CONTENT));
            setState(State.Done);
        }
        else
        {
            // task is intended for retrieving information from other peers
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
    };

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

    /*
     * (non-Javadoc)
     * 
     * @see org.hypergraphdb.peer.workflow.TaskActivity#handleMessage(org.hypergraphdb.peer.protocol.Message)
     * 
     * TODO: for now just overriding it, need to change when more complex
     * conversations are implemented.
     */
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
        public GetInterestsFactory()
        {
        }

        public TaskActivity<?> newTask(PeerInterface peerInterface, Object msg)
        {
            return new GetInterestsTask(peerInterface, msg);
        }

    }

}
