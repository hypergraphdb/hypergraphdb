package org.hypergraphdb.peer.jxta;

import java.util.ArrayList;
import java.util.List;

import net.jxta.document.Advertisement;
import net.jxta.protocol.PipeAdvertisement;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.peer.RemotePeer;
import org.hypergraphdb.peer.StorageService;
import org.hypergraphdb.peer.StorageService.Operation;
import org.hypergraphdb.peer.workflow.QueryTaskClient;
import org.hypergraphdb.peer.workflow.replication.RememberTaskClient;
import org.hypergraphdb.query.HGQueryCondition;

/**
 * @author Cipri Costa Remote peer implementation based on JXTA.
 */
public class JXTARemotePeer extends RemotePeer
{
    /**
     * The advertisement of the remote peer.
     */
    private Advertisement adv;

    public JXTARemotePeer(Advertisement adv)
    {
        this.adv = adv;
        setName(((PipeAdvertisement) adv).getName());
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.hypergraphdb.peer.RemotePeer#query(org.hypergraphdb.query.HGQueryCondition,
     *      boolean)
     */
    @Override
    public ArrayList<?> query(HGQueryCondition condition, boolean getObjects)
    {
        ArrayList<Object> targets = new ArrayList<Object>();
        targets.add(adv);

        QueryTaskClient queryTask = new QueryTaskClient(getLocalPeer(),
                                                        getLocalPeer().getTempDb(),
                                                        targets.iterator(),
                                                        condition, getObjects);
        
        getLocalPeer().getActivityManager().initiateActivity(queryTask);        
        return queryTask.getResult();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.hypergraphdb.peer.RemotePeer#get(org.hypergraphdb.HGHandle)
     */
    @Override
    public Object get(HGHandle handle)
    {
        ArrayList<Object> targets = new ArrayList<Object>();
        targets.add(adv);

        QueryTaskClient queryTask = new QueryTaskClient(getLocalPeer(),
                                                        getLocalPeer().getTempDb(),
                                                        targets.iterator(),
                                                        handle);
        getLocalPeer().getActivityManager().initiateActivity(queryTask);

        ArrayList<?> result = queryTask.getResult();

        if (result.size() > 0)
            return result.get(0);
        else
            return null;
    }

    @Override
    public void copyFrom(HGPersistentHandle handle)
    {
        Object atom = get(handle);

        getLocalPeer().getGraph().define(handle, atom);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.hypergraphdb.peer.RemotePeer#add(java.lang.Object)
     */
    @Override
    public HGHandle add(Object atom)
    {
        if (insideBatch())
        {
            addToBatch(new RememberTaskClient.RememberEntity(null,
                                                             atom,
                                                             StorageService.Operation.Create));
            return null;
        }
        else
        {
            RememberTaskClient activity = new RememberTaskClient(getLocalPeer(),
                                                                 atom,
                                                                 getLocalPeer().getLog(),
                                                                 null,
                                                                 adv,
                                                                 StorageService.Operation.Create);
            activity.run();
            return activity.getResult();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.hypergraphdb.peer.RemotePeer#define(org.hypergraphdb.HGPersistentHandle,
     *      java.lang.Object)
     */
    @Override
    public void define(HGPersistentHandle handle, Object atom)
    {
        if (insideBatch())
        {
            addToBatch(new RememberTaskClient.RememberEntity(
                                                             handle,
                                                             atom,
                                                             StorageService.Operation.Create));
        }
        else
        {
            RememberTaskClient activity = new RememberTaskClient(getLocalPeer(),
                                                                 atom,
                                                                 getLocalPeer().getLog(),
                                                                 handle,
                                                                 adv,
                                                                 StorageService.Operation.Create);
            activity.run();
        }
    }

    @Override
    public void copyTo(HGHandle handle)
    {
        Object atom = getLocalPeer().getGraph().get(handle);
        HGPersistentHandle persHandle = getLocalPeer().getGraph().getPersistentHandle(handle);

        if (insideBatch())
        {
            addToBatch(new RememberTaskClient.RememberEntity(persHandle,
                                                             atom,
                                                             StorageService.Operation.Copy));
        }
        else
        {
            RememberTaskClient activity = new RememberTaskClient(getLocalPeer(),
                                                                 atom,
                                                                 getLocalPeer().getLog(),
                                                                 persHandle,
                                                                 adv,
                                                                 StorageService.Operation.Copy);
            activity.run();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.hypergraphdb.peer.RemotePeer#remove(org.hypergraphdb.HGPersistentHandle)
     */
    @Override
    public HGHandle remove(HGPersistentHandle handle)
    {
        if (insideBatch())
        {
            addToBatch(new RememberTaskClient.RememberEntity(
                                                             handle,
                                                             null,
                                                             StorageService.Operation.Remove));
            return handle;
        }
        else
        {
            RememberTaskClient activity = new RememberTaskClient(getLocalPeer(),
                                                                 null,
                                                                 getLocalPeer().getLog(),
                                                                 handle,
                                                                 adv,
                                                                 Operation.Remove);
            activity.run();
            return activity.getResult();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.hypergraphdb.peer.RemotePeer#replace(org.hypergraphdb.HGPersistentHandle,
     *      java.lang.Object)
     */
    @Override
    public void replace(HGPersistentHandle handle, Object atom)
    {
        if (insideBatch())
        {
            addToBatch(new RememberTaskClient.RememberEntity(handle,
                                                             atom,
                                                             StorageService.Operation.Update));
        }
        else
        {
            RememberTaskClient activity = new RememberTaskClient(getLocalPeer(),
                                                                 atom,
                                                                 getLocalPeer().getLog(),
                                                                 handle,
                                                                 adv,
                                                                 Operation.Update);
            activity.run();
        }
    }

    @Override
    protected List<?> doFlush()
    {
        RememberTaskClient activity = new RememberTaskClient(getLocalPeer(),
                                                             getLocalPeer().getLog(),
                                                             adv, getBatch());
        activity.run();

        return activity.getResults();
    }
}
