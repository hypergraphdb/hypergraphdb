/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

import java.util.ArrayList;
import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.query.HGQueryCondition;

/**
 * @author Cipri Costa
 * 
 * Offers an interface to a remote peer. The peers are identified by name.
 * 
 * Subclasses will implement the actual communication with the remote peer
 * (get/add/remove/replace + query).
 * 
 */
public abstract class RemotePeer
{
    private String name;
    private HyperGraphPeer localPeer;

    private List<Object> operationsBatch;
    private boolean useBatch;

    public RemotePeer()
    {
        useBatch = false;
    }

    public RemotePeer(String name)
    {
        this.name = name;
        useBatch = false;
    }

    /**
     * Executes a query on the remote peer
     * 
     * @param condition
     *            a HGQueryCondition to be executed on the remote peer.
     * @param getObjects
     *            if true the actual objects are returned, otherwise the client
     *            will just get a set of handles.
     * @return The result of the remote query (depending on the getObjects
     *         parameter the list will contain the objects of just the handles)
     */
    public abstract ArrayList<?> query(HGQueryCondition condition,
                                       boolean getObjects);

    /**
     * 
     * @param handle
     *            The handle of the atom to be retrieved
     * @return The atom with the given handle from the remote peer. If no such
     *         handle exists on the remote peer, the function will return null
     */
    public abstract Object get(HGHandle handle);

    /**
     * Adds the atom on the remote peer. The operation is implemented using the
     * replication mechanism, so, even if it fails, it is registered in the logs
     * and will be sent to the target when the target will start a catch-up
     * phase.
     * 
     * @param atom
     *            The atom to be added
     * @return
     */
    public abstract HGHandle add(Object atom);

    /**
     * Similar to add but with a given handle
     * 
     * @param handle
     * @param atom
     */
    public abstract void define(HGPersistentHandle handle, Object atom);

    /**
     * Copies the atom from a given handle from the local peer to the remote
     * peer.
     * 
     * @param handle
     */
    public abstract void copyTo(HGHandle handle);

    /**
     * Copies the atom with a given handle from the remote peer to the local
     * peer.
     * 
     * @param handle
     */
    public abstract void copyFrom(HGPersistentHandle handle);

    /**
     * Removes the handle from the remote peer. The operation is implemented
     * using the replication mechanism, so, even if it fails, it is registered
     * in the logs and will be sent to the target when the target will start a
     * catch-up phase.
     * 
     * @param handle
     *            the handle to remove from the remote peer.
     * @return
     */
    public abstract HGHandle remove(HGPersistentHandle handle);

    /**
     * Replaces the atom with the given handle on the remote peer. The operation
     * is implemented using the replication mechanism, so, even if it fails, it
     * is registered in the logs and will be sent to the target when the target
     * will start a catch-up phase.
     * 
     * @param handle
     *            the handle of the atom to be replaced
     * @param atom
     *            the new atom
     */
    public abstract void replace(HGPersistentHandle handle, Object atom);

    /**
     * Starts a batch. After this call and until the endBatch call, all
     * functions that send data to the remote peer will be queued. In order to
     * send the messages use flushBatch or endBatch
     * 
     */
    public void startBatch()
    {
        operationsBatch = new ArrayList<Object>();
        useBatch = true;
    }

    /**
     * After this call, the operations will be imediatelly sent to the remote
     * peer. Also calls the flush method if there are any outstanding calls to
     * be sent.
     * 
     * @return
     */
    public List<?> endBatch()
    {
        List<?> result = null;
        if (operationsBatch.size() > 0)
        {
            result = doFlush();
        }

        useBatch = false;
        operationsBatch = null;

        return result;
    }

    /**
     * Sends the current list of queued operations to the remote peer.
     * 
     * @return
     */
    public List<?> flushBatch()
    {
        List<?> result = null;

        if (operationsBatch.size() > 0)
        {
            result = doFlush();
            operationsBatch.clear();
            return result;
        }

        return result;

    }

    protected abstract List<?> doFlush();

    public boolean insideBatch()
    {
        return useBatch;
    }

    protected void addToBatch(Object operation)
    {
        operationsBatch.add(operation);
    }

    protected List<Object> getBatch()
    {
        return operationsBatch;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public HyperGraphPeer getLocalPeer()
    {
        return localPeer;
    }

    public void setLocalPeer(HyperGraphPeer localPeer)
    {
        this.localPeer = localPeer;
    }

    public String toString()
    {
        String result = "RemotePeer(name=" + getName() + ")";
        return result;
    }
}
