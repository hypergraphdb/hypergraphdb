/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.util.HGUtils;

/**
 * 
 * <p>
 * This is a simple data structure that represents a HyperGraphDB peer identity. The
 * class is intended for use only by the HGDB API which will attempt to ensure uniqueness.
 * To store info about other peers, use the derived <code>HGPeerIdentity</code> class which
 * is only different from this one by its type. 
 * </p>
 * 
 * @author Borislav Iordanov
 * 
 */
public class PrivatePeerIdentity
{
    private HGPersistentHandle id;
    private String hostname;
    private String ipAddress;
    private String graphLocation;

    public PrivatePeerIdentity()
    {
        
    }
    
    public HGPeerIdentity makePublicIdentity()
    {
        HGPeerIdentity pid = new HGPeerIdentity();
        pid.setId(id);
        pid.setHostname(hostname);
        pid.setIpAddress(ipAddress);
        pid.setGraphLocation(graphLocation);
        return pid;
    }
    
    public HGPersistentHandle getId()
    {
        return id;
    }

    public void setId(HGPersistentHandle id)
    {
        this.id = id;
    }

    public String getHostname()
    {
        return hostname;
    }

    public void setHostname(String hostname)
    {
        this.hostname = hostname;
    }

    public String getIpAddress()
    {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress)
    {
        this.ipAddress = ipAddress;
    }

    public String getGraphLocation()
    {
        return graphLocation;
    }

    public void setGraphLocation(String graphLocation)
    {
        this.graphLocation = graphLocation;
    }
    
    public int hashCode()
    {
        return id == null ? 0 : id.hashCode();
    }
    
    public boolean equals(Object x)
    {
        if (! (x instanceof PrivatePeerIdentity))
            return false;
        else
            return HGUtils.eq(id, ((PrivatePeerIdentity)x).id);
    }
    
    public String toString()
    {
        return "HGPeerIdentity[" + id + "," + hostname + "," + 
            ipAddress + "," + graphLocation + /*"," + name + */ "]"; 
    }
}
