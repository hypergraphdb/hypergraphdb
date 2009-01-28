package org.hypergraphdb.peer;

import org.hypergraphdb.HGPersistentHandle;

/**
 * 
 * <p>
 * This is a simple data structure that represents a HyperGraphDB peer identity. The
 * class is package private and intended for use only by the HGDB API which will
 * attempt to ensure uniqueness 
 * </p>
 * 
 * @author Borislav Iordanov
 * 
 */
class PrivatePeerIdentity
{
    private HGPersistentHandle id;
    private String hostname;
    private String ipAddress;
    private String graphLocation;
    private String name;

    public HGPeerIdentity makePublicIdentity()
    {
        HGPeerIdentity pid = new HGPeerIdentity();
        pid.setId(id);
        pid.setHostname(hostname);
        pid.setIpAddress(ipAddress);
        pid.setGraphLocation(graphLocation);
        pid.setName(name);
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

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
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
}