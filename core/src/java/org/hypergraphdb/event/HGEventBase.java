package org.hypergraphdb.event;

public class HGEventBase implements HGEvent
{
    private Object source;
    
    public HGEventBase()
    {
        
    }
    
    public HGEventBase(Object source)
    {
        this.source = source;
    }
    
    public Object getSource()
    {
        return source;
    }
}