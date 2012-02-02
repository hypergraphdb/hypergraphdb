package hgtest.beans;

import org.hypergraphdb.util.HGUtils;

public class PrivateConstructible
{
    private String name = "Private";
    
    private PrivateConstructible()
    { 
        name = "I got privately constructed";
    }
    
    public PrivateConstructible(String name)
    {
        this();
        this.name = name;
    }
    
    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }
    
    public int hashCode()
    {
        return HGUtils.hashIt(name);
    }
    
    public boolean equals(Object x)
    {
        if (x == null || !this.getClass().equals(x.getClass()))
            return false;
        else
            return HGUtils.eq(name, ((PrivateConstructible)x).name);
    }
}
