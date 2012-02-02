package hgtest.beans;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.util.HGUtils;

/**
 * A link with a couple of bean properties. 
 */
public class BeanLink1 implements HGLink
{
    HGHandle [] targets = null;
    private int x;
    private String [] A;
    
    protected BeanLink1(HGHandle...args)
    {
        if (args == null)
        {
            targets = new HGHandle[0];
        }
        else
        {
            targets = new HGHandle[args.length];
            System.arraycopy(args, 0, targets, 0, args.length);
        }
    }
    
    public BeanLink1(int x, String [] A, HGHandle...args)
    {
        this(args);
    }
    
    public int hashCode() { return x; }
    public boolean equals(Object other) 
    { 
        if (other == null) return false;
        else if (!this.getClass().equals(other.getClass()))
            return false;
        BeanLink1 l = (BeanLink1)other;
        return HGUtils.eq(x, l.x) && HGUtils.eq(targets, l.targets) && HGUtils.eq(A, l.A);
    }
    
    public int getArity()
    {
        return targets.length;
    }

    
    public HGHandle getTargetAt(int i)
    {
        return targets[i];
    }

    
    public void notifyTargetHandleUpdate(int i, HGHandle handle)
    {
        targets[i] = handle;
    }

    
    public void notifyTargetRemoved(int i)
    {
        throw new UnsupportedOperationException();
    }


    public int getX()
    {
        return x;
    }


    public void setX(int x)
    {
        this.x = x;
    }


    public String[] getA()
    {
        return A;
    }


    public void setA(String[] a)
    {
        A = a;
    }    
}