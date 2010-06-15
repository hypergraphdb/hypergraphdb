package hgtest.storage;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGPlainLink;

public class TestLink extends HGPlainLink
{
    public static final HGPersistentHandle HANDLE = HGHandleFactory.makeHandle("bb9bdb36-cdcf-11dc-bd27-e1853813fbe2");

    public TestLink()
    {
        
    }
    
    public TestLink(HGHandle h)
    {
        super(h, HANDLE, HANDLE);
    }
    
    
    public TestLink(HGHandle... outgoingSet)
    {
        super(outgoingSet);
    }


    public static class Int implements Comparable<Int>
    {
        private Integer x = 0;
        
        public Int()
        {
            
        }
        
        public Int(Integer x)
        {
            this.x = x;
        }

        public Integer getX()
        {
            return x;
        }

        public void setX(Integer x)
        {
            this.x = x;
        }

        public int compareTo(Int o)
        {
           return x.compareTo(o.x);
        }
    }
 
}
