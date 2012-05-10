package hgtest.storage;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;

public class TestLink extends HGPlainLink
{
    public TestLink()
    {
        
    }
    
    public TestLink(HGHandle h)
    {
        super(h, h, h);
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
