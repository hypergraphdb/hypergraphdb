package hgtest.storage;

public class TestInt implements Comparable<TestInt>
{
    private Integer x = 0;
    
    public TestInt()
    {
        
    }
    
    public TestInt(Integer x)
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

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((x == null) ? 0 : x.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        TestInt other = (TestInt) obj;
        if (x == null)
        {
            if (other.x != null) return false;
        }
        else if (!x.equals(other.x)) return false;
        return true;
    }

    public int compareTo(TestInt o)
    {
       return x.compareTo(o.x);
    }

    @Override
    public String toString()
    {
        return "TI: " + x;
    }
    
    
}