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

    public int compareTo(TestInt o)
    {
       return x.compareTo(o.x);
    }
}