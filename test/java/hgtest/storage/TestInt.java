package hgtest.storage;

public class TestInt implements Comparable
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

    public int compareTo(Object o)
    {
       return x.compareTo(((TestInt)o).x);
    }

}
