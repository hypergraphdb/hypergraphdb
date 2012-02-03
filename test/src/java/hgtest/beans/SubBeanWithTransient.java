package hgtest.beans;

public class SubBeanWithTransient extends BeanWithTransient
{
    private int value;

    public int getValue()
    {
        return value;
    }

    public void setValue(int value)
    {
        this.value = value;
    }    
}