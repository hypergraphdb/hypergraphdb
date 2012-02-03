package hgtest.beans;

public class BeanWithTransient
{
    transient String tmpString;

    public String getTmpString()
    {
        return tmpString;
    }

    public void setTmpString(String tmpString)
    {
        this.tmpString = tmpString;
    }    
}
