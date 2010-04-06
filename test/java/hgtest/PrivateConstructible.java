package hgtest;

public class PrivateConstructible
{
    private String name = "Private";
    
    private PrivateConstructible()
    {        
    }

    public PrivateConstructible(String name)
    {
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
}
