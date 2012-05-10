package hgtest.beans;

public class DerivedBean extends SimpleBean
{
    private String derivedProperty = "";

    public String getDerivedProperty()
    {
        return derivedProperty;
    }

    public void setDerivedProperty(String derivedProperty)
    {
        this.derivedProperty = derivedProperty;
    }    
}