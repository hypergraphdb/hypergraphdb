package hgtest.beans;

import org.hypergraphdb.annotation.HGIgnore;

public class BeanIgnoreFieldVariants
{
    private String storedField = "default";
    private String storedNameMismatch = "differentFieldName";
    @HGIgnore
    private boolean ignoreMe = true;
    private int ignoreMeToo = 10;
    
    public String getStoredField()
    {
        return storedField;
    }
    public void setStoredField(String storedField)
    {
        this.storedField = storedField;
    }
    public String getStoredMismatch()
    {
        return storedNameMismatch;
    }
    public void setStoredMismatch(String storedNameMismatch)
    {
        this.storedNameMismatch = storedNameMismatch;
    }
    public boolean isIgnoreMe()
    {
        return ignoreMe;
    }
    public void setIgnoreMe(boolean ignoreMe)
    {
        this.ignoreMe = ignoreMe;
    }
    @HGIgnore
    public int getIgnoreMeToo()
    {
        return ignoreMeToo;
    }
    public void setIgnoreMeToo(int ignoreMeToo)
    {
        this.ignoreMeToo = ignoreMeToo;
    }
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (ignoreMe ? 1231 : 1237);
        result = prime * result + ignoreMeToo;
        result = prime * result
                + ((storedField == null) ? 0 : storedField.hashCode());
        result = prime
                * result
                + ((storedNameMismatch == null) ? 0 : storedNameMismatch
                        .hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BeanIgnoreFieldVariants other = (BeanIgnoreFieldVariants) obj;
        if (ignoreMe != other.ignoreMe)
            return false;
        if (ignoreMeToo != other.ignoreMeToo)
            return false;
        if (storedField == null)
        {
            if (other.storedField != null)
                return false;
        }
        else if (!storedField.equals(other.storedField))
            return false;
        if (storedNameMismatch == null)
        {
            if (other.storedNameMismatch != null)
                return false;
        }
        else if (!storedNameMismatch.equals(other.storedNameMismatch))
            return false;
        return true;
    }

    
}
