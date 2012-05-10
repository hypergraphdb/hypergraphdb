package hgtest.beans;

public class ComplexBean
{
	private String stableField;
	private String removedField;
	private SimpleBean stableNested;
	private SimpleBean removedNested;
	
	public String getStableField()
	{
		return stableField;
	}
	public void setStableField(String stableField)
	{
		this.stableField = stableField;
	}
	public String getRemovedField()
	{
		return removedField;
	}
	public void setRemovedField(String removedField)
	{
		this.removedField = removedField;
	}
	public SimpleBean getStableNested()
	{
		return stableNested;
	}
	public void setStableNested(SimpleBean stableNested)
	{
		this.stableNested = stableNested;
	}
	public SimpleBean getRemovedNested()
	{
		return removedNested;
	}
	public void setRemovedNested(SimpleBean removedNested)
	{
		this.removedNested = removedNested;
	}
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((removedField == null) ? 0 : removedField.hashCode());
        result = prime * result
                + ((removedNested == null) ? 0 : removedNested.hashCode());
        result = prime * result
                + ((stableField == null) ? 0 : stableField.hashCode());
        result = prime * result
                + ((stableNested == null) ? 0 : stableNested.hashCode());
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
        ComplexBean other = (ComplexBean) obj;
        if (removedField == null)
        {
            if (other.removedField != null)
                return false;
        }
        else if (!removedField.equals(other.removedField))
            return false;
        if (removedNested == null)
        {
            if (other.removedNested != null)
                return false;
        }
        else if (!removedNested.equals(other.removedNested))
            return false;
        if (stableField == null)
        {
            if (other.stableField != null)
                return false;
        }
        else if (!stableField.equals(other.stableField))
            return false;
        if (stableNested == null)
        {
            if (other.stableNested != null)
                return false;
        }
        else if (!stableNested.equals(other.stableNested))
            return false;
        return true;
    }
	
}