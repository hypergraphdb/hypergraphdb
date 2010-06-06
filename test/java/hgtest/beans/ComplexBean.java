package hgtest.beans;

import hgtest.SimpleBean;

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
}