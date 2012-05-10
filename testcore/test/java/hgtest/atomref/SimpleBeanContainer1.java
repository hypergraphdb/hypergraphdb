package hgtest.atomref;

import hgtest.beans.SimpleBean;

import org.hypergraphdb.annotation.AtomReference;

public class SimpleBeanContainer1
{
	@AtomReference("symbolic")
	private SimpleBean x;

	public SimpleBean getX()
	{
		return x;
	}

	public void setX(SimpleBean x)
	{
		this.x = x;
	}	
}
