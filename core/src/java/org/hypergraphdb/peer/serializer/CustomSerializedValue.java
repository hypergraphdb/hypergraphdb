package org.hypergraphdb.peer.serializer;

public class CustomSerializedValue
{
	private int pos;
	private Object value;
	
	public CustomSerializedValue()
	{
	
	}
	public CustomSerializedValue(Object value)
	{
		this.value = value;
	}

	public int getPos()
	{
		return pos;
	}

	public void setPos(int pos)
	{
		this.pos = pos;
	}

	public Object get()
	{
		return value;
	}

	public void setValue(Object value)
	{
		this.value = value;
	}
	
	
}
