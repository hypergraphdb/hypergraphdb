package org.hypergraphdb.peer.serializer;

/**
 * 
 * <p>
 * CustomSerializedValues are ultimately written by the {@link GenericSerializer} 
 * class which is based on the idea of writing an arbitrary object as a HGDB
 * atom and transferring it with this "generic" representation. This has the obvious
 * problems of being potentially inefficient. It doesn't pollute the underlying HGDB
 * of the peer as long as a temp HGDB is used. 
 * </p>
 *
 */
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