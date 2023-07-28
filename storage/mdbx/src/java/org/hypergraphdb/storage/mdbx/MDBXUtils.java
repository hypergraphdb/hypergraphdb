package org.hypergraphdb.storage.mdbx;

public class MDBXUtils
{
	public static void checkArgNotNull(Object value, String name)
	{
		if (value == null)
		{
			throw new IllegalArgumentException(
					"The " + name + " argument cannot be null");
		}
	}
}
