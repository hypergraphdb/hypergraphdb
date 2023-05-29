package org.hypergraphdb.storage.lmdb.type.util;

import java.nio.charset.StandardCharsets;

public class ByteString
{
	private String string;
	private byte[] bytes;

	public ByteString(String string)
	{
		this.string = string;
	}

	public ByteString(byte[] bytes)
	{
		this.bytes = bytes;
	}

	public String getString()
	{
		if (string == null)
		{
			string = new String(bytes, StandardCharsets.UTF_8);
		}
		return string;
	}

	public byte[] getBytes()
	{
		if (bytes == null)
		{
			bytes = string.getBytes(StandardCharsets.UTF_8);
		}
		return bytes;
	}

	public int length()
	{
		return getString().length();
	}

	public int size()
	{
		return getBytes().length;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		ByteString that = (ByteString) o;

		if (getString() != null ? !getString().equals(that.getString())
				: that.getString() != null)
			return false;

		return true;
	}

	@Override
	public int hashCode()
	{
		return getString() != null ? getString().hashCode() : 0;
	}
}
