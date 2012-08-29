/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDHandleFactory;

public class SerializationUtils
{
	public static Integer deserializeInt(InputStream in)
	{
		try
		{
			int ch1 = in.read();
			int ch2 = in.read();
			int ch3 = in.read();
			int ch4 = in.read();
			int i = ((ch1 & 0xFF) << 24) | ((ch2 & 0xFF) << 16)
					| ((ch3 & 0xFF) << 8) | (ch4 & 0xFF);

			return new Integer(i);
		}
		catch (IOException ex)
		{
			ex.printStackTrace();
		}
		return null;
	}

	public static void serializeInt(OutputStream out, Integer data)
	{
		// assume not null
		try
		{
			int v = ((Integer) data).intValue();
			out.write((byte) ((v >>> 24) & 0xFF));
			out.write((byte) ((v >>> 16) & 0xFF));
			out.write((byte) ((v >>> 8) & 0xFF));
			out.write((byte) ((v >>> 0) & 0xFF));
		}
		catch (IOException ex)
		{
			ex.printStackTrace();
		}
	}

	public static void serializeString(OutputStream out, String data)
	{
		byte[] byteData = data.getBytes();

		serializeInt(out, byteData.length);
		try
		{
			out.write(byteData);
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String deserializeString(InputStream in)
	{
		byte[] byteData = new byte[deserializeInt(in)];
		try
		{
			in.read(byteData);
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return new String(byteData);
	}

	public static void serializeUUID(OutputStream out, UUID id)
	{
		long msb = id.getMostSignificantBits();
		long lsb = id.getLeastSignificantBits();
		byte[] data = new byte[16];
		data[0] = (byte) ((msb >>> 56));
		data[1] = (byte) ((msb >>> 48));
		data[2] = (byte) ((msb >>> 40));
		data[3] = (byte) ((msb >>> 32));
		data[4] = (byte) ((msb >>> 24));
		data[5] = (byte) ((msb >>> 16));
		data[6] = (byte) ((msb >>> 8));
		data[7] = (byte) ((msb >>> 0));
		data[8] = (byte) ((lsb >>> 56));
		data[9] = (byte) ((lsb >>> 48));
		data[10] = (byte) ((lsb >>> 40));
		data[11] = (byte) ((lsb >>> 32));
		data[12] = (byte) ((lsb >>> 24));
		data[13] = (byte) ((lsb >>> 16));
		data[14] = (byte) ((lsb >>> 8));
		data[15] = (byte) ((lsb >>> 0));

		try
		{
			out.write(data);
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static UUID deserializeUUID(InputStream in)
	{
		byte[] data = new byte[16];

		try
		{
			in.read(data);
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		long msb = 0;
		long lsb = 0;

		for (int i = 0; i < 8; i++)
			msb = (msb << 8) | (data[i] & 0xff);
		for (int i = 8; i < 16; i++)
			lsb = (lsb << 8) | (data[i] & 0xff);
		return new UUID(msb, lsb);

	}

	public static void serializePersistentHandle(OutputStream out,
			HGPersistentHandle h)
	{
		try
		{
			out.write(h.toByteArray());
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public static HGPersistentHandle deserializePersistentHandle(InputStream in)
	{
		//  TODO: we need to get the actual handle factory here, but it's annoying
		//  to have to pass the parameter everywhere.
		HGHandleFactory handleFactory = new UUIDHandleFactory();
		byte[] data = new byte[handleFactory.anyHandle().toByteArray().length];
		try
		{
			for (int i = 0; i < data.length;)
				i += in.read(data, i, data.length - i);
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
		return handleFactory.makeHandle(data);
	}
}