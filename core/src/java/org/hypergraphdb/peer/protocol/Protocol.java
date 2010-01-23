/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.hypergraphdb.peer.Message;

/**
 * @author Cipri Costa Simple class for now that serializes messages. Will
 *         become the point where the format of the serialization is decided.
 */
public class Protocol
{
	private final static byte[] SIGNATURE = "HGBD".getBytes();

	// private static MessageFactory messageFactory = new MessageFactory();

	public Protocol()
	{
	}

	/**
	 * @param in
	 * @param session
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public Message readMessage(InputStream in) throws IOException
	{
		Object content = null;

		// get & verify signature
		if (ProtocolUtils.verifySignature(in, SIGNATURE))
		{
			ObjectSerializer serializer = new ObjectSerializer();
			content = serializer.deserialize(in);
		} 
		else
			throw new RuntimeException("ERROR: Signature does not match");

		if (content instanceof Map)
			return new Message((Map<String, Object>) content);
		else
			throw new RuntimeException(
					"Message content is null or not a map (i.e. a record-like object).");
	}

	public void writeMessage(OutputStream out, Message msg) throws IOException
	{
		writeSignature(out);

		// TODO serialization type should be configurable
		ObjectSerializer serializer = new ObjectSerializer();
		serializer.serialize(out, msg);

		// TODO no longer needed
		// session.setSerializer(serializer);
	}

	private void writeSignature(OutputStream out) throws IOException
	{
		out.write(SIGNATURE);

	}
}
