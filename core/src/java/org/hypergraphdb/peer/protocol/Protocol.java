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
    public Object readMessage(InputStream in) throws IOException
    {
        Object result = null;

        // get & verify signature
        if (ProtocolUtils.verifySignature(in, SIGNATURE))
        {
            ObjectSerializer serializer = new ObjectSerializer();
            result = serializer.deserialize(in);
        }
        else
        {
            System.out.println("ERROR: Signature does not match");
        }

        // TODO for now just returning the last response
        return result;
    }

    public Object handleResponse(InputStream in) throws IOException
    {
        Object result = null;
        if (ProtocolUtils.verifySignature(in, SIGNATURE))
            result = new ObjectSerializer().deserialize(in);
        return result;
    }

    // TODO can send multiple messages?
    public void writeMessage(OutputStream out, Object msg) throws IOException
    {
        writeSignature(out);

        // TODO serialization type should be configurable
        ObjectSerializer serializer = new ObjectSerializer();
        serializer.serialize(out, msg);

        // TODO no longer needed
        // session.setSerializer(serializer);
    }

    public void createResponse(OutputStream out, Object response)
            throws IOException
    {
        writeSignature(out);

        new ObjectSerializer().serialize(out, response);
        // write response

    }

    private void writeSignature(OutputStream out) throws IOException
    {
        out.write(SIGNATURE);

    }

}
