/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.jxta;

import java.io.InputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.MessageHandler;
import org.hypergraphdb.peer.protocol.Protocol;

class ConnectionHandler implements Runnable
{
	private Socket socket;
	private ExecutorService executorService;
	private MessageHandler messageHandler;
	
	public ConnectionHandler(Socket socket, 
	                         MessageHandler messageHandler,
	                         ExecutorService executorService)
	{
		this.socket = socket;
		this.executorService = executorService;
		this.messageHandler = messageHandler;
	}

	@SuppressWarnings("unchecked")
	private void handleRequest(Socket socket, ExecutorService executorService) 
	{
	    InputStream in = null;
        try 
        {
        	in = socket.getInputStream();
        	try
        	{
        		final Message msg = new Message((Map<String, Object>)new Protocol().readMessage(in));            		
                executorService.execute(new Runnable()
                {
                    public void run() 
                    { 
                        try { messageHandler.handleMessage(msg); }
                        catch (Throwable t) { t.printStackTrace(System.err); } 
                    }
                }
                );            		
        	}
        	catch(Exception ex)
            {
        		// TODO: where are those messages reported? Do we simply send a 
        		// NotUnderstand response?
            	ex.printStackTrace();
            	return;
            }
        } 
        catch (Exception ie) 
        {
            ie.printStackTrace(System.err);
        }
        finally
        {
            if (in != null) try { in.close(); } catch (Throwable t) { t.printStackTrace(System.err); }
            try { socket.close(); } catch (Throwable t) { t.printStackTrace(System.err); }                                
        }
    }

	public void run() 
	{
		handleRequest(socket, executorService);
	}
}