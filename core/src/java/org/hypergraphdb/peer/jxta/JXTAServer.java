/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.jxta;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import net.jxta.peergroup.PeerGroup;
import net.jxta.protocol.PipeAdvertisement;
import net.jxta.socket.JxtaServerSocket;

public class JXTAServer implements Runnable
{
    private volatile Thread thisThread;
	private JXTARequestHandler requestHandler;
    private JxtaServerSocket serverSocket = null;
    
	public JXTAServer(JXTARequestHandler requestHandler)
	{
		super();
		this.requestHandler = requestHandler;
	}

	public boolean initialize(PeerGroup peerGroup, PipeAdvertisement pipeAdv)
	{
        System.out.println("Starting ServerSocket");
        
        try 
        {
            serverSocket = new JxtaServerSocket(peerGroup, pipeAdv);
//            serverSocket.setSoTimeout(100);
     
            return true;
        } 
        catch (IOException e) 
        {
            System.out.println("failed to create a server socket");
            e.printStackTrace();
        }

        return false;
	}
	
    public void stop()
    {
        Thread t = thisThread;
        thisThread = null;
        if (t != null)
        {
            try { t.join(1000); } catch (InterruptedException e) { }
            if (t.isAlive())
                t.interrupt();
        }
    }
	
	public void run()
	{
	    thisThread = Thread.currentThread();
	    thisThread.setName("JXTA Server");
		try { if (serverSocket != null)
		{
			while (thisThread != null)
			{
	            try 
	            {
	                Socket socket = serverSocket.accept();
	                if (socket != null) 
	                {
	                    requestHandler.handleRequest(socket);
	                }
	            }
	            catch(SocketTimeoutException e)
	            {	            	
	            }
                catch (IOException e)
                {
                    if (thisThread != null) // thread interrupt will cause a SocketException
                        e.printStackTrace();
                }
                catch (Throwable t)
                {
                    t.printStackTrace(System.err);
                }
			}
			
			try
			{
				serverSocket.close();
			} 
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("JXTAServer stooped");
		} } 
		finally { thisThread = null; }
			
	}

	public JxtaServerSocket getServerSocket()
	{
		return serverSocket;
	}

	public void setServerSocket(JxtaServerSocket serverSocket)
	{
		this.serverSocket = serverSocket;
	}		

}
