package org.hypergraphdb.peer.jxta;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import net.jxta.peergroup.PeerGroup;
import net.jxta.protocol.PipeAdvertisement;
import net.jxta.socket.JxtaServerSocket;

public class JXTAServer implements Runnable
{
	private JXTARequestHandler requestHandler;
    private JxtaServerSocket serverSocket = null;
    private volatile boolean isRunning = false;
    
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
            serverSocket.setSoTimeout(100);
     
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
		isRunning = false;
	}
	
	public void run()
	{
		if (serverSocket != null)
		{
			isRunning = true;
		
			while (isRunning)
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
	            catch (Exception e) 
	            {
	                e.printStackTrace();
	            }
			}
			
			try
			{
				serverSocket.close();
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("JXTAServer stooped");
		}
			
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
