package hgtest.jxta;

import java.io.File;
import java.util.concurrent.ExecutionException;

import org.hypergraphdb.peer.HyperGraphPeer;

public class RdvRelay
{
	public static void main(String[] args)
	{

		HyperGraphPeer server = new HyperGraphPeer(new File("./config/rdvRelayConfig"));
		
		
		try
        {
            if (server.start("user", "pwd").get())
            {
            	System.out.println("Server started...");
            	
            	while(true)
            	{
            		try
            		{
            			Thread.sleep(10000);
            		} catch (InterruptedException e)
            		{
            			// TODO Auto-generated catch block
            			e.printStackTrace();
            		}
            	}
            }
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (ExecutionException e)
        {
            e.printStackTrace();
        }
	}
}
