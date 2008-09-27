package hgtest.jxta;

import java.io.File;

import org.hypergraphdb.peer.HyperGraphPeer;

public class RdvRelay
{
	public static void main(String[] args)
	{

		HyperGraphPeer server = new HyperGraphPeer(new File("./config/rdvRelayConfig"));
		
		
		if (server.start("user", "pwd"))
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
}
