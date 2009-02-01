package org.hypergraphdb.peer.jxta;

import java.net.Socket;

public interface JXTARequestHandler
{
	public void handleRequest(Socket socket);
}
