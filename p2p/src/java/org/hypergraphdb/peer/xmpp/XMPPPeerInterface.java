/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.xmpp;


import java.io.ByteArrayInputStream;

import java.io.InputStreamReader;
import java.security.Principal;
import java.util.ArrayList;
import java.util.concurrent.Future;

import mjson.Json;

import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.MessageHandler;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.NetworkPeerPresenceListener;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.util.CompletedFuture;
import org.jivesoftware.smack.*;
import org.jivesoftware.smack.filter.PacketFilter;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Packet;
import org.jivesoftware.smack.packet.Presence;
import org.jivesoftware.smackx.filetransfer.FileTransferListener;
import org.jivesoftware.smackx.filetransfer.FileTransferManager;
import org.jivesoftware.smackx.filetransfer.FileTransferRequest;
import org.jivesoftware.smackx.filetransfer.IncomingFileTransfer;
import org.jivesoftware.smackx.filetransfer.OutgoingFileTransfer;
import org.jivesoftware.smackx.muc.DefaultParticipantStatusListener;
import org.jivesoftware.smackx.muc.MultiUserChat;

/**
 * <p>
 * A peer interface implementation based upon the Smack library 
 * (see http://www.igniterealtime.org for more info). 
 * </p>
 * 
 * <p>
 * The connection is configured as a regular chat connection with
 * a server name, port, username and a password. Then peers are either
 * simply all users in this user's roster or all member of a chat room
 * or the union of both.  
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class XMPPPeerInterface implements PeerInterface
{
    // Configuration options.
    private String serverName;
    private Number port;
    private String user;
    private String password;
    private String roomId;
    private boolean ignoreRoster = false;
    private boolean anonymous;
    private boolean autoRegister;
    private int fileTransferThreshold;
    private HyperGraphPeer thisPeer;
    private ArrayList<NetworkPeerPresenceListener> presenceListeners = 
        new ArrayList<NetworkPeerPresenceListener>();
    private MessageHandler messageHandler;
     
    ConnectionConfiguration config = null;
    XMPPConnection connection;
    MultiUserChat room = null;
    FileTransferManager fileTransfer;
    
    public void configure(Json configuration)
    {
        serverName = configuration.at("serverUrl").asString();
        port = configuration.at("port", 5222).asInteger();
        user = configuration.at("user").asString();
        password = configuration.at("password").asString();
        roomId = configuration.has("room") ? configuration.at("room").asString() : null;
        ignoreRoster = configuration.at("ignoreRoster", false).asBoolean();
        autoRegister = configuration.at("autoRegister", false).asBoolean();
        anonymous = configuration.at("anonymous", false).asBoolean();
        fileTransferThreshold = configuration.at("fileTransferThreshold", 100*1024).asInteger();
        config = new ConnectionConfiguration(serverName, port.intValue());
        config.setRosterLoadedAtLogin(true);
        config.setReconnectionAllowed(true);
        SmackConfiguration.setPacketReplyTimeout(30000);
    }    
        
    private void reconnect()
    {
    	if (connection != null && connection.isConnected())
    		stop();
    	start();
    }

    private void processPeerJoin(String name)
    {
//    	System.out.println("peer joined: " + name);
        for (NetworkPeerPresenceListener listener : presenceListeners)
            listener.peerJoined(name);    	
    }
    
    private void processPeerLeft(String name)
    {
//    	System.out.println("peer left: " + name);
        for (NetworkPeerPresenceListener listener : presenceListeners)
            listener.peerLeft(name);    	
    }
    
    private void processMessage(Message msg)
    {
        //
        // Encapsulate message deserialization into a transaction because the HGDB might
        // be accessed during this process.
        // 
        Json M = null;
        if (thisPeer != null)
        	thisPeer.getGraph().getTransactionManager().beginTransaction();
        try
        {
//            ByteArrayInputStream in = new ByteArrayInputStream(StringUtils.decodeBase64(msg.getBody()));                        
//            M = Json.object((Map<String, Object>)new Protocol().readMessage(in));
            
            M = Json.read(msg.getBody());
        }
        catch (Exception t)
        {
            throw new RuntimeException(t);
        }
        finally
        {
            try { if (thisPeer != null) 
            	thisPeer.getGraph().getTransactionManager().endTransaction(false); }
            catch (Throwable t) { t.printStackTrace(System.err); }
        }
        messageHandler.handleMessage(M);                    
    	
    }
    
    private void initPacketListener()
    {
        connection.addPacketListener(new PacketListener() 
        {
            private void handlePresence(Presence presence)
            {
                String user = presence.getFrom();
                //System.out.println("Presence: " + user);
                Roster roster = connection.getRoster();
                String n = makeRosterName(user);
                //me - don't fire
                if(connection.getUser().equals(n)) return;
                //if user is not in the roster don't fire to listeners
                //the only exception is when presence is unavailable
                //because this could be fired after the user was removed from the roster
                //so we couldn't check this 
                if(roster.getEntry(n) == null && 
                   presence.getType() != Presence.Type.unavailable) return;
                if (presence.getType() == Presence.Type.subscribe)
                {
                    Presence reply = new Presence(Presence.Type.subscribed);
                    reply.setTo(presence.getFrom());
                    connection.sendPacket(reply);
                }
                else if (presence.getType() == Presence.Type.available)
                	processPeerJoin(user);
                else if (presence.getType() == Presence.Type.unavailable)
                	processPeerLeft(user);
            }
            
            private String makeRosterName(String name)
            {
                //input could be in following form
                //bizi@kobrix.syspark.net/67ae7b71-2f50-4aaf-85af-b13fe2236acb
                //test@conference.kobrix.syspark.net/bizi
                //bizi@kobrix.syspark.net
                //output should be:  
                //bizi@kobrix.syspark.net
                if(name.indexOf('/') < 0) return name;
                String first = name.substring(0, name.indexOf('/'));
                String second = name.substring(name.indexOf('/') + 1);
                if(second.length() != 36) return second + "@" + connection.getServiceName();
                try
                {
                    thisPeer.getGraph().getHandleFactory().makeHandle(second);
                   return first;
                }
                catch(NumberFormatException ex)
                {
                    return second;
                }
            }
            
            public void processPacket(Packet packet)
            {
                if (packet instanceof Presence)
                {
                	if (!ignoreRoster)
                		handlePresence((Presence)packet);
                    return;
                }
                try
                {
                	processMessage((Message)packet);
                }
                catch (Throwable t)
                {
                	// Maybe we should do a reply here? If an exception is thrown here,
                	// it means a framework/infrastructure bug...
                	t.printStackTrace(System.err);
                }
            }                
            },
           new PacketFilter() { public boolean accept(Packet p)               
           {
               //System.out.println("filtering " + p);
             if (p instanceof Presence) return true;
             if (! (p instanceof Message)) return false;
             Message msg = (Message)p;
             if (!msg.getType().equals(Message.Type.normal)) return false;
             Boolean hgprop = (Boolean)msg.getProperty("hypergraphdb");
             return hgprop != null && hgprop;                                         
        }});                	
    }

    private String roomJidToUser(String jid)
    {
    	String [] A = jid.split("/");
    	return A[1] + "@" + connection.getServiceName();
    }
    
    private void initRoomConnectivity()
    {
    	room = new MultiUserChat(getConnection(), roomId);
    	room.addParticipantStatusListener(new DefaultParticipantStatusListener() 
    	{	
            @Override
            public void joined(String participant)
            {
            	processPeerJoin(roomJidToUser(participant));
            }
	
            public void kicked(String participant, String actor, String reason)
            {
            	processPeerLeft(roomJidToUser(participant));
            }
	
            public void left(String participant)
            {
            	processPeerLeft(roomJidToUser(participant));
            }
        });    	
    }
    
    private void login() throws XMPPException
    {
        if (anonymous)
            connection.loginAnonymously();
        else
        {
            // maybe auto-register if login fails
            try
            {
                connection.login(user, 
                			     password, 
                			     thisPeer != null && thisPeer.getGraph() != null ? 
                			    		 thisPeer.getIdentity().getId().toString() : null);
            }
            catch (XMPPException ex)
            {
                //XMPPError error = ex.getXMPPError();
                if (/* error != null && 
                     error.getCondition().equals(XMPPError.Condition.forbidden.toString()) && */
                    ex.getMessage().indexOf("authentication failed") > -1 &&
                    autoRegister &&
                    connection.getAccountManager().supportsAccountCreation())
                {
                    connection.getAccountManager().createAccount(user, password);
                    connection.disconnect();
                    connection.connect();
                    connection.login(user, password);
                }
                else
                    throw ex;
            }
        }                	
    }
    
    public void start()
    {
        assert messageHandler != null : new NullPointerException("MessageHandler not specified.");
        connection = new XMPPConnection(config);
        try
        {                             
            connection.connect();
            connection.addConnectionListener(new MyConnectionListener());
            fileTransfer = new FileTransferManager(connection);
            fileTransfer.addFileTransferListener(new BigMessageTransferListener());
            
            // Before we login, we add all relevant listeners so that we don't miss
            // any messages.
           	initPacketListener();
           	
            login();
            
            // Now join the room (if any) and explicitly send a presence message
            // to all peer in the roster cause otherwise presence seems
            // to go unnoticed.
            if (roomId != null && roomId.trim().length() > 0)
            	initRoomConnectivity();                        
            if (room != null)
            	room.join(user);
            if (!ignoreRoster)
            {
	            final Roster roster = connection.getRoster();                                                
	            Presence presence = new Presence(Presence.Type.subscribe);
	            for (RosterEntry entry : roster.getEntries())
	            {
	                presence.setTo(entry.getUser());
	                connection.sendPacket(presence);
	            }
            }
        }
        catch (XMPPException e)
        {    
            if (connection != null && connection.isConnected())
                connection.disconnect();
            throw new RuntimeException(e);
        }                  
    }
    
    public boolean isConnected()
    {
        return connection != null && connection.isConnected();
    }
    
    public void stop()
    {
        if (connection != null)
            try { connection.disconnect(); } catch (Throwable t) { }
    }
    
    public Principal principal()
    {
    	return new Principal()
    	{
			public String getName()
			{
				return user;
			}    		
    	};
    }
    
    public PeerRelatedActivityFactory newSendActivityFactory()
    {
        return new PeerRelatedActivityFactory() {
            public PeerRelatedActivity createActivity()
            {
                return new PeerRelatedActivity()
                {
                    public Boolean call() throws Exception
                    {
                        
                        Json msg = getMessage();
                        if (!msg.has(Messages.REPLY_TO))
                        {
                            msg.set(Messages.REPLY_TO, connection.getUser());
                        }                        
                        String msgAsString = msg.toString();
                        if (msgAsString.length() > fileTransferThreshold)
                        {
                            OutgoingFileTransfer outFile = 
                                fileTransfer.createOutgoingFileTransfer((String)getTarget());
                            byte [] B = msgAsString.getBytes();
                            outFile.sendStream(new ByteArrayInputStream(B), 
                                               "", 
                                               B.length, 
                                               "");
                            return true;
                        }
                        else
                        {
                            try
                            {
                                Message xmpp = new Message((String)getTarget());                            
                                xmpp.setBody(msgAsString);
                                xmpp.setProperty("hypergraphdb", Boolean.TRUE);
                                connection.sendPacket(xmpp);                            
                                return true;
                            }
                            catch (Throwable t)
                            {
                                t.printStackTrace(System.err);
                                return false;
                            }
                        }
                    }                    
                };
            }
        };
    }
    
    public Future<Boolean> send(Object networkTarget, Json msg)
    {
        PeerRelatedActivityFactory activityFactory = newSendActivityFactory();
        PeerRelatedActivity act = activityFactory.createActivity(); 
        act.setTarget(networkTarget);
        act.setMessage(msg);
        if (thisPeer != null)
        	return thisPeer.getExecutorService().submit(act);
        else
        {
        	try
			{
				return new CompletedFuture<Boolean>(act.call());
			} 
        	catch (Exception e)
			{
        		throw new RuntimeException(e);
			}
        }
    }
    
    public void broadcast(Json msg)
    {
        for (HGPeerIdentity peer : thisPeer.getConnectedPeers())
            send(thisPeer.getNetworkTarget(peer), msg);
    }
    
    public HyperGraphPeer getThisPeer()
    {
        return thisPeer;
    }
    
    public void addPeerPresenceListener(NetworkPeerPresenceListener listener)
    {
        presenceListeners.add(listener);
    }
    
    public void removePeerPresenceListener(NetworkPeerPresenceListener listener)
    {
        presenceListeners.remove(listener);
    }
    
    public void setMessageHandler(MessageHandler messageHandler)
    {
        this.messageHandler = messageHandler;
    }

    
    public void setThisPeer(HyperGraphPeer thisPeer)
    {
        this.thisPeer = thisPeer;
    }
    
    public XMPPConnection getConnection()
    {
        return connection;
    }

    public String getServerName()
    {
        return serverName;
    }

    public void setServerName(String serverName)
    {
        this.serverName = serverName;
    }

    public Number getPort()
    {
        return port;
    }

    public void setPort(Number port)
    {
        this.port = port;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public boolean isAnonymous()
    {
        return anonymous;
    }

    public void setAnonymous(boolean anonymous)
    {
        this.anonymous = anonymous;
    }

    public boolean isAutoRegister()
    {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister)
    {
        this.autoRegister = autoRegister;
    }

	public int getFileTransferThreshold()
	{
		return fileTransferThreshold;
	}

	public void setFileTransferThreshold(int fileTransferThreshold)
	{
		this.fileTransferThreshold = fileTransferThreshold;
	}    
	
    private class BigMessageTransferListener implements FileTransferListener
    {
//        @SuppressWarnings("unchecked")
        public void fileTransferRequest(FileTransferRequest request)
        {
            if (thisPeer.getIdentity(request.getRequestor()) != null)
            {
                IncomingFileTransfer inFile = request.accept();
                Json M = null;
                java.io.InputStreamReader in = null;
                thisPeer.getGraph().getTransactionManager().beginTransaction();
                try
                {
                    in = new InputStreamReader(inFile.recieveFile());
                    // TODO - sometime in the presence of a firewall (happened in VISTA)
                    // the file is silently truncated. Here we can read the whole thing
                    // into a byte[] and compare the size to inFile.getFileSize() to
                    // make sure that we got everything. If the file is truncated, the 
                    // parsing of the message will fail for no apparent reason.
                    if (inFile.getFileSize() > Integer.MAX_VALUE)
                        throw new Exception("Message from " + request.getRequestor() + 
                                            " to long with " + inFile.getFileSize() + " bytes.");
                    StringBuilder sb = new StringBuilder();
                    char [] buf = new char[4096];
                    for (; ; ) {
                        int rsz = in.read(buf, 0, buf.length);
                        if (rsz < 0)
                            break;
                        sb.append(buf, 0, rsz);
                    }                    
//                    for (int count = in.read(buf); count != -1; count = in.read(buf))
//                        sb.append(buf,  0, count);
//                    M = new org.hypergraphdb.peer.Message((Map<String, Object>)
//                                  new Protocol().readMessage(new ByteArrayInputStream(B)));
                    M = Json.read(sb.toString());
                }
                catch (Throwable t)
                {
                    t.printStackTrace(System.err);
                    request.reject();
                }
                finally
                {
                    try { thisPeer.getGraph().getTransactionManager().endTransaction(false); }
                    catch (Throwable t) { t.printStackTrace(System.err); }
                    try { if ( in != null) in.close(); } catch (Throwable t) { t.printStackTrace(System.err); }
                }
                messageHandler.handleMessage(M);                
            }
            else
                request.reject();
        }        
    }
 
    private class MyConnectionListener implements ConnectionListener
    {

		public void connectionClosed()
		{
//			System.out.println("XMPP connection " + user + "@" + 
//					serverName + ":" + port + " closed gracefully.");
//			reconnect();
		}

		public void connectionClosedOnError(Exception ex)
		{
//			System.out.println("XMPP connection " + user + "@" + 
//					serverName + ":" + port + " closed exceptionally.");
			ex.printStackTrace(System.err);
			reconnect();
		}

		public void reconnectingIn(int arg0)
		{
//			System.out.println("Auto-reconnecting in " + arg0 + "...");
		}

		public void reconnectionFailed(Exception ex)
		{
//			System.out.println("XMPP auto-re-connection " + 
//					serverName + ":" + port + " failed.");
			ex.printStackTrace(System.err);
			reconnect();
		}

		public void reconnectionSuccessful()
		{
//			System.out.println("Auto-reconnection successful");
		}    	
    }
    
    static
    {
        // Force going through the XMPP server for every file transfer. This is rather
        // slowish, but otherwise it breaks especially for peers behind firewalls/NATs.
//        FileTransferNegotiator.IBB_ONLY = true;        
    }
}
