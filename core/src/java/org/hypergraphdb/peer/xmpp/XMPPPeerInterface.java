package org.hypergraphdb.peer.xmpp;

import static org.hypergraphdb.peer.Structs.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.MessageHandler;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.NetworkPeerPresenceListener;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerFilterEvaluator;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.peer.protocol.Protocol;
import org.jivesoftware.smack.*;
import org.jivesoftware.smack.filter.AndFilter;
import org.jivesoftware.smack.filter.MessageTypeFilter;
import org.jivesoftware.smack.filter.PacketTypeFilter;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Packet;
import org.jivesoftware.smack.packet.Presence;
import org.jivesoftware.smack.util.StringUtils;

public class XMPPPeerInterface implements PeerInterface
{
    // Configuration options.
    private String serverName;
    private Number port;
    private String user;
    private String password;
    private boolean anonymous;
    private boolean autoRegister;
    private HyperGraphPeer thisPeer;
    private ArrayList<NetworkPeerPresenceListener> presenceListeners = 
        new ArrayList<NetworkPeerPresenceListener>();
    private MessageHandler messageHandler;
     
    ConnectionConfiguration config = null;
    XMPPConnection connection;
    
    public void configure(Map<String, Object> configuration)
    {
        serverName = getPart(configuration, "serverUrl");
        port = getOptPart(configuration, 5222, "port");
        user = getPart(configuration, "user");
        password = getPart(configuration, "password");        
        config = new ConnectionConfiguration(serverName, port.intValue());
        config.setRosterLoadedAtLogin(true);
        config.setReconnectionAllowed(true);
    }    
    
    public void start()
    {
        assert messageHandler != null : new NullPointerException("MessageHandler not specified.");
        connection = new XMPPConnection(config);
        try
        {
            connection.connect();
            if (anonymous)
                connection.loginAnonymously();
            else
            {
                // maybe auto-register if login fails
                try
                {
                    connection.login(user, password, thisPeer.getIdentity().getId().toString());
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
            connection.addPacketListener(new PacketListener() {
                @SuppressWarnings("unchecked")
                public void processPacket(Packet packet)
                {
                    Message msg = (Message)packet;
                    Boolean hgprop = (Boolean)msg.getProperty("hypergraph");
                    if (hgprop == null || !hgprop)
                    {
                        System.out.println("Ignoring packet: " + packet);
                    }
                    //
                    // Encapsulate message deserialization into a transaction because the HGDB might
                    // be accessed during this process.
                    // 
                    org.hypergraphdb.peer.Message M = null;
                    thisPeer.getGraph().getTransactionManager().beginTransaction();
                    try
                    {
                        ByteArrayInputStream in = new ByteArrayInputStream(StringUtils.decodeBase64(msg.getBody()));                        
                        M = new org.hypergraphdb.peer.Message((Map<String, Object>)new Protocol().readMessage(in));                        
                    }
                    catch (Exception t)
                    {
                        throw new RuntimeException(t);
                    }
                    finally
                    {
                        try { thisPeer.getGraph().getTransactionManager().endTransaction(false); }
                        catch (Throwable t) { t.printStackTrace(System.err); }
                    }
                    messageHandler.handleMessage(M);                    
                }                
                },
               new AndFilter(new PacketTypeFilter(Message.class),
                              new MessageTypeFilter(Message.Type.normal)));
            final Roster roster = connection.getRoster();
            roster.addRosterListener(new RosterListener() 
            {
                public void entriesAdded(Collection<String> addresses) 
                {
                    System.out.println("New friends");
                }
                public void entriesDeleted(Collection<String> addresses) 
                {
                    System.out.println("Friends left");
                }
                public void entriesUpdated(Collection<String> addresses) 
                {
                    System.out.println("friends changed");
                }
                public void presenceChanged(Presence presence) 
                {
                    String user = presence.getFrom();
                    
                    System.out.println("Presence changed: " + presence.getFrom() + " " + presence);                    
                    Presence bestPresence = roster.getPresence(user);                   
                    if (bestPresence.getType() == Presence.Type.available)
                    {
                        for (NetworkPeerPresenceListener listener : presenceListeners)
                            listener.peerJoined(user);
                    }
                    else if (bestPresence.getType() == Presence.Type.unavailable)
                    {
                        for (NetworkPeerPresenceListener listener : presenceListeners)
                            listener.peerLeft(user);                        
                    }                        
                }
            });
            for (RosterEntry entry: roster.getEntries()) 
            {
                Presence presence = roster.getPresence(entry.getUser());
                if (presence.getType() == Presence.Type.available)
                    for (NetworkPeerPresenceListener listener : presenceListeners)
                        listener.peerJoined(presence.getFrom());
            }
            Presence presence = new Presence(Presence.Type.available);
            presence.setMode(Presence.Mode.available);
            for (RosterEntry entry : roster.getEntries())
            {
                presence.setTo(entry.getUser());
                connection.sendPacket(presence);
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
    
    public PeerRelatedActivityFactory newSendActivityFactory()
    {
        return new PeerRelatedActivityFactory() {
            public PeerRelatedActivity createActivity()
            {
                return new PeerRelatedActivity()
                {
                    public Boolean call() throws Exception
                    {
                        Message xmpp = new Message((String)getTarget());
                        org.hypergraphdb.peer.Message msg = getMessage();
                        if (getPart(msg, Messages.REPLY_TO) == null)
                        {
                            combine(msg, struct(Messages.REPLY_TO, connection.getUser()));
                        }                            
                        Protocol protocol = new Protocol();
                        ByteArrayOutputStream out = new ByteArrayOutputStream();                        
                        //
                        // Encapsulate message serialization into a transaction because the HGDB might
                        // be accessed during this process.
                        //                         
                        thisPeer.getGraph().getTransactionManager().beginTransaction();
                        try
                        {
                            protocol.writeMessage(out, getMessage());
                        }
                        finally
                        {
                            try { thisPeer.getGraph().getTransactionManager().endTransaction(false); }
                            catch (Throwable t) { t.printStackTrace(System.err); }
                        }                        
                        try
                        {
                            xmpp.setBody(StringUtils.encodeBase64(out.toByteArray()));
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
                };
            }
        };
    }
    
    public Future<Boolean> send(Object networkTarget,org.hypergraphdb.peer.Message msg)
    {
        PeerRelatedActivityFactory activityFactory = newSendActivityFactory();
        PeerRelatedActivity act = activityFactory.createActivity(); 
        act.setTarget(networkTarget);
        act.setMessage(msg);
        return thisPeer.getExecutorService().submit(act); 
    }
    
    public void broadcast(org.hypergraphdb.peer.Message msg)
    {
        for (HGPeerIdentity peer : thisPeer.getConnectedPeers())
            send(thisPeer.getNetworkTarget(peer), msg);
    }
    
    public HyperGraphPeer getThisPeer()
    {
        return thisPeer;
    }
    
    public PeerFilter newFilterActivity(PeerFilterEvaluator evaluator)
    {
        throw new UnsupportedOperationException();
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
}