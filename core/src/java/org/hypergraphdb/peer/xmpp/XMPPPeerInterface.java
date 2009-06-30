package org.hypergraphdb.peer.xmpp;

import static org.hypergraphdb.peer.Structs.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
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
import org.jivesoftware.smack.filter.PacketFilter;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Packet;
import org.jivesoftware.smack.packet.Presence;
import org.jivesoftware.smack.util.StringUtils;
import org.jivesoftware.smackx.filetransfer.FileTransferListener;
import org.jivesoftware.smackx.filetransfer.FileTransferManager;
import org.jivesoftware.smackx.filetransfer.FileTransferRequest;
import org.jivesoftware.smackx.filetransfer.IncomingFileTransfer;
import org.jivesoftware.smackx.filetransfer.OutgoingFileTransfer;

/**
 * <p>
 * A peer interface implementation based upon the Smack library 
 * (see http://www.igniterealtime.org for more info). 
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
    private boolean anonymous;
    private boolean autoRegister;
    private HyperGraphPeer thisPeer;
    private ArrayList<NetworkPeerPresenceListener> presenceListeners = 
        new ArrayList<NetworkPeerPresenceListener>();
    private MessageHandler messageHandler;
     
    ConnectionConfiguration config = null;
    XMPPConnection connection;
    FileTransferManager fileTransfer;
    
    public void configure(Map<String, Object> configuration)
    {
        serverName = getPart(configuration, "serverUrl");
        port = getOptPart(configuration, 5222, "port");
        user = getPart(configuration, "user");
        password = getPart(configuration, "password");
        autoRegister = getOptPart(configuration, false, "autoRegister");
        anonymous = getOptPart(configuration, false, "anonymous");
        config = new ConnectionConfiguration(serverName, port.intValue());
        config.setRosterLoadedAtLogin(true);
        config.setReconnectionAllowed(true);
        SmackConfiguration.setPacketReplyTimeout(30000);
    }    
    
    public void start()
    {
        assert messageHandler != null : new NullPointerException("MessageHandler not specified.");
        connection = new XMPPConnection(config);
        try
        {                             
            connection.connect();
            connection.addPacketListener(new PacketListener() 
            {
                private void handlePresence(Presence presence)
                {
                    String user = presence.getFrom();
                    if (presence.getType() == Presence.Type.subscribe)
                    {
                        Presence reply = new Presence(Presence.Type.subscribed);
                        reply.setTo(presence.getFrom());
                        connection.sendPacket(reply);
                    }
                    else if (presence.getType() == Presence.Type.available)
                    {
                        for (NetworkPeerPresenceListener listener : presenceListeners)
                            listener.peerJoined(user);
                    }
                    else if (presence.getType() == Presence.Type.unavailable)
                    {
                        for (NetworkPeerPresenceListener listener : presenceListeners)
                            listener.peerLeft(user);                        
                    }
                }
                
                @SuppressWarnings("unchecked")
                public void processPacket(Packet packet)
                {
                    if (packet instanceof Presence)
                    {
                        handlePresence((Presence)packet);
                        return;
                    }
                    Message msg = (Message)packet;
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
               new PacketFilter() { public boolean accept(Packet p)               
               {
                 if (p instanceof Presence) return true;
                 if (! (p instanceof Message)) return false;
                 Message msg = (Message)p;
                 if (!msg.getType().equals(Message.Type.normal)) return false;
                 Boolean hgprop = (Boolean)msg.getProperty("hypergraphdb");
                 return hgprop != null && hgprop;                                         
            }});            
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
            final Roster roster = connection.getRoster();            
/*            roster.addRosterListener(new RosterListener() 
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
            }); */
            fileTransfer = new FileTransferManager(connection);
            fileTransfer.addFileTransferListener(new BigMessageTransferListener());
                                    
//            for (RosterEntry entry: roster.getEntries()) 
//            {
//                Presence presence = roster.getPresence(entry.getUser());
//                if (presence.getType() == Presence.Type.available)
//                    for (NetworkPeerPresenceListener listener : presenceListeners)
//                        listener.peerJoined(presence.getFrom());
//            }
//            Presence presence = new Presence(Presence.Type.available);
//            presence.setMode(Presence.Mode.available);
//            for (RosterEntry entry : roster.getEntries())
//            {
//                presence.setTo(entry.getUser());
//                connection.sendPacket(presence);
//            }
            
            Presence presence = new Presence(Presence.Type.subscribe);
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
                            protocol.writeMessage(out, msg);
                        }
                        finally
                        {
                            try { thisPeer.getGraph().getTransactionManager().endTransaction(false); }
                            catch (Throwable t) { t.printStackTrace(System.err); }
                        }  
                        
                        byte [] data = out.toByteArray();
                        if (data.length > 100*1024) // anything above 100k is send as a file!
                        {
//                            System.out.println("Sending " + data.length + " byte of data as a file.");
                            OutgoingFileTransfer outFile = 
                                fileTransfer.createOutgoingFileTransfer((String)getTarget());
                            outFile.sendStream(new ByteArrayInputStream(data), 
                                               "", 
                                               data.length, 
                                               "");
                            return true;
                        }
                        else
                        {
                            try
                            {
                                Message xmpp = new Message((String)getTarget());                            
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
    
    private class BigMessageTransferListener implements FileTransferListener
    {
        @SuppressWarnings("unchecked")
        public void fileTransferRequest(FileTransferRequest request)
        {
            if (thisPeer.getIdentity(request.getRequestor()) != null)
            {
                
                IncomingFileTransfer inFile = request.accept();
                org.hypergraphdb.peer.Message M = null;
                java.io.InputStream in = null;
                thisPeer.getGraph().getTransactionManager().beginTransaction();
                try
                {
                    in = inFile.recieveFile();
                    // TODO - sometime in the presence of a firewall (happened in VISTA)
                    // the file is silently truncated. Here we can read the whole thing
                    // into a byte[] and compare the size to inFile.getFileSize() to
                    // make sure that we got everything. If the file is truncated, the 
                    // parsing of the message will fail for no apparent reason.
                    if (inFile.getFileSize() > Integer.MAX_VALUE)
                        throw new Exception("Message from " + request.getRequestor() + 
                                            " to long with " + inFile.getFileSize() + " bytes.");
                    byte [] B = new byte[(int)inFile.getFileSize()];
                    for (int count = 0; count < inFile.getFileSize(); )
                        count += in.read(B, count, (int)inFile.getFileSize() - count);
                    M = new org.hypergraphdb.peer.Message((Map<String, Object>)
                                  new Protocol().readMessage(new ByteArrayInputStream(B)));                        
                }
                catch (Throwable t)
                {
                    t.printStackTrace(System.err);
                    throw new RuntimeException(t);
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
    
    static
    {
        // Force going through the XMPP server for every file transfer. This is rather
        // slowish, but otherwise it breaks especially for peers behind firewalls/NATs.
//        FileTransferNegotiator.IBB_ONLY = true;        
    }
}