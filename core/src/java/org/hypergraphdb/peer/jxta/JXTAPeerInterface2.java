/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.jxta;

import static org.hypergraphdb.peer.Structs.*;

import java.io.File;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.exception.PeerGroupException;
import net.jxta.id.IDFactory;
import net.jxta.peer.PeerID;
import net.jxta.peergroup.PeerGroup;
import net.jxta.peergroup.PeerGroupID;
import net.jxta.pipe.PipeID;
import net.jxta.platform.NetworkConfigurator;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.PipeAdvertisement;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.MessageHandler;
import org.hypergraphdb.peer.NetworkPeerPresenceListener;
import org.hypergraphdb.peer.PeerFilter;
import org.hypergraphdb.peer.PeerFilterEvaluator;
import org.hypergraphdb.peer.PeerInterface;
import org.hypergraphdb.peer.PeerRelatedActivity;
import org.hypergraphdb.peer.PeerRelatedActivityFactory;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;

public class JXTAPeerInterface2 implements PeerInterface, JXTARequestHandler
{
    // Configuration parameters: not really necessary to maintain them as member
    // variables, could just hold the config map, but it adds in clarity somewhat.
    private String username;
    private String password;
    private String jxtaDir;
    private boolean deleteJxtaDir;
    private String peerName;
    private String peerGroup;
    private String peerGroupId;
    private boolean createGroup;
    private NetworkManager.ConfigMode mode = NetworkManager.ConfigMode.EDGE;
    private boolean needsRendezVous;
    private List<String> rendezVousSeeds;
    private boolean needsRelay;
    private List<String> relaySeeds;
    private Map<String, Object> tcpTransport;
    private Map<String, Object> httpTransport;
    
    // Runtime objects
    private HyperGraphPeer thisPeer;
    private ArrayList<NetworkPeerPresenceListener> presenceListeners = 
        new ArrayList<NetworkPeerPresenceListener>();
    private MessageHandler messageHandler;
    
    private boolean connected;
    private NetworkManager peerManager = null;
    private PeerGroupID groupId = null;
    private PeerGroup group = null;
    private PeerID peerId = null;
    PipeID incomingPipeId = null;
    PipeAdvertisement incomingPipe = null;
    JXTAServer jxtaServer = null;
    Set<PipeAdvertisement> presentPeers = Collections.synchronizedSet(new HashSet<PipeAdvertisement>());
    private AdvPublisher advPublisher;
    private AdvSubscriber advSubscriber;    
    
    private PeerGroup findGroup(PeerGroupID groupId)
    {
        final PeerGroup npg = peerManager.getNetPeerGroup();
        try
        {
            Enumeration<Advertisement> advs = npg.getDiscoveryService().getLocalAdvertisements(
                DiscoveryService.GROUP, "GID", groupId.toString());
            if (advs != null && advs.hasMoreElements())
                return npg.newGroup(advs.nextElement());
            
            final PeerGroup [] result = new PeerGroup[] { null };
            
            npg.getDiscoveryService().getRemoteAdvertisements(
                null, DiscoveryService.GROUP, "GID", groupId.toString(), 1,
                new DiscoveryListener() {
                public void discoveryEvent(DiscoveryEvent evnt)
                {
                    Advertisement adv = evnt.getSearchResults().nextElement();
                    System.out.println("Found remote group: " + adv);
                    try
                    {
                        result[0] = npg.newGroup(adv);
                    }
                    catch (PeerGroupException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }});
            for (int i = 0; i < 100 && result[0] == null; i++)
                Thread.sleep(100);
            return result[0];
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    private PeerGroup createAndPublishGroup(String name, PeerGroupID gid) throws Exception
    {           
        PeerGroup npg = peerManager.getNetPeerGroup();
        PeerGroup G = npg.newGroup(gid, 
                                   npg.getAllPurposePeerGroupImplAdvertisement(), 
                                   name, 
                                   "A HyperGraph peer group.");

        npg.getDiscoveryService().publish(G.getPeerGroupAdvertisement(),
                                          DiscoveryService.INFINITE_LIFETIME,
                                          DiscoveryService.NO_EXPIRATION);
        npg.getDiscoveryService().remotePublish(G.getPeerGroupAdvertisement(), 
                                                DiscoveryService.NO_EXPIRATION);
        return G;
    }    
    
    private void setupNetwork()
    {
        try
        {
            if (deleteJxtaDir)
                HGUtils.directoryRecurse(new File(jxtaDir), new Mapping<File, Boolean>() {
                    public Boolean eval(File f) { f.delete(); return true; }
                });
            peerManager = new NetworkManager(mode, peerName, new File(jxtaDir).toURI());
            groupId = PeerGroupID.create(URI.create(peerGroupId));
            NetworkConfigurator configurator = peerManager.getConfigurator();
            peerId = IDFactory.newPeerID(groupId, peerName.getBytes());
            configurator.setPeerID(peerId);
            peerManager.setPeerID(peerId);
            configurator.clearRendezvousSeeds();
            configurator.clearRelaySeeds();
            peerManager.setUseDefaultSeeds(false);           
            if (needsRendezVous)
            {
                configurator.setUseOnlyRendezvousSeeds(true);
                for (String uri : rendezVousSeeds)
                    configurator.addSeedRendezvous(URI.create(uri));
            }
            if (needsRelay)
            {
                for (String uri : relaySeeds)
                    configurator.addSeedRelay(URI.create(uri));
            }
            if (tcpTransport != null)
            {
                configurator.setTcpEnabled(getOptPart(tcpTransport, true, "enabled"));
                Number tcpPort = getOptPart(tcpTransport, JXTAConfig.DEFAULT_TCP_PORT, "port");
                configurator.setTcpPort(tcpPort.intValue());
//                configurator.setTcpStartPort(-1);
//                configurator.setTcpEndPort(-1);
                configurator.setTcpIncoming(true);
                configurator.setTcpOutgoing(true);
            }
            else
                configurator.setTcpEnabled(false);
            if (httpTransport != null)
            {
                configurator.setHttpEnabled(getOptPart(httpTransport, true, "enabled"));
                Number port = getOptPart(httpTransport, JXTAConfig.DEFAULT_HTTP_PORT, "port"); 
                configurator.setHttpPort(port.intValue());
                configurator.setHttpIncoming(true);
                configurator.setHttpOutgoing(true);
            }            
            else
                configurator.setHttpEnabled(false);
            configurator.setUseMulticast(true);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public void configure(Map<String, Object> jxtaConfig)
    {
        username = getOptPart(jxtaConfig, username, "user");
        password = getOptPart(jxtaConfig, password, "password");
        jxtaDir = getOptPart(jxtaConfig, jxtaDir, "jxtaDir");
        deleteJxtaDir = getOptPart(jxtaConfig, deleteJxtaDir, "deleteJxtaDir");
        peerName = getPart(jxtaConfig, "peerName");        
        peerGroup = getPart(jxtaConfig, "peerGroup");
        createGroup = getOptPart(jxtaConfig, false, "createGroup");
        peerGroupId = getPart(jxtaConfig, "peerGroupId");        
        assert peerGroupId != null : new RuntimeException("A peerGroupId has to be provided");
        
        if (needsRendezVous = getOptPart(jxtaConfig, needsRendezVous, "needsRendezVous"))
        {
            rendezVousSeeds = getPart(jxtaConfig, "rdvs");
            if (rendezVousSeeds == null || rendezVousSeeds.isEmpty())
                throw new RuntimeException("No rendez-vous seeds specified.");
        }
        if (needsRelay = getOptPart(jxtaConfig, needsRelay, "needsRelay"))
        {
            relaySeeds = getPart(jxtaConfig, "relays");
            if (relaySeeds == null || relaySeeds.isEmpty())
                throw new RuntimeException("No relay seeds specified.");            
        }
        String s_mode = getOptPart(jxtaConfig, mode.toString(), "mode");
        mode = NetworkManager.ConfigMode.valueOf(s_mode);
        tcpTransport = getOptPart(jxtaConfig, tcpTransport, "tcp");
        httpTransport = getOptPart(jxtaConfig, httpTransport, "http");        
        setupNetwork();
    }
    
    public void stop()
    {
        try
        {
            if (advPublisher != null)
            {
                advPublisher.stop();
                advPublisher = null;
            }
            if (advSubscriber != null)
            {
                advSubscriber.stop();
                advSubscriber = null;
            }
            
            if (jxtaServer != null)
                jxtaServer.stop();
            
            presentPeers.clear();
            incomingPipe = null;
            incomingPipeId = null;
            
            if (group != null)
            {
                if (group.isRendezvous())
                    group.getRendezVousService().stopRendezVous();        
                group.getEndpointService().stopApp();
                group.stopApp();
            }
            peerManager.stopNetwork();
        }
        finally
        {
            connected = false;
        }
    }
    
    public boolean isConnected()
    {
        return connected;
    }
    
    // Maybe this should be split into two steps: connect and then "run" for the heartbeats etc.
    // Cause connection could easily fail due to misconfiguration while running is going to be
    // much more resilient.
    public void start()
    {
        if (messageHandler == null)
            throw new NullPointerException("Specify a MessageHandler to the PeerInterface before starting it.");
        try
        {
            peerManager.startNetwork();
            if (mode == NetworkManager.ConfigMode.EDGE)
            {
                peerManager.getNetPeerGroup().getRendezVousService().setAutoStart(false);
                if (!peerManager.waitForRendezvousConnection(120000))
                    throw new RuntimeException("Failed to connect to RendezVous");
            }
            group = findGroup(groupId);
            if (group == null && createGroup)
                group = createAndPublishGroup(peerGroup, groupId);
            if (group == null)
                throw new RuntimeException("Unable to join group with ID " + groupId + ", createGroup flag=" + createGroup);
            group.startApp(null);
            if (mode == NetworkManager.ConfigMode.RENDEZVOUS || mode == NetworkManager.ConfigMode.RENDEZVOUS_RELAY)
                group.getRendezVousService().startRendezVous();
            incomingPipeId = IDFactory.newPipeID(group.getPeerGroupID());
            
            incomingPipe = HGAdvertisementsFactory.newPipeAdvertisement(incomingPipeId, peerName);

            jxtaServer = new JXTAServer(this);
            if (jxtaServer.initialize(group, incomingPipe))
                thisPeer.getExecutorService().execute(jxtaServer);
            
            thisPeer.getExecutorService().execute(advPublisher = new AdvPublisher());
            thisPeer.getExecutorService().execute(advSubscriber = new AdvSubscriber());
            connected = true;
        }
        catch (Exception ex)
        {
            try { peerManager.stopNetwork(); } catch (Throwable t) { }
            throw new RuntimeException(ex);
        }        
    }    
    
    public void broadcast(Message msg)
    {
        throw new UnsupportedOperationException();
    }
    
    public HyperGraphPeer getThisPeer()
    {
        return thisPeer;
    }
        
    public void setThisPeer(HyperGraphPeer thisPeer)
    {
        this.thisPeer = thisPeer;
    }
    
    public PeerFilter newFilterActivity(PeerFilterEvaluator evaluator)
    {
        throw new UnsupportedOperationException();
    }

    
    public PeerRelatedActivityFactory newSendActivityFactory()
    {
        return new JXTASendActivityFactory(group, incomingPipe);
    }
    
    public Future<Boolean> send(Object networkTarget, Message msg)
    {
        PeerRelatedActivityFactory activityFactory = newSendActivityFactory();
        PeerRelatedActivity act = activityFactory.createActivity(); 
        act.setTarget(networkTarget);
        act.setMessage(msg);
        return thisPeer.getExecutorService().submit(act); 
    }
 
    public void setMessageHandler(MessageHandler messageHandler)
    {
        this.messageHandler = messageHandler;
    }

    public void handleRequest(Socket socket)
    {
        thisPeer.getExecutorService().execute(new ConnectionHandler(socket, 
                                                                    messageHandler,
                                                                    thisPeer.getExecutorService()));
    }
    
    public void addPeerPresenceListener(NetworkPeerPresenceListener listener)
    {
        presenceListeners.add(listener);
    }
    
    public void removePeerPresenceListener(NetworkPeerPresenceListener listener)
    {
        presenceListeners.remove(listener);
    }
    
    private class AdvPublisher implements Runnable
    {       
        private volatile Thread thisThread = null;
        
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
            DiscoveryService discoveryService = group.getDiscoveryService();

            long expiration = 5 * 1000;// DiscoveryService.DEFAULT_EXPIRATION;
            long waittime = 5 * 1000;// DiscoveryService.DEFAULT_EXPIRATION;
            thisThread = Thread.currentThread();
            try
            {               
                while (thisThread != null)
                {
//                    System.out.println("Publishing pipe " + incomingPipe.getPipeID());
                    discoveryService.publish(incomingPipe, expiration, expiration);
                    discoveryService.remotePublish(incomingPipe, expiration);
                    try
                    {
                        Thread.sleep(waittime);
                    }
                    catch (InterruptedException e)
                    {
                        System.err.println("AdvPublish Thread interrupted.");
                        break;
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace(System.err);
                    }
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }  
            finally
            {
                thisThread = null;
            }
        }
    }

    private class AdvSubscriber implements Runnable, DiscoveryListener
    {
        private volatile Thread thisThread = null;
        
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
            long waittime = 1000L;
            DiscoveryService discoveryService = group.getDiscoveryService();
            thisThread = Thread.currentThread();
            try
            {
                Enumeration<Advertisement> localAdvs = 
                    discoveryService.getLocalAdvertisements(DiscoveryService.ADV, null, null);
                loadAdvs(localAdvs, "local");

                // Add ourselves as a DiscoveryListener for DiscoveryResponse
                // events
                discoveryService.addDiscoveryListener(this);
                while (thisThread != null)
                {
                    // System.out.println("Getting remote advertisements");
                    discoveryService.getRemoteAdvertisements(null,
                                                             DiscoveryService.ADV,
                                                             null, // don't care attribute
                                                             null, // don't care attribute
                                                             100, // each peer should return only 100 ADV
                                                             null);
                    try
                    {
                        Thread.sleep(waittime);
                    }
                    catch (InterruptedException e)
                    {
                        break;
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace(System.err);
                    }
                }
                
                discoveryService.removeDiscoveryListener(this);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }           
            finally
            {
                thisThread = null;
            }
        }

        public void discoveryEvent(DiscoveryEvent ev)
        {
            DiscoveryResponseMsg res = ev.getResponse();

            // let's get the responding peer's advertisement
            String peerName = ev.getSource().toString();

            // System.out.println(" [ Got a Discovery Response [" +
            // res.getResponseCount() + " elements] from peer : " + peerName + "
            // ]");
            /*
             * PeerAdvertisement peerAdv = res.getPeerAdvertisement();
             */
            // not interested in selfs advertisements
            Enumeration<Advertisement> advs = res.getAdvertisements();

            loadAdvs(advs, peerName);
        }

        private void loadAdvs(Enumeration<Advertisement> advs, String peerName)
        {
            Advertisement adv;

            if (advs != null)
            {
                int count = 0;
                while (advs.hasMoreElements())
                {
                    count++;
                    adv = advs.nextElement();
                    if (adv instanceof PipeAdvertisement)
                    {
//                        System.out.println("Got pipe:" + adv);                        
                        if (!presentPeers.contains(adv))
                        {
                            PipeID pipeId = (PipeID) ((PipeAdvertisement) adv).getPipeID();
//                            System.out.println("Got pipe " + pipeId);
                            if (!incomingPipeId.equals(pipeId))
                            {
                                presentPeers.add((PipeAdvertisement)adv);
//                                System.out.println("Add new advertisment, calling listeners.");
                                for (NetworkPeerPresenceListener listener : presenceListeners)
                                    listener.peerJoined(adv);
                            }
                        }
                    }
                }
//                System.out.println("Examined " + count + " advertisements.");
            }
        }
    }   
}
