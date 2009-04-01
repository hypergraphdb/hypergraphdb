package org.hypergraphdb.peer.jxta;

import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.getOptPart;
import static org.hypergraphdb.peer.Structs.getStruct;
import static org.hypergraphdb.peer.Structs.hasPart;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import net.jxta.credential.AuthenticationCredential;
import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.document.AdvertisementFactory;
import net.jxta.document.Element;
import net.jxta.document.MimeMediaType;
import net.jxta.document.StructuredDocument;
import net.jxta.endpoint.EndpointService;
import net.jxta.endpoint.MessageTransport;
import net.jxta.exception.PeerGroupException;
import net.jxta.id.ID;
import net.jxta.id.IDFactory;
import net.jxta.impl.endpoint.relay.RelayClient;
//import net.jxta.impl.membership.passwd.PasswdMembershipService;
import net.jxta.impl.peergroup.StdPeerGroupParamAdv;
import net.jxta.impl.rendezvous.RendezVousServiceInterface;
import net.jxta.impl.rendezvous.rpv.PeerView;
import net.jxta.membership.Authenticator;
import net.jxta.peer.PeerID;
import net.jxta.peergroup.PeerGroup;
import net.jxta.peergroup.PeerGroupID;
import net.jxta.pipe.PipeID;
import net.jxta.platform.ModuleSpecID;
import net.jxta.platform.NetworkConfigurator;
import net.jxta.platform.NetworkManager;
import net.jxta.platform.NetworkManager.ConfigMode;
import net.jxta.protocol.AccessPointAdvertisement;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.ModuleImplAdvertisement;
import net.jxta.protocol.PeerAdvertisement;
import net.jxta.protocol.PeerGroupAdvertisement;
import net.jxta.protocol.PipeAdvertisement;
import net.jxta.rendezvous.RendezVousService;

import org.hypergraphdb.peer.NetworkPeerPresenceListener;
import org.hypergraphdb.peer.PeerConfig;
import org.hypergraphdb.peer.RemotePeer;
import org.hypergraphdb.query.HGAtomPredicate;

/**
 * 
 * <p>
 * Handles problems related to the JXTA network : intialize, stop, discovery,
 * publishing, etc
 * </p>
 * 
 * @author Cipri Costa
 */
public class DefaultJXTANetwork implements JXTANetwork
{
    private static int RDV_WAIT_TIMEOUT = 3000;

    private static NetworkManager peerManager = null;
    private static PeerGroup netPeerGroup = null;
    private static PeerGroup hgdbGroup = null;

    private LinkedList<Advertisement> ownAdvs = new LinkedList<Advertisement>();
    private Map<Advertisement, HGAtomPredicate> peerAdvs = Collections.synchronizedMap(new HashMap<Advertisement, HGAtomPredicate>());
    private Map<Advertisement, String> peerAdvIds = new HashMap<Advertisement, String>();
    private List<NetworkPeerPresenceListener> peerPresenceListeners = 
        new ArrayList<NetworkPeerPresenceListener>(); 
    
    private Set<PipeID> ownPipes = new HashSet<PipeID>();
    private int advTimetoLive;
    private final static ModuleSpecID PROTECTED_GROUP_MSID = (ModuleSpecID) ID.create(URI.create("urn:jxta:uuid-DEADBEEFDEAFBABAFEEDBABE0000000133BF5414AC624CC8AD3AF6AEC2C8264306"));

    private AdvPublisher advPublisher;
    private AdvSubscriber advSubscriber;
    
    public boolean configure(Map<String, Object> config)
    {
        boolean result = false;
        NetworkManager.ConfigMode configMode = ConfigMode.ADHOC;
        String peerName = (String) getOptPart(config, "HGDBPeer", PeerConfig.PEER_NAME);        
        Map<String, Object> jxtaConfig = getStruct(config, JXTAConfig.CONFIG_NAME);
        
        // Start network
        try
        {                        
            String mode = (String) getOptPart(jxtaConfig, "ADHOC", JXTAConfig.MODE);
            String jxtaDir = (String) getOptPart(jxtaConfig, ".jxta",
                                                 JXTAConfig.JXTA_DIR);

            configMode = NetworkManager.ConfigMode.valueOf(NetworkManager.ConfigMode.class,
                                                           mode);

            System.out.println("Initializing instance " + peerName + " ...");
            URI configURI = new File(jxtaDir).toURI();

            System.out.println("Using config file: " + configURI.toString());

            peerManager = new NetworkManager(configMode, peerName, configURI);

            NetworkConfigurator configurator = peerManager.getConfigurator();
            configurator.setName(peerName);
            configureNetwork(configurator, jxtaConfig);

            dumpNetworkConfig(configurator);

            peerManager.startNetwork();

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        
        // Join custom group
        if (peerManager != null)
        {
            netPeerGroup = peerManager.getNetPeerGroup();
            try
            {
                String username = (String) jxtaConfig.get("user");
                String password = (String) jxtaConfig.get("password");
                createCustomGroup(jxtaConfig, username, password);
                if (hgdbGroup != null)
                {
                    System.out.println("Trying to join the group ... ");
                    result = joinCustomGroup(username,
                                             password,
                                             (configMode == ConfigMode.RENDEZVOUS || configMode == ConfigMode.RENDEZVOUS_RELAY));
                }

            }
            catch (Exception e)
            {
                e.printStackTrace();
            }          
        }
        else
        {
            System.out.println("PeerManager can not be instantiated");
        }

        // wait for rendezvous if needed
        Boolean needsRendezVous = (Boolean) getOptPart(jxtaConfig,
                                                       false,
                                                       JXTAConfig.NEEDS_RENDEZ_VOUS);
        System.out.println("Waiting for rendezvous: " + needsRendezVous);
        if (needsRendezVous)
        {
            boolean rdvFound = false;
            while (!rdvFound)
            {
                System.out.println("start waiting for rendezvous...");
                rdvFound = waitForRendezVous(hgdbGroup);

                try
                {
                    Thread.sleep(3000);
                }
                catch (InterruptedException e)
                {
                }
            }
        }

        // wait for realy
        Boolean needsRelay = (Boolean) getOptPart(jxtaConfig, false,
                                                  JXTAConfig.NEEDS_RELAY);
        System.out.println("Waiting for realy: " + needsRelay);
        if (needsRelay)
        {
            boolean relayFound = false;
            while (!relayFound)
            {
                System.out.println("start waiting for relays...");
                relayFound = waitForRelay();

                if (relayFound)
                    System.out.println("Relay found!");
                else
                {
                    System.out.println("No relays found...");

                    try
                    {
                        Thread.sleep(3000);
                    }
                    catch (InterruptedException e)
                    {
                    }
                }
            }
        }

        this.advTimetoLive = ((Long) getOptPart(jxtaConfig, 10000L,
                                                JXTAConfig.ADVERTISEMENT_TTL)).intValue();

        System.out.println("Finished initializing");

        return result;
    }

	public void stop()
	{
		if (peerManager != null)
		{
			advPublisher.stop();			
			advSubscriber.stop();
			
			if (netPeerGroup.isRendezvous())
				netPeerGroup.getRendezVousService().stopRendezVous();
			
			netPeerGroup.stopApp();

			if (hgdbGroup.isRendezvous())
				hgdbGroup.getRendezVousService().stopRendezVous();
			
			hgdbGroup.getEndpointService().stopApp();			
			hgdbGroup.stopApp();
			
			peerManager.stopNetwork();
		}
		else
		{
			System.out.println("Peer manager is null, nothing to stop here");
		}
	}
    
    private void configureNetwork(NetworkConfigurator configurator,
                                  Object config)
    {
        // peerid?

        // tcp transport
        if (hasPart(config, JXTAConfig.TCP))
        {
            Object tcpConfig = getPart(config, JXTAConfig.TCP);
            if (hasPart(tcpConfig, JXTAConfig.ENABLED))
                configurator.setTcpEnabled((Boolean) getPart(tcpConfig,
                                                             JXTAConfig.ENABLED));
            if (hasPart(tcpConfig, JXTAConfig.INCOMING))
                configurator.setTcpIncoming((Boolean) getPart(
                                                              tcpConfig,
                                                              JXTAConfig.INCOMING));
            if (hasPart(tcpConfig, JXTAConfig.OUTGOING))
                configurator.setTcpOutgoing((Boolean) getPart(
                                                              tcpConfig,
                                                              JXTAConfig.ENABLED));

            if (hasPart(tcpConfig, JXTAConfig.PORT))
                configurator.setTcpPort(((Long) getPart(tcpConfig,
                                                        JXTAConfig.PORT)).intValue());
            if (hasPart(tcpConfig, JXTAConfig.START_PORT))
                configurator.setTcpStartPort(((Long) getPart(
                                                             tcpConfig,
                                                             JXTAConfig.START_PORT)).intValue());
            if (hasPart(tcpConfig, JXTAConfig.END_PORT))
                configurator.setTcpEndPort(((Long) getPart(tcpConfig,
                                                           JXTAConfig.END_PORT)).intValue());
        }

        // http transport
        if (hasPart(config, JXTAConfig.HTTP))
        {
            Object httpConfig = getPart(config, JXTAConfig.HTTP);
            if (hasPart(httpConfig, JXTAConfig.ENABLED))
                configurator.setHttpEnabled((Boolean) getPart(
                                                              httpConfig,
                                                              JXTAConfig.ENABLED));
            if (hasPart(httpConfig, JXTAConfig.INCOMING))
                configurator.setHttpIncoming((Boolean) getPart(
                                                               httpConfig,
                                                               JXTAConfig.INCOMING));
            if (hasPart(httpConfig, JXTAConfig.OUTGOING))
                configurator.setHttpOutgoing((Boolean) getPart(
                                                               httpConfig,
                                                               JXTAConfig.ENABLED));

            if (hasPart(httpConfig, JXTAConfig.PORT))
                configurator.setHttpPort(((Long) getPart(httpConfig,
                                                         JXTAConfig.PORT)).intValue());
        }

        // relay config - which relays to use
        if (hasPart(config, JXTAConfig.RELAYS))
        {
            configurator.clearRelaySeeds();
            List<Object> relays = (List<Object>) getPart(config,
                                                         JXTAConfig.RELAYS);

            for (Object relay : relays)
            {
                if (relay instanceof String)
                {
                    configurator.addSeedRelay(URI.create((String) relay));
                }
            }
        }
        
        // rdv config - which rdvs to use
        if (hasPart(config, JXTAConfig.RENDEZVOUS))
        {
            configurator.clearRendezvousSeeds();
            List<Object> rdvs = (List<Object>) getPart(config,
                                                       JXTAConfig.RENDEZVOUS);

            for (Object rdv : rdvs)
            {
                if (rdv instanceof String)
                {
                    configurator.addSeedRendezvous(URI.create((String) rdv));
                }
            }
        }

        // pse (authentification)?
    }

    private boolean waitForRendezVous(PeerGroup group)
    {
        boolean rdvFound = false;

        RendezVousService rdv = group.getRendezVousService();
        Enumeration<ID> rdvs = rdv.getConnectedRendezVous();

        if (rdvs.hasMoreElements())
        {
            rdvFound = true;
            System.out.println("Rendezvous Connections :");
            RendezVousServiceInterface stdRdv;
            net.jxta.impl.rendezvous.StdRendezVousService stdRdvProvider = null;
            while (rdvs.hasMoreElements())
            {
                try
                {
                    ID connection = (PeerID) rdvs.nextElement();

                    if (rdv instanceof net.jxta.impl.rendezvous.RendezVousServiceInterface)
                    {
                        stdRdv = (RendezVousServiceInterface) rdv;
                        PeerView rpv = null;

                        System.out.print("\t" + connection);
                        if (null != stdRdv)
                        {
                            net.jxta.impl.rendezvous.RendezVousServiceProvider provider = stdRdv.getRendezvousProvider();

                            if (provider instanceof net.jxta.impl.rendezvous.StdRendezVousService)
                            {
                                stdRdvProvider = (net.jxta.impl.rendezvous.StdRendezVousService) provider;
                            }

                            rpv = stdRdv.getPeerView();
                        }

                        if (null != stdRdvProvider)
                        {
                            System.out.println("\t"
                                               + stdRdvProvider.getPeerConnection(connection));
                        }
                        else
                        {
                            String peerName = idToName(
                                                       group.getDiscoveryService(),
                                                       connection);
                            System.out.println("\t" + peerName);
                        }
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }

        return rdvFound;
    }

    private boolean waitForRelay()
    {
        boolean hasRelay = false;

        EndpointService endpoint = hgdbGroup.getEndpointService();

        Iterator<?> it = endpoint.getAllMessageTransports();

        while (it.hasNext())
        {
            MessageTransport mt = (MessageTransport) it.next();

            try
            {
                if (mt instanceof RelayClient)
                {
                    RelayClient er = (RelayClient) mt;
                    
                    List<?> allRelays = er.getActiveRelays(null);

                    if ((null != allRelays) && !allRelays.isEmpty())
                    {
                        hasRelay = true;
                        System.out.println("Active Relay Servers : ");
                        for (Object allRelay : allRelays)
                        {
                            AccessPointAdvertisement ap = (AccessPointAdvertisement) allRelay;
                            System.out.println("\t" + getPeerName(mt, ap)
                                               + " [" + ap.getPeerID() + "]");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }
        }

        return hasRelay;
    }

    private String getPeerName(MessageTransport mt, AccessPointAdvertisement adv)
    {

        EndpointService endpoint = mt.getEndpointService();
        DiscoveryService discovery = endpoint.getGroup().getDiscoveryService();

        PeerID id = adv.getPeerID();

        return idToName(discovery, id);
    }

    private String idToName(DiscoveryService discovery, ID id)
    {

        String idstring = id.toString();
        String name = null;

        try
        {
            Enumeration<Advertisement> res;

            if (id instanceof PeerID)
            {
                res = discovery.getLocalAdvertisements(DiscoveryService.PEER,
                                                       "PID", idstring);

                if (res.hasMoreElements())
                {
                    name = ((PeerAdvertisement) res.nextElement()).getName();
                }
            }
            else if (id instanceof PeerGroupID)
            {
                res = discovery.getLocalAdvertisements(DiscoveryService.GROUP,
                                                       "GID", idstring);

                if (res.hasMoreElements())
                {
                    name = ((PeerGroupAdvertisement) res.nextElement()).getName();
                }
            }
        }
        catch (IOException failed)
        {
            failed.printStackTrace();
        }

        return name;
    }

    private void createCustomGroup(Object config, String username, String passwd) throws Exception
    {
        String groupName = (String) getOptPart(config, "HGDBGroup",
                                               JXTAConfig.GROUP_NAME);

        System.out.println("Joining group " + groupName);
        PeerGroupID groupId = IDFactory.newPeerGroupID(netPeerGroup.getPeerGroupID(),
                                                       groupName.getBytes());

        // try to find it, if not, publish it
        Enumeration<Advertisement> advs;

        advs = netPeerGroup.getDiscoveryService().getLocalAdvertisements(DiscoveryService.GROUP,
                                                                         "GID",
                                                                         groupId.toString());
        if (advs != null)
        {
            if (advs.hasMoreElements())
            {
                Advertisement adv = advs.nextElement();
                if (adv != null)
                {
                    System.out.println("Found local group advertisement... ");
                    hgdbGroup = netPeerGroup.newGroup(adv);
                }
            }
        }

        if (hgdbGroup == null)
        {
            DiscoveryListener listener = new DiscoveryListener()
            {
                public void discoveryEvent(DiscoveryEvent evnt)
                {
                    Advertisement adv = evnt.getSearchResults().nextElement();

                    System.out.println("Remote group: " + adv);
                    System.out.println("Found remote group advertisement... ");
                    try
                    {
                        hgdbGroup = netPeerGroup.newGroup(adv);
                    }
                    catch (PeerGroupException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            };

            netPeerGroup.getDiscoveryService().getRemoteAdvertisements(null,
                                                                       DiscoveryService.GROUP,
                                                                       null,
                                                                       null,
                                                                       100,
                                                                       listener);

            try
            {
                Thread.sleep(3000);
            }
            catch (InterruptedException ex)
            {
            }

            netPeerGroup.getDiscoveryService().removeDiscoveryListener(listener);
        }

        if (hgdbGroup == null)
        {
            System.out.println("Can not find local group advertisements. Creating a new group ... ");

            ModuleImplAdvertisement implAdv = null;

            try
            {
                implAdv = createGroupAdv();
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }

            netPeerGroup.getDiscoveryService().publish(implAdv);
            netPeerGroup.getDiscoveryService().remotePublish(implAdv);

            PeerGroupAdvertisement groupAdv = (PeerGroupAdvertisement) AdvertisementFactory.newAdvertisement(PeerGroupAdvertisement.getAdvertisementType());
            groupAdv.setPeerGroupID(groupId);
            groupAdv.setModuleSpecID(implAdv.getModuleSpecID());
            groupAdv.setName(groupName);
            groupAdv.setDescription("HGDB Group");

/*            StructuredTextDocument loginInfo = 
                (StructuredTextDocument) StructuredDocumentFactory.newStructuredDocument(new MimeMediaType("text/xml"),"Parm");
            String loginString = username + ":"
                                 + PasswdMembershipService.makePsswd(passwd == null ? "password" : passwd)
                                 + ":";
            TextElement loginElement = loginInfo.createElement("login",
                                                               loginString);
            loginInfo.appendChild(loginElement);
            groupAdv.putServiceParam(PeerGroup.membershipClassID, loginInfo); */

            hgdbGroup = netPeerGroup.newGroup(groupAdv);

            System.out.println("publishing group...");
            netPeerGroup.getDiscoveryService().remotePublish(groupAdv);
            System.out.println("Group published successfully.");
        }
    }

    private boolean joinCustomGroup(String user, String passwd,
                                    boolean beRendevous) throws Exception
    {
        boolean isJoined = false;

        StructuredDocument myCredentials = null;

        AuthenticationCredential cred = new AuthenticationCredential(hgdbGroup,
                                                                     null,
                                                                     myCredentials);
        Authenticator auth = hgdbGroup.getMembershipService().apply(cred);

        if (auth != null)
        {
//            authenticateMe(auth, user, passwd);

            if (auth.isReadyForJoin())
            {
                try
                {
//                    Credential myCred = hgdbGroup.getMembershipService().join(auth);
                    isJoined = true;
                    System.out.println("Group joined");

                    if (beRendevous)
                    {
                        System.out.println("Starting group rendezvous...");
                        hgdbGroup.getRendezVousService().startRendezVous();
                    }
                }
                catch (Throwable ex)
                {
                    System.out.println("incorrect password");
                }
            }
            else
                System.out.println("group not joined 1");
        }
        else
            System.out.println("group not joined 2");

        return isJoined;
    }

    void authenticateMe(Authenticator auth, String login, String password)
    {
        Method[] methods = auth.getClass().getMethods();

        try
        {
            for (int meth = 0; meth < methods.length; meth++)
            {
                if (methods[meth].getName().equals("setAuth1Identity"))
                {
                    Object[] authLogin =
                    { login };
                    methods[meth].invoke(auth, authLogin);
                }
                else if (methods[meth].getName().equals("setAuth2_Password"))
                {
                    Object[] authPassword =
                    { password };
                    methods[meth].invoke(auth, authPassword);
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private ModuleImplAdvertisement createGroupAdv() throws Exception
    {
        ModuleImplAdvertisement newGroupImpl;

        newGroupImpl = netPeerGroup.getAllPurposePeerGroupImplAdvertisement();

        newGroupImpl.setModuleSpecID(PROTECTED_GROUP_MSID);
        newGroupImpl.setDescription("HGDB Protected Group");

        StdPeerGroupParamAdv params = new StdPeerGroupParamAdv(
                                                               newGroupImpl.getParam());

        Map services = params.getServices();

//        ModuleImplAdvertisement membershipAdv = (ModuleImplAdvertisement) services.get(PeerGroup.membershipClassID);

//        services.remove(PeerGroup.membershipClassID);

/*        ModuleImplAdvertisement implAdv = (ModuleImplAdvertisement) AdvertisementFactory.newAdvertisement(ModuleImplAdvertisement.getAdvertisementType());

        implAdv.setModuleSpecID(PasswdMembershipService.passwordMembershipSpecID);
        implAdv.setCode(PasswdMembershipService.class.getName());

        implAdv.setCompat(membershipAdv.getCompat());
        implAdv.setUri(membershipAdv.getUri());
        implAdv.setProvider(membershipAdv.getProvider());
        implAdv.setDescription("Password Membership Service");

        services.put(PeerGroup.membershipClassID, implAdv); */
        newGroupImpl.setParam((Element) params.getDocument(MimeMediaType.XMLUTF8));

        return newGroupImpl;
    }

    public void join(ExecutorService executorService)
    {
    	advPublisher = new AdvPublisher();
        executorService.execute(advPublisher);
        
        advSubscriber = new AdvSubscriber();
        executorService.execute(advSubscriber);
    }

    public PeerGroup getPeerGroup()
    {
        return netPeerGroup;
    }

    public void publishAdv(Advertisement adv)
    {
        synchronized (this)
        {
            ownAdvs.addLast(adv);
        }
        netPeerGroup.getDiscoveryService().remotePublish(adv);
    }

    public Advertisement getPipeAdv()
    {
        return ownAdvs.getFirst();
    }

    public Set<Advertisement> getAdvertisements()
    {
        return peerAdvs.keySet();
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
            DiscoveryService discoveryService = hgdbGroup.getDiscoveryService();

            long expiration = 5 * 1000;// DiscoveryService.DEFAULT_EXPIRATION;
            long waittime = 5 * 1000;// DiscoveryService.DEFAULT_EXPIRATION;
            thisThread = Thread.currentThread();
            try
            {            	
                while (thisThread != null)
                {
                    for (Advertisement adv : ownAdvs)
                    {
                        // System.out.println("publishing: " +
                        // adv.getID().toString());
                        discoveryService.publish(adv, advTimetoLive, expiration);
                        discoveryService.remotePublish(adv, expiration);
                    }
                    try
                    {
                        Thread.sleep(waittime);
                    }
                    catch (Exception e)
                    {
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
            DiscoveryService discoveryService = hgdbGroup.getDiscoveryService();
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
                                                             null, null, 100,
                                                             null);
                    try
                    {
                        Thread.sleep(waittime);
                    }
                    catch (Exception e)
                    {
                        // ignored
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
                while (advs.hasMoreElements())
                {
                    adv = advs.nextElement();
                    if (adv instanceof PipeAdvertisement)
                    {
                        if (!peerAdvs.containsKey(adv))
                        {
                            PipeID pipeId = (PipeID) ((PipeAdvertisement) adv).getPipeID();
//                            System.out.println("Received new adv: " + adv);
                            if (!ownPipes.contains(pipeId))
                            {
                                peerAdvs.put(adv, null);
                                peerAdvIds.put(adv, peerName);
                                System.out.println("Add new advertisment, calling listeners.");
                                for (NetworkPeerPresenceListener listener : peerPresenceListeners)
                                    listener.peerJoined(adv);
                            }
//                            else
  //                              System.out.println("but in own pipes...");
                        }
                    }
                }
            }
        }

    }

    public boolean hasRemotePipes()
    {
        return !peerAdvs.isEmpty();
    }

    private void dumpNetworkConfig(NetworkConfigurator configurator)
    {
        System.out.println("Configuration: ");
        System.out.println("	PeerID = " + configurator.getPeerID().toString());
        System.out.println("	Name = " + configurator.getName());

    }

    public void addOwnPipe(PipeID pipeId)
    {
        ownPipes.add(pipeId);
    }

    public HGAtomPredicate getAtomInterests(Object peerId)
    {
        return peerAdvs.get(peerId);
    }

    public void setAtomInterests(Object peerId, HGAtomPredicate interest)
    {
        System.out.println("Peer " + ((PipeAdvertisement) peerId).getName()
                           + " is interested in " + interest);
        peerAdvs.put((Advertisement) peerId, interest);
    }

    public Object getPeerId(Object peer)
    {
        return peerAdvIds.get(peer);
    }

    public void waitForRemotePipe()
    {
        while (peerAdvs.isEmpty())
        {
            System.out.println("DefaultJXTANetwork: waiting for remote pipe advertisement");
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public List<RemotePeer> getConnectedPeers()
    {
        ArrayList<RemotePeer> peers = new ArrayList<RemotePeer>();
        for (Entry<Advertisement, String> advId : peerAdvIds.entrySet())
        {
            peers.add(new JXTARemotePeer(advId.getKey()));
        }

        return peers;
    }

    public RemotePeer getConnectedPeer(String peerName)
    {
        RemotePeer peer = null;
        for (Entry<Advertisement, String> advId : peerAdvIds.entrySet())
        {
            if (peerName.equals(((PipeAdvertisement) advId.getKey()).getName()))
            {
                peer = new JXTARemotePeer(advId.getKey());
                break;
            }
        }

        return peer;
    }

    public void addPeerPresenceListener(NetworkPeerPresenceListener listener)
    {
        peerPresenceListeners.add(listener);
    }

    public void removePeerPresenceListener(NetworkPeerPresenceListener listener)
    {
        peerPresenceListeners.remove(listener);
    }    
}