package org.hypergraphdb.peer;

import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.getOptPart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.peer.bootstrap.AffirmIdentityBootstrap;
import org.hypergraphdb.peer.log.Log;
import org.hypergraphdb.peer.replication.GetInterestsTask;
import org.hypergraphdb.peer.serializer.GenericSerializer;
import org.hypergraphdb.peer.serializer.JSONReader;
import org.hypergraphdb.peer.workflow.ActivityManager;
import org.hypergraphdb.peer.workflow.ActivityResult;
import org.hypergraphdb.peer.workflow.AffirmIdentity;
import org.hypergraphdb.storage.HGStoreSubgraph;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.TwoWayMap;

/**
 * 
 *
 * Main class for the local peer. It will start the peer (set up the interface and register in the network) given a configuration.
 * 
 * The class will wrap an existing HyperGraph instance. It is possible to create an instance of this class with an existing HyperGraph, or 
 * allow the peer to create its own based on the configuration properties.  
 * (if it is needed).
 * 
 * @author Cipri Costa
 */
@SuppressWarnings("unchecked")
public class HyperGraphPeer 
{
	/**
	 * The object used to configure the peer.
	 */
	private Map<String, Object> configuration;
	
	/**
	 * Object used for communicating with other peers
	 */
	private PeerInterface peerInterface = null;
	
	/**
	 * Manage the logic and scheduling of peer activities.
	 */
	private ActivityManager activityManager = null;
	
	/**
	 * A map of arbitrary objects shared between activities.
	 */
	private Map<String, Object> context = 
	    Collections.synchronizedMap(new HashMap<String, Object>());
	
	/**
	 * The local database of the peer. The peer will be listening on any changes to the local database and replciate them accordingly.
	 */
	private HyperGraph graph = null;
	
	/**
	 * The HGDB-based long term identity of this peer.
	 */
	private HGPeerIdentity identity = null;
	
    private TwoWayMap<Object, HGPeerIdentity> peerIdentities = 
        new TwoWayMap<Object, HGPeerIdentity>(); 
	
    private List<PeerPresenceListener> peerListeners = 
        new ArrayList<PeerPresenceListener>();
    
	/**
	 * Temporary storage for all types of things, including replication and subgraph serialization.
	 */
	private HyperGraph tempGraph = null;
		
	/**
	 * The log of operations that happened in the local database.
	 */
	private Log log;
	

	/**
	 * Execution of all peer asynchronous activities goes through this ExecutorService.  
	 */
	private ExecutorService executorService = null;
	
	// Assuming 'configuration' is set, initialize the rest of the member variables.
	private void init()
	{
		Number threadPoolSize = (Number)configuration.get("threadPoolSize");
		if (threadPoolSize == null || threadPoolSize.intValue() <= 0)
			executorService = Executors.newCachedThreadPool();
		else
			executorService = Executors.newFixedThreadPool(threadPoolSize.intValue());
		activityManager = new ActivityManager(this);		
	}
	
	/**
	 * Creates a peer from a JSON object.
	 * @param configuration
	 */
	public HyperGraphPeer(Map<String, Object> configuration)
	{
		this.configuration = configuration;
		init();
	}
	
	/**
	 * Creates a peer from a JSON object and a given local database.
	 * @param configuration
	 */
	public HyperGraphPeer(Map<String, Object> configuration, HyperGraph graph)
	{
		this(configuration);		
		this.graph = graph;
	}
	
	/**
	 * Creates a peer from a file containing the JSON object
	 * @param configFile
	 */
	public HyperGraphPeer(File configFile)
	{
		loadConfig(configFile);
		init();
	}
	
	/**
	 * Creates a peer from a file containing the JSON object and a given local database.
	 * @param configFile
	 */
	public HyperGraphPeer(File configFile, HyperGraph graph)
	{
		this(configFile);
		this.graph = graph;
	}
	
	public static HGPeerIdentity getIdentity(HyperGraph graph, String peerName)
	{
	    HGPeerIdentity identity = null;	    
        java.net.InetAddress localMachine = null;
        try
        {
            localMachine = java.net.InetAddress.getLocalHost();
//            System.out.println("Local machine identified: " + 
//                               localMachine.getHostName() + "/" +
//                               localMachine.getHostAddress());    
        }
        catch (UnknownHostException ex)
        {
            // TODO: how to we deal with this?
            ex.printStackTrace(System.err);             
        }       
        List<PrivatePeerIdentity> all = hg.getAll(graph, hg.type(PrivatePeerIdentity.class));       
        if (all.isEmpty())
        {             
            // Create new identity.
            PrivatePeerIdentity id = new PrivatePeerIdentity();
            id.setId(HGHandleFactory.makeHandle());
            id.setGraphLocation(graph.getLocation());
            id.setHostname(localMachine.getHostName());
            id.setIpAddress(localMachine.getHostAddress());
            id.setName(peerName);
            graph.define(id.getId(), id);
            return identity = id.makePublicIdentity();
        }
        else if (all.size() > 1)
            throw new RuntimeException("More than one identity on peer - a bug or a malicious activity.");
        else
        {
            if (!HGUtils.eq(all.get(0).getName(), peerName))
            {
                all.get(0).setName(peerName);
                graph.update(all.get(0));
            }
            identity = all.get(0).makePublicIdentity();
            if (!HGUtils.eq(identity.getHostname(), localMachine.getHostName()) ||
                !HGUtils.eq(identity.getIpAddress(), localMachine.getHostAddress()) ||
                !HGUtils.eq(identity.getGraphLocation(), graph.getLocation()))
            {
                // Need to create a new identity.
                graph.remove(identity.getId());
                HGPersistentHandle newId = HGHandleFactory.makeHandle();
                identity.setId(newId);
                all.get(0).setId(newId);
                graph.define(newId, all.get(0));
            }
            return identity;
        }
	    
	}
	
	/**
	 * <p>
	 * Return this peer's identity.
	 * </p>
	 */
	public HGPeerIdentity getIdentity()
	{
	    if (identity != null)
	        return identity;
	    if (graph == null)
	        throw new RuntimeException("Can't get peer identity because this peer is not bound to a graph.");
	    return getIdentity(graph, (String)getOptPart(configuration, "HGDBPeer", PeerConfig.PEER_NAME));
	}
	
	private void loadConfig(File configFile)
	{
		configuration = loadConfiguration(configFile);
	}
	
	private static String getContents(File file) throws IOException
	{
		StringBuilder contents = new StringBuilder();
	
		BufferedReader input =  new BufferedReader(new FileReader(file));
		try 
		{
			String line = null; 
			while (( line = input.readLine()) != null)
			{
				contents.append(line);
				contents.append(System.getProperty("line.separator"));
			}
		}
	    finally 
	    {
	    	input.close();
	    }

	    return contents.toString();
	}
		
	public static Map<String,Object> loadConfiguration(File configFile)	
	{
	    JSONReader reader = new JSONReader();
	    try
	    {
	        return (Map<String, Object>)getPart(reader.read(getContents(configFile)));	            
	    } 
	    catch (IOException e)
	    {
	        throw new RuntimeException(e);	        
	    }
	}
	
	/**
	 * Starts the peer and leaves it in a state where all its functions are available.
	 * 
	 * @param user The user name to use when the group is joined.
	 * @param passwd Password to use to authenticate against the group.
	 * @return
	 */
	public Future<Boolean> start(String user, String passwd)
	{
	    return executorService.submit(new Callable<Boolean>() 
	    {
	    public Boolean call() {    
	    
		boolean status = false;
		
		if (configuration != null)
		{
			//get required objects
			try
			{
				String option = getOptPart(configuration, null, PeerConfig.TEMP_DB);				
				if (!HGUtils.isEmpty(option))
				{
					tempGraph = HGEnvironment.get(option);
					GenericSerializer.setTempDB(tempGraph);
				}

				option = getOptPart(configuration, null, PeerConfig.LOCAL_DB);
				if (graph == null && !HGUtils.isEmpty(option))
				{
					graph = HGEnvironment.get(option);					
				}
				
				//load and start interface
				String peerInterfaceType = getPart(configuration, PeerConfig.INTERFACE_TYPE);
				peerInterface = (PeerInterface)Class.forName(peerInterfaceType).getConstructor().newInstance();
				peerInterface.setThisPeer(HyperGraphPeer.this);
				Map<String, Object> interfaceConfig = getPart(configuration, "interfaceConfig");
				if (interfaceConfig == null)
				    throw new RuntimeException("Missing interfaceConfig configuration parameter.");
				peerInterface.configure(interfaceConfig);
				
				status = true;

				boolean managePresence = false; // manage presence only if AffirmIdentity activity is bootstrapped
				
                // Call all bootstrapping operations configured:                    
                List<?> bootstrapOperations = getOptPart(configuration, null, "bootstrap");                 
                if (bootstrapOperations != null)
                    for (Object x : bootstrapOperations)
                    {
                        String classname = getPart(x, "class");
                        if (AffirmIdentityBootstrap.class.getName().equals(classname))
                            managePresence = true;
                        if (classname == null)
                            throw new RuntimeException("No 'class' specified in bootstrap operation.");
                        Map<String, Object> config = getPart(x, "config");
                        if (config == null)
                            config = new HashMap<String, Object>();
                        BootstrapPeer boot = (BootstrapPeer)Thread.currentThread().getContextClassLoader().loadClass(classname).newInstance();
                        boot.bootstrap(HyperGraphPeer.this, config);
                    }       
                
                if (managePresence)
                    peerInterface.addPeerPresenceListener(
                       new NetworkPeerPresenceListener()
                       {
                           public void peerJoined(Object target)
                           {
                               if (getIdentity(target) != null) // already known?
                                   return;
                               AffirmIdentity task = new AffirmIdentity(HyperGraphPeer.this, target);
                               activityManager.initiateActivity(task);
                           }
                           public void peerLeft(Object target) 
                           { 
                               unbindNetworkTargetFromIdentity(target); 
                           }
                       });					
             
				// the order of the following 3 statements is important
                activityManager.start();
	            peerInterface.setMessageHandler(activityManager);
				peerInterface.start();
				
                //configure services
                if (tempGraph != null)	                
	        		log = new Log(tempGraph, peerInterface);
			}
			catch (Exception ex)
			{			    
			    HGUtils.throwRuntimeException(ex);
			}
		}
		else 
		{
			System.out.println("Can not start HGBD: configuration not loaded");
		}
		
		return status;
		
	    }});
	}
	
	public void stop()
	{
	    //
	    // Gives chance to all threads to exit:
	    //
        activityManager.stop();	    
		if (peerInterface != null)
			peerInterface.stop();
		
		//
		// Force exit of any remaining running threads.
		//
		this.executorService.shutdownNow();
		
	    activityManager.clear();	 
	    this.peerIdentities.clear();
	    
		if (tempGraph != null)
			tempGraph.close();
	}
		
	/**
	 * Announces the interests of this peer. All the other peers that notice this announcement will send any content that matches the given predicate,
	 * regardless of whether this peer is on or off line.
	 * 
	 * @param pred An atom predicate that needs to be matched by an atom in order for any operations on the atom to be sent to this peer.
	 */
/*	public void setAtomInterests(HGAtomPredicate pred)
	{
		peerInterface.setAtomInterests(pred);
		
		PublishInterestsTask publishTask = new PublishInterestsTask(this, pred);
		publishTask.run();
	} */

	//TODO use streams?
	public StorageGraph getSubgraph(HGHandle handle)
	{
	    HGPersistentHandle pHandle = graph.getPersistentHandle(handle);
		if (graph.getStore().containsLink(pHandle))
		{
			System.out.println("Handle found in local repository");
			return new HGStoreSubgraph(pHandle, graph.getStore());			
		}
		else 
		{
			System.out.println("Handle NOT found in local repository");
			return null;
		}
	}

	/**
	 * will broadcast messages and update the peers knowledge of the neighboring peers
	 */
	public void updateNetworkProperties()
	{
		GetInterestsTask task = new GetInterestsTask(this);
		ActivityResult result = null;
		try
		{
		    result = activityManager.initiateActivity(task).get();
		}
		catch (Exception ex)
		{
		    throw new RuntimeException(ex);
		}
		if (result.getException() != null)
		    HGUtils.throwRuntimeException(result.getException());
	}

	public Log getLog()
	{
		return log;
	}

	public void setLog(Log log)
	{
		this.log = log;
	}

	public HyperGraph getGraph()
	{
		return graph;
	}

	/**
	 * 
	 * @return A list with all the connected peers in the form of RemotePeer objects.
	 */
	public Set<HGPeerIdentity> getConnectedPeers()
	{
	    return peerIdentities.getYSet();
	    /*
		List<RemotePeer> peers = peerInterface.getPeerNetwork().getConnectedPeers();
		
		for(RemotePeer peer : peers)
		{
			peer.setLocalPeer(this);
		}
		
		return peers; */
	}

	public HyperGraph getTempDb()
	{
		return tempGraph;
	}

	public ActivityManager getActivityManager()
	{
	    return activityManager;
	}
	
	public PeerInterface getPeerInterface()
	{
		return peerInterface;
	}
	
	public ExecutorService getExecutorService()
	{
		return executorService;
	}
	
    public HGPeerIdentity getIdentity(Object networkTarget)
    {
        synchronized (peerIdentities)
        {
            return peerIdentities.getY(networkTarget);
        }
    }
	
    public Object getNetworkTarget(HGPeerIdentity id)
    {
        synchronized (peerIdentities)
        {
            return peerIdentities.getX(id);
        }
    }
    
    public void bindIdentityToNetworkTarget(HGPeerIdentity id, Object networkTarget)
    {
        synchronized (peerIdentities)
        {            
            HGPeerIdentity oldId = peerIdentities.getY(networkTarget);
            if (oldId != null && oldId.equals(id))
                return;
            peerIdentities.add(networkTarget, id);
            graph.define(id.getId(), id);
            for (PeerPresenceListener listener : peerListeners)
                listener.peerJoined(id);
        }
    }
    
    public void unbindNetworkTargetFromIdentity(Object networkTarget)    
    {
        synchronized (peerIdentities)
        {        
            HGPeerIdentity id = peerIdentities.getY(networkTarget);
            if (id == null)
                return;
            peerIdentities.removeX(networkTarget);
            for (PeerPresenceListener listener : peerListeners)
                listener.peerLeft(id);            
        }            
    }
    
    /**
     * <p>
     * The <code>objectContext</code> is just a peer-global map of objects that
     * are shared between activities. Such objects can be instantiated at configuration
     * time by <code>BootstrapPeer</code> implementation and/or create, removed or
     * modified at a later time. The map is merely a convenience way to store and
     * refer to such peer-wide objects.
     * </p> 
     */
    public Map<String, Object> getObjectContext()
    {
        return context;
    }
    
    public Map<String, Object> getConfiguration()
    {
        return  configuration;
    }
    
    public void addPeerPresenceListener(PeerPresenceListener listener)
    {
        peerListeners.add(listener);
    }
    
    public void removePeerPresenceListener(PeerPresenceListener listener)
    {
        peerListeners.remove(listener);
    }
}