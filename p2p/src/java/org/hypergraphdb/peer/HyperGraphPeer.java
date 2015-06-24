/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;



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

import mjson.Json;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.peer.bootstrap.AffirmIdentityBootstrap;
import org.hypergraphdb.peer.log.Log;
import org.hypergraphdb.peer.replication.GetInterestsTask;
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
public class HyperGraphPeer 
{
	/**
	 * The object used to configure the peer.
	 */
	private Json configuration;
	
	/**
	 * Holds any exception that prevents the peer from starting up.
	 */
	private Exception startupFailedException = null;
	
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
        Collections.synchronizedList(new ArrayList<PeerPresenceListener>());
    
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
		Json threadPoolSize = configuration.at(PeerConfig.THREAD_POOL_SIZE);
		if (threadPoolSize == null || threadPoolSize.asInteger() <= 0)
			executorService = Executors.newCachedThreadPool();
		else
			executorService = Executors.newFixedThreadPool(threadPoolSize.asInteger());			
		activityManager = new ActivityManager(this);		
	}
	
	/**
	 * Creates a peer from a JSON object.
	 * @param configuration
	 */
	public HyperGraphPeer(Json configuration)
	{
		this.configuration = configuration;
	}
	
	/**
	 * Creates a peer from a JSON object and a given local database.
	 * @param configuration
	 */
	public HyperGraphPeer(Json configuration, HyperGraph graph)
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
	
	public static HGPeerIdentity getIdentity(final HyperGraph graph, final String peerName)
	{
	    return graph.getTransactionManager().ensureTransaction(new Callable<HGPeerIdentity>() { public HGPeerIdentity call() {
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
            id.setId(graph.getHandleFactory().makeHandle());
            id.setGraphLocation(graph.getLocation());
            id.setHostname(localMachine.getHostName());
            id.setIpAddress(localMachine.getHostAddress());
//            id.setName(peerName);
            System.out.println("DEFINE PEER IDENTITY: " + peerName);
            graph.define(id.getId(), id);
            return identity = id.makePublicIdentity();
        }
        else if (all.size() > 1)
        {
//            for (PrivatePeerIdentity ii : all)
//                System.out.println("IDD : " + ii.getName() + " - " + ii.getId());
            throw new RuntimeException("More than one identity on peer - a bug or a malicious activity.");
        }
        else
        {
        	final PrivatePeerIdentity pid = all.get(0);
//            if (!HGUtils.eq(pid.getName(), peerName))
//            {
//                pid.setName(peerName);
//                System.out.println("UPDATE PEER IDENTITY: " + peerName);                
//                graph.update(pid);
//            }
            identity = pid.makePublicIdentity();
            if (!HGUtils.eq(identity.getHostname(), localMachine.getHostName()) ||
                !HGUtils.eq(identity.getIpAddress(), localMachine.getHostAddress()) ||
                !HGUtils.eq(identity.getGraphLocation(), graph.getLocation()))
            {
                // Need to create a new identity.
            	final java.net.InetAddress machine = localMachine;
//            	identity = graph.getTransactionManager().ensureTransaction(new Callable<HGPeerIdentity>() {
//            	public HGPeerIdentity call()
//            	{
//	                graph.remove(pid.getId());
//	                HGPersistentHandle newId = graph.getHandleFactory().makeHandle();
//	                pid.setId(newId);
//	                pid.setGraphLocation(graph.getLocation());
//	                pid.setHostname(machine.getHostName());
//	                pid.setIpAddress(machine.getHostAddress());
//	                System.out.println("REDEFINE PEER IDENTITY: " + peerName);
//	                graph.define(newId, pid);
//	                return pid.makePublicIdentity();
//            	}}, 
//            	HGTransactionConfig.DEFAULT);
                graph.remove(pid.getId());
                HGPersistentHandle newId = graph.getHandleFactory().makeHandle();
                pid.setId(newId);
                pid.setGraphLocation(graph.getLocation());
                pid.setHostname(machine.getHostName());
                pid.setIpAddress(machine.getHostAddress());
                System.out.println("REDEFINE PEER IDENTITY: " + peerName);
                graph.define(newId, pid);
                identity = pid.makePublicIdentity();
            }
            return identity;
        }
	    }});
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
	    return getIdentity(graph, configuration.at(PeerConfig.PEER_NAME, "HGDBPeer").asString());
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
		
	public static Json loadConfiguration(File configFile)	
	{
	    try
	    {
	        return Json.read(getContents(configFile));	            
	    } 
	    catch (IOException e)
	    {
	        throw new RuntimeException(e);	        
	    }
	}
	
	/**
	 * <p>Return <code>start()</code>, parameters ignored.</p>
	 * @deprecated
	 */
	public Future<Boolean> start(String ignored1, String ignored2)
	{
		return start();
	}
	
	/**
	 * Starts the peer and leaves it in a state where all its functions are available.
	 * 
	 * @param user The user name to use when the group is joined.
	 * @param passwd Password to use to authenticate against the group.
	 * @return
	 */
	public Future<Boolean> start()
	{
	    init();
	    this.startupFailedException = null;
	    return executorService.submit(new Callable<Boolean>() 
	    {
	    public Boolean call() {    
	    
		boolean status = false;
		
		if (configuration != null)
		{
			try
			{
				String option = configuration.at(PeerConfig.LOCAL_DB, "").asString();
				if (graph == null && !HGUtils.isEmpty(option))
				{
					graph = HGEnvironment.get(option);					
				}
				
				//load and start interface
				String peerInterfaceType = configuration.at(PeerConfig.INTERFACE_TYPE).asString();
				peerInterface = (PeerInterface)Class.forName(peerInterfaceType).getConstructor().newInstance();
				peerInterface.setThisPeer(HyperGraphPeer.this);
				Json interfaceConfig = configuration.at(PeerConfig.INTERFACE_CONFIG);
				if (interfaceConfig != null)
					peerInterface.configure(interfaceConfig);
				
				status = true;

				boolean managePresence = false; // manage presence only if AffirmIdentity activity is bootstrapped
				
                // Call all bootstrapping operations configured:                    
                Json bootstrapOperations = configuration.at(PeerConfig.BOOTSTRAP, Json.array());                 
                for (Json x : bootstrapOperations.asJsonList())
                {
                    String classname = x.at("class").asString();
                    if (AffirmIdentityBootstrap.class.getName().equals(classname))
                        managePresence = true;
                    if (classname == null)
                        throw new RuntimeException("No 'class' specified in bootstrap operation.");
                    Json config = x.at("config", Json.object());
                    //2012.03.28 Use HGUtils to get classloader so we can configure one.
                    ClassLoader cl = HGUtils.getClassLoader(graph);
                    BootstrapPeer boot = (BootstrapPeer)cl.loadClass(classname).newInstance();
                    //BootstrapPeer boot = (BootstrapPeer)Thread.currentThread().getContextClassLoader().loadClass(classname).newInstance();
                    boot.bootstrap(HyperGraphPeer.this, config);
                }       
                
                if (managePresence)
                    peerInterface.addPeerPresenceListener(
                       new NetworkPeerPresenceListener()
                       {
                           public void peerJoined(Object target)
                           {
//                               System.out.println("peer join: " + target);
                               if (getIdentity(target) != null) // already known?
                                   return;
//                               System.out.println("exchanging identity: " + target);                               
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
				status = false;
				HyperGraphPeer.this.startupFailedException = ex;
//			    ex.printStackTrace(System.err);			    
//			    HGUtils.throwRuntimeException(ex);
			}
		}
		else 
		{
			HyperGraphPeer.this.startupFailedException = 
				new Exception("Can not start HGBD: configuration not loaded");
		}
		
		return status;
		
	    }});
	}
	
	public void stop()
	{
	    //
	    // Gives chance to all threads to exit:
	    //
		try { activityManager.stop(); } catch (Throwable t) { }
		try 
		{
			if (peerInterface != null)
				peerInterface.stop();
		}
		catch (Throwable t) { }
		//
		// Force exit of any remaining running threads.
		//
		try { this.executorService.shutdownNow(); } catch (Throwable t) { }
		
		try { activityManager.clear();	  } catch (Throwable t) { }
		try { this.peerIdentities.clear(); } catch (Throwable t) { }
	    
		try 
		{
			if (tempGraph != null)
				tempGraph.close();
		}
		catch (Throwable t) { }
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
//			System.out.println("Handle found in local repository");
			return new HGStoreSubgraph(pHandle, graph.getStore());			
		}
		else 
		{
//			System.out.println("Handle NOT found in local repository");
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

//	public HyperGraph getTempDb()
//	{
//		return tempGraph;
//	}

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
    
    public void bindIdentityToNetworkTarget(final HGPeerIdentity id, final Object networkTarget)
    {
        synchronized (peerIdentities)
        {            
            HGPeerIdentity oldId = peerIdentities.getY(networkTarget);
            if (oldId != null && oldId.equals(id))
                return;
            peerIdentities.add(networkTarget, id);
            graph.getTransactionManager().transact(new Callable<Object>() {
              public Object call() {  
                if (graph.get(id.getId()) == null)
                    graph.define(id.getId(), id);
                else
                    graph.replace(id.getId(), id);
                return null;
              }
            });
            for (PeerPresenceListener listener : peerListeners)
                listener.peerJoined(id);
            //System.out.println("Added peer " + networkTarget + ":" + id + " to " + this.getIdentity());
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
    
    public Json getConfiguration()
    {
        return configuration;
    }

    /**
     * <p>Return the exception (if any) that prevents this peer from starting up.</p>
     */
    public Exception getStartupFailedException()
    {
    	return this.startupFailedException;
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