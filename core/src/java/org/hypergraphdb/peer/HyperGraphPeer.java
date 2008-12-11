package org.hypergraphdb.peer;

import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.getOptPart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.log.Log;
import org.hypergraphdb.peer.serializer.GenericSerializer;
import org.hypergraphdb.peer.serializer.JSONReader;
import org.hypergraphdb.peer.workflow.CatchUpTaskClient;
import org.hypergraphdb.peer.workflow.GetInterestsTask;
import org.hypergraphdb.peer.workflow.PublishInterestsTask;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.util.HGUtils;

/**
 * @author Cipri Costa
 *
 * Main class for the local peer. It will start the peer (set up the interface and register in the network) given a configuration.
 * 
 * The class will wrap an existing HyperGraph instance. It is possible to create an instance of this class with an existing HyperGraph, or 
 * allow the peer to create its own based on the configuration properties.  
 * (if it is needed).
 * 
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
	 * The local database of the peer. The peer will be listening on any changes to the local database and replciate them accordingly.
	 */
	private HyperGraph graph = null;
	
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
	
	@SuppressWarnings("unchecked")
	private void loadConfig(File configFile)
	{
		JSONReader reader = new JSONReader();

		try
		{
			configuration = (Map<String, Object>)getPart(reader.read(getContents(configFile)));
			
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	private String getContents(File file) throws IOException
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
	
	/**
	 * Starts the peer and leaves it in a state where all its functions are available.
	 * 
	 * @param user The user name to use when the group is joined.
	 * @param passwd Password to use to authenticate against the group.
	 * @return
	 */
	public boolean start(String user, String passwd)
	{
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
				if (!HGUtils.isEmpty(option))
					graph = HGEnvironment.get(option);
				
				
				//load and start interface
				String peerInterfaceType = getPart(configuration, PeerConfig.INTERFACE_TYPE);
				peerInterface = (PeerInterface)Class.forName(peerInterfaceType).getConstructor().newInstance();
				
				if (peerInterface.configure(configuration))
				{
					status = true;
				
					peerInterface.run(executorService);
					
	                //configure services
	                if (tempGraph != null)	                
		        		log = new Log(tempGraph, peerInterface);
	        		//TODO: this should not be an indefinite wait ... 
	        		if (graph == null)
	                	peerInterface.getPeerNetwork().waitForRemotePipe();						

					// Call all bootstrapping operations configured:					
					List<?> bootstrapOperations = getOptPart(configuration, null, "bootstrap");					
					if (bootstrapOperations != null)
						for (Object x : bootstrapOperations)
						{
							String classname = getPart(x, "class");
							if (classname == null)
								throw new RuntimeException("No 'class' specified in bootstrap operation.");
							Map<String, Object> config = getPart(x, "config");
							if (config == null)
								config = new HashMap<String, Object>();
							BootstrapPeer boot = (BootstrapPeer)Class.forName(classname).newInstance();
							boot.bootstrap(this, config);
						} 
				}
			}
			catch(Exception ex)
			{
				ex.printStackTrace();				
			}
		}
		else 
		{
			System.out.println("Can not start HGBD: configuration not loaded");
		}
		
		return status;
	}
	
	/**
	 * Initializes a catch-up phase. During this all the known peers will be connected to see if any information has been sent to this peer 
	 * while it was off line. If there is any, the peer should not resume normal operations until this task completes. 
	 */
	public void catchUp()
	{
		CatchUpTaskClient catchUpTask = new CatchUpTaskClient(peerInterface, null, this);
		catchUpTask.run();
	}
	
	/**
	 * Announces the interests of this peer. All the other peers that notice this announcement will send any content that matches the given predicate,
	 * regardless of whether this peer is on or off line.
	 * 
	 * @param pred An atom predicate that needs to be matched by an atom in order for any operations on the atom to be sent to this peer.
	 */
	public void setAtomInterests(HGAtomPredicate pred)
	{
		peerInterface.setAtomInterests(pred);
		
		PublishInterestsTask publishTask = new PublishInterestsTask(peerInterface, pred);
		publishTask.run();
	}
	

	void stop()
	{
		
	}

/*	private HGHandle storeSubgraph(Subgraph subGraph, HGStore store)
	{
		return SubgraphManager.store(subGraph, store);
	}
*/	
	//TODO use streams?
	public Subgraph getSubgraph(HGHandle handle)
	{
		if (graph.getStore().containsLink((HGPersistentHandle)handle))
		{
			System.out.println("Handle found in local repository");
			return new Subgraph(graph, (HGPersistentHandle)handle);			
		}else {
			System.out.println("Handle NOT found in local repository");
			return null;
		}
	}

	/**
	 * will broadcast messages and update the peers knowledge of the neighboring peers
	 */
	public void updateNetworkProperties()
	{
		GetInterestsTask task = new GetInterestsTask(peerInterface);
		
		task.run();
	}

	public Log getLog()
	{
		return log;
	}

	public void setLog(Log log)
	{
		this.log = log;
	}

	public HyperGraph getHGDB()
	{
		return graph;
	}

	/**
	 * 
	 * @return A list with all the connected peers in the form of RemotePeer objects.
	 */
	public List<RemotePeer> getConnectedPeers()
	{
		List<RemotePeer> peers = peerInterface.getPeerNetwork().getConnectedPeers();
		
		for(RemotePeer peer : peers)
		{
			peer.setLocalPeer(this);
		}
		
		return peers;
	}
	
	/**
	 * Returns a remote peer with the given name (if it is connected at that point - otherwise null).
	 * 
	 * If multiple peers are registered with the same name, there is no guarantees as to which will be returned.
	 * 
	 * @param peerName
	 * @return
	 */
	public RemotePeer getRemotePeer(String peerName)
	{
		RemotePeer peer = peerInterface.getPeerNetwork().getConnectedPeer(peerName);
		peer.setLocalPeer(this);
		return peer;
	}

	public HyperGraph getTempDb()
	{
		return tempGraph;
	}

	public PeerInterface getPeerInterface()
	{
		return peerInterface;
	}
	
	public ExecutorService getExecutorService()
	{
		return executorService;
	}
}