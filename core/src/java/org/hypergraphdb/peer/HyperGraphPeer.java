package org.hypergraphdb.peer;

import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.getOptPart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.log.Log;
import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.peer.serializer.GenericSerializer;
import org.hypergraphdb.peer.serializer.JSONReader;
import org.hypergraphdb.peer.workflow.CatchUpTaskClient;
import org.hypergraphdb.peer.workflow.CatchUpTaskServer;
import org.hypergraphdb.peer.workflow.GetInterestsTask;
import org.hypergraphdb.peer.workflow.PublishInterestsTask;
import org.hypergraphdb.peer.workflow.QueryTaskServer;
import org.hypergraphdb.peer.workflow.RememberTaskServer;
import org.hypergraphdb.query.HGAtomPredicate;

/**
 * @author Cipri Costa
 *
 * Main class for the local peer. It will start the peer (set up the interface and register in the network) given a configuration.
 * 
 * The class will wrap an existing HyperGraph instance. It is possible to create an instance of this class with an existing HyperGraph, or 
 * allow the peer to create its own based on the configuration properties. A separate hyperGraph instance will be created for the temporary storage 
 * (if it is needed).
 * 
 */
public class HyperGraphPeer 
{
	/**
	 * The object used to configure the peer.
	 */
	private Object configuration;
	
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
	 * Object that monitors the lcoal database and propagates changes accordingly.
	 */
	private StorageService storage;
	
	/**
	 * Creates a peer from a JSON object.
	 * @param configuration
	 */
	public HyperGraphPeer(Object configuration)
	{
		this.configuration = configuration;
	}
	
	/**
	 * Creates a peer from a JSON object and a given local database.
	 * @param configuration
	 */
	public HyperGraphPeer(Object configuration, HyperGraph graph)
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
	
	private void loadConfig(File configFile)
	{
		JSONReader reader = new JSONReader();

		try
		{
			configuration = getPart(reader.read(getContents(configFile)));
			
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
				boolean hasTempDb = (Boolean)getOptPart(configuration, true, PeerConfig.HAS_TEMP_STORAGE);
				if (hasTempDb)
				{
					//create cache database - this should eventually be an actual cache, not just another database
					tempGraph = HGEnvironment.get((String)getOptPart(configuration, ".tempdb", PeerConfig.TEMP_DB));
				}
				
				GenericSerializer.setTempDB(tempGraph);

				//create the local database
				boolean hasLocalStorage = (Boolean)getPart(configuration, PeerConfig.HAS_LOCAL_STORAGE);
				if (hasLocalStorage && (graph == null))
				{
					graph = HGEnvironment.get((String)getOptPart(configuration, ".hgdb", PeerConfig.LOCAL_DB));
				}
				
				//load and start interface
				String peerInterfaceType = (String)getPart(configuration, PeerConfig.INTERFACE_TYPE);
				peerInterface = (PeerInterface)Class.forName(peerInterfaceType).getConstructor().newInstance();
				
				if (peerInterface.configure(configuration, user, passwd))
				{
					status = true;
				
					Thread thread = new Thread(peerInterface, "peerInterface");
	                thread.start();
	                
	                //configure services
	                if (hasLocalStorage || hasTempDb)
	                {
	        			registerTasks();

		        		log = new Log(tempGraph, peerInterface);

		        		//TODO: this should not be an indefinite wait ... 
		        		if (!hasLocalStorage)
		        		{
		                	peerInterface.getPeerNetwork().waitForRemotePipe();
		                }
		        		
						storage = new StorageService(graph, tempGraph, peerInterface, log);

	                }	
				}
			}
			catch(Exception ex)
			{
				System.out.println("Can not start HGBD: " + ex);
			}
		}
		else 
		{
			System.out.println("Can not start HGBD: configuration not loaded");
		}
		
		return status;
	}

	/**
	 * Registers the tasks that can be created by the peer interface when a message is received.
	 */
	private void registerTasks()
	{
		peerInterface.registerTaskFactory(Performative.CallForProposal, HGDBOntology.REMEMBER_ACTION, new RememberTaskServer.RememberTaskServerFactory(this));
		peerInterface.registerTaskFactory(Performative.Request, HGDBOntology.ATOM_INTEREST, new PublishInterestsTask.PublishInterestsFactory());
		peerInterface.registerTaskFactory(Performative.Request, HGDBOntology.QUERY, new QueryTaskServer.QueryTaskFactory(this));
		peerInterface.registerTaskFactory(Performative.Request, HGDBOntology.CATCHUP, new CatchUpTaskServer.CatchUpTaskServerFactory(this));
		peerInterface.registerTaskFactory(Performative.Inform, HGDBOntology.ATOM_INTEREST, new GetInterestsTask.GetInterestsFactory());
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

	/**
	 * Registers an application provided type in the database type systems. 
	 * All peers that handle a given type must have the type registered
	 * a priori (and with the same handle).
	 * @param handle
	 * @param clazz
	 */
	public void registerType(HGPersistentHandle handle, Class<?> clazz)
	{
		if (storage != null)
		{
			storage.registerType(handle, clazz);
		}
	}

	public HyperGraph getHGDB()
	{
		return graph;
	}

	public StorageService getStorage()
	{
		return storage;
	}

	public void setStorage(StorageService storage)
	{
		this.storage = storage;
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
}