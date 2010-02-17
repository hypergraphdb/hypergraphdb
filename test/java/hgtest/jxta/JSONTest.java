package hgtest.jxta;

import static org.hypergraphdb.peer.Structs.getHGAtomPredicate;
import static org.hypergraphdb.peer.Structs.getHGQueryCondition;
import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.hgPredicate;
import static org.hypergraphdb.peer.Structs.list;
import static org.hypergraphdb.peer.Structs.object;
import static org.hypergraphdb.peer.Structs.struct;
import static org.hypergraphdb.peer.Structs.svalue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import net.jxta.platform.NetworkManager;

import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.peer.Structs;
import org.hypergraphdb.peer.protocol.ObjectSerializer;
import org.hypergraphdb.peer.serializer.JSONReader;
import org.hypergraphdb.peer.serializer.JSONWriter;
import org.hypergraphdb.query.AtomPartCondition;
import org.hypergraphdb.query.AtomProjectionCondition;
import org.hypergraphdb.query.ComparisonOperator;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.MapCondition;
import org.hypergraphdb.query.Nothing;
import org.hypergraphdb.query.SubsumedCondition;
import org.hypergraphdb.query.SubsumesCondition;
import org.hypergraphdb.query.TypedValueCondition;
import org.hypergraphdb.query.impl.LinkProjectionMapping;

public class JSONTest
{
	public static void main(String[] args)
	{	
		testJXTAConfig();
//		testTypePerserved();
		
/*		doValue(struct("test", new Timestamp(100)));

		
		Object value = struct(PERFORMATIVE, Performative.CallForProposal, ACTION, REMEMBER_ACTION, 
				SEND_TASK_ID, UUID.randomUUID());
		doSerialize(value);
				
		System.out.println(getPart(value, SEND_TASK_ID));	
		System.out.println(getPart(value, PERFORMATIVE));	
*/		
//		testQueries();

		//testCustomObjects();
		
		//testMessages();
	}
	private static void testJXTAConfig()
	{
		Object jxtaConf = Structs.struct("peerName", "nameOfPeer", 
				"peerGroup", "nameOfGroup", 
				"jxta", Structs.struct(
					"advTimeToLive", 1*10*1000,
					"needsRendezVous", false,
					"needsRelay", false,
					"mode", NetworkManager.ConfigMode.ADHOC
				));

		doValue(jxtaConf);
		
		String jxtaConfString = getContents("./jxtaConfig");
		System.out.println(jxtaConfString);
		
		JSONReader reader = new JSONReader();

		Object result = null;
		result = getPart(reader.read(jxtaConfString));
	
		System.out.println("read: " + result);

		
		/*		JXTAPeerConfiguration jxtaConf = new JXTAPeerConfiguration();
		jxtaConf.setPeerName("peerName");
		jxtaConf.setPeerGroupName("groupName");
		jxtaConf.setAdvTimeToLive(1*5*1000);//set to 5 minutes
		//jxtaConf.setNeedsRdvConn(true);
		jxtaConf.setNeedsRdvConn(false);
		//jxtaConf.setNeedsRelayConn(true);
		jxtaConf.setNeedsRelayConn(false);
		jxtaConf.setMode(NetworkManager.ConfigMode.ADHOC);
*/
		
	}
	
	private static String getContents(String fileName) 
	{
		StringBuilder contents = new StringBuilder();
	
		try 
		{
			BufferedReader input =  new BufferedReader(new FileReader(new File(fileName)));
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
	    }
	    catch (IOException ex)
	    {
	      ex.printStackTrace();
	    }
	    
	    return contents.toString();
	}
	
	private static void testTypePerserved()
	{
		AtomPartCondition cond = hg.part("a", 1, ComparisonOperator.EQ);
		
		Object result = doValue(cond);
		
		if (result.getClass().equals(AtomPartCondition.class))
		{
			System.out.println(((AtomPartCondition)result).getValue().getClass());
		}
		
	}
	private static void testCustomObjects() throws IOException
	{
		doSerialize(struct("custom", object("test"), "standard", hg.arity(100)));
		doSerialize(struct("test", list(object("test1"),object("test2"),object("test3"))));
		doSerialize(struct("test", list(object("test1"),object("test2"),struct("a", object("test3"), "b", object("test4")))));
	}
	
	
	private static void testMessages() throws IOException
	{
		Object result;
		result = doSerialize(
			list("call-for-proposal",
			    struct("action", "remember-all",
			             "predicate", hgPredicate(hg.type(String.class))))
		);
		
		System.out.println(getPart(result, 0));
		System.out.println(getPart(result, 1, "action"));
		System.out.println(getPart(result, 1, "predicate"));
		System.out.println(getPart(result, 1, "predicate", 1, "javaClass"));
	}
	
	public static Object doSerialize(Object value) throws IOException
	{
		System.out.println("  Serialized: " + value);

		ObjectSerializer serializer = new ObjectSerializer();
		
		ByteArrayOutputStream out = new ByteArrayOutputStream(); 
		serializer.serialize(out, value);

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

		Object result = serializer.deserialize(in);
		
		System.out.println("Deserialized: " + result);	
		
		return result;
	}

	public static void testQueries()
	{	
		
		doValue(Nothing.Instance);
		
		//all
		doValue(hg.all());

		//arity
		doValue(hg.arity(100));

		//atompart
		doValue(hg.part("test", "value", ComparisonOperator.EQ));
		doValue(hg.part("a.b.c.d.e", true, ComparisonOperator.EQ));		
			
		//type
		doValue(hg.type(String.class));
		
		//value
		doValue(hg.value(100.12, ComparisonOperator.GTE));
		
		//bfs
		doValue(hg.bfs(UUIDPersistentHandle.makeHandle()));
		
		//dfs
		doValue(hg.dfs(UUIDPersistentHandle.makeHandle()));

		//incident
		doValue(hg.incident(UUIDPersistentHandle.makeHandle()));
		
		//orderedLink
		doValue(hg.orderedLink(UUIDPersistentHandle.makeHandle(), UUIDPersistentHandle.makeHandle(), UUIDPersistentHandle.makeHandle()));
		
		//subsumed
		doValue(hg.subsumed(UUIDPersistentHandle.makeHandle()));
		doValue(new SubsumedCondition("test"));
		
		//subsumes
		doValue(hg.subsumes(UUIDPersistentHandle.makeHandle()));
		doValue(new SubsumesCondition("test"));
		
		//target
		doValue(hg.target(UUIDPersistentHandle.makeHandle()));
		
		//typed value
		doValue(new TypedValueCondition(String.class, "test"));
		doValue(new TypedValueCondition(UUIDPersistentHandle.makeHandle(), "test"));
		
		//type plus
		doValue(hg.typePlus(String.class));
		doValue(hg.typePlus(UUIDPersistentHandle.makeHandle()));
		
		//composite
		//projection
		doValue(new AtomProjectionCondition("test", hg.all()));
		
		//not
		doValue(hg.not(hg.arity(1)));

		//and
		doValue(hg.and());
		doValue(hg.and(hg.all(), hg.arity(1)));	
		
		//or
		doValue(hg.or());
		doValue(hg.or(hg.arity(100), hg.arity(1)));	
		
		doValue(new MapCondition(hg.all(), new LinkProjectionMapping(1)));
			
		doValue(hg.link(UUIDPersistentHandle.makeHandle(), UUIDPersistentHandle.makeHandle(), UUIDPersistentHandle.makeHandle()));
	}
	
	public static Object doValue(Object x)
	{
		JSONWriter writer = new JSONWriter(false);
	
		Object svalue = svalue(x);
		String strValue = writer.write(svalue);		
		System.out.println(strValue);
	
		JSONReader reader = new JSONReader();

		Object result = null;
		if (x instanceof HGAtomPredicate) result = getHGAtomPredicate(reader.read(strValue));
		else if (x instanceof HGQueryCondition) result = getHGQueryCondition(reader.read(strValue));
		else result = getPart(reader.read(strValue));
	
		System.out.println("read: " + result);
		
		return result;
	}
}
