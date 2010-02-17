package hgtest.jxta;

import java.beans.IntrospectionException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.peer.protocol.ObjectSerializer;

public class SerializationTest {
	public static void main(String[] args) throws IntrospectionException, IOException{
				
		testObject(null);
			
		testObject("a string");
		
		testObject(Integer.MAX_VALUE);
		testObject(0);
		
		testObject(new SimpleBean("test"));		
		
		testVariousTees();
		
		testArrays();
		
		testHGTypes();
		
		testSomeOtherJavaObjects();
	}

	private static void testSomeOtherJavaObjects() throws IOException
	{
		testObject(SimpleBean.class);
		
	}

	private static void testHGTypes() throws IOException
	{
		testObject(UUIDPersistentHandle.makeHandle());
		
	}

	private static void testArrays() throws IOException
	{
		testObject(new String[]{});
		
		testObject(new String[]{"a string"});
		
		testObject(new String[]{"string 1", "string 2"});
		
		testObject(new String[]{"string 1", null, "string 2"});
		
		testObject(new Object[]{0, null, "string 1", new SimpleBean("test")});
		
		Object array[] = null;
		
		array = new Object[1];
		array[0] = array;
		testObject(array);
		
		testObject(new Object[]{0, new SimpleBean("test"), new String[]{"string 1", "string 2"}});
	}

	private static void testVariousTees() throws IOException {

		TreeNode head = null;
		head = new TreeNode("0", null, null);
		testObject(head);
		
		TreeNode leaf1 = new TreeNode("l1", null, null);
		TreeNode leaf2 = new TreeNode("l2", null, null);
		head = new TreeNode("H", leaf1, leaf2);
		testObject(head);
		
		head = new TreeNode("head", null, null);
		head.setLeft(head);
		head.setRight(head);
		testObject(head);
		
		TreeNode node1 = new TreeNode("0", null, null);
		TreeNode node2 = new TreeNode("1", node1, node1);
		TreeNode node3 = new TreeNode("2", node1, node2);
		node1.setLeft(node3);
		
		testObject(node3);
		
	}
		
	public static void testObject(Object value) throws IOException{
		ObjectSerializer serializer = new ObjectSerializer();

		System.out.println("  Serialized: " + formatObject(value));
		
		ByteArrayOutputStream out = new ByteArrayOutputStream(); 
		serializer.serialize(out, value);
		
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		
		Object result = serializer.deserialize(in);
		
		System.out.println("Deserialized: " + formatObject(result));
	}
	
	public static String formatObject(Object data)
	{
		if (data == null) return "null";
		else
		{
			if (data.getClass().isArray())
			{
				String result = "Array (" + Integer.toString(((Object[])data).length) + "): ";
				for(int i=0;i<((Object[])data).length; i++)
				{
					//self reference ... 
					if (((Object[])data)[i] == data)
					{
						result += "refThisArray";
					}else{
						result += formatObject(((Object[])data)[i]) + "; ";						
					}
				}
				return result;
			}else{
				return data.toString();
			}
		}
	}
}
