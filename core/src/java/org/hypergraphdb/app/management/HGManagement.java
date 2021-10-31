package org.hypergraphdb.app.management;

import java.io.BufferedReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.type.TypeUtils;

/**
 * <p>
 * This class implements top-level management operations as static methods.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class HGManagement 
{
	public static boolean isInstalled(HyperGraph graph, HGApplication app)
	{		
		return hg.findOne(graph, hg.and(hg.type(app.getClass()), hg.eq("name", app.getName()))) != null;
	}
	
	public static void ensureInstalled(HyperGraph graph, HGApplication app)
	{
		if (!isInstalled(graph, app))
		{
			app.install(graph);
			try { graph.add(app); }
			catch (Exception ex) { app.uninstall(graph); }
		}
	}	
	
	public static void remove(HyperGraph graph, HGApplication app)
	{
		HGHandle h = hg.findOne(graph, hg.and(hg.type(app.getClass()), hg.eq("name", app.getName())));
		if (h != null)
		{
			app.uninstall(graph);
			graph.remove(h);
		}
	}
	
	public static void defineTypeClasses(HyperGraph graph, String resource)
	{
		InputStream in = null;
		try
		{
			in = HGManagement.class.getResourceAsStream(resource);
			Properties props = new Properties();
			props.load(in);
			for (Iterator<Map.Entry<Object, Object>> i = props.entrySet().iterator(); i.hasNext(); )
			{
				Map.Entry<Object, Object> e = (Map.Entry<Object, Object>)i.next();
				Class<?> clazz = Class.forName(e.getKey().toString().trim());
				HGPersistentHandle handle = graph.getHandleFactory().makeHandle(e.getValue().toString().trim());
				graph.getTypeSystem().defineTypeAtom(handle, clazz);
			}
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}
		finally
		{
			if (in != null) try { in.close(); } catch (Throwable t) { }
		}
	}
	
	public static void undefineTypeClasses(HyperGraph graph, String resource)
	{
		InputStream in = null;
		try
		{
			in = HGManagement.class.getResourceAsStream(resource);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			// We delete them in reverse order which gives some control over what's going
			// on.
			LinkedList<HGHandle> handles = new LinkedList<HGHandle>();
			for (String line = reader.readLine(); line != null; line = reader.readLine())
			{
				line = line.trim();
				if (line.startsWith("#") || line.length() == 0)
					continue;
				String [] tokens = line.split("="); 
				handles.addFirst(graph.getHandleFactory().makeHandle(tokens[1].trim()));
			}
			for (HGHandle h : handles)
			{
//				System.out.println("Deleting type: " + graph.get(h));
				if (!TypeUtils.deleteInstances(graph, h))
				{
					throw new Exception("Unable to delete type instances of type '" + h + "'");
				}
			}
			for (HGHandle h : handles)
			{
				graph.remove(h);
			}
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}
		finally
		{
			if (in != null) try { in.close(); } catch (Throwable t) { }
		}
	}
	
	public static void loadPrimitiveTypes(HyperGraph graph, String resource)
	{
		graph.getTypeSystem().storePrimitiveTypes(resource);		
	}
	
	// TODO: we need to unload 
}