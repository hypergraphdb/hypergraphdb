package org.hypergraphdb.maintenance;

import java.net.URI;

import org.hypergraphdb.*;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.BAtoString;
import org.hypergraphdb.type.JavaTypeSchema;

public class Upgrade
{
	private static void from11to12(String location)
	{
		HyperGraph graph = HGEnvironment.get(location);
		// Change classnames to URIs for runtime type mapping
		HGIndex classtohg = graph.getStore().getBidirectionalIndex("hg_typesystem_java2hg_types",
				BAtoString.getInstance(),
                BAtoHandle.getInstance(graph.getHandleFactory()),
                null,
                true);
		HGIndex uritohg = graph.getStore().getBidirectionalIndex("hg_typesystem_uri2hg_types",
				BAtoString.getInstance(),
                BAtoHandle.getInstance(graph.getHandleFactory()),
                null,
                true);
		
		HGSearchResult<String> rs = classtohg.scanKeys();
		while (rs.hasNext())
		{
			String classname = rs.next();
			URI uri = JavaTypeSchema.classNameToURI(classname);
			uritohg.addEntry(uri.toString(), classtohg.findFirst(classname));
		}
		rs.close();
		graph.close();
	}
	
	public static void main(String [] argv)
	{
		from11to12("c:/data/becky");
	}
}
