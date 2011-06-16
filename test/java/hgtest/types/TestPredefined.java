package hgtest.types;

import java.net.URI;

import org.hypergraphdb.*;

import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.util.HGUtils;

public class TestPredefined 
{	
	public static void main(String [] argv)
	{
	    System.out.println(byte[][][].class.getName());
	    String tmpdir = System.getProperty("java.io.tmpdir");
	    System.out.println("Using tmp dir " + tmpdir);
	    String location = tmpdir + "/hgpredefined";
	    HGUtils.dropHyperGraphInstance(location);
		HyperGraph graph = new HyperGraph(location);		
		try
		{
			HGPersistentHandle TYPEHANDLE = graph.getHandleFactory().makeHandle("7023c8e3-3ae4-11dc-872e-b08ac7fa685c");
			HGAtomType type = new APredefinedType();
			graph.getTypeSystem().addPredefinedType(TYPEHANDLE, type, (URI)null);		
			HGHandle h = graph.add(new TestPredefined(), TYPEHANDLE);
			HGSearchResult rs = graph.find(hg.type(TYPEHANDLE));
			try
			{
				while (rs.hasNext())
				{
					HGHandle hh = (HGHandle)rs.next();
					System.out.println("atom " + graph.get(hh));
				}
			}
			finally
			{
				rs.close();
			}
		}
		finally
		{
			graph.close();
		}
	}
}