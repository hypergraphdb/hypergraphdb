package hgtest.types;

import org.hypergraphdb.*;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.handle.UUIDHandleFactory;
import org.hypergraphdb.type.HGAtomType;

public class TestPredefined 
{
	private static final HGPersistentHandle TYPEHANDLE = UUIDHandleFactory.I.makeHandle("7023c8e3-3ae4-11dc-872e-b08ac7fa685c");
	
	public static void main(String [] argv)
	{
		HyperGraph graph = new HyperGraph("c:/temp/hgpredefined");
		
		try
		{
			HGAtomType type = new APredefinedType();
			graph.getTypeSystem().addPredefinedType(TYPEHANDLE, type, null);		
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