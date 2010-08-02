package hgtest.types;

import org.hypergraphdb.*;

import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.type.HGAtomType;

public class TestPredefined 
{	
	public static void main(String [] argv)
	{
		HyperGraph graph = new HyperGraph("c:/temp/hgpredefined");		
		try
		{
			HGPersistentHandle TYPEHANDLE = graph.getHandleFactory().makeHandle("7023c8e3-3ae4-11dc-872e-b08ac7fa685c");
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