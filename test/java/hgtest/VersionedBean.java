package hgtest;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.CompositeIndexer;
import org.hypergraphdb.indexing.HGKeyIndexer;
import org.hypergraphdb.util.HGUtils;

public class VersionedBean
{
	private String id;
	private int ver;
	
	public String getId()
	{
		return id;
	}
	public void setId(String id)
	{
		this.id = id;
	}
	public int getVer()
	{
		return ver;
	}
	public void setVer(int ver)
	{
		this.ver = ver;
	}
	
	public String toString()
	{
		return "[" + id + "," + ver + "]";		
	}
	
	private static void add(HyperGraph graph, String id, int ver)
	{		
		VersionedBean x = new VersionedBean();
		x.setId(id);
		x.setVer(ver);
		graph.add(x);
	}
	
	@SuppressWarnings("unchecked")
    public static void main(String [] argv)
	{
		String location = "c:/temp/hgtest";
		HGUtils.dropHyperGraphInstance(location);
		HyperGraph graph = HGEnvironment.get(location);
		
		HGHandle type = graph.getTypeSystem().getTypeHandle(VersionedBean.class);
		
		CompositeIndexer indexer = new CompositeIndexer(type, new HGKeyIndexer[]{ new ByPartIndexer(type, "id"),
				   new ByPartIndexer(type, "ver")});
		graph.getIndexManager().register(indexer);
		graph.getIndexManager().register(new ByPartIndexer(type, "ver"));
		add(graph, "x", 1);
		add(graph, "x", 10);
		add(graph, "x", 5);
		
		add(graph, "t", 10);
		add(graph, "t", 2);
		add(graph, "t", 9);
		
		graph.close();
		
		graph = HGEnvironment.get(location);
		
		HGQuery query = HGQuery.make(graph, hg.and(hg.type(VersionedBean.class), hg.eq("ver", 9)));
		/*HGSearchResult<HGHandle> L = */query.execute();
		
		
		HGSortIndex idx = (HGSortIndex)graph.getIndexManager().getIndex(indexer);
		
		HGSearchResult<HGHandle> scan = idx.scanKeys();
		while (scan.hasNext())
		{
			System.out.println(graph.get((HGHandle)idx.findFirst(scan.next())));
		}
		
		VersionedBean bean = new VersionedBean();
		bean.setId("x");
		bean.setVer(2);
		HGSearchResult<HGHandle> rs = idx.findGT(indexer.getKey(graph, bean));
		if (!rs.hasNext())
			System.out.println("Oops, not found");
		else
		{
			bean = graph.get(rs.next());
			System.out.println("Found " + bean);
		}
		rs.close();
	}
}