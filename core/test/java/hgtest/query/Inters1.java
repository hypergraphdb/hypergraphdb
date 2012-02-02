package hgtest.query;

import java.util.ArrayList;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.indexing.ByPartIndexer;


public class Inters1
{
	HyperGraph graph;
	
	void prepare()
	{
		HGHandle th = graph.getTypeSystem().getTypeHandle(IntTuple.class);
		graph.getIndexManager().register(new ByPartIndexer(th, "x"));
		graph.getIndexManager().register(new ByPartIndexer(th, "y"));
		graph.getIndexManager().register(new ByPartIndexer(th, "z"));
		graph.getIndexManager().register(new ByPartIndexer(th, "w"));
	}
	
	void go()
	{
		for (int i = 0; i < 1000; i++)
		{
			IntTuple bean = new IntTuple();
			bean.setX((int)(Math.random()*100));
			bean.setY((int)(Math.random()*100));
			bean.setZ((int)(Math.random()*100));
			bean.setW((int)(Math.random()*100));
			
			HGHandle existing = hg.findOne(graph, 
				hg.and(hg.type(IntTuple.class), 
					   hg.eq("x", bean.getX()),
					   hg.eq("y", bean.getY()),
					   hg.eq("z", bean.getZ()),
					   hg.eq("w", bean.getW())));
			if (existing == null)
				graph.add(bean);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		Inters1 t = new Inters1();
		t.graph = HGEnvironment.get("c:/temp/graphs/test");
		t.prepare();
		ArrayList<Long> times = new ArrayList<Long>();
		for (int i = 0; i < 10; i++)
		{
			long start = System.currentTimeMillis();
			t.go();
			times.add(System.currentTimeMillis() - start);
		}
		t.graph.close();
		for (Long x : times)
			System.out.println((double)x/1000.0);
	}
}