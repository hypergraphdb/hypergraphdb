package hgtest.utils;

import java.util.ArrayList;

import hgtest.T;
import hgtest.beans.BeanLink1;
import hgtest.beans.SimpleBean;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HyperGraph;

public class DataSets
{
	public static void populate(HyperGraph graph, int nodes)
	{
		ArrayList<HGHandle> L2 = new ArrayList<HGHandle>();
		ArrayList<HGHandle> L3 = new ArrayList<HGHandle>();
		ArrayList<HGHandle> L4 = new ArrayList<HGHandle>();
		ArrayList<HGHandle> L5 = new ArrayList<HGHandle>();
		
		for (int i = 0; i < nodes; i++)
		{
			int x = i % 100;
			SimpleBean node = new SimpleBean();
			node.setIntProp(i);
			HGHandle nodeHandle = graph.add(node);
			if (x < 5)
			{
				graph.add(new HGPlainLink(nodeHandle));
			}
			else if (x < 10)
			{
				L5.add(nodeHandle);
				if (L5.size() == 5)
				{
					graph.add(new HGPlainLink(L5.toArray(new HGHandle[0])));
					L5.clear();
				}
			}
			else if (x < 30)
			{
				L3.add(nodeHandle);
				if (L3.size() == 3)
				{
					graph.add(new HGPlainLink(L3.toArray(new HGHandle[0])));
					L3.clear();
				}
			}
			else if (x < 50)
			{
				L4.add(nodeHandle);
				if (L4.size() == 4)
				{
					graph.add(new HGPlainLink(L4.toArray(new HGHandle[0])));
					L4.clear();
				}
			}
			else
			{
				L2.add(nodeHandle);
				if (L2.size() == 2)
				{
					graph.add(new HGPlainLink(L2.toArray(new HGHandle[0])));
					L2.clear();
				}
			}
		}
	}
}