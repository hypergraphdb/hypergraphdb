package hgtest;

import hgtest.beans.Ifc1;
import hgtest.beans.Ifc2;

import org.hypergraphdb.HGEnvironment;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.HGDepthFirstTraversal;
import org.hypergraphdb.algorithms.SimpleALGenerator;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.Top;
import org.hypergraphdb.util.Pair;

public class TestInterface {
	public static void main(String[] args) {
		String databaseLocation = args[0];
		HyperGraph graph = null;

		// ...
		try {
			graph = HGEnvironment.get(databaseLocation);
			HGTypeSystem typeSystem = graph.getTypeSystem();

		  HGHandle hdlIfc1Tp = typeSystem.getTypeHandle(Ifc1.class);
		  HGHandle hdlIfc2Tp = typeSystem.getTypeHandle(Ifc2.class);
		  HGHandle topHdl = typeSystem.getTypeHandle(Top.class);

		  HGSubsumes mysub = hg.getOne(graph, hg.orderedLink(hdlIfc1Tp, hdlIfc2Tp));
		  System.out.println("sub: " + mysub);
		  
		  HGDepthFirstTraversal traversal = 
		      new HGDepthFirstTraversal(topHdl, new SimpleALGenerator(graph));

		  while (traversal.hasNext()) {
		      Pair<HGHandle, HGHandle> current = traversal.next();
		      HGLink l = (HGLink)graph.get(current.getFirst());
		      Object atom = graph.get(current.getSecond());

		      if (l instanceof HGSubsumes) {
		      	HGSubsumes sub = (HGSubsumes)l;
		      	HGAtomType genType = typeSystem.getType(sub.getGeneral());
		      	HGAtomType specType = typeSystem.getType(sub.getSpecific());
			      System.out.println("Visiting atom " + atom + 
			                         " pointed to by subsumes(" + genType + "," + specType);
		      }
		      else {
			      System.out.println("Visiting atom " + atom + 
                " pointed to by " + l);
		      }
		  }
		}
		catch (Throwable t) {
			t.printStackTrace();
		}
		finally {
			if (graph != null) {
				graph.close();
			}
		}
	}
}
