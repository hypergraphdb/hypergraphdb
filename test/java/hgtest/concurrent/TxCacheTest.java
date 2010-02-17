package hgtest.concurrent;

import org.hypergraphdb.*;
import org.hypergraphdb.HGQuery.hg;

public class TxCacheTest
{
    public static void main(String [] argv)
    {
        String graphLocation = "/tmp/txgraph"; 
        HyperGraph graph = HGEnvironment.get(graphLocation);
        System.out.println("Atom count " + hg.count(graph, hg.all()));
        graph.close();
    }
}