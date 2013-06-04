package hgtest;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HyperGraph;


public class DefaultGraphFactory implements GraphFactory
{
    public HyperGraph createGraph(String location)
    {
        return HGEnvironment.get(location, new HGConfiguration());
    }
}