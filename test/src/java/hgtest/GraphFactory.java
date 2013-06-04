package hgtest;

import org.hypergraphdb.HyperGraph;

public interface GraphFactory
{
    HyperGraph createGraph(String location);    
}