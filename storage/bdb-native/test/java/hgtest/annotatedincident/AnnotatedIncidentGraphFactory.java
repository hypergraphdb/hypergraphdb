package hgtest.annotatedincident;

import org.hypergraphdb.HGConfiguration;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.bdb.BDBStorageImplementation;
import org.hypergraphdb.storage.incidence.QueryByTypedIncident;
import org.hypergraphdb.storage.incidence.TypeAndPositionIncidenceAnnotator;
import org.hypergraphdb.storage.incidence.TypedIncidentCondition;
import org.hypergraphdb.storage.incidence.TypedIncidentToQuery;

import hgtest.GraphFactory;

public class AnnotatedIncidentGraphFactory implements GraphFactory
{

    public HyperGraph createGraph(String location)
    {        
        HGConfiguration config = new HGConfiguration();
        BDBStorageImplementation storage = new BDBStorageImplementation();
        storage.setIncidentAnnotator(new TypeAndPositionIncidenceAnnotator());
        config.setStoreImplementation(storage);
        HyperGraph graph = HGEnvironment.get(location, config);
        config.getQueryConfiguration().addTransform(new QueryByTypedIncident(graph));
        config.getQueryConfiguration().addCompiler(TypedIncidentCondition.class, new TypedIncidentToQuery());
        return graph;
    }
}