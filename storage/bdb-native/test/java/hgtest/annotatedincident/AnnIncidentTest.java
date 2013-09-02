package hgtest.annotatedincident;

import java.util.Collections;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.query.And;
import org.hypergraphdb.storage.bdb.BDBStorageImplementation;
import org.hypergraphdb.storage.incidence.QueryByTypedIncident;
import org.hypergraphdb.storage.incidence.TypeAndPositionIncidenceAnnotator;
import org.hypergraphdb.storage.incidence.TypedIncidentCondition;
import org.hypergraphdb.storage.incidence.TypedIncidentToQuery;
import org.hypergraphdb.util.HGUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import hgtest.HGTestBase;
import hgtest.beans.BeanLink1;

public class AnnIncidentTest extends HGTestBase
{
    @BeforeClass
    public void setUp()
    {
        System.out.println("java.library.path"  + System.getProperty("java.library.path"));
        HGUtils.dropHyperGraphInstance(getGraphLocation());
        
        // open graph
        HGConfiguration config = new HGConfiguration();
        BDBStorageImplementation storage = new BDBStorageImplementation();
        storage.setIncidentAnnotator(new TypeAndPositionIncidenceAnnotator());
        config.setStoreImplementation(storage);
        this.graph = HGEnvironment.get(getGraphLocation(), config);
        config.getQueryConfiguration().addContractTransform(And.class, new QueryByTypedIncident());
        config.getQueryConfiguration().addCompiler(TypedIncidentCondition.class, new TypedIncidentToQuery());
    }
    
    @AfterClass    
    public void tearDown()
    {
        graph.close();
        HGUtils.dropHyperGraphInstance(getGraphLocation());
    }
    
    @Test
    public void testAnnotatedIncident()
    {
        HGHandle a = graph.add("node");
        HGHandle l1 = graph.add(new BeanLink1(0, null, a));
        HGQuery<HGHandle> q = hg.make(HGHandle.class, graph).compile(
            hg.and(hg.type(BeanLink1.class), hg.incident(a))
        );
        
        Assert.assertEquals(q.findInSet(), Collections.singleton(l1));
    }
    
    public static void main(String [] args)
    {
        AnnIncidentTest test = new AnnIncidentTest();
        try
        {
            test.setUp();
            test.testAnnotatedIncident();
            test.tearDown();
        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }
    }
}