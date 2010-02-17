package hgtest;

import java.io.File;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class HGTestBase
{
    protected HyperGraph graph;
    
    public void reopenDb()
    {
        graph.close();
        graph = HGEnvironment.get(getGraphLocation());
    }
    
    public String getGraphLocation()
    {
        return T.getTmpDirectory() + File.separator + "hgtest"; 
    }
    
    @BeforeClass
    public void setUp()
    {
        graph = HGEnvironment.get(getGraphLocation());
    }
    
    @AfterClass    
    public void tearDown()
    {
        HGUtils.dropHyperGraphInstance(getGraphLocation());        
    }
}