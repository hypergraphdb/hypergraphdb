package hgtest;


import java.io.File;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class HGTestBase
{
    protected HyperGraph graph;
    protected HGConfiguration config = new HGConfiguration();
    
    public void reopenDb()
    {
        graph.close();
        graph.open(graph.getLocation());
    }
    
    public String getGraphLocation()
    {
        return T.getTmpDirectory() /* "/home/borislav/data" */ + File.separator + "hgtest"; 
    }
    
    public HyperGraph getGraph()
    {
        return graph;
    }
    
    public void openGraph()
    {
        GraphFactory gfac = HGUtils.getImplementationOf("hgtest.GraphFactory", "hgtest.DefaultGraphFactory");
        graph = gfac.createGraph(getGraphLocation());
    }
    
    @BeforeClass
    public void setUp()
    {
        HGUtils.dropHyperGraphInstance(getGraphLocation());
        openGraph();
    }
    
    @AfterClass    
    public void tearDown()
    {
        graph.close();
        HGUtils.dropHyperGraphInstance(getGraphLocation());
    }
    
    
    // backport 1.0, remove later
    
       
    static void directoryRecurse(File top, Mapping<File, Boolean> mapping) 
    {        
        File[] subs = top.listFiles();        
        if (subs != null) 
        {        
            for(File sub : subs)
            {
                if (sub.isDirectory()) 
                    directoryRecurse(sub, mapping);
                mapping.eval(sub);            
            }            
            mapping.eval(top);
        }        
    }      
}