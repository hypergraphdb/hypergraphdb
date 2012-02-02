package hgtest;

import java.io.File;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class HGTestBase
{
    protected HyperGraph graph;
    
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
    
    @BeforeClass
    public void setUp()
    {
        HGUtils.dropHyperGraphInstance(getGraphLocation());
        graph = HGEnvironment.get(getGraphLocation(), new HGConfiguration());
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