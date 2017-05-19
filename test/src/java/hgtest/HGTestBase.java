package hgtest;


import java.io.File;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class HGTestBase
{
    protected static HyperGraph graph;
    protected static HGConfiguration config = new HGConfiguration();
    
    public static void reopenDb()
    {
        graph.close();
        graph.open(graph.getLocation());
    }
    
    public static String getGraphLocation()
    {
        return T.getTmpDirectory() /* "/home/borislav/data" */ + File.separator + "hgtest"; 
    }
    
    public static HyperGraph getGraph()
    {
        return graph;
    }
    
    public static void openGraph()
    {
        GraphFactory gfac = HGUtils.getImplementationOf("hgtest.GraphFactory", "hgtest.DefaultGraphFactory");
        graph = gfac.createGraph(getGraphLocation());
    }
    
    @BeforeClass
    public static void setUp()
    {
    	try
    	{
	        HGUtils.dropHyperGraphInstance(getGraphLocation());
	        openGraph();
    	}
    	catch (Throwable t)
    	{
    		t.printStackTrace(System.err);
    	}
    }
    
    @AfterClass    
    public static void tearDown()
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