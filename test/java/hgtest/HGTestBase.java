package hgtest;

import java.io.File;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.Mapping;
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
        return /* T.getTmpDirectory() */ "/home/borislav/data" + File.separator + "hgtest"; 
    }
    
    public HyperGraph getGraph()
    {
        return graph;
    }
    
    @BeforeClass
    public void setUp()
    {
        graph = HGEnvironment.get(getGraphLocation());
        System.out.println("Using graph " + graph.getLocation());
    }
    
    @AfterClass    
    public void tearDown()
    {
        dropHyperGraphInstance(getGraphLocation());        
    }
    
    
    // backport 1.0, remove later
    
    
    public static void dropHyperGraphInstance(String location)
    {
        if (HGEnvironment.isOpen(location))
        {
            HGEnvironment.get(location).close();
        }
        directoryRecurse(new File(location), 
                         new Mapping<File, Boolean>()
                         {
                            public Boolean eval(File f)
                            {
                                return f.delete();
                            }
                         }
        );
    }
    
    public static void directoryRecurse(File top, Mapping<File, Boolean> mapping) 
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