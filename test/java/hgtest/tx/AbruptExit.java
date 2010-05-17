package hgtest.tx;

import java.io.File;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HyperGraph;

/**
 * This needs to be run repeatedly in a batch file - it will fail if the DB
 * gets corrupted. One can also manually stop the program while it is running.
 * You can pass the DB location as the first argument, otherwise /tmp/hgabruptexit
 * will be used. 
 * 
 * @author Borislav Iordanov
 *
 */
public class AbruptExit
{    
    static HyperGraph graph = null;
    
    public static void main(String [] argv)
    {
        String location = System.getProperty("java.io.tmpdir") + "hgabruptexit";
        if (argv.length > 0)
            location = argv[0];
        System.out.println("Using DB " + location);
        graph = HGEnvironment.get(location);
        try
        {
            Thread.sleep((long)(Math.random()*1000));
        }
        catch (InterruptedException ex)
        {
            
        }
        System.exit(-1);
    }
}
