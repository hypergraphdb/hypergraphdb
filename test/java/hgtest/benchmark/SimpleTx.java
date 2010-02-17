package hgtest.benchmark;

import org.hypergraphdb.*;

public class SimpleTx
{
    public static void main(String[] argv)
    {
        HyperGraph graph = HGEnvironment.get("/tmp/hgphilip");
        graph.getTransactionManager().beginTransaction();
        String name = "philippe";
        HGHandle handleName = graph.add(name);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 20000; i++)
        {
            String current = "currrent" + i;
            HGHandle currhandle = graph.add(current);
            HGValueLink link = new HGValueLink("rel", handleName);
            graph.add(link);

            graph.getTransactionManager().commit();
            graph.getTransactionManager().beginTransaction();

        }
        graph.getTransactionManager().commit();
        
        System.out.println("Time=" + (System.currentTimeMillis()-start)/1000);
    }
}
