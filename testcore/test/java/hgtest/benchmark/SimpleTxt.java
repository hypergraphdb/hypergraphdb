package hgtest.benchmark;

import org.hypergraphdb.*;

public class SimpleTxt
{
   public static void main(String[] argv)
   {
       HyperGraph graph = HGEnvironment.get("c:/data/graphs/hgphilip");       
       String name = "philippe";
       HGHandle currhandle = graph.add(name);

       long start = System.currentTimeMillis();
       for (int i = 1; i <= 20000; i++)
       {       
           graph.getTransactionManager().beginTransaction();               
           String current = "currrent" + i;//*j;
           currhandle = graph.add(current);
           HGValueLink link = new HGValueLink("rel", currhandle);
           graph.add(link);
           graph.getTransactionManager().commit();               
       }       
       System.out.println("Time=" + (System.currentTimeMillis()-start)/1000);
   }
}