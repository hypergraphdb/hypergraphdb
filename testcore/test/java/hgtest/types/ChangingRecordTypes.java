package hgtest.types;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.annotation.HGIgnore;
import org.hypergraphdb.type.HGAtomType;
import org.testng.annotations.Test;

import hgtest.HGTestBase;

public class ChangingRecordTypes extends HGTestBase
{
    public static class Bean 
    {
        private String name;   
        //@HGIgnore
        private String name2;
        
        public String getName()
        {
            return name;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        public String getName2()
        {
            return name2;
        }

        public void setName2(String name2)
        {
            this.name2 = name2;
        }
        public String toString()
        {
            return name + " -- " + name2;
        }
        
//        public String toString()
//        {
//            return name;
//        }                
    }

    public void addData()
    {
        Bean b = new Bean();
        b.setName("hyperhyper");
        b.setName2("second");
        graph.add(b);     
        
        this.reopenDb();
        
        for (Bean x : (List<Bean>)(List)graph.getAll(hg.type(Bean.class)))
            System.out.println(x);
    }
    
    public void updateType(final Class<?> clazz)
    {
        graph.getTransactionManager().ensureTransaction(new Callable<Object>(){
            public Object call()
            {
                HGHandle typeHandle = graph.getTypeSystem().getTypeHandle(clazz);
                HGHandle tmpHandle = graph.getHandleFactory().makeHandle();
                URI typeUri = graph.getTypeSystem().getSchema().toTypeURI(clazz);
                graph.getTypeSystem().getSchema().defineType(typeUri, tmpHandle);
                HGAtomType newType = graph.get(tmpHandle);
                graph.replace(typeHandle, newType);
                graph.remove(tmpHandle);
                return null;
            }
        });        
    }
   
    public void listThem(String s)
    {
        System.out.println(s);
        for (Bean x : (List<Bean>)(List)graph.getAll(hg.type(Bean.class)))
            System.out.println(x);
    }
    
	@Test 
	public void testFieldAdded()
	{
        for (Bean x : (List<Bean>)(List)graph.getAll(hg.type(Bean.class)))
            System.out.println(x);
	    
	    graph.getTransactionManager().ensureTransaction(new Callable<Object>(){
	        public Object call()
	        {
	            HGHandle typeHandle = graph.getTypeSystem().getTypeHandle(Bean.class);
	            HGHandle dummyType = graph.getHandleFactory().makeHandle();
	            URI typeUri = graph.getTypeSystem().getSchema().toTypeURI(Bean.class);
	            graph.getTypeSystem().getSchema().defineType(typeUri, dummyType);
	            HGAtomType newType = graph.get(dummyType);
	            graph.replace(typeHandle, newType);
	            graph.remove(dummyType);
	            return null;
	        }
	    });
        this.reopenDb();
//        for (Bean x : (List<Bean>)(List)graph.getAll(hg.type(Bean.class)))
//            x.setName2("Another " + x.getName());
        
        Bean b = new Bean();
        b.setName("hyperhyperTypechanged");
        b.setName2("second typed changed");
        graph.add(b);     

        listThem("After type change and new atoms added...");
	}	
}