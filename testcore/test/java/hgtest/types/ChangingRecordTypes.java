package hgtest.types;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.annotation.HGIgnore;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGProjection;
import org.hypergraphdb.type.JavaBeanBinding;
import org.hypergraphdb.type.Record;
import org.hypergraphdb.type.RecordType;
import org.hypergraphdb.type.Slot;
import org.hypergraphdb.util.HGUtils;
import org.junit.Test;

import hgtest.HGTestBase;

public class ChangingRecordTypes extends HGTestBase
{
//    public static class Bean 
//    {
//        private String name;           
//        public String getName() { return name; }
//        public void setName(String name) { this.name = name; }
//        public String toString() { return "Bean.name=" + name; }
//    }
//    public void addData()
//    {
//        Bean b = new Bean();
//        b.setName("First1 Last1");
//        graph.add(b);     
//        b = new Bean();
//        b.setName("First2 Last2");
//        graph.add(b);
//        for (Bean x : (List<Bean>)(List)graph.getAll(hg.type(Bean.class)))
//            System.out.println(x);
//    }

	public static class Bean
	{
		private String firstName, lastName;
		public String getFirstName() { return firstName; }
		public void setFirstName(String firstName) { this.firstName = firstName;  }
		public String getLastName() { return lastName; }
		public void setLastName(String lastName) { this.lastName = lastName; }
    	public String toString() { return "Bean.firstName=" + firstName + ", lastName=" + lastName; }    
	}
	
	
//    public void addFieldToClass() throws NotFoundException
//    {
//    	ClassPool pool = ClassPool.getDefault();
//    	CtClass cc = pool.get("hgtest.types.ChangingRecordTypes.Bean");    
//    	cc.addField(new CtField);
//    }
    
    
    public void updateType()
    {
    	final Class clazz = Bean.class;
        final HGHandle typeHandle = graph.getTypeSystem().getTypeHandle(Bean.class);
        final URI typeUri = graph.getTypeSystem().getSchema().toTypeURI(clazz);            
        graph.getTypeSystem().setTypeForClass(graph.getTypeSystem().getNullType(), clazz);        
    	reopenDb(); 	
        graph.getTransactionManager().ensureTransaction(new Callable<Object>(){
            public Object call()
            {
            	// First, find the type atom from the class
                // Then dissociate the class from that HGDB type, so now it's treated
                // as plain record.
                
              HGHandle newTypeHandle = graph.getHandleFactory().makeHandle();
              graph.getTypeSystem().defineTypeAtom(newTypeHandle, typeUri);// getSchema().defineType(typeUri, newTypeHandle);
            	
            	RecordType recType = graph.get(typeHandle);
            	List<Record> existingBeans = graph.getAll(hg.type(typeHandle));
            	HGProjection nameSlot = recType.getProjection("name");
            	for (Record rec : existingBeans)
            	{
            		Object name = nameSlot.project(rec);
            		System.out.println("name is  " + name);
            		String [] split = name.toString().split(" ");
            		Bean b = new Bean();
            		b.setFirstName(split[0]);
            		b.setLastName(split[1]);
            		graph.replace(graph.getHandle(rec), b);
            	}
//            	
//                HGAtomType newType = graph.get(tmpHandle);
//                graph.replace(typeHandle, newType);
//                graph.remove(tmpHandle);
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
        
        listThem("After type change and new atoms added...");
	}	
	
    public static void main(String[] argv)
    {
        ChangingRecordTypes test = new ChangingRecordTypes();
//        HGUtils.dropHyperGraphInstance(test.getGraphLocation());
        test.openGraph();
        try
        {
//        	test.addData();
        	test.updateType();
        	test.listThem("Blabla");
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
        }
        finally
        {
//            test.tearDown();
        }
    }	
}