package hgtest.types;

import java.util.Calendar;

import org.hypergraphdb.HGQuery.hg;
import org.junit.Assert;
import org.junit.Test;
import hgtest.AtomOperation;
import hgtest.HGTestBase;
import hgtest.RandomStringUtils;
import hgtest.T;
import static hgtest.AtomOperationKind.*;

public class TestPrimitives extends HGTestBase
{    
    @Test
    public void testBoolean()
    {
        new BasicOperations(graph).executeAndVerify(new AtomOperation[]
        {        
            new AtomOperation(add, true),
            new AtomOperation(add, false),
            new AtomOperation(add, new boolean[] {true, true, false, true, false, false })                
        }, 
        false,
        false,
        true);
    }
    
    @Test
    public void testByte()
    {
        new BasicOperations(graph).executeAndVerify(new AtomOperation[]
        {          
              new AtomOperation(add, new Byte((byte) 5)),
              new AtomOperation(add, Byte.MIN_VALUE),
              new AtomOperation(add, Byte.MAX_VALUE),
              new AtomOperation(add, new byte[] {0, 12, -1, 4, 3 })                  
         }, 
         false,
         false,
         true);        
    }
    
    @Test
    public void testChar()
    {
        new BasicOperations(graph).executeAndVerify(new AtomOperation[]
        {          
            new AtomOperation(add, '\n'),
            new AtomOperation(add, '\0'),
            new AtomOperation(add, Character.MAX_VALUE),
            new AtomOperation(add, Character.MIN_VALUE),
            new AtomOperation(add, new char[] {'a', 'z', '\b', '\0' })                  
        }, 
        false,
        false,
        true);         
    }
    
    @Test
    public void testFloat()
    {
        new BasicOperations(graph).executeAndVerify(new AtomOperation[]
          {          
              new AtomOperation(add, Math.random()),
              new AtomOperation(add, Float.MAX_VALUE),
              new AtomOperation(add, Float.MIN_VALUE),
              new AtomOperation(add, new float[] { (float)Math.random(), (float)Math.random(), (float)Math.random() }),
              new AtomOperation(add, new float[] {})              
          }, 
          false,
          false,
          true);  
    }  
    
    @Test
    public void testDouble()
    {
        double [] A = new double[1000];
        for (int i = 0; i < A.length; i++) A[i] = Math.random();        
        new BasicOperations(graph).executeAndVerify(new AtomOperation[]
          {          
              new AtomOperation(add, Math.random()),
              new AtomOperation(add, Double.MAX_VALUE),
              new AtomOperation(add, Double.MIN_VALUE),
              new AtomOperation(add, A),
              new AtomOperation(add, new double[] {})              
          }, 
          false,
          false,
          true);        
    }   
    
    @Test
    public void testInteger()
    {
        new BasicOperations(graph).executeAndVerify(new AtomOperation[]
          {          
              new AtomOperation(add, (int)Math.random()*100000),
              new AtomOperation(add, Integer.MAX_VALUE),
              new AtomOperation(add, Integer.MIN_VALUE),
              new AtomOperation(add, new int[] {2,45,3345,3453,1,8940,4356345}),
              new AtomOperation(add, new int[] {})              
          }, 
          false,
          false,
          true);        
    }
    
    @Test
    public void testLong()
    {
        new BasicOperations(graph).executeAndVerify(new AtomOperation[]
          {          
              new AtomOperation(add, (long)Math.random()*10000000l),
              new AtomOperation(add, Long.MAX_VALUE),
              new AtomOperation(add, Long.MIN_VALUE),
              new AtomOperation(add, new long[] {Long.MIN_VALUE + 1, Long.MAX_VALUE - 1,3345,3453,1,8940,345}),
              new AtomOperation(add, new long[] {})              
          }, 
          false,
          false,
          true);        
    } 
    
    @Test
    public void testShort()
    {
        new BasicOperations(graph).executeAndVerify(new AtomOperation[]
          {          
              new AtomOperation(add, (short)(Math.random()*1000)),
              new AtomOperation(add, Short.MAX_VALUE),
              new AtomOperation(add, Short.MIN_VALUE),
              new AtomOperation(add, new short[] {Short.MIN_VALUE + 1, Short.MAX_VALUE - 1, 16535, 0,1,3234}),
              new AtomOperation(add, new short[] {})              
          }, 
          false,
          false,
          true);        
    } 
 
    @Test
    public void testString()
    {
        new BasicOperations(graph).executeAndVerify(new AtomOperation[]
          {          
              new AtomOperation(add, (String)null, graph.getTypeSystem().getTypeHandle(String.class)),
              new AtomOperation(add, ""),
              new AtomOperation(add, "a"),
              new AtomOperation(add, "alkgdfgh3pq85gqinga;sga\3\ngdf" + '\0' + "gs93qlugagsg"),
              new AtomOperation(add, new String[] {"fdsg", "\0", "gsdfgsdsg", "", null})              
          },
          false,
          false,
          true);
    }
    
    @Test
    public void testStringIndex()
    {
    	String [] A = new String[] {
    	    "abc",
    	    "bca",
    	    "cba",
    	    RandomStringUtils.random(10 + T.random(10)),
    	    RandomStringUtils.random(10 + T.random(10)),
    	    RandomStringUtils.random(10 + T.random(10)),
    	    RandomStringUtils.random(10 + T.random(10))
    	};
    	 
    	for (String s : A)
    	{
    		graph.add(s);
    		if (T.random(10) % 2 == 0)
    			graph.add(s);
    	}
    	
    	for (String s : A)
    		Assert.assertNotNull(hg.findOne(graph, hg.eq(s)));
    }
    
    @Test 
    public void testDates()
    {
        new BasicOperations(graph).executeAndVerify(new AtomOperation[]
          {          
              new AtomOperation(add, new java.util.Date()),
              new AtomOperation(add, Calendar.getInstance()),
              new AtomOperation(add, new java.sql.Timestamp(new java.util.Date().getTime()))              
          }, 
          false,
          false,
          true);                
    }   
}