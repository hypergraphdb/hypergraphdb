package hgtest.query;

import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGQuery.hg;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import hgtest.HGTestBase;

public class TraversalsTests extends HGTestBase
{
	@Test
	public void testDepthFirstOnCircle()
	{
		HGHandle A = graph.add("A");
		HGHandle B = graph.add("B");
		HGHandle C = graph.add("C");
		HGHandle D = graph.add("D");
		HGHandle E = graph.add("E");
		
		graph.add(new HGPlainLink(A, B));
		graph.add(new HGPlainLink(B, C));
		graph.add(new HGPlainLink(C, D));
		graph.add(new HGPlainLink(D, E));
		graph.add(new HGPlainLink(E, A));
		
		for (Object x : graph.getAll(hg.dfs(A)))
			System.out.println(x);
		
		List<HGHandle> sequence = graph.findAll(hg.dfs(A));
		sequence.add(0,  A); 
		Assert.assertArrayEquals(new HGHandle[] {A, B, C, D, E}, sequence.toArray(new HGHandle[0]));
	}
	
//	@Test
//	public void testBreadthFirstOnCircle()
//	{
//		HGHandle A = graph.add("A");
//		HGHandle B = graph.add("B");
//		HGHandle C = graph.add("C");
//		HGHandle D = graph.add("D");
//		HGHandle E = graph.add("E");
//		
//		graph.add(new HGPlainLink(A, B));
//		graph.add(new HGPlainLink(B, C));
//		graph.add(new HGPlainLink(C, D));
//		graph.add(new HGPlainLink(D, E));
//		
//		for (Object x : graph.getAll(hg.bfs(A)))
//			System.out.println(x);
//		List<HGHandle> sequence = graph.findAll(hg.bfs(A));
//		sequence.add(0,  A); 
//		Assert.assertArrayEquals(new HGHandle[] {A, E, B, C, D}, sequence.toArray(new HGHandle[0]));
//	}
	
    
    public static void main(String []argv)
    {
    	int maxIterations = 10;
        JUnitCore junit = new JUnitCore();
        Result result = null;
        int iter = 0;
        do
        {
        	result = junit.run(Request.method(TraversalsTests.class, "testDepthFirstOnCircle"));
        	iter++;
        	System.out.println("Failures " + result.getFailureCount());
	        if (result.getFailureCount() > 0)
	        {
	            for (Failure failure : result.getFailures())
	            {
	                failure.getException().printStackTrace();
	            }
	        }
        } while (result.getFailureCount() == 0 && iter < maxIterations);
    }	
}
