package hgtest.links;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGValueLink;
import org.junit.Test;

import hgtest.HGTestBase;

public class TestValueLink extends HGTestBase
{
	public static class Bean 
	{
		private String name = "unnamed";
		private int value = 100;
		
		public Bean(String name, int value)
		{
			this.name = name;
			this.value = value;
		}
		
		public String getName()
		{
			return name;
		}
		public void setName(String name)
		{
			this.name = name;
		}
		public int getValue()
		{
			return value;
		}
		public void setValue(int value)
		{
			this.value = value;
		}
	}
	
	
	@Test
	public void testBeanPayload()
	{
		try 
		{
			Bean bean = new Bean("hi", 100);
			HGHandle a1 = getGraph().add("atom1");
			HGHandle a2 = getGraph().add(10);
			HGHandle vhandle = getGraph().add(new HGValueLink(bean, a1, a2));		
		}
		catch (Throwable t)
		{
			t.printStackTrace(System.err);
		}
	}
}