package hgtest.beans;

import java.io.File;

import org.hypergraphdb.*;

public class NestedContainer 
{
	private String s;
	static
	{
		NestedContainer nc = new NestedContainer();
		NestedContainer.NonStaticNested nsc = nc.new NonStaticNested(); 
	}
	
	public static class StaticNested
	{
		private String x, y;

		public String getX() {
			return x;
		}

		public void setX(String x) {
			this.x = x;
		}

		public String getY() {
			return y;
		}

		public void setY(String y) {
			this.y = y;
		}		
	}
	
	public class NonStaticNested
	{
		private String w;

		public String getW() {
			return w;
		}

		public void setW(String w) {
			this.w = w;
		}		
	}

	public String getS() {
		return s;
	}

	public void setS(String s) {
		this.s = s;
	}
	
	public NonStaticNested makeNS()
	{
		return new NonStaticNested();
	}
	
	public static void main(String argv[])
	{
		NestedContainer nc = new NestedContainer();
		nc.s = "this is s";
		NestedContainer.NonStaticNested x = nc.new NonStaticNested();
		x.setW("this is w");
		HyperGraph hg = new HyperGraph("c:/temp/hgnested");
		try
		{
			HGHandle h = hg.add(x);
		}
		finally
		{
			hg.close();
		}
	}
}