package hgtest.atomref;

import org.hypergraphdb.annotation.AtomReference;
import org.hypergraphdb.*;

import hgtest.CircRefBean;
import hgtest.CircRefOtherBean;

public class TestHGAtomRef
{
	@AtomReference("hard")
	private CircRefBean ref1;
	@AtomReference("symbolic")
	private CircRefBean ref2;
	@AtomReference("floating")
	private CircRefOtherBean other;
	
	public CircRefBean getRef1()
	{
		return ref1;
	}
	public void setRef1(CircRefBean ref1)
	{
		this.ref1 = ref1;
	}
	public CircRefBean getRef2()
	{
		return ref2;
	}
	public void setRef2(CircRefBean ref2)
	{
		this.ref2 = ref2;
	}
	public CircRefOtherBean getOther()
	{
		return other;
	}
	public void setOther(CircRefOtherBean other)
	{
		this.other = other;
	}
	
	public static void main(String [] argv)
	{
		HyperGraph graph = new HyperGraph("c:/temp/testHG");
		TestHGAtomRef refering = new TestHGAtomRef();
		CircRefBean ref1 = CircRefBean.make();
		refering.setRef1(ref1);
		refering.setRef2(CircRefBean.make());
		CircRefOtherBean other = new CircRefOtherBean();
		other.setRef(ref1);
		refering.setOther(other);
		HGHandle h = graph.add(refering);
		
		graph.remove(h);
	}
}