package hgtest.atomref;

import org.hypergraphdb.annotation.AtomReference;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import hgtest.CircRefBean;
import hgtest.CircRefOtherBean;
import hgtest.HGTestBase;
import hgtest.beans.Car;
import hgtest.beans.Person;

public class TestHGAtomRef extends HGTestBase
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
	
	@Test
	public void testRefInParentType()
	{
	    Car car = new Car();
	    car.setMake("Honda");
	    car.setYear(1997);
	    Person person = new Person();
	    person.setFirstName("Toto");
	    person.setLastName("Cutunio");
	    car.setOwner(person);
	    
	    HGHandle carHandle = graph.add(car);
	    this.reopenDb();
	    car = graph.get(carHandle);
	    Assert.assertNotNull(graph.getHandle(car.getOwner()));
	}
	
	public static void main(String [] argv)
	{
	    TestHGAtomRef test = new TestHGAtomRef();
        HGUtils.dropHyperGraphInstance(test.getGraphLocation());
        test.setUp();        
        try
        {
            test.testRefInParentType();
        }
        finally
        {
            test.tearDown();
        }        
	    
	}
}