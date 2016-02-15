package hgtest.atomref;

import java.util.HashSet;

import org.hypergraphdb.HGQuery.hg;

import org.hypergraphdb.annotation.AtomReference;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.*;
import org.junit.Assert;
import org.junit.Test;

import hgtest.beans.CircRefBean;
import hgtest.beans.CircRefOtherBean;
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
    public void testNullRef()
    {
        Car car = new Car();
        car.setOwner(null);
        HGHandle carHandle = graph.add(car);
        Assert.assertEquals(hg.findOne(graph, hg.and(hg.type(Car.class), hg.eq("make", null))), carHandle);
        Assert.assertEquals(hg.findOne(graph, hg.and(hg.type(Car.class), hg.eq("owner", null))), carHandle);
        Assert.assertNull(hg.findOne(graph, hg.and(hg.type(Car.class), hg.eq("owner.email", "blabla"))));
        Assert.assertNull(hg.findOne(graph, hg.and(hg.type(Car.class), hg.eq("owner.email", null))));
        reopenDb();
        Assert.assertEquals(hg.findOne(graph, hg.and(hg.type(Car.class), hg.eq("make", null))), carHandle);        
        Assert.assertEquals(hg.findOne(graph, hg.and(hg.type(Car.class), hg.eq("owner", null))), carHandle);
        Assert.assertNull(hg.findOne(graph, hg.and(hg.type(Car.class), hg.eq("owner.email", "blabla"))));
        Assert.assertNull(hg.findOne(graph, hg.and(hg.type(Car.class), hg.eq("owner.email", null))));        
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
	    reopenDb();
	    car = graph.get(carHandle.getPersistent());
	    Assert.assertNotNull(graph.getHandle(car.getOwner()));
	    graph.remove(carHandle.getPersistent());
	}
	
	@Test
	public void testDanglingDetect()
	{
		HashSet<HGHandle> cleanup = new HashSet<HGHandle>();
	    Float cost = new Float(500.0);
	    Integer ten = new Integer(10);	    
	    HGHandle hten = graph.add(ten);
	    cleanup.add(hten);
	   // System.out.println("hten=" + hten);
	    HGHandle hcost = graph.add(cost);
	    cleanup.add(hcost);
	    //System.out.println("hcost=" + hcost);
        Car car = new Car();
        car.setMake("Honda");
        car.setYear(1997);
        Person person = new Person();
        person.setFirstName("Toto");
        person.setLastName("Cutunio");
        car.setOwner(person);
        car.setAge(ten);
        car.setCost(cost);
        
        HGHandle hcar = graph.add(car);
        cleanup.add(hcar);
        //System.out.println("hcar=" + hcar);
        Assert.assertNotNull(hcar);
        HGHandle hperson = graph.getHandle(person);
        Assert.assertNotNull(hperson);        
        
        try
        {
            Assert.assertFalse(graph.remove(hten));
            Assert.fail("Atom removal creates dangling reference.");
            Assert.assertFalse(graph.remove(hcost));
            Assert.fail("Atom removal creates dangling reference.");
        }
        catch (HGRemoveRefusedException ex) { }
        
        reopenDb();
        try
        {
            Assert.assertFalse(graph.remove(hten.getPersistent()));
            Assert.fail("Atom removal creates dangling reference.");
            Assert.assertFalse(graph.remove(hcost.getPersistent()));
            Assert.fail("Atom removal creates dangling reference.");
        }
        catch (HGRemoveRefusedException ex) { }
        Assert.assertTrue(graph.remove(hperson.getPersistent()));
        
        // Test with dangling detect ignored.
        this.graph.close();
        HGConfiguration config = new HGConfiguration();
        config.setPreventDanglingAtomReferences(false);
        this.graph = HGEnvironment.get(graph.getLocation(), config);
        Assert.assertTrue(graph.remove(hten.getPersistent()));
        Assert.assertTrue(graph.remove(hcost.getPersistent()));
        for (HGHandle h : cleanup)
        {
        	h = h.getPersistent();
        	if (graph.get(h) != null)
        	{
        		//System.out.println("Remove " + h);
        		graph.remove(h);
        	}
        }
	}
	
	public static void main(String [] argv)
	{
	    TestHGAtomRef test = new TestHGAtomRef();
        HGUtils.dropHyperGraphInstance(test.getGraphLocation());
        test.setUp();        
        try
        {
            test.testDanglingDetect();
            //System.out.println("Test completed successfully.");
        }
        finally
        {
            test.tearDown();
        }        
	    
	}
}