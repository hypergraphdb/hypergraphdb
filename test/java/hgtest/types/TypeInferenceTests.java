package hgtest.types;

import static hgtest.AtomOperationKind.add;

import static hgtest.AtomOperationKind.remove;
import hgtest.AtomOperation;
import hgtest.HGTestBase;
import hgtest.beans.BeanLink1;
import hgtest.beans.BeanLink2;
import hgtest.beans.BeanWithTransient;
import hgtest.beans.PrivateConstructible;
import hgtest.beans.SubBeanWithTransient;

import java.awt.Color;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.type.HGCompositeType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * 
 * <p>
 * Test the creation of HGDB types from Java classes. Each tests adds an atom of
 * a new type that the HGDB type system is supposed to map properly, reopens the
 * DB and verify that the value is the same through a "deep" equals comparison.
 * </p>
 */
public class TypeInferenceTests extends HGTestBase
{
    BasicOperations basics;

    @BeforeClass
    public void setUp()
    {
        super.setUp();
        basics = new BasicOperations(graph);
    }
    
    public static void main(String[] args)
    {
        new TypeInferenceTests().test();
    }

    public void test()
    {
        setUp();
        testConstructibleLinks();
        testPrivateConstructibleBean();
        testSimpleMap();
        testSimpleCollection();
        testSimpleBean();
        testSerializable();
        tearDown();
    }

    @Test
    public void testConstructibleLinks()
    {
        HGHandle x = graph.add("x");
        HGHandle y = graph.add("y");
        HGHandle z = graph.add("z");
        BeanLink1 l = new BeanLink1(14, new String[] { "x", "y", "z" },
                new HGHandle[] { x, y, z });
        BeanLink2 l2 = new BeanLink2(x, y, z);
        basics.executeAndVerify(new AtomOperation(add, l), true, true);
        basics.executeAndVerify(new AtomOperation(add, l2), true, true);
        basics.executeAndVerify(new AtomOperation[] {
                new AtomOperation(remove, x), new AtomOperation(remove, y),
                new AtomOperation(remove, z) }, false, false, true);
    }

    @Test
    public void testPrivateConstructibleBean()
    {
        basics.executeAndVerify(new AtomOperation(add,
                new PrivateConstructible("from test")), true, true);
    }

    @Test
    public void testSimpleMap()
    {
        Map map = createTestMap();
        Map map1 = createTestMap();
        new BasicOperations(graph).executeAndVerify(new AtomOperation[] {
                new AtomOperation(add, map)},
                false, false, true);
        HGHandle mapH = graph.add(map);
        HGHandle mapH1 = graph.add(map);
        graph.close();
        graph.open(getGraphLocation());
        map = graph.get(mapH);
        map1 = graph.get(mapH1);
        Assert.assertEquals(map.size(), map1.size());
        for(Object key : map.keySet())
            Assert.assertEquals(map.get(key), map1.get(key));
        graph.remove(mapH); graph.remove(mapH1);
    }
    
    @Test
    public void testSimpleCollection()
    {
        Collection coll = createTestCollection();
        Collection coll1 = createTestCollection();
        new BasicOperations(graph).executeAndVerify(new AtomOperation[] {
                new AtomOperation(add, coll)},
                false, false, true);
        HGHandle collH = graph.add(coll);
        HGHandle collH1 = graph.add(coll);
        graph.close();   graph.open(getGraphLocation());
        coll = graph.get(collH);
        coll1 = graph.get(collH1);
        Assert.assertEquals(coll.size(), coll1.size());
        Iterator it = coll1.iterator();
        for(Object obj : coll)
        {
            Assert.assertEquals(obj, it.next());
        }
        graph.remove(collH); graph.remove(collH1);

    }

    @Test
    public void testSimpleBean()
    {
        SimpleBean bean = new SimpleBean(125, "test", Color.gray);
        SimpleBean bean1 = new SimpleBean(125, "test", Color.gray);
        new BasicOperations(graph).executeAndVerify(new AtomOperation[] {
                new AtomOperation(add, bean)},
                false, false, true);
        HGHandle beanH = graph.add(bean);
        HGHandle beanH1 = graph.add(bean);
        graph.close();   graph.open(getGraphLocation());
        bean = graph.get(beanH);
        bean1 = graph.get(beanH1);
        Assert.assertEquals(bean, bean1);
        graph.remove(beanH); graph.remove(beanH1);
    }

    @Test
    public void testSerializable()
    {
       Color c = new Color(128, 128, 128);
       new BasicOperations(graph).executeAndVerify(new AtomOperation[] {
               new AtomOperation(add, c)},  false, false, true);
       HGHandle colorH = graph.add(c);
       graph.close();   graph.open(getGraphLocation());
       c = graph.get(colorH);
       Assert.assertEquals(c, new Color(128, 128, 128));
       graph.remove(colorH);
    }
    
    @Test
    public void testTransientFields()
    {
        HGCompositeType type = graph.getTypeSystem().getAtomType(BeanWithTransient.class);
        try
        {
            type.getProjection("tmpString");
            Assert.fail("projection found!");
        }
        catch (HGException ex)
        {
            Assert.assertTrue(ex.getMessage().indexOf("Could not find projection") > -1);            
        }
        type = graph.getTypeSystem().getAtomType(SubBeanWithTransient.class);
        try
        {
            type.getProjection("tmpString");
            Assert.fail("projection found!");
        }
        catch (HGException ex)
        {
            Assert.assertTrue(ex.getMessage().indexOf("Could not find projection") > -1);
        }
    }
    
    private Map createTestMap()
    {
        Map map = new HashMap();
        map.put(14, "14");
        map.put("key", "some_key");
        map.put("private_bean", new PrivateConstructible("from test"));
        return map;
    }
    
    private Collection createTestCollection()
    {
        Collection coll = new ArrayList();
        coll.add(14);
        coll.add("some_key");
        coll.add(new PrivateConstructible("from test"));
        return coll;
    }
}