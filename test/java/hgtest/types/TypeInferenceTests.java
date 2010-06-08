package hgtest.types;

import java.util.HashMap;
import java.util.Map;

import org.hypergraphdb.HGHandle;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static hgtest.AtomOperationKind.*;
import static org.testng.Assert.*;
import hgtest.AtomOperation;
import hgtest.HGTestBase;
import hgtest.beans.BeanLink1;
import hgtest.beans.BeanLink2;
import hgtest.beans.PrivateConstructible;

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
        Map map = new HashMap();
        map.put(14, "14");
        map.put("key", "some_key");
        map.put("private_bean", new PrivateConstructible("from test"));
        new BasicOperations(graph).executeAndVerify(new AtomOperation[] {
                new AtomOperation(add, map) },
                false, false, true);
    }

    @Test
    public void testSimpleCollection()
    {

    }

    @Test
    public void testSimpleBean()
    {

    }

    @Test
    public void testSerializable()
    {

    }
}