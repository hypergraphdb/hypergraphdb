package hgtest.types;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomTypeBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import hgtest.HGTestBase;

public class PredefinedReplacement extends HGTestBase
{
    public static class MyIntType extends HGAtomTypeBase
    {
        public Object make(HGPersistentHandle handle,
                LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
        {
            byte [] B = graph.getStore().getData(handle);
            String s = new String(B);
            return Integer.parseInt(s);
        }

        public void release(HGPersistentHandle handle)
        {
            graph.getStore().removeData(handle);
        }

        public HGPersistentHandle store(Object instance)
        {
            Integer x = (Integer)instance;
            return graph.getStore().store(x.toString().getBytes());
        }
    }
    
    @Test
    public void testReplacePredefinedType()
    {
        HGPersistentHandle myTypeHandle = graph.getHandleFactory().makeHandle();
        graph.getTypeSystem().addPredefinedType(myTypeHandle, new MyIntType(), Integer.class);
        super.reopenDb();
        Assert.assertEquals(graph.getTypeSystem().getTypeHandle(Integer.class), myTypeHandle);
        Assert.assertEquals(graph.getTypeSystem().getAtomType(Integer.class).getClass(), MyIntType.class);
        HGHandle h1 = graph.add(8573);
        HGHandle h2 = graph.add(0);
        HGHandle h3 = graph.add(Integer.MAX_VALUE);
        super.reopenDb();
        Assert.assertEquals(graph.get(h1), 8573);
        Assert.assertEquals(graph.get(h2), 0);
        Assert.assertEquals(graph.get(h3), Integer.MAX_VALUE);
    }
}