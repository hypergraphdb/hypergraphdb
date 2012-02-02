package hgtest.tx;

import hgtest.HGTestBase;

import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.*;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 * Test transaction failures.
 * 
 * @author Borislav Iordanov
 *
 */
public class BasicTxTests extends HGTestBase
{
    @Test
    public void addAbort()
    {
        HGHandle live = null;
        Object x = "txAtomAddFail";
        graph.getTransactionManager().beginTransaction();
        live = graph.add(x);
        graph.getTransactionManager().abort();
        assertNull(graph.getHandle(x));
        assertEquals(hg.count(graph, hg.eq(x)), 0);
        assertNull(graph.getCache().get(live.getPersistent()));        
        assertNull(graph.get(live));        
    }
    
    @Test
    public void cacheFreezeAbort()
    {
        Object x = "cacheFreezeAbortTest";
        HGHandle h = graph.add(x);
        graph.getTransactionManager().beginTransaction();
        graph.freeze(h);
        graph.getTransactionManager().abort();
        assertFalse(graph.isFrozen(h));
        graph.remove(h);
    }    
    
    @Test
    public void cacheFreezeAddAbort()
    {
        Object x = "cacheFreezeAddAbortTest";
        HGHandle h = null;
        graph.getTransactionManager().beginTransaction();
        h = graph.add(x);
        graph.freeze(h);
        graph.getTransactionManager().abort();
        assertFalse(graph.isFrozen(h));
        assertNull(graph.get(h));
    }   
    
    @Test
    public void defineAbort()
    {
        HGPersistentHandle persistent = graph.getHandleFactory().makeHandle();
        Object x = "txDefineAbort";
        graph.getTransactionManager().beginTransaction();
        graph.define(persistent, x);
        graph.getTransactionManager().abort();
        assertEquals(hg.count(graph, hg.eq(x)), 0);
        assertNull(graph.getCache().get(persistent));        
        assertNull(graph.getHandle(x));
    }    
    
    @Test
    public void removeAbort()
    {
        Object x = Math.random();
        HGHandle h = graph.add(x);
        HGPersistentHandle pHandle = graph.getPersistentHandle(h);
        graph.getTransactionManager().beginTransaction();
        graph.remove(h);
        graph.getTransactionManager().abort();
        assertNotNull(graph.getCache().get(x));
        assertSame(h, graph.getCache().get(pHandle));
        assertEquals(graph.get(h), x);
        graph.remove(h);
    }  
    
    @Test
    public void replaceAbort()
    {
        Object x = Math.random();
        Object y = Math.random();
        HGHandle h = graph.add(x);
        HGPersistentHandle pHandle = graph.getPersistentHandle(h);
        graph.getTransactionManager().beginTransaction();
        graph.replace(h, y);
        graph.getTransactionManager().abort();
        assertNotNull(graph.getCache().get(x));
        assertSame(h, graph.getCache().get(pHandle));
        assertNull(graph.getHandle(y));
        assertEquals(graph.get(h), x);
        graph.remove(h);
    }    
    
    @Test
    public void linkAddAbort()
    {
        Object x = Math.random();
        Object y = Math.random();
        HGHandle hx = graph.add(x), hy = graph.add(y);
        // make sure the incidence sets are loaded in the cache...
        graph.getIncidenceSet(hx);
        graph.getIncidenceSet(hy);
        graph.getTransactionManager().beginTransaction();
        graph.add(new HGPlainLink(hx, hy));
        graph.getTransactionManager().abort();
        assertEquals(graph.getIncidenceSet(hx).size(), 0);
        assertEquals(graph.getIncidenceSet(hy).size(), 0);
        graph.remove(hx);
        graph.remove(hy);
    }
    
    @Test
    public void linkRemoveAbort()
    {
        Object x = Math.random();
        Object y = Math.random();
        HGHandle hx = graph.add(x), hy = graph.add(y);
        // make sure the incidence sets are loaded in the cache...
        graph.getIncidenceSet(hx);
        graph.getIncidenceSet(hy);
        HGHandle lh = graph.add(new HGPlainLink(hx, hy));
        graph.getTransactionManager().beginTransaction();
        graph.remove(lh);
        graph.getTransactionManager().abort();
        assertEquals(graph.getIncidenceSet(hx).size(), 1);
        assertEquals(graph.getIncidenceSet(hy).size(), 1);
        graph.remove(hx);
        graph.remove(hy);
        graph.remove(lh);
    }    
    
    //@Test
    public void readStress()
    {
        Object x = "txReadStress";
        HGPersistentHandle pHandle = graph.getPersistentHandle(graph.add(x));
        graph.getTransactionManager().beginTransaction();
        // how much do we need here to blow up the memory
        // those tests should be done with an explicit heap max set relatively low...
        for (int i = 0; i < 10000000; i++)
            graph.get(pHandle);
        graph.getTransactionManager().commit();
        graph.remove(pHandle);
    }
}