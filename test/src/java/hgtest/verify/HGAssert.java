package hgtest.verify;

import java.util.HashSet;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGAtomPredicate;
import org.testng.Assert;

public class HGAssert
{
    private HyperGraph graph;
    
    public HGAssert(HyperGraph graph)
    {
        this.graph = graph;
    }
    
    public static HGAssert with(HyperGraph graph) { return new HGAssert(graph); } 
    
    public HGAssert eq(int [] A, int [] B) { HGAssert.assertEquals(A, B); return this; }    
    public static void assertEquals(int [] A, int [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public HGAssert eq(boolean [] A, boolean [] B) { HGAssert.assertEquals(A, B); return this; }
    public static void assertEquals(boolean [] A, boolean [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public HGAssert eq(byte [] A, byte [] B) { HGAssert.assertEquals(A, B); return this; }
    public static void assertEquals(byte [] A, byte [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public HGAssert eq(char [] A, char [] B) { HGAssert.assertEquals(A, B); return this; }
    public static void assertEquals(char [] A, char [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B.toString() + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A.toString());
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public HGAssert eq(double [] A, double [] B) { HGAssert.assertEquals(A, B); return this; }
    public static void assertEquals(double [] A, double [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public HGAssert eq(float [] A, float [] B) { HGAssert.assertEquals(A, B); return this; }
    public static void assertEquals(float [] A, float [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public HGAssert eq(short [] A, short [] B) { HGAssert.assertEquals(A, B); return this; }
    public static void assertEquals(short [] A, short [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public HGAssert eq(long [] A, long [] B) { HGAssert.assertEquals(A, B); return this; }
    public static void assertEquals(long [] A, long [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public HGAssert eq(Object x, Object y) { HGAssert.assertEqualsDispatch(x, x); return this; }
    public static void assertEqualsDispatch(Object x, Object y)
    {
        if (x == y)
            return;
        
        if (x instanceof boolean [])
            assertEquals((boolean[])x, (boolean[])y);
        else if (x instanceof byte [])
            assertEquals((byte[])x, (byte[])y);
        else if (x instanceof char [])
            assertEquals((char[])x, (char[])y);
        else if (x instanceof double [])
            assertEquals((double[])x, (double[])y);
        else if (x instanceof float [])
            assertEquals((float[])x, (float[])y);
        else if (x instanceof int [])
            assertEquals((int[])x, (int[])y);
        else if (x instanceof long [])
            assertEquals((long[])x, (long[])y);
        else if (x instanceof short[])
            assertEquals((short[])x, (short[])y);
        else if (x instanceof Object[])
            Assert.assertEquals((Object[])x, (Object[])y);
        else 
            Assert.assertEquals(x, y);        
    }
    
    public HGAssert checkLinkage(HGHandle h)
    {
        HGPersistentHandle ph = graph.getPersistentHandle(h);
        LinkConsistency lc = new LinkConsistency(graph);
        if (!lc.isLinkConsistent(ph) || !lc.isIncidenceSetConsistent(ph))
            throw new AssertionError("Linkage of " + ph + " is not consistent.");
        return this;
    }
    
    public HGAssert isFalse(HGHandle atom, HGAtomPredicate predicate)
    {
        if (predicate.satisfies(graph, atom))
            throw new AssertionError("Predicate " + predicate + " succeeded on atom " + atom);
        return this;        
    }
    
    public HGAssert isTrue(HGHandle atom, HGAtomPredicate predicate)
    {
        if (!predicate.satisfies(graph, atom))
            throw new AssertionError("Predicate " + predicate + " failed on atom " + atom);
        return this;
    }
 
    public HGAssert valueOf(HGHandle atom, Object value)
    {
        eq(graph.get(atom), value);
        return this;
    }
    
    /**
     * 
     * Check whether the atoms form a proper subgraph (they are interconnected amongst 
     * themselves, but not connected to anything else).
     * @param atoms
     * @return
     */
    public HGAssert subgraph(HGHandle...atoms)
    {
        HashSet<HGHandle> S = new HashSet<HGHandle>();
        for (HGHandle h : atoms)
            S.add(h);
        for (HGHandle h : atoms)
        {
            for (HGHandle inc : graph.getIncidenceSet(h))
                if (!S.contains(inc))
                    throw new AssertionError("Atom " + inc + " links to " + h + ", but is not part of the subgraph atom list.");
            Object x = graph.get(h);
            if (x instanceof HGLink)
            {
                HGLink l = (HGLink)x;
                for (int i = 0; i < l.getArity(); i++)
                    if (!S.contains(l.getTargetAt(i)))
                        throw new AssertionError("Atom " + h + " links to " + l.getTargetAt(i) + "which is not part of the subgraph atom list.");
 
            }
        }
        return this;
    }
}