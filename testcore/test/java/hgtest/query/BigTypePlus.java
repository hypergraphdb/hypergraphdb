package hgtest.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.atom.HGRel;
import org.hypergraphdb.atom.HGRelType;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.query.AnalyzedQuery;
import org.hypergraphdb.query.QueryCompile;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import hgtest.HGTestBase;

/**
 * 
 * The test creates a hierarchy of thousands of types, each
 * having a few instances with some links in between them. 
 * 
 * If then queries with a parallelized hg.typePlus query. The 
 * idea is that the parallelized version should be faster than
 * the sequential version if the parallelization is judiciously 
 * implemented.  
 * 
 * The type constructor used to create the types is the HGRelTypeConstructor. We
 * start the process by creating a top-level relation type, named "t", and then
 * proceed to create sub-types each which a different index (an integer) with names
 * t>0, t>1, t>2...,t>N. Then for each of those we create further sub-types, t>1>0, 
 * t>1>1 etc. and we iterate this up to 'maxDepth'. When we reach the max depth
 * each type node in this tree gets N/maxDepth HGRel instances. 
 *   
 * Each instances is also a link. Before the whole type hierarchy is created, we
 * create N Integer atoms that serve as targets to the HGRel links. So each HGRel
 * instance is link of some of those N integer target atoms. The targets for a given
 * link are selected in a very straightforward way: the type name has the form 
 * t>n1>n2>n3>...tmaxDepth, so simply each index n1, n2 etc. is taken as a target
 * to link to.
 * 
 * @author borislav
 *
 */
public class BigTypePlus extends HGTestBase
{
    static int maxDepth = 3;
    static int N = 100;
    static HGHandle [] alltargets;
    
    @BeforeClass
    public static void setUp()
    {
        HGTestBase.setUp();
        HGHandle reltypeHandle = getGraph().getTypeSystem().getTypeHandle(HGRelType.class);
        getGraph().getIndexManager().register(new ByPartIndexer(reltypeHandle, "name"));        
        alltargets = new HGHandle[N];
        for (int i = 0; i < N; i ++)
            alltargets[i] = graph.add(new Integer(i));
        ArrayList<HGHandle> currentLevel = new ArrayList<HGHandle>();
        currentLevel.add(graph.add(new HGRelType("t"))); // the root of the hierarchy
        for (int level = 1; level <= maxDepth; level++)
        {
            System.out.println("Level " + level);
            ArrayList<HGHandle> nextLevel = new ArrayList<HGHandle>();
            for (HGHandle hParent : currentLevel)
            {  
                HGRelType parent = graph.get(hParent);
                String prefix = parent.getName();
                System.out.println(prefix);
                for (int i = 0; i < N/level; i++)
                {
                    String typename = prefix + ">" + i;
                    HGHandle sub = graph.add(new HGRelType(typename));
                    nextLevel.add(sub);
                    graph.getTypeSystem().assertSubtype(hParent, sub);
                    if (level == maxDepth) // create instances here
                    {
                        for (int j = 0; j < N/(level + 1); j++)
                        {
                            String [] types = typename.split(">");
                            HGHandle [] targets = new HGHandle[types.length];
                            for (int k = 1; k < types.length; k++)
                                targets[k] = alltargets[Integer.parseInt(types[k])];
                            targets[0] = alltargets[j];
                            graph.add(new HGRel(targets), sub);
                        }
                    }
                }
            }
            currentLevel = nextLevel;
        }
        // How to link HGRel instances here? What's the formula?
    }
    
    @AfterClass    
    public static void tearDown()
    {
//        super.tearDown();
        graph.close();
    }

    private void runNormal(HGHandle node, HGHandle t1, HGHandle t2)
    {
        HGQuery<HGHandle> q = HGQuery.make(HGHandle.class, graph).compile(
                hg.and(hg.typePlus(node),
                       hg.incident(t1),
                       hg.incident(t2)));
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++)
        {
            hg.count(q);
        }
        System.out.println("normal -> " +  (System.currentTimeMillis() - start)/1000.0 + "s");
        
    }
    
    private void runParallel(HGHandle node, HGHandle t1, HGHandle t2)
    {
        QueryCompile.parallel();
        HGQuery q = HGQuery.make(HGHandle.class, graph).compile(
                hg.and(hg.typePlus(node),
                        hg.incident(t1),
                        hg.incident(t2)));
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++)
            hg.count(q);
        System.out.println("Parallelized -> " + (System.currentTimeMillis() - start)/1000.0 + "s");        
    }
    
    @Test
    public void testManySubtypes()
    {
        Map<String, Object> analyzeOptions = new HashMap<String, Object>();
        analyzeOptions.put(AnalyzedQuery.SCAN_THRESHOLD, 3000);
        AnalyzedQuery aq = QueryCompile.analyze(graph, 
                    hg.and(hg.type(HGRelType.class), hg.eq("name", "t>1>1")), 
                    analyzeOptions);
        System.out.println(aq.getAnalysisResult(AnalyzedQuery.SCAN_THRESHOLD));
        HGHandle node = hg.findOne(graph, hg.and(hg.type(HGRelType.class), hg.eq("name", "t>1>1")));
        HGHandle t1 = hg.findOne(graph, hg.eq(1));
        HGHandle t2 = hg.findOne(graph, hg.eq(2));

        analyzeOptions.clear();
        analyzeOptions.put(AnalyzedQuery.INTERSECTION_THRESHOLD, 1000);
        aq = QueryCompile.analyze(this.getGraph(),
                hg.and(hg.typePlus(node),
                        hg.incident(t1),
                        hg.incident(t2)),
                analyzeOptions);

        System.out.println(aq.getAnalysisResult(AnalyzedQuery.INTERSECTION_THRESHOLD));
        
        //        runNormal(node, t1, t2);
//        runParallel(node, t1, t2);
//        System.out.println("again: " + graph.count(hg.typePlus(node)));
        //q.compile(condition)
    }
}