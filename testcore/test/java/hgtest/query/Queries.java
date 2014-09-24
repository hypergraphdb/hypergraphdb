package hgtest.query;

import hgtest.HGTestBase;

import hgtest.T;
import hgtest.beans.Car;
import hgtest.beans.Folder;
import hgtest.beans.Person;
import hgtest.beans.SimpleBean;
import hgtest.beans.Transport;
import hgtest.beans.Truck;
import hgtest.utils.RSUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.GraphClassics;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.query.BFSCondition;
import org.hypergraphdb.query.DFSCondition;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.LinkCondition;
import org.hypergraphdb.query.OrderedLinkCondition;
import org.hypergraphdb.query.SubsumedCondition;
import org.hypergraphdb.query.SubsumesCondition;
import org.hypergraphdb.query.TargetCondition;
import org.hypergraphdb.query.TypePlusCondition;
import org.hypergraphdb.query.impl.TraversalBasedQuery;
import org.hypergraphdb.type.Top;
import org.hypergraphdb.util.HGUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

//@Test(sequential=true)
public class Queries extends HGTestBase
{
    private static final String ALIAS_PREFIX = "NestedBean.InnerBean.number";
    final static int COUNT = 11;
    final static int DUPLICATED_NUM = 5;
    final static int ALIAS_COUNT = 5;
    HGSortIndex<Integer, HGHandle> index;

    boolean value_link_or_normal_link = true;   

    public static void main(String[] args)
    {
        Queries q = new Queries();
        q.test();        
    }

    void test()
    {
        setUp();
        testValueLinkSearch();
        testAtomPartCondition();
        testAtomTypeCondition();
        testAtomValueCondition();
        testSubsumedCondition();
        testSubsumesCondition();
        testMapCondition();             // BJE fails here
        testTypePlusCondition();
        testTypedValueCondition();
        testFilteredLinkTarget();
        
        //make 2 passes with different link types
        for (int i = 0; i < 2; i++)
        {
            if(i == 1)
            {
                value_link_or_normal_link = !value_link_or_normal_link;
                create_simple_subgraph();
            }
            testArityCondition();
            testBFSCondition();
            testDFSCondition();
            testIncidentCondition();
            testLinkCondition();
            testOrderedLinkCondition();
            testTargetCondition();
        }
        tearDown();
    }

    @Test
    public void testOrCondition()
    {
        HGHandle h1 = graph.add("OR-TEST-1");
        HGHandle h2 = graph.add("OR-TEST-2");
        for (String s : (List<String>)(List)hg.getAll(graph, hg.or(hg.eq("OR-TEST-1"), hg.eq("OR-TEST-2"))))
            System.out.println(s);
    }
    
    @Test
    public void testArityCondition()
    {
        Assert.assertNotNull(hg.findOne(graph, hg.and(linkCondition(),
               hg.arity(0))));
        Assert.assertNotNull(hg.findOne(graph, hg.and(linkCondition(),
                hg.arity(3))));
    }

    @Test
    public void testAtomPartCondition()
    {
        // empty RS
        HGQueryCondition cond = hg.and(hg.type(NestedBean.class), hg.eq(
                "innerBean.number", 5), hg.gt("innerBean.number", 5));
        HGQuery<HGHandle> q = HGQuery.make(graph, cond);
        Assert.assertEquals(RSUtils.countRS(q.execute(), true), 0);

        // nested part condition
        cond = hg.and(hg.type(NestedBean.class), hg.eq("innerBean.number", 5));
        q = HGQuery.make(graph, cond);
        Assert.assertNotSame(RSUtils.countRS(q.execute(), true), 0);
    }


    @Test
    public void testIncidentCondition()
    {
        HGHandle emptyH = hg.findOne(graph, 
                                     hg.and(linkCondition(), hg.arity(0)));
        Assert.assertNull(hg.findOne(graph, hg.incident(emptyH)));
        HGHandle not_emptyH = hg.findOne(graph, 
                                         hg.and(linkCondition(), hg.arity(2)));
        Assert.assertEquals(hg.count(graph, hg.incident(not_emptyH)), 1);
    }

    @Test
    public void testLinkCondition()
    {
        HGHandle linkH = hg.findOne(graph, hg.and(linkCondition(), hg
                .arity(2)));
        // empty
        LinkCondition cond = new LinkCondition(new HGHandle[0]);
        Assert.assertTrue(cond.satisfies(graph, linkH));
        // missing
        cond = new LinkCondition(new HGHandle[] { getNestedBeanHandle(5) });
        Assert.assertFalse(cond.satisfies(graph, linkH));
        // present
        cond = new LinkCondition(new HGHandle[] { getNestedBeanHandle(1) });
        Assert.assertTrue(cond.satisfies(graph, linkH));
    }

    @Test
    public void testTargetCondition()
    {
        HGHandle linkH = hg.findOne(graph, hg.and(linkCondition(), hg
                .arity(2)));
        // HGPlainLink o = graph.get(linkH);
        HGHandle tgtH = getNestedBeanHandle(1);
        // boolean is_target = false;
        // for(int i = 0; i < o.getArity(); i++)
        // if(o.getTargetAt(i).equals(tgtH))
        // {
        // is_target = true;
        // break;
        // }
        // Assert.assertTrue(is_target);
        TargetCondition cond = new TargetCondition(linkH);
        Assert.assertTrue(cond.satisfies(graph, tgtH));
        Assert.assertFalse(cond.satisfies(graph, getNestedBeanHandle(5)));
    }

    @Test
    public void testOrderedLinkCondition()
    {
        HGHandle linkH = hg.findOne(graph, hg.and(linkCondition(), hg
                .arity(2)));
        List<HGHandle> L = hg.findAll(graph, hg.and(linkCondition(), hg.orderedLink(
                getNestedBeanHandle(0), getNestedBeanHandle(1))));
        Assert.assertEquals(L.size(), 1);
        Assert.assertEquals(L.get(0), linkH);
        // empty
        OrderedLinkCondition cond = new OrderedLinkCondition(new HGHandle[0]);
        Assert.assertTrue(cond.satisfies(graph, linkH));
        // missing
        cond = new OrderedLinkCondition(
                new HGHandle[] { getNestedBeanHandle(5) });
        Assert.assertFalse(cond.satisfies(graph, linkH));
        // present
        cond = new OrderedLinkCondition(
                new HGHandle[] { getNestedBeanHandle(1) });
        Assert.assertTrue(cond.satisfies(graph, linkH));
        // present both
        cond = new OrderedLinkCondition(new HGHandle[] {
                getNestedBeanHandle(0), getNestedBeanHandle(1) });
        Assert.assertTrue(cond.satisfies(graph, linkH));
        // present both - places exchanged
        cond = new OrderedLinkCondition(new HGHandle[] {
                getNestedBeanHandle(1), getNestedBeanHandle(0) });
        Assert.assertFalse(cond.satisfies(graph, linkH));
    }

    @Test
    public void testPositionedLinkCondition()
    {
    	HGHandle [] A = new HGHandle[10];
    	for (int i = 0; i < A.length; i++)
    		A[i] = graph.add("sdsfd" + T.random(Integer.MAX_VALUE));
    	ArrayList<HGHandle> links = new ArrayList<HGHandle>();
    	for (int i = 0; i < 5; i++)
    		links.add(graph.add(new HGPlainLink(A)));
    	Assert.assertTrue(hg.findAll(graph, hg.incidentAt(A[0], 0)).containsAll(links));
    	Assert.assertTrue(hg.findAll(graph, hg.incidentAt(A[A.length-1], -1)).containsAll(links));
    	Assert.assertTrue(hg.findAll(graph, hg.incidentAt(A[5], 3, 7)).containsAll(links));
    	Assert.assertTrue(hg.findAll(graph, hg.incidentAt(A[3], -4, -1)).isEmpty());
    }
    
    @Test
    public void testAtomTypeCondition()
    {
        HGQueryCondition cond = hg.type(NestedBean.class);
        HGQuery<HGHandle> q = HGQuery.make(graph, cond);
        Assert.assertEquals(RSUtils.countRS(q.execute(), true), COUNT);
    }

    @Test
    public void testAtomValueCondition()
    {
        // empty
        HGHandle emptyH = hg.findOne(graph, hg.and(hg.type(NestedBean.class),
                hg.eq(NestedBean.create(1444))));
        Assert.assertNull(emptyH);
        // present
        HGHandle not_emptyH = hg.findOne(graph, hg.and(hg
                .type(NestedBean.class), hg.eq(NestedBean.create(4))));
        Assert.assertNotNull(not_emptyH);
        // duplicated
        HGSearchResult<HGHandle> res = graph.find(hg.and(hg
                .type(NestedBean.class), hg.eq(NestedBean
                .create(DUPLICATED_NUM))));
        Assert.assertEquals(RSUtils.countRS(res, true), 2);
    }

    @Test
    public void testTypedValueCondition()
    {
        // tested in testAtomValueCondition()
    }

    @Test
    public void testTypePlusCondition()
    {
        HashSet<HGHandle> added = new HashSet<HGHandle>(); 
        added.add(graph.add(new NestedBean.ExExInnerBean1()));
        added.add(graph.add(new NestedBean.InnerBean()));
        added.add(graph.add(new NestedBean.ExInnerBean1()));
        added.add(graph.add(new NestedBean.ExInnerBean2()));
        try
        {
            // All the above
            HGQuery<HGHandle> q = hg.make(HGHandle.class, graph).compile(hg.typePlus(NestedBean.InnerBean.class));            
            List<HGHandle> res = q.findAll();
    //        hg.make(HGHandle.class, graph).conf("parallel").compile()
            Assert.assertEquals(res.size(), 4);
    
            // ExInnerBean1 + ExExInnerBean1
            res = hg.findAll(graph, hg.typePlus(NestedBean.ExInnerBean1.class));
            Assert.assertEquals(res.size(), 2);
    
            // All nested beans
            res = hg.findAll(graph, hg.typePlus(NestedBean.class));
            Assert.assertEquals(res.size(), COUNT);
        }
        finally
        {
            for (HGHandle h : added)
                graph.remove(h);
        }
    }

   
    @Test
    public void testMapCondition()
    {
        // HGHandle linkH =
        // hg.findOne(graph, hg.and(hg.type(getLinkType()), hg.arity(2)));
        // HGHandle linkH1 =
        // hg.findOne(graph, hg.and(hg.type(getLinkType()), hg.arity(4)));
        Double length = GraphClassics.dijkstra(
                // linkH1, linkH,
                getNestedBeanHandle(5), getNestedBeanHandle(6),
                new DefaultALGenerator(graph));
        // Assert.assertEquals(length, 1.0); //??? should be 1
        length = GraphClassics.dijkstra(getNestedBeanHandle(5),
                getNestedBeanHandle(3), new DefaultALGenerator(graph));
        // Assert.assertEquals(length, 2.0);
        Double length1 = GraphClassics.dijkstra(getNestedBeanHandle(6),
                getNestedBeanHandle(3), new DefaultALGenerator(graph));
        Assert.assertEquals(length, length1);                               //BJE fails here
        Double length2 = GraphClassics.dijkstra(getNestedBeanHandle(6),
                getNestedBeanHandle(3), new DefaultALGenerator(graph));
        Assert.assertEquals(length1, length2);
        Double length3 = GraphClassics.dijkstra(getNestedBeanHandle(0),
                getNestedBeanHandle(1), new DefaultALGenerator(graph));
        Assert.assertEquals(length3, 1.0);

    }

    @Test
    public void testSubsumedCondition()
    {
        HGHandle h = graph.getTypeSystem().getTypeHandle(Top.class);
        HGHandle h1 = graph.getTypeSystem().getTypeHandle(Boolean.class);
        SubsumedCondition cond = new SubsumedCondition(h);
        Assert.assertTrue(cond.satisfies(graph, h1));
        cond = new SubsumedCondition(h1);
        Assert.assertFalse(cond.satisfies(graph, h));
    }

    @Test
    public void testSubsumesCondition()
    {
        HGHandle h = graph.getTypeSystem().getTypeHandle(Top.class);
        HGHandle h1 = graph.getTypeSystem().getTypeHandle(Float.class);
        SubsumesCondition cond = new SubsumesCondition(h1);
        Assert.assertTrue(cond.satisfies(graph, h));
        cond = new SubsumesCondition(h);
        Assert.assertFalse(cond.satisfies(graph, h1));
    }

    @Test
    public void testBFSCondition()
    {
        HGHandle needH = hg.findOne(graph, hg.and(linkCondition(), hg
                .arity(2)));

        BFSCondition rs = hg.bfs(needH);
//        HGTraversal tr = rs.getTraversal(graph);
//        List<HGHandle> list = new ArrayList<HGHandle>();
//        while (tr.hasNext())
//            list.add(tr.next().getSecond());

        TraversalBasedQuery tbs = new TraversalBasedQuery(rs
                .getTraversal(graph), TraversalBasedQuery.ReturnType.both);
        int both = RSUtils.countRS(tbs.execute(), true);
         tbs = new TraversalBasedQuery(rs.getTraversal(graph),
         TraversalBasedQuery.ReturnType.links);
         int links = RSUtils.countRS(tbs.execute(), true);
         tbs = new TraversalBasedQuery(rs.getTraversal(graph),
         TraversalBasedQuery.ReturnType.targets);
         int targets = RSUtils.countRS(tbs.execute(), true);
         Assert.assertEquals(both, targets);
         // 2 links + 2 targets
         Assert.assertEquals(links, targets);

    }

    @Test
    public void testFilterOutIndexedSubtypes()
    {
        Person somebody = new Person();
        somebody.setEmail("hyperbla@test.com");
        somebody.setFirstName("Hip");
        somebody.setLastName("Horry");
        Car car = new Car();
        car.setMake("Honda");
        car.setYear(2010);
        car.setOwner(somebody);
        HGHandle carhandle = graph.add(car);
        Truck truck = new Truck();
        truck.setCapacity(2000);
        truck.setAge(14);
        truck.setOwner(somebody);
//        somebody.setEmail("tbone@steak.com");
//        somebody.setFirstName("Fat");
//        somebody.setLastName("Honey");
        HGHandle truckhandle = graph.add(truck);
        
        HGHandle link = graph.add(new HGValueLink("forsale", carhandle, truckhandle));
        
        HGQuery<HGHandle> qry = HGQuery.make(HGHandle.class, graph).compile(
                hg.and(hg.type(Car.class), 
                        hg.eq("owner.email", "hyperbla@test.com"), 
                        hg.target(link))                
        );
        List<HGHandle> L = qry.findAll();
        Assert.assertEquals(L, Collections.singletonList(carhandle));
    }
    
    @Test
    public void testFilteredLinkTarget()
    {
        Folder folder01 = new Folder("Folder 01");
        HGHandle folder01Handle = graph.add(folder01);
        Folder folder02 = new Folder("Folder 02");
        HGHandle folder02Handle = graph.add(folder02);
        HGValueLink link1 = new HGValueLink("link", folder01Handle, folder02Handle);
        graph.add(link1);
        
        HGQueryCondition cond01 = hg.type(Folder.class);
        List<HGHandle> handleList01 = hg.findAll(graph, cond01);
        Assert.assertTrue(handleList01.contains(folder01Handle));
        Assert.assertTrue(handleList01.contains(folder02Handle));
        Assert.assertEquals(handleList01.size(), 2);
        
        HGQueryCondition cond02 = hg.and(
                hg.type(Folder.class),
                hg.eq("name", "Folder 01"));
        List<HGHandle> handleList02 = hg.findAll(graph, cond02);
        Assert.assertEquals(handleList02.size(), 1);
        Assert.assertTrue(handleList02.contains(folder01Handle));   
        
        HGQueryCondition cond03 = hg.apply(
                    hg.targetAt(graph, 1),
                    hg.orderedLink(folder01Handle, hg.anyHandle()));
        List<HGHandle> handleList03 = hg.findAll(graph, cond03);
        Assert.assertEquals(handleList03.size(), 1);
        Assert.assertTrue(handleList03.contains(folder02Handle));   
        
        HGQueryCondition cond04 = hg.and(
                hg.type(Folder.class),
                hg.apply(
                    hg.targetAt(graph, 1),
                    hg.orderedLink(folder02Handle, hg.anyHandle())));
        HGQuery<HGHandle> query = HGQuery.make(graph, cond04);
        List<HGHandle> handleList04 = hg.findAll(query);        
        Assert.assertEquals(handleList04.size(), 0);
        Assert.assertFalse(handleList04.contains(folder02Handle));          
    }
    
    @Test
    public void testDFSCondition()
    {
        HGHandle needH = hg.findOne(graph, hg.and(linkCondition(), hg
                .arity(2)));

        DFSCondition rs = hg.dfs(needH);
        TraversalBasedQuery tbs = new TraversalBasedQuery(rs
                .getTraversal(graph), TraversalBasedQuery.ReturnType.both);
         int both = RSUtils.countRS(tbs.execute(), true);
         tbs = new TraversalBasedQuery(rs.getTraversal(graph),
         TraversalBasedQuery.ReturnType.links);
         int links = RSUtils.countRS(tbs.execute(), true);
         tbs = new TraversalBasedQuery(rs.getTraversal(graph),
         TraversalBasedQuery.ReturnType.targets);
         int targets = RSUtils.countRS(tbs.execute(), true);
         Assert.assertEquals(both, targets);
         Assert.assertEquals(links, targets);
    }

    @Test
    public void testValueLinkSearch()
    {
        Assert.assertNotNull(hg.findOne(graph, 
                hg.and(hg.type(SimpleBean.class), hg.eq("strProp", "nestbeansLink"))));
        HGHandle somenested = hg.findOne(graph, hg.type(NestedBean.class));
        Assert.assertNotNull(somenested);
        Assert.assertNotNull(hg.findOne(graph, hg.and(hg.type(SimpleBean.class), hg.incident(somenested))));
    }
    
    @BeforeClass
    public void setUp()
    {
        super.setUp();
        HGTypeSystem ts = graph.getTypeSystem();
        HGHandle typeH = ts.getTypeHandle(NestedBean.InnerBean.class);
        for (int i = 0; i < ALIAS_COUNT; i++)
            ts.addAlias(typeH, ALIAS_PREFIX + i);

        index = (HGSortIndex<Integer, HGHandle>) graph.getIndexManager()
                .<Integer, HGHandle> register(
                        new ByPartIndexer(typeH, "number"));        
        graph.getIndexManager().register(new ByPartIndexer<String>(
            ts.getTypeHandle(Transport.class),
            "owner.email"            
        ));
        ArrayList<HGHandle> nbeans = new ArrayList<HGHandle>();
        for (int i = 0; i < COUNT - 1; i++)
        {
            HGHandle h = graph.add(NestedBean.create(i));
            nbeans.add(h);
        }
        
        SimpleBean sbean = new SimpleBean();
        sbean.setStrProp("nestbeansLink");
        graph.add(new HGValueLink(sbean, nbeans.toArray(new HGHandle[0])));
        
        // duplicated value
        graph.add(NestedBean.create(DUPLICATED_NUM));

        create_simple_subgraph();
    }

    @AfterClass
    public void tearDown()
    {
        // List<HGHandle> list = hg.findAll(graph, hg.type(NestedBean.class));
        // for (HGHandle handle : list)
        // graph.remove(handle);
        // HGTypeSystem ts = graph.getTypeSystem();
        // for (int i = 0; i < ALIAS_COUNT; i++)
        // try
        // {
        // ts.removeAlias(ALIAS_PREFIX + i);
        // }
        // catch (Throwable t)
        // {
        // }
        // List<HGIndexer> indexers =
        // graph.getIndexManager().getIndexersForType(
        // graph.getTypeSystem().getTypeHandle(NestedBean.InnerBean.class));
        // if (indexers != null) for (HGIndexer indexer : indexers)
        // graph.getIndexManager().deleteIndex(indexer);
        super.tearDown();
    }

    private HGHandle create_simple_subgraph()
    {
        HGHandle linkH = graph.add(makeLink(getNestedBeanHandle(0), 
                                            getNestedBeanHandle(1)));
        System.out.println("linkH="+linkH);
        HGHandle linkH1 = graph.add(makeLink(getNestedBeanHandle(2),
                                             getNestedBeanHandle(3), 
                                             linkH));
        graph.add(makeLink(new HGHandle[0]));
        graph.add(makeLink(getNestedBeanHandle(4), 
                           getNestedBeanHandle(5),
                           getNestedBeanHandle(6), 
                           getNestedBeanHandle(2), 
                           linkH1));
        return linkH;
    }

    private HGHandle getNestedBeanHandle(int num)
    {
        return hg.findOne(graph, hg.and(hg.type(NestedBean.class), hg.eq(
                "innerBean.number", num)));
    }

    private HGLink makeLink(HGHandle... outgoingSet)
    {
        return (value_link_or_normal_link) ? new HGValueLink("SOMETHING",
                outgoingSet) : new TestLink(outgoingSet);
    }

    private HGQueryCondition linkCondition()
    {
        return value_link_or_normal_link ? 
                /*hg.and(hg.type(String.class), hg.eq("SOMETHING"))*/hg.eq("SOMETHING")  : hg.type(TestLink.class);
    }
//    private Class<?> getLinkType()
//    {
//        //returns the value type of the HGValueLink
//        return value_link_or_normal_link ? String.class  : TestLink.class;
//    }

    private static class TestLink extends HGPlainLink
    {

        public TestLink(HGHandle... outgoingSet)
        {
            super(outgoingSet);
        }
    }

}
