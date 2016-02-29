package hgtest.p2p;

import hgtest.HGTestBase;
import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGRelType;
import org.hypergraphdb.atom.HGRel;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.serializer.HGPeerJsonFactory;
import org.hypergraphdb.query.AtomProjectionCondition;
import org.hypergraphdb.query.ComparisonOperator;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.IndexCondition;
import org.hypergraphdb.query.IndexedPartCondition;
import org.hypergraphdb.query.Nothing;
import org.hypergraphdb.query.TypedValueCondition;
import org.hypergraphdb.util.Mapping;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryToJsonTests extends HGTestBase
{
	void testAny(Object x)
	{
		Json j = Json.object("data", x);
		j = Json.read(j.toString());
		System.out.println(j);
		Object x2 = Messages.fromJson(j.at("condition"));
		Assert.assertEquals(x, x2);		
	}
	
	void testCondition(HGQueryCondition C)
	{
		Json jq = Json.object("condition", C);
		jq = Json.read(jq.toString());
		System.out.println(jq);
		HGQueryCondition cond2 = Messages.fromJson(jq.at("condition"));
		Assert.assertEquals(C, cond2);
	}
	
    @BeforeClass
	public static void setUp()
	{
    	HGTestBase.setUp();
    	Json.attachFactory(HGPeerJsonFactory.getInstance().setHyperGraph(getGraph()));
	}
    
    @AfterClass
	public static void tearDown()
	{
		Json.detachFactory();
	}

	@Test
	public void testAtomType()
	{
		testCondition(hg.type(String.class));
	}
	
	@Test
	public void testAnyAtom()
	{
		testCondition(hg.all());
	}
		
	@Test
	public void testArityCondition()
	{
		testCondition(hg.arity(143));
	}
	
	@Test
	public void testAtomValue()
	{
		testCondition(hg.eq("fg", "3534"));
		testCondition(hg.eq("x.y.a", 3534.0));
		testCondition(hg.eq(new HGRel(graph.getHandleFactory().makeHandle(), graph.getHandleFactory().anyHandle())));
	}
	
	@Test
	public void testAtomPartRegEx()
	{
		testCondition(hg.matches("name", "[a-zM-P]+"));
		testCondition(hg.matches("[a-zM-P]+"));
	}
	
	@Test
	public void testAtomProjectionCondition()
	{
		testCondition(new AtomProjectionCondition(new String[]{"a", "B"}, hg.eq(Boolean.TRUE)));
	}
	
	@Test
	public void testConjuction()
	{
		testCondition(hg.and(hg.type(HGRelType.class), hg.eq("name", "blabla")));
	}
	
	@Test
	public void testBFSCondition()
	{
		testCondition(hg.bfs(hg.constant(graph.getTypeSystem().getTypeHandle(String.class)),
					  hg.type(HGSubsumes.class),
					  hg.type(graph.getTypeSystem().getTop()),
					  false, true));
	}
	
	@Test
	public void testDFSCondition()
	{
		testCondition(hg.dfs(hg.constant(graph.getTypeSystem().getTypeHandle(String.class)),
					  hg.type(HGSubsumes.class),
					  hg.type(graph.getTypeSystem().getTop()),
					  false, true));
	}
	
	@Test
	public void testDisconnected()
	{
		testCondition(hg.disconnected());
	}
	
	@Test
	public void testIncident()
	{
		HGHandle atom = graph.add("testIncident");
		testCondition(hg.incident(atom));
		// variable serialization involves the VarContext and that's too much for now
//		testCondition(hg.incident(hg.var("bla")));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testIndexCondition()
	{
		HGHandle t = graph.getTypeSystem().getTypeHandle(String.class);
		testCondition(new IndexCondition(graph.getStore().getIndex(HyperGraph.TYPES_INDEX_NAME), t));
	}

	@Test
	public void testIndexPartCondition()
	{
		HGHandle t = graph.getTypeSystem().getTypeHandle(String.class);
		testCondition(new IndexedPartCondition(t,
						graph.getStore().getIndex(HyperGraph.TYPES_INDEX_NAME), 
						t,
						ComparisonOperator.EQ));
	}

	@Test
	public void testIsCondition()
	{
		HGHandle atom = graph.add("testIs");
		testCondition(hg.is(atom));
	}

	@Test
	public void testLinkCondition()
	{
		HGHandle atom1 = graph.add("testLink1"), atom2 = graph.add("testLink2");
		testCondition(hg.link(atom1, atom2));
	}

	public static class TestMapping implements Mapping<HGHandle, HGHandle>
	{
		public HGHandle eval(HGHandle h) { return h; }
		public int hashCode() { return 0; }
		public boolean equals(Object x) { return x instanceof TestMapping; }
	}
	
	@Test
	public void testMapCondition()
	{
		HGHandle atom1 = graph.add("testMapping1"), atom2 = graph.add("testMapping2");
		testCondition(hg.apply(new TestMapping(), hg.link(atom1, atom2)));
	}
	
	@Test
	public void testNotCondition()
	{
		HGHandle atom1 = graph.add("testNot1"), atom2 = graph.add("testNot2");
		testCondition(hg.not(hg.link(atom1, atom2)));
	}
	
	@Test
	public void testNothingCondition()
	{
		testCondition(Nothing.Instance);
	}
	
	@Test
	public void testDisjuction()
	{
		testCondition(hg.or(hg.type(HGRelType.class), hg.eq("name", "blabla")));
	}

	@Test
	public void testOrderedLinkCondition()
	{
		HGHandle atom1 = graph.add("testOrderedLink1"), atom2 = graph.add("testOrderedLink2");
		testCondition(hg.orderedLink(atom1, atom2));
	}

	@Test
	public void testPositionedIncidentLink()
	{
		HGHandle atom = graph.add("testPositionedIncident");
		testCondition(hg.incidentAt(atom, 4, 10));
	}
	
	@Test
	public void testSubgraphContainsCondition()
	{
		HGHandle atom = graph.add("testSubgraphContainsCondition");
		testCondition(hg.contains(atom));
	}
	
	@Test
	public void testSubgraphMemberCondition()
	{
		HGHandle atom = graph.add("testSubgraphContainsCondition");
		testCondition(hg.memberOf(atom));
	}

	@Test
	public void testSubsumedCondition()
	{
		HGHandle t1 = graph.getTypeSystem().getTypeHandle(String.class);		
		testCondition(hg.subsumed(t1));
	}

	@Test
	public void testSubsumesCondition()
	{
		HGHandle t1 = graph.getTypeSystem().getTypeHandle(String.class);		
		testCondition(hg.subsumes(t1));
	}
	
	@Test
	public void testTargetCondition()
	{
		HGHandle t1 = graph.getTypeSystem().getTypeHandle(String.class);		
		testCondition(hg.target(t1));
	}

	@Test
	public void testTypedValueCondition()
	{
		HGHandle t1 = graph.getTypeSystem().getTypeHandle(String.class);		
		testCondition(new TypedValueCondition(String.class, "asdfasd", ComparisonOperator.GT));
		testCondition(new TypedValueCondition(t1, "asdfasd", null));
	}

	@Test
	public void testTypePlusCondition()
	{
		HGHandle t1 = graph.getTypeSystem().getTypeHandle(String.class);		
		testCondition(hg.typePlus(t1));
		testCondition(hg.typePlus(Number.class));
	}
	
    public static void main(String[] argv)
    {
    	QueryToJsonTests test = new QueryToJsonTests();
        try
        {
            QueryToJsonTests.setUp();
            test.testAtomValue();
            System.out.println("test passed successfully");
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
        }
        finally
        {
        	QueryToJsonTests.tearDown();
        }
    }	
}