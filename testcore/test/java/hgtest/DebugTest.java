package hgtest;

import java.util.List;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.query.AtomPartCondition;
import org.hypergraphdb.type.Top;
import org.hypergraphdb.type.javaprimitive.StringType;

import hgtest.atomref.TestHGAtomRef;
import hgtest.beans.Car;
import hgtest.beans.Person;
import hgtest.indexing.PropertyIndexingTests;
import hgtest.query.IndexEnumTypeTest;
import hgtest.query.Queries;
import hgtest.query.QueryCompilation;
import hgtest.tx.DataTxTests;
import hgtest.tx.NestedTxTests;
import hgtest.types.TestPrimitives;

public class DebugTest
{
	public static class TestQuery
	{
		public static void go(String databaseLocation)
		{
			HyperGraph graph = null;

			try
			{
				HGConfiguration config = new HGConfiguration();
				config.setUseSystemAtomAttributes(false);
				graph = HGEnvironment.get(databaseLocation, config);
				List<Object> children = hg
						.getAll(graph, hg.typePlus(Top.class));
				System.out.println("size:" + children.size());
			}
			catch (Throwable t)
			{
				t.printStackTrace();
			}
			finally
			{
				if (graph != null)
				{
					graph.close();
				}
			}
		}
	}

	public static void main(String[] argv)
	{
//		TestQuery.go("/tmp/alain");
		TestHGAtomRef test = new TestHGAtomRef();
		test.setUp();
		try
		{
			test.testDanglingDetect();
			test.testNullRef();
			test.testRefInParentType();
		}
		finally
		{
			test.tearDown();
		}
	}
}