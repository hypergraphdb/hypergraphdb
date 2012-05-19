package hgtest;

import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.atom.AtomProjection;

import hgtest.atomref.TestHGAtomRef;
import hgtest.beans.Car;
import hgtest.beans.Person;
import hgtest.query.IndexEnumTypeTest;
import hgtest.query.QueryCompilation;

public class DebugTest
{
	public static void main(String []argv)
	{
		TestHGAtomRef test = new TestHGAtomRef();
		test.setUp();
		try
		{
			test.testNullRef();
		}
		finally
		{
			test.tearDown();
		}
	}
}
