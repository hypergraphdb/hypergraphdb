package hgtest;

import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.atom.AtomProjection;

import hgtest.atomref.TestHGAtomRef;
import hgtest.beans.Car;
import hgtest.beans.Person;
import hgtest.query.IndexEnumTypeTest;
import hgtest.query.Queries;
import hgtest.query.QueryCompilation;

public class DebugTest
{
	public static void main(String []argv)
	{
		Queries test = new Queries();
		test.setUp();
		try
		{
			test.testPositionedLinkCondition();
		}
		finally
		{
			test.tearDown();
		}
	}
}
