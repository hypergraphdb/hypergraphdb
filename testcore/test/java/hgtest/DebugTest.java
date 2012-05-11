package hgtest;

import hgtest.query.QueryCompilation;

public class DebugTest
{
	public static void main(String []argv)
	{
		QueryCompilation test = new QueryCompilation();
		test.setUp();
		try
		{
			test.testVariableReplacement();
		}
		finally
		{
			test.tearDown();
		}
	}
}
