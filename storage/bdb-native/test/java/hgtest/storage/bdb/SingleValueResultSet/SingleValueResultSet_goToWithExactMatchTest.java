package hgtest.storage.bdb.SingleValueResultSet;

import hgtest.storage.bdb.TestUtils;
import org.hypergraphdb.HGRandomAccessResult;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class SingleValueResultSet_goToWithExactMatchTest extends
		SingleValueResultSet_goToTestBasis
{
	private static final boolean EXACT_MATCH = true;

	@Test
	public void thereIsOneValueButItIsLessThanDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		TestUtils.putKeyValuePair(environment, database, 1, 10);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(20,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereIsOneValueButItIsGreaterThanDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		TestUtils.putKeyValuePair(environment, database, 1, 10);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(9,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test(enabled = false)
	public void thereIsOneValueAndItIsEqualToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		TestUtils.putKeyValuePair(environment, database, 1, 10);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(10,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreTwoValuesButAllOfThemAreLessThanDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(30,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreTwoValuesButAllOfThemAreGreaterThanDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(-10,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test(enabled = false)
	public void thereAreTwoValuesAndOneOfThemIsEqualToDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(10,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreThreeValuesButAllOfThemAreLessThanDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		TestUtils.putKeyValuePair(environment, database, 3, 30);

		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(50,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreThreeValuesButAllOfThemAreGreaterThanDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		TestUtils.putKeyValuePair(environment, database, 3, 30);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(-10,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test(enabled = false)
	public void thereAreThreeValuesAndOneOfThemIsEqualToDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		TestUtils.putKeyValuePair(environment, database, 2, 4);
		TestUtils.putKeyValuePair(environment, database, 3, 9);
		TestUtils.putKeyValuePair(environment, database, 4, 16);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(9,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void anotherCaseWhenThereAreThreeValuesAndOneOfThemIsEqualToDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		TestUtils.putKeyValuePair(environment, database, 3, 30);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(30,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}
}
