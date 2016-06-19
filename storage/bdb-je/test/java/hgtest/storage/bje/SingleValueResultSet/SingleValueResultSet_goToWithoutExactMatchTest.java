package hgtest.storage.bje.SingleValueResultSet;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGRandomAccessResult.GotoResult;
import org.junit.After;
import org.junit.Test;

import hgtest.storage.bje.TestUtils;

public class SingleValueResultSet_goToWithoutExactMatchTest extends
		SingleValueResultSet_goToTestBasis
{
	private static final boolean EXACT_MATCH = false;

	@Test
	public void returnsNothing_whenThereIsOneValue_andItIsLessThanDesired()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(20,
				EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsNothing_whenThereIsOneValue_andItIsGreaterThanDesired()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(9,
				EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsFound_whenThereIsOneValue_andItIsEqualToDesired()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(10,
				EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Test
	public void returnsNothing_whenThereAreTwoValues_andAllOfThemAreLessThanDesired()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(30,
				EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsNothing_whenThereAreTwoValues_andAllOfThemAreGreaterThanDesired()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(
				-10, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsFound_whenThereAreTwoValues_andOneOfThemIsEqualToDesired()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(10,
				EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Test
	public void returnsNothing_whenThereAreTwoValues_andDesiredValueIsBetweenThem()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(15,
				EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsNothing_whenThereAreThreeValues_andAllOfThemAreLessThanDesired()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		TestUtils.putKeyValuePair(environment, database, 3, 30);

		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(50,
				EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsNothing_whenThereAreThreeValues_andAllOfThemAreGreaterThanDesired()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		TestUtils.putKeyValuePair(environment, database, 3, 30);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(
				-10, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsFound_whenThereAreThreeValues_andOneOfThemIsEqualToDesired()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 2, 4);
		TestUtils.putKeyValuePair(environment, database, 3, 9);
		TestUtils.putKeyValuePair(environment, database, 4, 16);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(9,
				EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Test
	public void returnsNothing_whenThereAreThreeValues_andOneOfThemIsEqualToDesired()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		TestUtils.putKeyValuePair(environment, database, 3, 30);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(30,
				EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsNoting_whenThereAreThreeValues_andDesireValueIsBetweenThem()
			throws Exception
	{
		TestUtils.putKeyValuePair(environment, database, 1, 10);
		TestUtils.putKeyValuePair(environment, database, 2, 20);
		TestUtils.putKeyValuePair(environment, database, 3, 30);
		startupCursor();
		createMocksForTheGoTo();

		final HGRandomAccessResult.GotoResult actualResult = resultSet.goTo(25,
				EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

    @After
    public void shutdown() {
        shutdownCursor();
    }
}
