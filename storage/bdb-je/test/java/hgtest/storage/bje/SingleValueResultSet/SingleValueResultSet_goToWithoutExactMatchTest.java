package hgtest.storage.bje.SingleValueResultSet;

import com.sleepycat.je.DatabaseEntry;
import org.easymock.EasyMock;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.SingleValueResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class SingleValueResultSet_goToWithoutExactMatchTest extends
		SingleValueResultSet_goToTestBasis
{
	private static final boolean EXACT_MATCH = false;

	@Test
	public void thereIsOneValueButItIsLessThanDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(database, 1, 10);
		startupCursor();
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(20,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereIsOneValueButItIsGreaterThanDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(database, 1, 10);
		startupCursor();
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(9,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereIsOneValueAndItIsEqualToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		putKeyValuePair(database, 1, 10);
		startupCursor();
		startupMocks();

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

		putKeyValuePair(database, 1, 10);
		putKeyValuePair(database, 2, 20);
		startupCursor();
		startupMocks();

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

		putKeyValuePair(database, 1, 10);
		putKeyValuePair(database, 2, 20);
		startupCursor();
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(-10,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreTwoValuesAndOneOfThemIsEqualToDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		putKeyValuePair(database, 1, 10);
		putKeyValuePair(database, 2, 20);
		startupCursor();
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(10,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreTwoValuesAndDesiredValueIsBetweenThem()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(database, 1, 10);
		putKeyValuePair(database, 2, 20);
		startupCursor();
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(15,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreThreeValuesButAllOfThemAreLessThanDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		putKeyValuePair(database, 1, 10);
		putKeyValuePair(database, 2, 20);
		putKeyValuePair(database, 3, 30);

		startupCursor();
		startupMocks();

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

		putKeyValuePair(database, 1, 10);
		putKeyValuePair(database, 2, 20);
		putKeyValuePair(database, 3, 30);
		startupCursor();
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(-10,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreThreeValuesAndOneOfThemIsEqualToDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		putKeyValuePair(database, 2, 4);
		putKeyValuePair(database, 3, 9);
		putKeyValuePair(database, 4, 16);
		startupCursor();
		startupMocks();

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

		putKeyValuePair(database, 1, 10);
		putKeyValuePair(database, 2, 20);
		putKeyValuePair(database, 3, 30);
		startupCursor();
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = resultSet.goTo(30,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

    @Test
    public void thereAreThreeValuesAndDesireValueIsBetweenThem()
            throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        putKeyValuePair(database, 1, 10);
        putKeyValuePair(database, 2, 20);
        putKeyValuePair(database, 3, 30);
        startupCursor();
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = resultSet.goTo(25,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }
}
