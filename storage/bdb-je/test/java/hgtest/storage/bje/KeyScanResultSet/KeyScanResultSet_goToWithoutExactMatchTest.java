package hgtest.storage.bje.KeyScanResultSet;

import static hgtest.storage.bje.TestUtils.putKeyValuePair;
import static org.hamcrest.core.Is.is;
import static org.hypergraphdb.HGRandomAccessResult.GotoResult;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KeyScanResultSet_goToWithoutExactMatchTest extends
		KeyScanResultSet_goToTestBasis
{
	private static final boolean EXACT_MATCH = false;

	@Test
	public void returnsClose_whenThereIsOneKey_andItIsGreaterThanDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(-5, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.close));
	}

	@Test
	public void returnsNothing_whenThereIsOneKey_andItIsLessThanDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(5, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsFound_whenThereIsOneKey_andItIsEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(1, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Test
	public void returnsClose_whenThereAreTwoKeys_andAllOfThemAreGreaterToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(-5, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.close));
	}

	@Test
	public void returnsNothing_whenThereAreTwoKeys_andAllOfThemAreLessThanDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(5, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsFound_whenThereAreTwoKeys_andSomeOfThemAreEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(2, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Test
	public void returnsFound_whenThereAreTwoKeys_andAllOfThemAreEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 1, "I");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(1, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Test
	public void returnsClose_whenThereAreSeveralKeys_andAllOfThemAreGreaterThanDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(-5, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.close));
	}

	@Test
	public void returnsNothing_whenThereAreSeveralKeys_andAllOfThemAreLessThanDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(5, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsFound_whenThereAreSeveralKeys_andAllOfThemAreEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 1, "I");
		putKeyValuePair(realCursor, 1, "first");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(1, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Test
	public void returnsFound_whenThereAreSeveralKeys_andSomeOfThemAreEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(3, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Before
	public void startup()
	{
		startupCursor();
	}

	@After
	public void shutdown()
	{
		shutdownCursor();
	}
}
