package hgtest.storage.bje.KeyScanResultSet;

import static hgtest.storage.bje.TestUtils.putKeyValuePair;
import static org.hamcrest.core.Is.is;
import static org.hypergraphdb.HGRandomAccessResult.GotoResult;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KeyScanResultSet_goToWithExactMatchTest extends
		KeyScanResultSet_goToTestBasis
{
	protected static final boolean EXACT_MATCH = true;

	@Test
	public void returnsNothing_whenThereIsOneKey_andItIsNotEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(2, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsFound_whenThereOneKey_andItIsEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(1, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Test
	public void returnsNothing_whenThereIsOneKey_andItIsCloseToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(2, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsNothing_whenThereAreTwoItems_andNeitherOfThemIsEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(-2, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsNothing_whenThereAreTwoItems_andOnOfThemIsCloseToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 3, "three");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(2, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.nothing));
	}

	@Test
	public void returnsFound_whenThereAreTwoItems_andOneOfThemIsEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(2, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Test
	public void returnsNothing_whenThereAreThreeItems_andNeitherOfThemIsEqualToDesired()
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
	public void returnsFound_whenThereAreThreeItems_andOneOfThemIsEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		createMocksForTheGoTo();

		final GotoResult actualResult = keyScan.goTo(2, EXACT_MATCH);

		assertThat(actualResult, is(GotoResult.found));
	}

	@Test
	public void returnsFound_whenThereAreThreeItems_andSeveralOfThemAreEqualToDesired()
			throws Exception
	{
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		putKeyValuePair(realCursor, 3, "III");
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
