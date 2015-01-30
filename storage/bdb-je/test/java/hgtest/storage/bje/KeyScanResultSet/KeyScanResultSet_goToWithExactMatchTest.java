package hgtest.storage.bje.KeyScanResultSet;

import com.sleepycat.je.*;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSet_goToWithExactMatchTest extends
		KeyScanResultSet_goToTestBasis
{
	private static final boolean EXACT_MATCH = true;

	@Test
	public void thereIsOneKeyButItIsNotEqualToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereOneKeyButItIsEqualToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(1,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereOneKeyButItIsCloseToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreTwoItemsButThereIsNotDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(-2,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreTwoItemsAndOnOfThemIsCloseToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 3, "three");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreTwoItemsAndOnOfThemIsEqualToDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreThreeItemsButThereIsNotDesired() throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(5,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreThreeItemsAndOneOfThemIsEqualToDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
				EXACT_MATCH);

		assertEquals(actual, expected);
		shutdownCursor();
	}

	@Test
	public void thereAreThreeItemsAndSeveralOfThemAreEqualToDesired()
			throws Exception
	{
		final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

		startupCursor();
		putKeyValuePair(realCursor, 1, "one");
		putKeyValuePair(realCursor, 2, "two");
		putKeyValuePair(realCursor, 3, "three");
		putKeyValuePair(realCursor, 3, "III");
		startupMocks();

		final HGRandomAccessResult.GotoResult actual = keyScan.goTo(3,
				EXACT_MATCH);
		assertEquals(actual, expected);
		shutdownCursor();
	}
}
