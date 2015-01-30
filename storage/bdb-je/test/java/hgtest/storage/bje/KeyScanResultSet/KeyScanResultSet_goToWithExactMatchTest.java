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
		KeyScanResultSetTestBasis
{
	protected static final boolean EXACT_MATCH = true;

	protected Cursor realCursor;
	protected Transaction transactionForTheRealCursor;

	protected final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
	protected KeyScanResultSet<Integer> keyScan;

	protected void startupCursor() throws Exception
	{
		transactionForTheRealCursor = environment.beginTransaction(null, null);
		realCursor = database.openCursor(transactionForTheEnvironment, null);
	}

	protected void startupMocks()
	{
		final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor).times(2);
		PowerMock.replayAll();
		keyScan = new KeyScanResultSet<Integer>(fakeCursor, null, converter);
	}

	protected void shutdownCursor()
	{
		realCursor.close();
		transactionForTheRealCursor.commit();
	}

	protected void putKeyValuePair(Cursor realCursor, final Integer key,
			final String value)
	{
		realCursor.put(
				new DatabaseEntry(new TestUtils.ByteArrayConverterForInteger()
						.toByteArray(key)),
				new DatabaseEntry(new TestUtils.ByteArrayConverterForString()
						.toByteArray(value)));
	}

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
