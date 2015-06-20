package hgtest.storage.bje.SingleKeyResultSet;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import hgtest.storage.bje.ResultSetTestBasis;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.SingleKeyResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;

/**
 * @author Yuriy Sechko
 */
public class SingleKeyResultSet_isOrderedTest extends ResultSetTestBasis
{
	@Test
	public void test() throws Exception
	{
		final Cursor realCursor = database.openCursor(
				transactionForTheEnvironment, null);
		realCursor.put(new DatabaseEntry(new byte[] { 1, 2, 3, 4 }),
				new DatabaseEntry(new byte[] { 1, 2, 3, 4 }));
		final BJETxCursor fakeCursor = PowerMock
				.createStrictMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor).times(2);
		PowerMock.replayAll();
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
		final SingleKeyResultSet<Integer> resultSet = new SingleKeyResultSet(
				fakeCursor, null, converter);

		final boolean isOrdered = resultSet.isOrdered();

		assertFalse(isOrdered);
		realCursor.close();
	}
}
