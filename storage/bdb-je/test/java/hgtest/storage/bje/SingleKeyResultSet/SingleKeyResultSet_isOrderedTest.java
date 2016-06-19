package hgtest.storage.bje.SingleKeyResultSet;


import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertFalse;

import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.SingleKeyResultSet;
import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;

import hgtest.storage.bje.ResultSetTestBasis;
import hgtest.storage.bje.TestUtils;

public class SingleKeyResultSet_isOrderedTest extends ResultSetTestBasis
{
	@Test
	public void test() throws Exception
	{
		final Cursor realCursor = database.openCursor(
				transactionForTheEnvironment, null);
		realCursor.put(new DatabaseEntry(new byte[] { 1, 2, 3, 4 }),
				new DatabaseEntry(new byte[] { 1, 2, 3, 4 }));
		final BJETxCursor fakeCursor = createStrictMock(BJETxCursor.class);
		expect(fakeCursor.cursor()).andReturn(realCursor).times(4);
		replay(fakeCursor);
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
		final SingleKeyResultSet<Integer> resultSet = new SingleKeyResultSet<>(
				fakeCursor, null, converter);

		final boolean isOrdered = resultSet.isOrdered();
		assertFalse(isOrdered);

		realCursor.close();
	}
}
