package hgtest.storage.bdb.SingleValueResultSet;

import com.sleepycat.db.*;
import hgtest.storage.bdb.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bdb.BDBTxCursor;
import org.hypergraphdb.storage.bdb.SingleValueResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;

/**
 *
 */
public class SingleValueResultSet_isOrderedTest extends
		SingleValueResultSet_goToTestBasis
{
	@Test
	public void test() throws Exception
	{
		final Transaction transactionForAddingTestData = environment
				.beginTransaction(null, null);
		database.put(transactionForTheEnvironment, new DatabaseEntry(
				new byte[] { 0, 0, 0, 1 }), new DatabaseEntry(new byte[] { 0,
				0, 0, 2 }));
		transactionForAddingTestData.commit();
		final Cursor realCursor = secondaryDatabase.openCursor(
				transactionForTheEnvironment, null);
		final DatabaseEntry stubKey = new DatabaseEntry();
		final DatabaseEntry stubValue = new DatabaseEntry();
		realCursor.getFirst(stubKey, stubValue, LockMode.DEFAULT);
		final BDBTxCursor fakeCursor = PowerMock
				.createStrictMock(BDBTxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
		PowerMock.replayAll();
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
		final SingleValueResultSet<Integer> resultSet = new SingleValueResultSet(
				fakeCursor, null, converter);

		final boolean isOrdered = resultSet.isOrdered();

		assertFalse(isOrdered);
		realCursor.close();
	}
}
