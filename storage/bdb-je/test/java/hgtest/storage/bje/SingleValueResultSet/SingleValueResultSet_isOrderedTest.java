package hgtest.storage.bje.SingleValueResultSet;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.SingleValueResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;

/**
 * @author Yuriy Sechko
 */
public class SingleValueResultSet_isOrderedTest extends
		SingleValueResultSetTestBasis
{
	@Test
	public void test() throws Exception
	{
		// put some data into primary database
		final Transaction transactionForAddingTestData = environment
				.beginTransaction(null, null);
		database.put(transactionForTheEnvironment, new DatabaseEntry(
				new byte[] { 0, 0, 0, 1 }), new DatabaseEntry(new byte[] { 0,
				0, 0, 2 }));
		transactionForAddingTestData.commit();
		final SecondaryCursor realCursor = secondaryDatabase.openCursor(
				transactionForTheEnvironment, null);
		final DatabaseEntry stubKey = new DatabaseEntry();
		final DatabaseEntry stubValue = new DatabaseEntry();
		// initialize secondary cursor
		realCursor.getFirst(stubKey, stubValue, LockMode.DEFAULT);
		final BJETxCursor fakeCursor = PowerMock
				.createStrictMock(BJETxCursor.class);
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
