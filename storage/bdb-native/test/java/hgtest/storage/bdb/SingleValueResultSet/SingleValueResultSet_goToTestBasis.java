package hgtest.storage.bdb.SingleValueResultSet;

import org.easymock.EasyMock;
import org.hypergraphdb.storage.bdb.SingleValueResultSet;
import org.powermock.api.easymock.PowerMock;

/**
 * @author Yuriy Sechko
 */
public class SingleValueResultSet_goToTestBasis extends
		SingleValueResultSetTestBasis
{
	protected SingleValueResultSet<Integer> resultSet;

	protected void createMocksForTheGoTo()
	{
		createMocksForTheConstructor();
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
		PowerMock.replayAll();
		resultSet = new SingleValueResultSet<Integer>(fakeCursor, null,
				converter);
	}
}
