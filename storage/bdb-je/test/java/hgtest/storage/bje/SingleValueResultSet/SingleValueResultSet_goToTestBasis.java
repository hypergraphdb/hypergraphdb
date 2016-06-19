package hgtest.storage.bje.SingleValueResultSet;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import org.hypergraphdb.storage.bje.SingleValueResultSet;

public abstract class SingleValueResultSet_goToTestBasis extends
		SingleValueResultSetTestBasis
{
	protected SingleValueResultSet<Integer> resultSet;

	protected void createMocksForTheGoTo()
	{
		createMocksForTheConstructor();
		expect(fakeCursor.cursor()).andReturn(realCursor);
		replay(fakeCursor);
		resultSet = new SingleValueResultSet<>(fakeCursor, null,
				converter);
	}
}
