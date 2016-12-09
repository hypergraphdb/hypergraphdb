package hgtest.storage.bje.BJETxCursor;

import static org.easymock.EasyMock.createStrictMock;
import static org.junit.rules.ExpectedException.none;

import org.easymock.EasyMock;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import com.sleepycat.je.Cursor;

public class BJETxCursorTestBasis
{
	protected final Cursor mockedCursor = createStrictMock(Cursor.class);

	protected final TransactionBJEImpl mockedTx = createStrictMock(TransactionBJEImpl.class);

	@Rule
	public final ExpectedException below = none();

	@Before
	public void resetMocks() throws Exception
	{
		EasyMock.reset(mockedCursor, mockedTx);
	}

	@After
	public void verifyMocks() throws Exception
	{
		EasyMock.verify(mockedCursor, mockedTx);
	}

	protected void replay()
	{
		EasyMock.replay(mockedCursor, mockedTx);
	}
}
