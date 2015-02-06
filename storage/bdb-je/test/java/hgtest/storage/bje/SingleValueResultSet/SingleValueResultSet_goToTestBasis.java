package hgtest.storage.bje.SingleValueResultSet;

import com.sleepycat.je.*;
import hgtest.storage.bje.ResultSetTestBasis;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.PlainSecondaryKeyCreator;
import org.hypergraphdb.storage.bje.SingleValueResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * @author Yuriy Sechko
 */
public class SingleValueResultSet_goToTestBasis extends ResultSetTestBasis
{
    protected static final String SECONDARY_DATABASE_NAME = "test_database";

    protected SecondaryDatabase secondaryDatabase;
    protected SecondaryCursor realCursor;
    protected SingleValueResultSet<Integer> resultSet;

    final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();

    @BeforeMethod
    public void resetMocksAndDeleteTestDirectory() throws Exception
    {
        super.resetMocksAndDeleteTestDirectory();
    }

    @AfterMethod
    public void verifyMocksAndDeleteTestDirectory() throws Exception
    {
        PowerMock.verifyAll();
        transactionForTheEnvironment.commit();
        secondaryDatabase.close();
        database.close();
        environment.close();
        TestUtils.deleteDirectory(envHome);
    }

    protected void startupEnvironment() throws Exception
    {
        super.startupEnvironment();
        final SecondaryConfig secondaryConfig = new SecondaryConfig();
        secondaryConfig.setAllowCreate(true).setReadOnly(false)
                .setTransactional(true);
        secondaryConfig.setKeyCreator(PlainSecondaryKeyCreator.getInstance());
        secondaryDatabase = environment.openSecondaryDatabase(
                transactionForTheEnvironment, SECONDARY_DATABASE_NAME,
                database, secondaryConfig);
    }

    protected void startupCursor()
    {
        realCursor = secondaryDatabase.openCursor(transactionForTheEnvironment,
                null);
        final DatabaseEntry stubKey = new DatabaseEntry();
        final DatabaseEntry stubValue = new DatabaseEntry();
        // initialize secondary cursor
        realCursor.getFirst(stubKey, stubValue, LockMode.DEFAULT);
    }

    protected void startupMocks()
    {
        final BJETxCursor fakeCursor = PowerMock
                .createStrictMock(BJETxCursor.class);
        EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor).times(2);
        PowerMock.replayAll();
        resultSet = new SingleValueResultSet<Integer>(fakeCursor, null,
                converter);
    }

    protected void shutdownCursor()
    {
        realCursor.close();
    }

    protected void putKeyValuePair(final Database database, final Integer key,
                                   final Integer value)
    {
        final Transaction transactionForAddingTestData = environment
                .beginTransaction(null, null);
        database.put(
                transactionForTheEnvironment,
                new DatabaseEntry(new TestUtils.ByteArrayConverterForInteger()
                        .toByteArray(key)),
                new DatabaseEntry(new TestUtils.ByteArrayConverterForInteger()
                        .toByteArray(value)));
        transactionForAddingTestData.commit();
    }


}
