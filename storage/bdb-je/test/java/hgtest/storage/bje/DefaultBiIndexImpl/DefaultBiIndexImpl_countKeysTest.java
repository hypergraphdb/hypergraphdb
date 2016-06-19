package hgtest.storage.bje.DefaultBiIndexImpl;

import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.junit.Ignore;
import org.junit.Test;

public class DefaultBiIndexImpl_countKeysTest extends
		DefaultBiIndexImplTestBasis {
    @Test
    public void throwsException_whenKeyIsNull() throws Exception {
        startupIndex();

        try {
            below.expect(NullPointerException.class);
            indexImpl.countKeys(null);
        } finally {
            indexImpl.close();
        }
    }

    @Ignore("Implementation has been changed, need to clarify the intention of test case")
    @Test
    public void thereAreNotEntriesAdded() throws Exception {
        final long expected = 0;

        startupIndex();

        final long actual = indexImpl.countKeys("this value doesn't exist");

        assertEquals(actual, expected);
        indexImpl.close();
    }

    @Ignore("Implementation has been changed, need to clarify the intention of test case")
    @Test
    public void thereAreSeveralEntriesByDesiredValueDoesNotExist()
            throws Exception {
        final long expected = 0;

        startupIndex();
        indexImpl.addEntry(1, "one");
        indexImpl.addEntry(2, "two");
        indexImpl.addEntry(3, "three");

        final long actual = indexImpl.countKeys("none");

        assertEquals(actual, expected);
        indexImpl.close();
    }

    @Test
    @Ignore("Implementation has been changed, need to clarify the intention of test case")
    public void thereIsOnDesiredValue() throws Exception {
        final long expected = 1;

        startupIndex();
        indexImpl.addEntry(22, "twenty two");
        indexImpl.addEntry(33, "thirty three");

        final long actual = indexImpl.countKeys("twenty two");

        assertEquals(actual, expected);
        indexImpl.close();
    }

    @Test
    @Ignore("Implementation has been changed, need to clarify the intention of test case")
    public void thereAreSeveralDesiredValues() throws Exception {
        final long expected = 2;

        startupIndex();
        indexImpl.addEntry(1, "one");
        indexImpl.addEntry(2, "two");
        indexImpl.addEntry(11, "one");

        final long actual = indexImpl.countKeys("one");

        assertEquals(actual, expected);
        indexImpl.close();
    }

    @Test
    public void throwsException_whenIndexIsNotOpenedAhead() throws Exception {
        replay(mockedStorage);
        indexImpl = new DefaultBiIndexImpl<>(INDEX_NAME, mockedStorage,
                transactionManager, keyConverter, valueConverter, comparator,
                null);

        below.expect(NullPointerException.class);
        indexImpl.countKeys("some value");
    }
}
