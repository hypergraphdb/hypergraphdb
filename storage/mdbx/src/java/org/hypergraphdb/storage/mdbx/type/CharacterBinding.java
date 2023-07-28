package org.hypergraphdb.storage.mdbx.type;

import com.castortech.mdbxjni.DatabaseEntry;

/**
 * A concrete <code>TupleBinding</code> for a <code>Character</code> primitive
 * wrapper or a <code>char</code> primitive.
 *
 * <p>There are two ways to use this class:</p>
 * <ol>
 * <li>When using the {@link com.sleepycat.db} package directly, the static
 * methods in this class can be used to convert between primitive values and
 * {@link DatabaseEntry} objects.</li>
 * <li>When using the {@link com.sleepycat.collections} package, an instance of
 * this class can be used with any stored collection.  The easiest way to
 * obtain a binding instance is with the {@link
 * TupleBinding#getPrimitiveBinding} method.</li>
 * </ol>
 *
 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
 */
public class CharacterBinding extends TupleBinding<Character> {

    private static final int CHAR_SIZE = 2;

    // javadoc is inherited
    @Override
		public Character entryToObject(TupleInput input) {

        return input.readChar();
    }

    // javadoc is inherited
    @Override
		public void objectToEntry(Character object, TupleOutput output) {

        output.writeChar(object);
    }

    // javadoc is inherited
    @Override
		protected TupleOutput getTupleOutput(Character object) {

        return sizedOutput();
    }

    /**
     * Converts an entry buffer into a simple <code>char</code> value.
     *
     * @param entry is the source entry buffer.
     *
     * @return the resulting value.
     */
    public static char entryToChar(DatabaseEntry entry) {

        return entryToInput(entry).readChar();
    }

    /**
     * Converts a simple <code>char</code> value into an entry buffer.
     *
     * @param val is the source value.
     *
     * @param entry is the destination entry buffer.
     */
    public static void charToEntry(char val, DatabaseEntry entry) {

        outputToEntry(sizedOutput().writeChar(val), entry);
    }

    /**
     * Returns a tuple output object of the exact size needed, to avoid
     * wasting space when a single primitive is output.
     */
    private static TupleOutput sizedOutput() {

        return new TupleOutput(new byte[CHAR_SIZE]);
    }
}
