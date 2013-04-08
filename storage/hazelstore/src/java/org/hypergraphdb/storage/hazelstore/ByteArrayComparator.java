package org.hypergraphdb.storage.hazelstore;

import java.io.Serializable;
import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]>, Serializable {

    public ByteArrayComparator() {    }

    /*

    // taken from PureJavaComparator from
    // http://code.google.com/p/guava-libraries/source/browse/guava/src/com/google/common/primitives/UnsignedBytes.java?r=0af256f339334e7f50a3cdb5103de14bc9f96d33
    @Override public int compare(byte[] left, byte[] right) {
        int minLength = Math.min(left.length, right.length);
        for (int i = 0; i < minLength; i++) {
            int result = UnsignedBytes.compare(left[i], right[i]);
            if (result != 0) {
                return result;
            }
        }
        return left.length - right.length;
    }

 
*/
// taken from http://code.google.com/p/flatmap/source/browse/trunk/src/java/com/spinn3r/flatmap/ByteArrayComparator.java
/*
    public int compare(  byte[] b1,  byte[] b2 ) {
        if ( b1.length != b2.length ) {
            String msg = String.format( "differing lengths: %d vs %d", b1.length, b2.length );
            throw new RuntimeException( msg );
        }
        for( int i = 0; i < b1.length; ++i ) {
            if ( b1[i] < b2[i] )
                return -1;
            if ( b1[i] > b2[i] )
                return 1;
            if ( b1[i] == b2[i] ) {
                //we're not done comparing yet.
                if ( i < b1.length - 1 )
                    continue;
                return 0;
            }
        }
        throw new RuntimeException();
    }
    */

    //according to http://stackoverflow.com/a/5108711 this snippet originates at apache hbase
    public int compare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
}

