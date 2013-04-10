package org.hypergraphdb.storage.hazelstore;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;

public final class KryoSerializer
{
    /* buffer size */
    private static final int BUFFER_SIZE = 1024;

    private final Kryo kryo;

    public KryoSerializer()
    {
        kryo = new Kryo();
        kryo.setReferences( false );
    }


    public <T> byte[] serialize( T obj )
            throws IOException
    {
        Class<?> clazz = obj.getClass();

        checkRegiterNeeded( clazz );

        byte[] buffer = new byte[BUFFER_SIZE];
        Output output = new Output( buffer, -1 );
        output.setBuffer( buffer, -1 );
        kryo.writeObject( output, obj );
        return output.toBytes();
    }


    public <T> T deserialize( byte[] source, Class<T> clazz )
            throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        checkRegiterNeeded( clazz );
        byte[] buffer = new byte[BUFFER_SIZE];
        Input input = new Input( buffer );
        input.setBuffer( source );
        return kryo.readObject( input, clazz );
    }

    private void checkRegiterNeeded( Class<?> clazz )
    {
        kryo.register( clazz );
    }

}
