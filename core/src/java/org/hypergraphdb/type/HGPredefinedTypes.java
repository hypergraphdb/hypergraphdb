/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandleFactory;

/**
 * 
 * <p>
 * This class contains static references to all HyperGraphDB predefined types.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGPredefinedTypes
{	
//	public static final PredefinedTypeDescriptor HANDLE = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("486181ea-7121-11da-a5ed-d76291339024"),
//			"org.hypergraphdb.type.HGHandleType",
//			new String[]
//            {
//				"org.hypergraphdb.HGHandle",
//				"org.hypergraphdb.HGPersistentHandle",
//				"org.hypergraphdb.handle.HGLiveHandle", 
//				"org.hypergraphdb.handle.UUIDPersistentHandle"
//            }
//	);
//
//	public static final PredefinedTypeDescriptor BYTE = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("ab8854fb-0d34-11da-ac60-932fd7ea200d"),
//			"org.hypergraphdb.type.javaprimitive.ByteType",
//			new String[]
//            {
//				"java.lang.Byte"
//            }
//	);
//
//	public static final PredefinedTypeDescriptor CHAR = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("66f68f3c-0d45-11da-ac60-932fd7ea200d"),
//			"org.hypergraphdb.type.javaprimitive.CharType",
//			new String[]
//            {
//				"java.lang.Character"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor BOOLEAN = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("91de4b2d-0d45-11da-ac60-932fd7ea200d"),
//			"org.hypergraphdb.type.javaprimitive.BooleanType",
//			new String[]
//            {
//				"java.lang.Boolean"
//            }
//	);
//
//	public static final PredefinedTypeDescriptor FLOAT = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("a6531c2e-0d45-11da-ac60-932fd7ea200d"),
//			"org.hypergraphdb.type.javaprimitive.FloatType",
//			new String[]
//            {
//				"java.lang.Float"
//            }
//	);
//
//	public static final PredefinedTypeDescriptor DOUBLE = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("be8d80af-0d45-11da-ac60-932fd7ea200d"),
//			"org.hypergraphdb.type.javaprimitive.DoubleType",
//			new String[]
//            {
//				"java.lang.Double"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor INTEGER = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("d04af9e0-0d45-11da-ac60-932fd7ea200d"),
//			"org.hypergraphdb.type.javaprimitive.IntType",
//			new String[]
//            {
//				"java.lang.Integer"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor LONG = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("e2394711-0d45-11da-ac60-932fd7ea200d"),
//			"org.hypergraphdb.type.javaprimitive.LongType",
//			new String[]
//            {
//				"java.lang.Long"
//            }
//	);
//
//	public static final PredefinedTypeDescriptor SHORT = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("fdd944c2-0d45-11da-ac60-932fd7ea200d"),
//			"org.hypergraphdb.type.javaprimitive.ShortType",
//			new String[]
//            {
//				"java.lang.Short"
//            }
//	);
//
//	public static final PredefinedTypeDescriptor STRING = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("eeee128b-c5c1-11d9-bfe0-4b9280693a83"),
//			"org.hypergraphdb.type.javaprimitive.StringType",
//			new String[]
//            {
//				"java.lang.String"
//            }
//	);
//
//	public static final PredefinedTypeDescriptor INT_ARRAY = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("6c46f2f2-04a7-11db-aae2-8dc354b70291"),
//			"org.hypergraphdb.type.javaprimitive.IntPrimitiveArrayType",
//			new String[]
//            {
//				"[I"
//            }
//	);
//
//	public static final PredefinedTypeDescriptor BOOLEAN_ARRAY = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("b9a25de3-04bc-11db-aae2-8dc354b70291"),
//			"org.hypergraphdb.type.javaprimitive.BooleanPrimitiveArrayType",
//			new String[]
//            {
//				"[Z"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor BYTE_ARRAY = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("4ccf8574-04bd-11db-aae2-8dc354b70291"),
//			"org.hypergraphdb.type.javaprimitive.BytePrimitiveArrayType",
//			new String[]
//            {
//				"[B"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor CHAR_ARRAY = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("3a3b2255-04bf-11db-aae2-8dc354b70291"),
//			"org.hypergraphdb.type.javaprimitive.CharPrimitiveArrayType",
//			new String[]
//            {
//				"[C"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor DOUBLE_ARRAY = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("4e25f0e6-04c1-11db-aae2-8dc354b70291"),
//			"org.hypergraphdb.type.javaprimitive.DoublePrimitiveArrayType",
//			new String[]
//            {
//				"[D"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor FLOAT_ARRAY = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("9eb2c047-04c3-11db-aae2-8dc354b70291"),
//			"org.hypergraphdb.type.javaprimitive.FloatPrimitiveArrayType",
//			new String[]
//            {
//				"[F"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor LONG_ARRAY = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("2ebcfcf8-04c4-11db-aae2-8dc354b70291"),
//			"org.hypergraphdb.type.javaprimitive.LongPrimitiveArrayType",
//			new String[]
//            {
//				"[J"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor SHORT_ARRAY = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("ea102869-04c4-11db-aae2-8dc354b70291"),
//			"org.hypergraphdb.type.javaprimitive.ShortPrimitiveArrayType",
//			new String[]
//            {
//				"[S"
//            }
//	);
//
//	public static final PredefinedTypeDescriptor SLOT = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("f0f59c28-07c9-11da-831d-8d375c1471fe"),
//			"org.hypergraphdb.type.SlotType",
//			new String[]
//            {
//				"org.hypergraphdb.type.Slot"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor RECORD = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("ae9e93e7-07c9-11da-831d-8d375c1471fe"),
//			"org.hypergraphdb.type.RecordTypeConstructor",
//			new String[]
//            {
//				"org.hypergraphdb.type.RecordType"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor ABSTRACT = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("787bdd6b-8d29-11da-9641-f34293e5a85b"),
//			"org.hypergraphdb.type.AbstractTypeConstructor",
//			new String[]
//            {
//				"org.hypergraphdb.type.HGAbstractType",
//				"org.hypergraphdb.type.HGAbstractCompositeType"
//            }
//			
//	);
//	
//	public static final PredefinedTypeDescriptor ATOM_SET = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("623b1568-9fe4-11da-b2eb-93b1a0fe35e8"),
//			"org.hypergraphdb.atom.AtomSetType",
//			new String[]
//            {
//				"org.hypergraphdb.atom.HGAtomSet"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor ARRAY = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("79960d98-e51d-11da-8136-c58f96521980"),
//			"org.hypergraphdb.type.ArrayType",
//			new String[]
//            {
//				"[Ljava.lang.Object;"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor COLLECTION = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("4d3b3e26-0d59-11db-b807-8dc2e665907b"),
//			"org.hypergraphdb.type.CollectionTypeConstructor",
//			new String[]
//            {
//				"org.hypergraphdb.type.CollectionType"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor MAP = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("2e8be758-1ff9-11db-b132-1d51dd755b13"),
//			"org.hypergraphdb.type.MapTypeConstructor",
//			new String[]
//            {
//				"org.hypergraphdb.type.MapType"
//            }
//	);
//	
//	public static final PredefinedTypeDescriptor SERIALIZABLE = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("34e91416-37da-11db-b1e1-8ab779d916a7"),
//			"org.hypergraphdb.type.SerializableType",
//			new String[]
//            {
//				"java.io.Serializable"
//            }
//	);
//
//	public static final PredefinedTypeDescriptor NULL = new PredefinedTypeDescriptor(
//			HGHandleFactory.makeHandle("db733325-19d5-11db-8b55-23bc8177d6ec"),
//			"org.hypergraphdb.type.NullType"
//	);
//	
//	public static final PredefinedTypeDescriptor [] all = new PredefinedTypeDescriptor[]
//   	{
//   		HANDLE,
//   		BYTE,
//   		CHAR,
//   		BOOLEAN,
//   		FLOAT,
//   		DOUBLE,
//   		INTEGER,
//   		LONG,
//   		SHORT,
//   		STRING,
//   		INT_ARRAY,
//   		BOOLEAN_ARRAY,
//   		BYTE_ARRAY,
//   		CHAR_ARRAY,
//   		DOUBLE_ARRAY,
//   		FLOAT_ARRAY,
//   		LONG_ARRAY,
//   		SHORT_ARRAY,
//   		SLOT,
//   		RECORD,
//   		ABSTRACT,
//   		ATOM_SET,
//   		ARRAY,
//   		COLLECTION,
//   		MAP,
//   		SERIALIZABLE,
//   		NULL
//   	};	
}
