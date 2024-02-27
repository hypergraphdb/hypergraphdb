/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.samples;import org.rocksdb.*;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Demonstrates using Transactions on a TransactionDB with
 * varying isolation guarantees
 */
public class IteratorSamples
{
  private static final String dbPath = "/tmp/rocksdb_transaction_example";

  public static final void main(final String args[]) throws RocksDBException {

	try(final Options options = new Options()
		.setCreateIfMissing(true);
		final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
		final TransactionDB txnDb =
			TransactionDB.open(options, txnDbOptions, dbPath)) {

	  try (final WriteOptions writeOptions = new WriteOptions();
		   final ReadOptions readOptions = new ReadOptions()) {

//        iterator_created_from_db_should_respect_bounds(txnDb, writeOptions, readOptions);
		 setting_a_snapshot_on_read_options_should_change_the_result_set(txnDb, writeOptions, readOptions);
	  }
	}
  }

  /**
   * Demonstrates "Read Committed" isolation
   */
  private static void iterator_created_from_db_should_respect_bounds(final TransactionDB txnDb,
	  final WriteOptions writeOptions, final ReadOptions readOptions)
	  throws RocksDBException {

	// Start a transaction


	  // Write a key in this transaction
	  for (int i = 0; i < 100; i++)
	  {
		txnDb.put(bytes(i), new byte[0]);
	  }

	  var lower = bytes(40);
	  var upper = bytes(55);
	  System.out.println("Upper is:");
	  printbytes(upper);
	  System.out.println("------");

	  var iter =  txnDb.newIterator(new ReadOptions().setIterateLowerBound(new Slice(lower)).setIterateUpperBound(new Slice(upper)));
	  iter.seekToFirst();
	  while(iter.isValid())
	  {
		var  k = iter.key();
		printbytes(k);
		iter.next();
	  }

  }

  private static void iterator_created_from_tx_should_respect_bounds(final TransactionDB txnDb,
		  final WriteOptions writeOptions, final ReadOptions readOptions)
		  throws RocksDBException {


	for (int i = 0; i < 100; i++)
	{
	  txnDb.put(bytes(i), new byte[0]);
	}

	var lower = bytes(40);
	var upper = bytes(55);
	System.out.println("Upper is:");
	printbytes(upper);
	System.out.println("------");

	try(final TransactionOptions txnOptions = new TransactionOptions()
			.setSetSnapshot(true);
		final Transaction txn =
				txnDb.beginTransaction(writeOptions, txnOptions)) {

	  var iter =  txnDb.newIterator(
			  new ReadOptions()
					  .setIterateLowerBound(new Slice(lower))
					  .setIterateUpperBound(new Slice(upper)));
	  iter.seekToFirst();
	  while(iter.isValid())
	  {
		var  k = iter.key();
		printbytes(k);
		iter.next();
	  }

	}


  }
  private static void setting_a_snapshot_on_read_options_should_change_the_result_set(final TransactionDB txnDb,
		  final WriteOptions writeOptions, final ReadOptions readOptions)
		  throws RocksDBException {

	//write 50-100 outside the transaction
	for (int i = 50; i < 100; i++)
	{
	  txnDb.put(bytes(i), new byte[0]);
	}

	var lower = bytes(30);
	var upper = bytes(60);
	System.out.println("Upper is:");
	printbytes(upper);
	System.out.println("------");

	try(final TransactionOptions txnOptions = new TransactionOptions()
			.setSetSnapshot(true);
		final Transaction txn =
				txnDb.beginTransaction(writeOptions, txnOptions)) {

	  System.out.println("--- before");

	  var iter =  txn.getIterator(
			  new ReadOptions()
					  .setSnapshot(txn.getSnapshot())
					  .setIterateLowerBound(new Slice(lower))
					  .setIterateUpperBound(new Slice(upper)));

	  /*
	  This will print the 50-60 range which exists before the tx started
	   */
	  iter.seekToFirst();
	  while(iter.isValid())
	  {
		var  k = iter.key();
		printbytes(k);
		iter.next();
	  }

	  /*
	  add 0-30 in the transaction
	  those will be visible in the tx
	   */

	  for (int i = 0; i < 40; i++)
	  {
		txn.put(bytes(i), new byte[0]);
	  }

	  /*
	  add 30-50 outside the transaction
	  those will not be visible because we will be
	  reading from the snapshot which was taken
	  at the beginning of the transaction
	   */
	  for (int i = 40; i < 50; i++)
	  {
		txnDb.put(bytes(i), new byte[0]);
	  }

	  System.out.println("--- after");

	  iter =  txn.getIterator(
			  new ReadOptions()
					  .setSnapshot(txn.getSnapshot())
					  .setIterateLowerBound(new Slice(lower))
					  .setIterateUpperBound(new Slice(upper)));
	  iter.seekToFirst();
	  while(iter.isValid())
	  {
		var  k = iter.key();
		printbytes(k);
		iter.next();
	  }
	}

  }

  private static byte[] bytes(int a)
  {
	return  ByteBuffer.allocate(4).putInt(a).array();
  }

  private static void printbytes(byte[] bytes)
  {
	ByteBuffer buff = ByteBuffer.wrap(bytes);
	int i = buff.getInt();
	for (byte b : bytes) {
	  System.out.format("0x%x ", b);
	}
	System.out.printf(": %s", i);
	System.out.println();

  }


  /**
   * Demonstrates "Repeatable Read" (Snapshot Isolation) isolation
   */
  private static void repeatableRead(final TransactionDB txnDb,
	  final WriteOptions writeOptions, final ReadOptions readOptions)
	  throws RocksDBException {

	final byte key1[] = "ghi".getBytes(UTF_8);
	final byte value1[] = "jkl".getBytes(UTF_8);

	// Set a snapshot at start of transaction by setting setSnapshot(true)
	try(final TransactionOptions txnOptions = new TransactionOptions()
		  .setSetSnapshot(true);
		final Transaction txn =
			txnDb.beginTransaction(writeOptions, txnOptions)) {

	  final Snapshot snapshot = txn.getSnapshot();

	  // Write a key OUTSIDE of transaction
	  txnDb.put(writeOptions, key1, value1);

	  // Attempt to read a key using the snapshot.  This will fail since
	  // the previous write outside this txn conflicts with this read.
	  readOptions.setSnapshot(snapshot);

	  try {
		final byte[] value = txn.getForUpdate(readOptions, key1, true);
		throw new IllegalStateException();
	  } catch(final RocksDBException e) {
		assert(e.getStatus().getCode() == Status.Code.Busy);
	  }

	  txn.rollback();
	} finally {
	  // Clear snapshot from read options since it is no longer valid
	  readOptions.setSnapshot(null);
	}
  }

  /**
   * Demonstrates "Read Committed" (Monotonic Atomic Views) isolation
   *
   * In this example, we set the snapshot multiple times.  This is probably
   * only necessary if you have very strict isolation requirements to
   * implement.
   */
  private static void readCommitted_monotonicAtomicViews(
	  final TransactionDB txnDb, final WriteOptions writeOptions,
	  final ReadOptions readOptions) throws RocksDBException {

	final byte keyX[] = "x".getBytes(UTF_8);
	final byte valueX[] = "x".getBytes(UTF_8);

	final byte keyY[] = "y".getBytes(UTF_8);
	final byte valueY[] = "y".getBytes(UTF_8);

	try (final TransactionOptions txnOptions = new TransactionOptions()
		.setSetSnapshot(true);
		 final Transaction txn =
			 txnDb.beginTransaction(writeOptions, txnOptions)) {

	  // Do some reads and writes to key "x"
	  Snapshot snapshot = txnDb.getSnapshot();
	  readOptions.setSnapshot(snapshot);
	  byte[] value = txn.get(readOptions, keyX);
	  txn.put(valueX, valueX);

	  // Do a write outside of the transaction to key "y"
	  txnDb.put(writeOptions, keyY, valueY);

	  // Set a new snapshot in the transaction
	  txn.setSnapshot();
	  txn.setSavePoint();
	  snapshot = txnDb.getSnapshot();
	  readOptions.setSnapshot(snapshot);

	  // Do some reads and writes to key "y"
	  // Since the snapshot was advanced, the write done outside of the
	  // transaction does not conflict.
	  value = txn.getForUpdate(readOptions, keyY, true);
	  txn.put(keyY, valueY);

	  // Decide we want to revert the last write from this transaction.
	  txn.rollbackToSavePoint();

	  // Commit.
	  txn.commit();
	} finally {
	  // Clear snapshot from read options since it is no longer valid
	  readOptions.setSnapshot(null);
	}
  }
}