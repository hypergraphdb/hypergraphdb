/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.samples;import org.rocksdb.*;

import java.util.concurrent.CountDownLatch;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Demonstrates using Transactions on a TransactionDB with
 * varying isolation guarantees
 */
public class MyTransactionSample
{
  private static final String dbPath = "/tmp/rocksdb_transaction_example";

  public static final void main(final String args[]) throws RocksDBException {

	try(final Options options = new Options()
		.setCreateIfMissing(true);
		final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
		final TransactionDB txnDb = TransactionDB.open(options, txnDbOptions, dbPath)) {
//        final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options,  dbPath)) {

	  try (final WriteOptions writeOptions = new WriteOptions();
		   final ReadOptions readOptions = new ReadOptions()) {


//        on_OTDB_if_snapshot_tx_write_after_outside_write_fails_on_commit(txnDb, writeOptions, readOptions);
//        on_OTDB_tx_write_before_outside_write_should_fail_on_commit(txnDb, writeOptions, readOptions);
//        on_OTDB_tx_writе_after_outside_write_should_succeed(txnDb, writeOptions, readOptions);
//        on_OTDB_tx_write_before_and_after_outside_write_fails_on_commit(txnDb, writeOptions, readOptions));


//        on_TDB_tx_write_after_outside_write_should_succeed(txnDb, writeOptions, readOptions);
//        on_TDB_if_snapshot_tx_write_after_outside_write_should_fail_on_commit(txnDb, writeOptions, readOptions);
//        on_TDB_tx_write_before_outside_write_should_fail_on_outside_write(txnDb, writeOptions, readOptions);
//        on_TDB_tx_write_before_and_after_outside_write_fails_on_oustide_write(txnDb, writeOptions, readOptions)
//           on_TDB_only_snapshot_keys_are_locked_when_written_to_before_tx_write(txnDb, writeOptions, readOptions);

		//        writingAfterAnotherTransactionSucceedsShouldSucceed(txnDb, writeOptions, readOptions);
//        readCommitted(txnDb, writeOptions, readOptions);
//        repeatableRead(txnDb, writeOptions, readOptions);
//        readCommitted_monotonicAtomicViews(txnDb, writeOptions, readOptions);
		testSnapshots1(txnDb, writeOptions, readOptions);
	  }
	}
  }

  /**
   * Demonstrates "Read Committed" isolation
   */
  private static void readCommitted(final OptimisticTransactionDB database,
	  final WriteOptions writeOptions, final ReadOptions readOptions)
	  throws RocksDBException {

	final byte key1[] = "abc".getBytes(UTF_8);
	final byte value1[] = "this is value 1".getBytes(UTF_8);

	final byte key2[] = "xyz".getBytes(UTF_8);
	final byte value2[] = "this is value 2".getBytes(UTF_8);

	// Start a transaction
	try(final Transaction txn = database.beginTransaction(writeOptions)) {

	  txn.setSnapshot();

	  //2. W2(key)
	  /*
	   In the case of lock based transactions, this fails,
	   because we are updating the same record outside the transaction
	   which has not been yet committed. So the outside write will fail
	   and the value written in the transaction will take precedence

	   In the case of optimistic transaction this succeeds. However, the commit
	   of the transaction fails as the value was modified outside the transaction
	   after the first write in the transaction.
	   */
	  try
	  {
		database.put(key1, value1);
	  }
	  catch (Exception e)
	  {
		System.out.println("Error while saving value outside the transaction");
		e.printStackTrace();
	  }

	  //1. W1(key)
	  txn.put(key1, value2);

	  //3. C1()
	  txn.commit();
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	var res = database.get(key1);

	System.out.println(res == null ? "null" : new String(res, UTF_8));
  }

  final static byte[] key1 = "key".getBytes(UTF_8);
  final static byte[] key2 = "key2".getBytes(UTF_8);
  final static byte[] value1 = "this is value 1".getBytes(UTF_8);
  final static byte[] value2 = "this is value 2".getBytes(UTF_8);

  /*
  OptimisticDB:
  Start Tx
  Write outside tx
  Write inside tx
  -- proves that on commit we check against the version overwritten by
  the first write
   */
  private static void on_OTDB_tx_writе_after_outside_write_should_succeed(
		  final OptimisticTransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {
	try(final Transaction txn = database.beginTransaction(writeOptions)) {
	  //W2(key)
	  try
	  {
		database.put(key1, value1);
	  }
	  catch (Exception e)
	  {
		System.out.println("Error while saving value outside the transaction");
		e.printStackTrace();
	  }
	  txn.put(key1, value2);
	  /*
	  We check for writing conflicts
	   */
	  txn.commit();
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	var res = database.get(key1);

	System.out.println(res == null ? "null" : new String(res, UTF_8));

  }


  private static void on_OTDB_tx_write_before_outside_write_should_fail_on_commit(
		  final OptimisticTransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {
	try(final Transaction txn = database.beginTransaction(writeOptions)) {
	  //W2(key)
	  txn.put(key1, value2);
	  try
	  {
		database.put(key1, value1);
	  }
	  catch (Exception e)
	  {
		System.out.println("Error while saving value outside the transaction");
		e.printStackTrace();
	  }
	  txn.commit(); //this fails
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	var res = database.get(key1);

	System.out.println(res == null ? "null" : new String(res, UTF_8));

  }

  private static void on_TDB_tx_write_after_outside_write_should_succeed(
		  final TransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {
	try(final Transaction txn = database.beginTransaction(writeOptions)) {
	  //W2(key)
	  try
	  {
		database.put(key1, value1); //this fails
	  }
	  catch (Exception e)
	  {
		System.out.println("Error while saving value outside the transaction");
		e.printStackTrace();
	  }
	  txn.put(key1, value2);
	  txn.commit();
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	var res = database.get(key1);

	System.out.println(res == null ? "null" : new String(res, UTF_8));

  }

  private static void on_TDB_if_snapshot_tx_write_after_outside_write_should_fail_on_commit(
		  final TransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {
	try(final Transaction txn = database.beginTransaction(writeOptions)) {
	  /*
	  this should not make any difference
	   */
	  txn.setSnapshot();
	  //W2(key)
	  try
	  {
		database.put(key1, value1); //this fails
	  }
	  catch (Exception e)
	  {
		System.out.println("Error while saving value outside the transaction");
		e.printStackTrace();
	  }
	  txn.put(key1, value2);
	  txn.commit();
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	var res = database.get(key1);

	System.out.println(res == null ? "null" : new String(res, UTF_8));

  }

  /*
  TransactionDB (lock based)
  Start two transactions - T1, T2
  only set snapshot to one of the transactions - T1
  write to 2 keys outside the transactions - OW(K1), OW(K2)
  write to the 2 keys inside the transactions T1W(K1), T2W(K2)
  Observe that the snapshot did its job and prevented T1W(K1) but allowed T2W(K2)
  I would not expect that because we have a lock based DB. So when we perform
  OW(K1) we lock K1
  and prevented the sna

  I would not expect the snapshot to do anything on a lock based DB.
  I would expect a OW to succeed if it happens before the WT)
  I would T1W to succeed

  Instead, if the tx has created a snapshot, T1W(K1) fails.
  So it seems OW(K1) locked the key and stored it in the 'context' of the
  opened snapshot.
  Then T1W(K1) tries to acquire the lock from the snapshot and fails
  T2W(K2) however is free to modify K2

   */
  private static void on_TDB_only_snapshot_keys_are_locked_when_written_to_before_tx_write(
		  final TransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {


	try(final Transaction txn = database.beginTransaction(writeOptions); final Transaction txn2 = database.beginTransaction(writeOptions)) {
	  /*
	  The snapshot is set only to
	   */
	  txn.setSnapshot();
	  try
	  {
		database.put(key1, value1);
		database.put(key2, value1);
	  }
	  catch (Exception e)
	  {
		System.out.println("Error while saving value outside the transaction");
		e.printStackTrace();
	  }
	  try
	  {
		txn.put(key1, value2);
	  }
	  catch (Exception e)
	  {
		System.out.println("Error writing to key1");
		e.printStackTrace();
	  }
	  try
	  {
		txn2.put(key2, value2);
	  }
	  catch (Exception e)
	  {
		System.out.println("Error writing to key2");
		e.printStackTrace();
	  }
	  txn.commit();
	  txn2.commit();
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	var res = database.get(key1);
	var res1 = database.get(key2);

	System.out.println(res == null ? "null" : new String(res, UTF_8));
	System.out.println(res == null ? "null" : new String(res1, UTF_8));

  }

  private static void on_TDB_tx_write_before_outside_write_should_fail_on_outside_write(
		  final TransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {
	try(final Transaction txn = database.beginTransaction(writeOptions)) {
	  //W2(key)
	  txn.put(key1, value2);
	  try
	  {
		database.put(key1, value1); //this fails
	  }
	  catch (Exception e)
	  {
		System.out.println("Error while saving value outside the transaction");
		e.printStackTrace();
	  }
	  txn.commit();
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	var res = database.get(key1);

	System.out.println(res == null ? "null" : new String(res, UTF_8));

  }

  /*
  If we set the snapshot at the start of the Tx, and then write outside the
  tx,
  the first write in the tx will fail  (on commit)
   */
  private static void on_OTDB_if_snapshot_tx_write_after_outside_write_fails_on_commit(
		  final OptimisticTransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {
	try(final Transaction txn = database.beginTransaction(writeOptions)) {
	  txn.setSnapshot();
	  try
	  {
		//w2(key)
		database.put(key1, value1);
	  }
	  catch (Exception e)
	  {
		System.out.println("Error while saving value outside the transaction");
		e.printStackTrace();
	  }
	  //w1(key)
	  txn.put(key1, value2);
	  //c1()
	  txn.commit(); //this fails, because the tx snapshot is taken before the outside change
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	var res = database.get(key1);

	System.out.println(res == null ? "null" : new String(res, UTF_8));
  }


  private static void on_OTDB_tx_write_before_and_after_outside_write_fails_on_commit(
		  final OptimisticTransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {
	try(final Transaction txn = database.beginTransaction(writeOptions)) {
	  txn.put(key1, value2);
	  try
	  {
		//w2(key)
		database.put(key1, value1);
	  }
	  catch (Exception e)
	  {
		System.out.println("Error while saving value outside the transaction");
		e.printStackTrace();
	  }
	  //w1(key)
	  txn.put(key1, value2);
	  //c1()
	  txn.commit(); //this fails, because the tx snapshot is taken before the outside change
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	var res = database.get(key1);

	System.out.println(res == null ? "null" : new String(res, UTF_8));
  }

  private static void on_TDB_tx_write_before_and_after_outside_write_fails_on_oustide_write(
		  final TransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {
	try(final Transaction txn = database.beginTransaction(writeOptions)) {
	  txn.put(key1, value2);
	  try
	  {
		//w2(key)
		database.put(key1, value1);
	  }
	  catch (Exception e)
	  {
		System.out.println("Error while saving value outside the transaction");
		e.printStackTrace();
	  }
	  //w1(key)
	  txn.put(key1, value2);
	  //c1()
	  txn.commit(); //this fails, because the tx snapshot is taken before the outside change
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	var res = database.get(key1);

	System.out.println(res == null ? "null" : new String(res, UTF_8));
  }

  private static void testSnapshots(
		  final TransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {
	try (final Transaction txn = database.beginTransaction(writeOptions)) {
	  database.put(key1, value2);
	  txn.put(key2, value2);
	  System.out.println("database iterator before commit");
	  try (var it = database.newIterator())
	  {
		it.seekToFirst();
		while(it.isValid())
		{
		  System.out.println(String.format("k: %s ;v: %s", new String(it.key()), new String(it.value())));
		  it.next();
		}
	  }
	  System.out.println("transaction iterator before commit");
	  try (var it = txn.getIterator(readOptions))
	  {
		it.seekToFirst();
		while(it.isValid())
		{
		  System.out.println(String.format("k: %s ;v: %s", new String(it.key()), new String(it.value())));
		  it.next();
		}
	  }

	  txn.commit(); //this fails, because the tx snapshot is taken before the outside change
	}
	catch (Exception e)
	{
	  e.printStackTrace();
	}
	System.out.println("Iterator after commit");
	try (var it = database.newIterator())
	{
	  it.seekToFirst();
	  while(it.isValid())
	  {
		System.out.println(String.format("k: %s ;v: %s", new String(it.key()), new String(it.value())));
		it.next();
	  }
	}
  }

  private static void testSnapshots1(
		  final TransactionDB database,
		  final WriteOptions writeOptions,
		  final ReadOptions readOptions)
		  throws RocksDBException
  {
	try (final Transaction txn = database.beginTransaction(writeOptions))
	{
	  txn.setSnapshot();
	  txn.put(key2, value2);
	  var res = txn.get(new ReadOptions().setSnapshot(txn.getSnapshot()), key2);
	  System.out.println(new String(res));
	}
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