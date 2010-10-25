package hgtest.tx;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import hgtest.HGTestBase;
import hgtest.T;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.transaction.HGTransactionManager;

public class DataTxTests extends HGTestBase {
	private int atomsCount = 20; 
	private int threadCount = 5;
	
	
	private boolean log = true;

	private ArrayList<Throwable> errors = new ArrayList<Throwable>();

	public static class SimpleData {
		private int threadId = -1;
		private int idx = -1;
		private int value;

		public SimpleData() {
		}

		public SimpleData(int threadId, int idx, int val) {
			this.threadId = threadId;
			this.idx = idx;
			this.value = val;
		}

		@Override
		public String toString() {
			return "SimpleData [threadId=" + threadId + ", idx=" + idx
					+ ", value=" + value + "]";
		}

		public int getThreadId() {
			return threadId;
		}

		public void setThreadId(int threadId) {
			this.threadId = threadId;
		}

		public int getIdx() {
			return idx;
		}

		public void setIdx(int idx) {
			this.idx = idx;
		}

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}
	}

	Object makeAtom(int i) {
		return new SimpleData(-1, i, i);
	}

   public void verifyData() {
		for (int i = 0; i < atomsCount; i++) {
			SimpleData x = hg.getOne(graph,
					hg.and(hg.type(SimpleData.class), hg.eq("idx", i)));
			if (log && x.getIdx() == 0)
                T.getLogger("DataTxTests").info("verifyData: " + x);
			assertNotNull(x);
			assertEquals(x.getIdx() + threadCount, x.getValue());
		}
	}

	private void increment(final int threadId, final HGHandle atomX) {
		final SimpleData l = graph.get(atomX);
		final HGTransactionManager txman = graph.getTransactionManager();
		txman.transact(new Callable<Integer>() {
			public Integer call() {
				if (log && l.getIdx() == 0)
                    T.getLogger("DataTxTests").info("Increment " + l + ":" + threadId + ":" + atomX);
				l.setValue(l.getValue() + 1);
				l.setThreadId(threadId);
				graph.update(l);
				if (log && l.getIdx() == 0) T.getLogger("DataTxTests").info("After increment " + l);
				return 0;
			}
		});
		try {
			Thread.sleep((long) Math.random() * 100);
		} catch (InterruptedException ex) {
		}

	}

	private void incrementValues(int threadId) {
		for (int i = 0; i < atomsCount; i++) {
			HGHandle hi = hg.findOne(graph,
					hg.and(hg.type(SimpleData.class), hg.eq("idx", i)));
			assertNotNull(hi);
			increment(threadId, hi);
		}
	}

	// @Test
	public void testConcurrentLinkCreation() {
		for (int i = 0; i < atomsCount; i++)
			hg.assertAtom(graph, makeAtom(i));
		ExecutorService pool = Executors.newFixedThreadPool(10);
		for (int i = 0; i < threadCount; i++) {
			final int j = i;
			pool.execute(new Runnable() {
				public void run() {
					try {
						incrementValues(j);
					} catch (Throwable t) {
						t.printStackTrace(System.err);
						errors.add(t);
					}
				}
			});
		}
		try {
			pool.shutdown();
			pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException ex) {
			System.out.println("testTxMap interrupted.");
			return;
		}
		assertEquals(errors.size(), 0);
		verifyData();
	}

	
	public static void main(String[] argv) {
		DataTxTests test = new DataTxTests();
		dropHyperGraphInstance(test.getGraphLocation());
		test.setUp();
		try {
			test.graph.getTransactionManager().conflicted.set(0);
			test.graph.getTransactionManager().successful.set(0);
			test.testConcurrentLinkCreation();
			System.out.println("Done, CONFLICTS="
					+ test.graph.getTransactionManager().conflicted.get()
					+ ", SUCCESSFUL="
					+ test.graph.getTransactionManager().successful.get());
		} finally {
			test.tearDown();
		}
	}
}
