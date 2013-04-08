package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.HGRandomAccessResult
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import org.hypergraphdb.util.CountMe


object EmptySR extends HGRandomAccessResult[Any] with CountMe{
  def hasPrev = false

  def nope {
    throw new NoSuchElementException("This is an emtpy HGSearchResult")
  }

  def prev() = nope; Nil

  def current(){nope; Nil}

  def hasNext = false

  def next(){nope; Nil}

  def remove() {nope}

  def goTo(value: Any, exactMatch: Boolean) = GotoResult.nothing;

  def goAfterLast() {nope}

  def goBeforeFirst() {nope}

  def close() {}

  def isOrdered = false

  def count() = 0
}
