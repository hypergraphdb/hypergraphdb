package org.hypergraphdb.query.impl;

import java.util.concurrent.Future;

import org.hypergraphdb.HGSearchResult;

public interface AsyncSearchResult<T> extends HGSearchResult<Future<T>>
{
    Future<Boolean> hasNextAsync();
    Future<Boolean> hasPrevAsync();
}
