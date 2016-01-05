package com.tudou.isearch.indexer.agent;

import java.util.Map;

public interface ProduceAgent<V> {
	public V produce(V v);
	public Map<String, String> status();
}
