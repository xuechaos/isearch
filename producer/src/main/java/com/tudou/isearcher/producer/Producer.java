package com.tudou.isearcher.producer;

import java.util.Map;


public interface Producer<V> {
	public V produce(V v);
	public Map<String, String> status();
}
