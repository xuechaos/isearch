package com.tudou.isearch.producer;

import java.util.concurrent.Callable;

public interface Producer<V> extends Callable<V> {
	public V produce();
}
