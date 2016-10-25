package com.tudou.isearch.producer;

import com.tudou.isearch.monitor.Monitor;

/**
 * 生产者：生产待索引的数据。不同结构的数据，需要不能的生产者。
 * 
 * @author chenheng
 *
 * @param <V> 
 */
public abstract class AbstractProducer<V> implements Producer<V> {
	Monitor monitor;
	
	public AbstractProducer(Monitor monitor) {
		this.monitor = monitor;
	}

	/**
	 * 线程调度
	 */
	@Override
	public V call() throws Exception {
		return produce();
	}
	
	/**
	 * 实现如何生产数据细节
	 * @return
	 */
	public abstract V produce();
}
