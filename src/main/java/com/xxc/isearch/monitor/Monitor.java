package com.tudou.isearch.monitor;

public interface Monitor {
	public void produceEvent();
	public void consumeEvent();
	public long getTotalProduction();
	public long getTotalConsumption();
	public void summary();
}
