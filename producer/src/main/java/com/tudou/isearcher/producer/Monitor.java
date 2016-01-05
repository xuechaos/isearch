package com.tudou.isearcher.producer;

import java.util.Map;


public interface Monitor {
	public void produceEvent();

	public void consumeEvent();

	public Map<String,String> summary();
}
