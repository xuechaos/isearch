package com.tudou.isearch.monitor;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

public class IsearchMonitor implements Monitor {

	private static final Logger logger = Logger.getLogger(IsearchMonitor.class);

	private AtomicLong totalProduction;
	private AtomicLong totalConsumption;
	
	public IsearchMonitor() {
		totalProduction = new AtomicLong();
		totalConsumption = new AtomicLong();
	}
	
	public void produceEvent() {
		totalProduction.incrementAndGet();
	}
	
	public void consumeEvent() {
		totalConsumption.incrementAndGet();
	}

	public long getTotalProduction() {
		return totalProduction.longValue();
	}

	public long getTotalConsumption() {
		return totalConsumption.longValue();
	}

	@Override
	public void summary() {
		logger.info(">>> Total Production: " + totalProduction.longValue());
		logger.info(">>>> Total consumption: " + totalConsumption.longValue());
		Runtime rt = Runtime.getRuntime();
		// Bytes to MB
		long totalMem = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
		logger.info(">>>> Total Memory: " + totalMem);
	}
	
}
