package com.tudou.isearch.indexer;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.tudou.isearch.Model;

public class DeleteQueue<T extends Model> {
	private static final Logger logger = Logger.getLogger(DeleteQueue.class);
	private String uniqueKeyName;
	private LinkedBlockingQueue<String> queue;
	//private static final int DELETE_QUEUE_CAPACITY = 1000;
	
	public DeleteQueue() {
		//this.queue = new LinkedBlockingQueue<String>(DELETE_QUEUE_CAPACITY);
		this.queue = new LinkedBlockingQueue<String>();
	}
	
	public void add(T model) {
		if (this.uniqueKeyName == null) {
			this.uniqueKeyName = model.getUniqueKeyName();
		}
		try {
			this.queue.put(model.getUniqueKeyValue());
		} catch (InterruptedException e) {
			logger.error("!!!! InterruptedException", e);
		}
	}
	
	public String getUniqueKeyName() {
		return uniqueKeyName;
	}

	public LinkedBlockingQueue<String> getQueue() {
		return queue;
	}
}
