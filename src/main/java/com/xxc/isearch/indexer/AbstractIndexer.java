package com.tudou.isearch.indexer;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.tudou.isearch.Model;
import com.tudou.isearch.monitor.Monitor;
/**
 * 索引抽象类.主要实现了线程的run方法.具体的实现依赖于子类的consume方法实现.
 * 
 * @author chenheng
 *
 */
public abstract class AbstractIndexer<T extends Model> implements Indexer<T> {
	/**
	 * 索引运行时监视器,用于监视索引实例运行时的情况.
	 */
	Monitor monitor;
	/**
	 * 索引管理器
	 */
	Manager<T> iManager;
	
	private static final Logger logger = Logger.getLogger(RAMIndexer.class);

	public AbstractIndexer(Monitor monitor) {
		this.monitor = monitor;
	}

	public void run() {
		try {
			consume();
		} catch (Exception e) {
			logger.error("!!!! io exception!", e);
		}
	}
	
	@Override
	public void setManager(Manager<T> manager) {
		this.iManager = manager;
	}
	
	@Override
	public void delete(Model model) throws IOException {
		// TODO default do nothing.
	}
}
