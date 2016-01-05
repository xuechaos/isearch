package com.tudou.isearch.indexer;

import org.apache.log4j.Logger;

import com.tudou.isearch.common.Model;
/**
 * 索引抽象类.主要实现了线程的run方法.具体的实现依赖于子类的consume方法实现.
 * 
 * @author chenheng
 *
 */
public abstract class AbstractIndexer<T extends Model> implements Indexer<T> {
	/**
	 * 索引管理器
	 */
	Manager<T> manager;
	
	private static final Logger logger = Logger.getLogger(AbstractIndexer.class);

	public void run() {
		try {
			consume();
		} catch (Exception e) {
			logger.error("!!!! io exception!", e);
		}
	}
	
	@Override
	public void setManager(Manager<T> manager) {
		this.manager = manager;
	}
}
