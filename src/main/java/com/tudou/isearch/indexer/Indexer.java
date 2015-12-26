package com.tudou.isearch.indexer;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;

import com.tudou.isearch.Model;

/**
 * 索引接口,继承自线程Runnable接口;<br>
 * 可通过调结线程数量来提高建立索引的速度.<br>
 * 主要用于消费待索引数据,并为之建立索引.
 * 
 * @author chenheng
 *
 */
public interface Indexer<T extends Model> extends Runnable {
	/**
	 * 消费待索引数据,并为之建立索引
	 */
	public void consume();
	/**
	 * 删除model索引数据
	 * @param model
	 * @throws IOException 
	 */
	public void delete(Model model) throws IOException;
	/**
	 * 设置Indexer Manager
	 * @param manager
	 */
	public void setManager(Manager<T> manager);
	/**
	 * 获取主目录
	 * @return
	 */
	public Directory getDirectory();
	/**
	 * 获取段目录
	 * @return
	 */
	public Directory getSegmentDirectory();
	/**
	 * 获取主索引读
	 * @return
	 */
	public DirectoryReader getReader();
	/**
	 * 获取段索引读
	 * @return
	 */
	public DirectoryReader getSegmentReader();
	/**
	 * 清空段索引
	 * @throws IOException
	 */
	public void clearSegmentIndex() throws IOException;
}
