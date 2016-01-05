package com.tudou.isearch.indexer;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;

import com.tudou.isearch.common.Model;

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
	public void cleanSegmentIndex() throws IOException;
	/**
	 * 手动的合并内存索引到文件块中
	 * @throws InterruptedException 
	 */
	public void mergeManually();
	/**
	 * 做文件块索引合并到聚合文件索引时,需要关闭文件块写,以释放相应的目录锁.
	 * @return
	 */
	public boolean closeSegmentWriter();
	/**
	 * 重置内存和文件块
	 * @param initRam
	 * @param initFs
	 * @throws IOException 
	 */
	public void reinit(boolean initRam, boolean initFs) throws IOException;
}
