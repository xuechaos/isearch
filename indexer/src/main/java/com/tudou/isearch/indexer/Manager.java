package com.tudou.isearch.indexer;

import java.io.IOException;

import com.tudou.isearch.common.Model;


public interface Manager<T extends Model> {

	public DirectoryWrapper getDirectoryWrapper();

	public ReaderWrapper getReaderWrapper();
	
	/**
	 * 手动的合并索引
	 * @throws IOException 
	 */
	public void mergeManually();
}
