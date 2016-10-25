package com.tudou.isearch.indexer;

import java.io.IOException;

import com.tudou.isearch.Model;

public interface Manager<T extends Model> {
	public void deleteIndex(T model) throws IOException;

	public DirectoryWrapper getDirectoryWrapper();

	public ReaderWrapper getReaderWrapper();
}
