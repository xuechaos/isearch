package com.tudou.isearch.indexer;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;

import com.tudou.isearch.Model;

public class IFilterDirectoryReader<T extends Model> extends
		FilterDirectoryReader {

	public IFilterDirectoryReader(DirectoryReader in, SubReaderWrapper wrapper)
			throws IOException {
		super(in, wrapper);
	}

	@Override
	protected DirectoryReader doWrapDirectoryReader(DirectoryReader in)
			throws IOException {
		return in;
	}
	
	public DirectoryReader doWrapDirectoryReader(DirectoryReader in, DeleteQueue<T> deleteQueue)
			throws IOException {
		DirectoryReader dr = openIfChanged(in);
		if (dr != null) {
			dr = new IFilterDirectoryReader<T>(dr,
				new ISubReaderWrapper<T>(deleteQueue));
		}
		return dr;
	}

	public static class ISubReaderWrapper<T extends Model> extends SubReaderWrapper {
		private DeleteQueue<T> deleteQueue;
		
		public ISubReaderWrapper(DeleteQueue<T> deleteQueue) {
			this.deleteQueue = deleteQueue;
		}
		
		@Override
		public LeafReader wrap(LeafReader reader) {
			return new IFilterLeafReader<T>(reader, deleteQueue);
		}

	}

}
