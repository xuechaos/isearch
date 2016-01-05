package com.tudou.isearch.indexer;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;


public class IFilterDirectoryReader extends
		FilterDirectoryReader {

	
	public IFilterDirectoryReader(DirectoryReader in, DeleteQueue deleteQueue)
			throws IOException {
		super(in, new ISubReaderWrapper(deleteQueue));
	}

	@Override
	protected DirectoryReader doWrapDirectoryReader(DirectoryReader in)
			throws IOException {
		return in;
	}
	
	public static DirectoryReader doWrapDirectoryReader(DirectoryReader in,
			DeleteQueue deleteQueue) throws IOException {
		DirectoryReader dr = openIfChanged(in);
		if (dr != null) {
			dr = new IFilterDirectoryReader(dr, deleteQueue);
		}
		return dr;
	}

	public static DirectoryReader wrap(DirectoryReader in,
			DeleteQueue deleteQueue) throws IOException {

		return new IFilterDirectoryReader(in, deleteQueue);
	}
	
	public static class ISubReaderWrapper extends SubReaderWrapper {
		
		private DeleteQueue deleteQueue;
		
		public ISubReaderWrapper(DeleteQueue deleteQueue) {
			this.deleteQueue = deleteQueue;
		}
		
		@Override
		public LeafReader wrap(LeafReader reader) {
			return new IFilterLeafReader(reader, deleteQueue);
		}

	}

}
