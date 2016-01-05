package com.tudou.isearch.indexer;

import java.util.List;

import org.apache.lucene.index.DirectoryReader;

public class ReaderWrapper {
	private List<DirectoryReader> ramReaders; // 内存读
	private List<DirectoryReader> segmentReaders; // 文件块读
	private DirectoryReader assembledReader;// 聚合文件读

	public ReaderWrapper(List<DirectoryReader> ramReaders,
			List<DirectoryReader> segmentReaders, DirectoryReader assembledReader) {
		super();
		this.ramReaders = ramReaders;
		this.segmentReaders = segmentReaders;
		this.assembledReader = assembledReader;
	}

	public List<DirectoryReader> getRamReaders() {
		return ramReaders;
	}

	public void setRamReaders(List<DirectoryReader> ramReaders) {
		this.ramReaders = ramReaders;
	}

	public List<DirectoryReader> getSegmentReaders() {
		return segmentReaders;
	}

	public void setSegmentReaders(List<DirectoryReader> segmentReaders) {
		this.segmentReaders = segmentReaders;
	}

	public DirectoryReader getAssembledReader() {
		return assembledReader;
	}

	public void setAssembledReader(DirectoryReader assembledReader) {
		this.assembledReader = assembledReader;
	}
}
