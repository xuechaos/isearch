package com.tudou.isearch.indexer;

import java.util.List;

import org.apache.lucene.store.Directory;
/**
 * 索引目录包装<br>
 * 包括:<br>
 * 1) 内存块索引列表;<br>
 * 2) 文件块索引列表;<br>
 * 3) 聚合文件块索引列表; <br>
 * 
 * @author chenheng
 *
 */
public class DirectoryWrapper {
	/**
	 * 内存块索引列表
	 */
	private List<Directory> ramDirectories;
	/**
	 * 文件块索引列表
	 */
	private List<Directory> segmentDirectories;
	/**
	 * 聚合文件块索引列表
	 */
	private Directory assembledDirectory; 
	
	public DirectoryWrapper(List<Directory> ramDirectories,
			List<Directory> segmentDirectories, Directory assembledDirectory) {
		this.ramDirectories = ramDirectories;
		this.segmentDirectories = segmentDirectories;
		this.assembledDirectory = assembledDirectory;
	}

	public List<Directory> getRamDirectories() {
		return ramDirectories;
	}

	public void setRamDirectories(List<Directory> ramDirectories) {
		this.ramDirectories = ramDirectories;
	}

	public List<Directory> getSegmentDirectories() {
		return segmentDirectories;
	}

	public void setSegmentDirectories(List<Directory> segmentDirectories) {
		this.segmentDirectories = segmentDirectories;
	}

	public Directory getAssembledDirectory() {
		return assembledDirectory;
	}

	public void setAssembledDirectory(Directory assembledDirectory) {
		this.assembledDirectory = assembledDirectory;
	}
}
