package com.tudou.isearch.searcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;

import com.tudou.isearch.indexer.DirectoryWrapper;
import com.tudou.isearch.indexer.ReaderWrapper;

/**
 * 
 * @author chenheng
 *
 */
public class SearcherHandler {
	private static final Logger logger = Logger
			.getLogger(SearcherHandler.class);
	/**
	 * 空集合,请不要往此集合中放入任何文档.为避免空指定问题.
	 */
	private final static Set<Document> emptySet = new HashSet<Document>();
	
	/**
	 * 搜索器
	 */
	private Searcher searcher;

	public SearcherHandler(Analyzer analyzer) {
		this.searcher = new DefaultSearcher(analyzer);
	}

	public Set<Document> search(String searchField, String searchValue,
			DirectoryWrapper wrapper) {
		return search(searchField, searchValue, wrapper, true, true, true);
	}

	public Set<Document> search(String searchField, String searchValue,
			DirectoryWrapper wrapper, boolean searchRam, boolean searchSegment,
			boolean searchAssembled) {
		Map<String, String> condition = new HashMap<String, String>();
		condition.put(searchField, searchValue);
		return search(condition, wrapper, searchRam, searchSegment,
				searchAssembled);
	}

	public Set<Document> search(Map<String, String> searchCondition,
			DirectoryWrapper wrapper) {
		return search(searchCondition, wrapper, true, true, true);
	}

	public Set<Document> search(Map<String, String> searchCondition,
			DirectoryWrapper wrapper, boolean searchRam, boolean searchSegment,
			boolean searchAssembled) {
		if (wrapper == null) {
			logger.warn("!!!! no any available lucene directory!");
			return SearcherHandler.emptySet;
		}
		Set<Document> docSet = new HashSet<Document>();
		List<Directory> directories = new ArrayList<Directory>();
		if ( searchSegment ) {
			directories.addAll(wrapper.getSegmentDirectories());
		}
		if ( searchAssembled ) {
			directories.add(wrapper.getAssembledDirectory());
		}
		if ( searchRam ) {
			directories.addAll(wrapper.getRamDirectories());
		}
		try {
			docSet.addAll(searcher.query(directories, searchCondition));
		} catch (ParseException | IOException e) {
			logger.error("!!!! Exception", e);
		}
		return docSet;
	}

	public Set<Document> search(String searchField, String searchValue,
			ReaderWrapper wrapper, boolean searchRam, boolean searchSegment,
			boolean searchAssembled) {
		Map<String, String> condition = new HashMap<String, String>();
		condition.put(searchField, searchValue);
		return search(condition, wrapper, searchRam, searchSegment,
				searchAssembled);
	}

	public Set<Document> search(Map<String, String> searchCondition,
			ReaderWrapper wrapper) {
		return search(searchCondition, wrapper, true, true, true);
	}

	public Set<Document> search(Map<String, String> searchCondition,
			ReaderWrapper wrapper, boolean searchRam, boolean searchSegment,
			boolean searchAssembled) {
		if (wrapper == null) {
			logger.warn("!!!! no any available lucene directory!");
			return SearcherHandler.emptySet;
		}
		Set<Document> docSet = new HashSet<Document>();
		List<DirectoryReader> readers = new ArrayList<DirectoryReader>();
		if ( searchSegment ) {
			readers.addAll(wrapper.getSegmentReaders());
		}
		if ( searchAssembled ) {
			readers.add(wrapper.getAssembledReader());
		}
		if ( searchRam ) {
			readers.addAll(wrapper.getRamReaders());
		}
		try {
			docSet.addAll(searcher.query(searchCondition, readers));
		} catch (ParseException | IOException e) {
			logger.error("!!!! Exception", e);
		}
		return docSet;
	}
}
