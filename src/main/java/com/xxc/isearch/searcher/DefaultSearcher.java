package com.tudou.isearch.searcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;

/**
 * 默认搜索器
 * 
 * @author chenheng
 *
 */
public class DefaultSearcher implements Searcher {
	private static final Logger logger = Logger
			.getLogger(DefaultSearcher.class);

	/**
	 * Lucene分析器
	 */
	private Analyzer analyzer;
	/**
	 * 每页最大命中数
	 */
	protected int hitsPerPage;

	/**
	 * 默认每页最大命中数
	 */
	private static final int DEFAULT_HITSPERPAGE = 50;
	/**
	 * 空集合<br>
	 * 注意请不要往此集合中放任何文档.主要用于避免空指针问题.
	 */
	private static final Set<Document> emptySet = new HashSet<Document>();

	public DefaultSearcher(Analyzer analyzer) {
		this(analyzer, DEFAULT_HITSPERPAGE);
	}

	public DefaultSearcher(Analyzer analyzer, int hitsPerPage) {
		this.analyzer = analyzer;
		this.hitsPerPage = hitsPerPage;
	}

	public void setAnalyzer(Analyzer analyzer) {
		this.analyzer = analyzer;
	}

	public void setHitsPerPage(int hitsPerPage) {
		this.hitsPerPage = hitsPerPage;
	}

	@Override
	public Set<Document> query(Directory directory, String searchField,
			String searchValue) throws ParseException, IOException {
		Map<String, String> condition = new HashMap<String, String>();
		condition.put(searchField, searchValue);
		return query(directory, condition);
	}

	@Override
	public Set<Document> query(Directory directory,
			Map<String, String> searchCondition) throws ParseException,
			IOException {
		List<Directory> directories = new LinkedList<Directory>();
		directories.add(directory);
		return query(directories, searchCondition);
	}

	@Override
	public Set<Document> query(List<Directory> directories,
			Map<String, String> searchCondition) throws ParseException,
			IOException {
		if (directories.size() == 0) {
			return DefaultSearcher.emptySet;
		}
		int size = searchCondition.size();
		String[] queries = new String[size];
		String[] fields = new String[size];
		int i = 0;
		for (Entry<String, String> entry : searchCondition.entrySet()) {
			queries[i] = entry.getValue();
			fields[i] = entry.getKey();
			i++;
		}
		Query query = MultiFieldQueryParser.parse(queries, fields, analyzer);
		List<IndexReader> readers = new LinkedList<IndexReader>();
		for (Directory directory : directories) {
			IndexReader reader = DirectoryReader.open(directory);
			readers.add(reader);
		}
		MultiReader multiReader = new MultiReader(
				readers.toArray(new IndexReader[0]));
		TopScoreDocCollector collector = TopScoreDocCollector
				.create(hitsPerPage);
		IndexSearcher searcher = new IndexSearcher(multiReader);
		searcher.search(query, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;
		logger.debug(">>>> searcher hits length: " + hits.length);
		Set<Document> docs = new HashSet<Document>();
		for (ScoreDoc sd : hits) {
			Document doc = searcher.doc(sd.doc);
			docs.add(doc);
		}
		multiReader.close();
		return docs;
	}

	@Override
	public Set<Document> query(String searchField, String searchValue,
			DirectoryReader reader) throws ParseException, IOException {
		if ( reader == null) {
			return DefaultSearcher.emptySet;
		}
		Map<String, String> condition = new HashMap<String, String>();
		condition.put(searchField, searchValue);
		return query(condition, reader);
	}

	@Override
	public Set<Document> query(Map<String, String> searchCondition,
			DirectoryReader reader) throws ParseException, IOException {
		if (reader == null) {
			return DefaultSearcher.emptySet;
		}
		List<DirectoryReader> readers = new LinkedList<DirectoryReader>();
		readers.add(reader);
		return query(searchCondition, readers);
	}
	
	@Override
	public Set<Document> query(Map<String, String> searchCondition,
			List<DirectoryReader> readers) throws ParseException, IOException {
		if (readers == null || readers.size() == 0) {
			return DefaultSearcher.emptySet;
		}
		int size = searchCondition.size();
		String[] queries = new String[size];
		String[] fields = new String[size];
		int i = 0;
		for (Entry<String, String> entry : searchCondition.entrySet()) {
			queries[i] = entry.getValue();
			fields[i] = entry.getKey();
			i++;
		}
		Query query = MultiFieldQueryParser.parse(queries, fields, analyzer);
		MultiReader multiReader = new MultiReader(
				readers.toArray(new IndexReader[0]));
		TopScoreDocCollector collector = TopScoreDocCollector
				.create(hitsPerPage);
		IndexSearcher searcher = new IndexSearcher(multiReader);
		searcher.search(query, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;
		logger.debug(">>>> searcher hits length: " + hits.length);
		Set<Document> docs = new HashSet<Document>();
		for (ScoreDoc sd : hits) {
			Document doc = searcher.doc(sd.doc);
			docs.add(doc);
		}
		return docs;
	}
}
