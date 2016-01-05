package com.tudou.isearch.indexer.agent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Resource;

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
import org.apache.lucene.store.AlreadyClosedException;

import com.tudou.isearch.common.SimpleModel;
import com.tudou.isearch.indexer.Manager;
import com.tudou.isearch.indexer.ReaderWrapper;

public class SimpleSearchAgent implements SearchAgent<SimpleModel> {

	private static final Logger logger = Logger
			.getLogger(SimpleSearchAgent.class);
	/**
	 * 空集合,请不要往此集合中放入任何文档.为避免空指定问题.
	 */
	private final List<SimpleModel> emptyList = new ArrayList<SimpleModel>();

	@Resource(name = "analyzer")
	private Analyzer analyzer;

	@Resource(name = "indexerManager")
	private Manager<?> manager;

	private int hitsPerPage;

	@Override
	public List<SimpleModel> search(String searchField, String searchValue) {
		Map<String, String> condition = new HashMap<String, String>();
		condition.put(searchField, searchValue);
		return search(condition);
	}

	@Override
	public List<SimpleModel> search(Map<String, String> searchCondition) {
		//int i = 3;
		//while (--i >= 0) {
			try {
				return reenterableSearch(searchCondition);
			} catch (AlreadyClosedException e) {
			}
		//}
		//logger.warn("!!!! retry 3 times while AlreadyClosedException throwed, but still failed!");
		return emptyList;
	}

	private List<SimpleModel> reenterableSearch(
			Map<String, String> searchCondition) throws AlreadyClosedException {
		ReaderWrapper wrapper = manager.getReaderWrapper();
		if (wrapper == null) {
			logger.warn("!!!! no any available lucene directory!");
			return emptyList;
		}
		Set<Document> docSet = new HashSet<Document>();
		List<DirectoryReader> readers = new ArrayList<DirectoryReader>();
		List<DirectoryReader> segReaders = wrapper.getSegmentReaders();
		if (segReaders != null && !segReaders.isEmpty()) {
			readers.addAll(segReaders);
		}
		//List<DirectoryReader> ramReaders = wrapper.getRamReaders();
		//if (ramReaders != null && !ramReaders.isEmpty()) {
		//	readers.addAll(ramReaders);
		//}
		//DirectoryReader assembReader = wrapper.getAssembledReader();
		//if (assembReader != null) {
		//	readers.add(assembReader);
		//}
		try {
			docSet.addAll(query(searchCondition, readers));
		} catch (ParseException | IOException e) {
			logger.error("!!!! Exception", e);
		}
		List<SimpleModel> list = new ArrayList<SimpleModel>();
		for (Document doc : docSet) {
			list.add(SimpleModel.fromDocument(doc));
		}
		return list;
	}

	private Set<Document> query(Map<String, String> searchCondition,
			List<DirectoryReader> readers) throws ParseException, IOException {
		if (readers.size() == 0 || searchCondition == null
				|| searchCondition.isEmpty()) {
			return new HashSet<Document>();
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

	public Analyzer getAnalyzer() {
		return analyzer;
	}

	public void setAnalyzer(Analyzer analyzer) {
		this.analyzer = analyzer;
	}

	public int getHitsPerPage() {
		return hitsPerPage;
	}

	public void setHitsPerPage(int hitsPerPage) {
		this.hitsPerPage = hitsPerPage;
	}

	public Manager<?> getManager() {
		return manager;
	}

	public void setManager(Manager<?> manager) {
		this.manager = manager;
	}
}
