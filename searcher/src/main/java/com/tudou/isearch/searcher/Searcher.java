package com.tudou.isearch.searcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;

/**
 * 搜索器
 * 
 * @author chenheng
 * 
 */
public interface Searcher<T> {

	/**
	 * 搜索特定字段的特定值
	 * 
	 * @param searchField
	 *            搜索字段
	 * @param searchValue
	 *            搜索值
	 * @return 搜索值命中文档集
	 * @throws ParseException
	 * @throws IOException
	 */
	public List<T> search(String searchField, String searchValue)
			throws ParseException, IOException;

	/**
	 * 
	 * @param searchCondition
	 *            搜索条件map, key字段, value为搜索值
	 * @return 搜索值命中文档集
	 * @throws ParseException
	 * @throws IOException
	 */
	public List<T> search(Map<String, String> searchCondition)
			throws ParseException, IOException;
}
