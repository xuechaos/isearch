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
public interface Searcher {

	/**
	 * 搜索特定目录下的特定字段的特定值
	 * @param directory 索引目录
	 * @param searchField 搜索字段
	 * @param searchValue 搜索值
	 * @return 搜索值命中文档集
	 * @throws ParseException
	 * @throws IOException
	 */
	public Set<Document> query(Directory directory, String searchField,
			String searchValue) throws ParseException, IOException;

	/**
	 * 搜索特定目录下的文档
	 * @param directory 索引目录
	 * @param searchCondition 搜索条件map, key字段, value为搜索值
	 * @return 搜索值命中文档集
	 * @throws ParseException
	 * @throws IOException
	 */
	public Set<Document> query(Directory directory,
			Map<String, String> searchCondition) throws ParseException,
			IOException;
	
	/**
	 * 搜索特定目录集下的文档
	 * @param directories 索引目录集合
	 * @param searchCondition 搜索条件map, key字段, value为搜索值
	 * @return 搜索值命中文档集
	 * @throws ParseException
	 * @throws IOException
	 */
	public Set<Document> query(List<Directory> directories,
			Map<String, String> searchCondition) throws ParseException,
			IOException;
	/**
	 * 搜索特定只读器下的特定字段的特定值
	 * @param searchField 搜索字段
	 * @param searchValue 搜索值
	 * @param reader 只读器
	 * @return 搜索值命中文档集
	 * @throws ParseException
	 * @throws IOException
	 */
	public Set<Document> query(String searchField, String searchValue,
			DirectoryReader reader) throws ParseException, IOException;
	
	/**
	 * 搜索特定只读器下的文档
	 * @param searchCondition 搜索条件map, key字段, value为搜索值
	 * @param reader 只读器
	 * @return 搜索值命中文档集
	 * @throws ParseException
	 * @throws IOException
	 */
	public Set<Document> query(Map<String, String> searchCondition,
			DirectoryReader reader) throws ParseException, IOException;
	
	/**
	 * 搜索特定只读器列表下的文档
	 * @param searchCondition 搜索条件map, key字段, value为搜索值
	 * @param readers 只读器列表
	 * @return 搜索值命中文档集
	 * @throws ParseException
	 * @throws IOException
	 */
	public Set<Document> query(Map<String, String> searchCondition,
			List<DirectoryReader> readers) throws ParseException, IOException;
}
