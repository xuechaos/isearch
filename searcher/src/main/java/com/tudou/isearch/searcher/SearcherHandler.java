package com.tudou.isearch.searcher;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.tudou.isearch.common.Model;
import com.tudou.isearch.indexer.agent.SearchAgent;

/**
 * 
 * @author chenheng
 * 
 */
@Service("searcherHandler")
public class SearcherHandler<T extends Model> implements Searcher<T> {
	private static final Logger logger = Logger
			.getLogger(SearcherHandler.class);

	@Resource(name = "simpleSearchAgent")
	private SearchAgent<T> agent;

	@Override
	public List<T> search(String searchField, String searchValue) {
		return agent.search(searchField, searchValue);
	}

	@Override
	public List<T> search(Map<String, String> searchCondition) {
		return agent.search(searchCondition);
	}
}
