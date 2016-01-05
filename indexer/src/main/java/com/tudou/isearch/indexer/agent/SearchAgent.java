package com.tudou.isearch.indexer.agent;

import java.util.List;
import java.util.Map;

public interface SearchAgent<T> {
	public List<T> search(String searchField, String searchValue);

	public List<T> search(Map<String, String> searchCondition);
}
