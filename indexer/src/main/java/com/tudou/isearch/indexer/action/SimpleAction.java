package com.tudou.isearch.indexer.action;


import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.tudou.isearch.common.BaseAction;
import com.tudou.isearch.common.SimpleModel;
import com.tudou.isearch.indexer.IndexerManager3;
import com.tudou.isearch.indexer.Manager;
import com.tudou.isearch.indexer.RAMIndexer4;
import com.tudou.isearch.indexer.agent.SimpleSearchAgent;

@Controller
public class SimpleAction extends BaseAction<SimpleModel> {

	private static final Logger logger = Logger.getLogger(SimpleAction.class);

	@Resource(name = "indexerManager")
	private Manager<SimpleModel> indexerManager;
	@Resource(name = "simpleSearchAgent")
	private SimpleSearchAgent simpleSearchAgent;

	@RequestMapping(value = "/mergeManually", method = RequestMethod.PUT)
	public @ResponseBody
	JsonResponse<?> mergeManually(HttpServletRequest req,
			HttpServletResponse resp) {
		logger.debug(">>>> merge ram directory to fs directory manually...");
		indexerManager.mergeManually();
		return success();
	}
	//Test
	@RequestMapping(value = "/createIndex")
	public @ResponseBody
	JsonResponse<?> createIndex(HttpServletRequest req,
			HttpServletResponse resp, @RequestParam(value = "id") String id,
			@RequestParam(value = "desc") String desc) {
		logger.debug(">>>> createIndex by ram directory to ...");
		try {
			SimpleModel simpleModel = new SimpleModel();
			simpleModel.setId(id);
			simpleModel.setDesc(desc);
			IndexerManager3<SimpleModel> iManager3 = (IndexerManager3<SimpleModel>) indexerManager;
			RAMIndexer4<SimpleModel> rIndexer = (RAMIndexer4<SimpleModel>) iManager3
					.getRamIndexers().get(0);
			rIndexer.syncBuildDocumentWithoutCommit(simpleModel);
		} catch (Exception e) {
			logger.error("SimpleAction.createIndex", e);
		}
		return success();
	}
	@RequestMapping(value = "/search")
	public @ResponseBody
	JsonResponse<?> search(HttpServletRequest req,
			HttpServletResponse resp, @RequestParam(value = "id") String id){
		
		List<SimpleModel> resultList = simpleSearchAgent.search("simpleId", id);
		return success(resultList);
	}
	
	@RequestMapping(value = "/deleteSegIndex")
	public @ResponseBody
	JsonResponse<?> deleteSegIndex(HttpServletRequest req,
			HttpServletResponse resp, @RequestParam(value = "id") String id){
		
		IndexerManager3<SimpleModel> iManager3 = (IndexerManager3<SimpleModel>) indexerManager;
		RAMIndexer4<SimpleModel> rIndexer = (RAMIndexer4<SimpleModel>) iManager3
				.getRamIndexers().get(0);
		rIndexer.deleteSegmentIndexesTest(id);
		return success();
	}
	
}
