package com.tudou.isearch.searcher.action;

import java.io.IOException;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.tudou.isearch.common.BaseAction;
import com.tudou.isearch.common.SimpleModel;
import com.tudou.isearch.searcher.Searcher;

@Scope("singleton")
@Controller
public class SimpleSearchAction extends BaseAction<SimpleModel> {
	private static final Logger logger = Logger
			.getLogger(SimpleSearchAction.class);

	@Resource(name = "searcherHandler")
	private Searcher<SimpleModel> searcher;

	@RequestMapping(value = "/index")
	public String index(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		return "search";
	}

	@RequestMapping(value = "/q", method = RequestMethod.GET)
	public String search(HttpServletRequest request,
			HttpServletResponse response,
			@RequestParam(value = "id") String id, Model model)
			throws IOException {
		try {
			List<SimpleModel> list = searcher.search(SimpleModel.UNIQUE_KEY, id);
			model.addAttribute("resultList", list);
		} catch (ParseException e) {
			e.printStackTrace();
			logger.error("!!!! ParseException", e);
		}
		return "search";
	}

	@RequestMapping(value = "/jsonq", method = RequestMethod.GET)
	public @ResponseBody
	JsonResponse<?> search(HttpServletRequest request,
			HttpServletResponse response, @RequestParam(value = "id") String id)
			throws IOException {
		SimpleModel sm = new SimpleModel();
		try {
			List<SimpleModel> list = searcher.search(sm.getUniqueKeyName(), id);
			return success(list);
		} catch (ParseException e) {
			e.printStackTrace();
			logger.error("!!!! ParseException", e);
			return fail(null);
		}
	}
}
