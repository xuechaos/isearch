package com.tudou.isearcher.producer.action;

import java.util.Map;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.tudou.isearch.common.BaseAction;
import com.tudou.isearch.common.SimpleModel;
import com.tudou.isearcher.producer.Producer;

@Controller
public class SimpleAction extends BaseAction<SimpleModel> {

	private static final Logger logger = Logger.getLogger(SimpleAction.class);

	@Resource(name="simpleProducer")
	private Producer<SimpleModel> simpleProducer;

	@RequestMapping(value = "/simpleModel", method = RequestMethod.POST)
	public @ResponseBody
	JsonResponse<?> pushSimpleModel(HttpServletRequest req,
			HttpServletResponse resp, @RequestBody SimpleModel model) {
		logger.debug(">>>> push simple model(" + model + ") to queue...");
		if (model.getId() == null) {
			return error("必须提供Id");
		}
		model = simpleProducer.produce(model);
		if (model != null) {
			return success(model);
		} else {
			return fail(model);
		}
	}
	
	@RequestMapping(value = "/simpleModel/status", method = RequestMethod.GET)
	public @ResponseBody
	JsonResponse<?> simpleModelStatus(HttpServletRequest req,
			HttpServletResponse resp) {
		logger.debug(">>>> simple model status...");
		Map<String,String> st = simpleProducer.status();
		return status(st);
	}

}
