package com.tudou.isearcher.producer;

import java.util.Map;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.tudou.isearch.common.SimpleModel;
import com.tudou.isearch.indexer.agent.ProduceAgent;

@Service("simpleProducer")
public class SimpleProducer implements Producer<SimpleModel> {

	private final static Logger logger = Logger.getLogger(SimpleProducer.class);
	/**
	 * 生产数据中间缓冲队列；解决生产数据与消费数据不匹配的问题。<br>
	 * 目标是能快速生产快速消费.
	 */
	@Resource(name="simpleProduceAgent")
	private ProduceAgent<SimpleModel> agent;

	/**
	 * 生产待索引数据，并放入中间队列中等待消费
	 */
	@Override
	public SimpleModel produce(SimpleModel model) {
		return agent.produce(model);
	}
	
	public Map<String, String> status() {
		return agent.status();
	}
}
