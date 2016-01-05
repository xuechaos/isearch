package com.tudou.isearch.common;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.lucene.document.Document;

public abstract class Model implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 模型转换为对应的lucene document.<br>
	 * @param doc
	 */
	public abstract void toDocument(Document doc);
	/**
	 * 返回唯一键值
	 * @return
	 */
	public abstract String getUniqueKeyValue();
	/**
	 * 返回唯一键名称
	 * @return
	 */
	public abstract String getUniqueKeyName();
	
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}
