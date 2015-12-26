package com.tudou.isearch;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.lucene.document.Document;

public abstract class Model {
	
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
