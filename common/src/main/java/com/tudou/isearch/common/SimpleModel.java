package com.tudou.isearch.common;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

import com.tudou.isearch.common.Model;

public class SimpleModel extends Model {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String id;
	private String desc;
	public static final String UNIQUE_KEY = "simpleId";

	@Override
	public void toDocument(Document doc) {
		doc.add(new StringField(getUniqueKeyName(), id, Field.Store.YES));
		doc.add(new StringField("desc", desc, Field.Store.YES));
	}
	
	public static SimpleModel fromDocument(Document doc) {
		SimpleModel sm = new SimpleModel();
		sm.setId(doc.getField(UNIQUE_KEY).stringValue());
		sm.setDesc(doc.getField("desc").stringValue());;
		return sm;
	}

	@Override
	public String getUniqueKeyValue() {
		return id;
	}

	@Override
	public String getUniqueKeyName() {
		return UNIQUE_KEY;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}
}
