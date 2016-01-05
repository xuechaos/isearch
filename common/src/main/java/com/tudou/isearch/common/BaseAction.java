package com.tudou.isearch.common;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class BaseAction<T> {
	private static final Logger logger = Logger.getLogger(BaseAction.class);
	
	public JsonResponse<?> success() {
		JsonResponse<T> json = new JsonResponse<T>(0, null);
		return json;
	}

	public JsonResponse<?> success(T t) {
		JsonResponse<T> json = new JsonResponse<T>(0, t);
		return json;
	}
	
	public JsonResponse<?> success(List<T> t) {
		JsonResponse<List<T>> json = new JsonResponse<List<T>>(0, t);
		return json;
	}
	
	public JsonResponse<Map<String,String>> status(Map<String,String> map) {
		JsonResponse<Map<String,String>> json = new JsonResponse<Map<String,String>>(0, map);
		return json;
	}
	
	public JsonResponse<?> fail(T t) {
		JsonResponse<T> json = new JsonResponse<T>(1, t);
		return json;
	} 
	
	public JsonResponse<?> error(String t) {
		JsonResponse<String> json = new JsonResponse<String>(1, t);
		return json;
	}  
	
	public static class JsonResponse<T> {
		private int code;
		private T data;
		
		public JsonResponse(int code, T data) {
			this.code = code;
			this.data = data;
		}

		public int getCode() {
			return code;
		}

		public void setCode(int code) {
			this.code = code;
		}

		public T getData() {
			return data;
		}

		public void setData(T data) {
			this.data = data;
		}
	}
}
