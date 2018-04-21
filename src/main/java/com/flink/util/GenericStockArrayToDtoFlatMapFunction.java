package com.flink.util;

import java.util.List;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import com.flink.dto.Message;

public class GenericStockArrayToDtoFlatMapFunction<T> extends RichFlatMapFunction<List<Message>, Message> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(List<Message> data, Collector<Message> out) throws Exception {
		for (Message liveData : data) {
			out.collect(liveData);
		}

	}

}