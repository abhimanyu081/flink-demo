package com.flink.util;

import org.apache.flink.api.common.functions.FilterFunction;

import com.flink.dto.Message;

public class CustomFilterFunction implements FilterFunction<Message>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public boolean filter(Message msg) throws Exception {
		if(msg.getPercentageChange()>0) {
			return true;
		}
		return false;
	}

}
