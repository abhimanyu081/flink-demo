package com.flink.util;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import com.flink.dto.Message;

public class CustomRedisMapper implements RedisMapper<Message>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public RedisCommandDescription getCommandDescription() {
		return new RedisCommandDescription(RedisCommand.ZADD, "Gainers");
	}

	public String getKeyFromData(Message msg) {
		return msg.getMsgId();
	}

	public String getValueFromData(Message msg) {
		return String.valueOf(msg.getPercentageChange());
	}

}
