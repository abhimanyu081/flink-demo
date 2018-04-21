package com.flink.service;

public interface DataStreamService {
	
	public void addKafkaSource(String topicPattern) throws Exception;

}
