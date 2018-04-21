package com.flink.main;

import com.flink.service.DataStreamService;
import com.flink.serviceImpl.DataStreamServiceImpl;
import com.flink.util.Util;

public class StreamJob {

	public static void main(String[] args) {
		DataStreamService dataService = new DataStreamServiceImpl();
		try {
			dataService.addKafkaSource(Util.TOPIC);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
