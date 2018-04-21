package com.flink.util;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.flink.dto.Message;

public class KafkaFlinkGenericDesrializerSchema<T> extends AbstractDeserializationSchema<List<Message>>{

	
	public KafkaFlinkGenericDesrializerSchema() {
		
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public List<Message> deserialize(byte[] byteArr) throws IOException {


		if (byteArr == null) {
			return null;
		}
		ObjectMapper mapper = new ObjectMapper();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
		try {
			List<Message> msgObject = mapper.readValue(byteArr, new TypeReference<List<Message>>() { });
			return msgObject;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	
	}

}
