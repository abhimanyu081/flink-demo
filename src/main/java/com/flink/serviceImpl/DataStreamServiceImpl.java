package com.flink.serviceImpl;

import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flink.dto.Message;
import com.flink.service.DataStreamService;
import com.flink.util.CustomFilterFunction;
import com.flink.util.CustomRedisMapper;
import com.flink.util.GenericStockArrayToDtoFlatMapFunction;
import com.flink.util.KafkaFlinkGenericDesrializerSchema;

public class DataStreamServiceImpl implements DataStreamService {
	
	private Logger LOG = LoggerFactory.getLogger(getClass());

	private StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
	
	TypeInformation<List<Message>> typeInformation = TypeInformation.of(new TypeHint<List<Message>>() {
	});


	public void addKafkaSource(String topicPattern) throws Exception {
		
		LOG.info("TOPIC = {}",topicPattern);

		FlinkKafkaConsumer011<List<Message>> kafkaConsumerSource = new FlinkKafkaConsumer011<List<Message>>(topicPattern,
				new KafkaFlinkGenericDesrializerSchema<List<Message>>(), getKafkaConsumerProperties());

		LOG.info("Kakfa Consumer created.");
		
		DataStream<List<Message>> stream=environment.addSource(kafkaConsumerSource, getProducedType());
		
		LOG.info("Data Stream source added....");
		
		DataStream<Message> convertedStream=stream.flatMap(new GenericStockArrayToDtoFlatMapFunction<List<Message>>()).setParallelism(32);
		
		LOG.info("Data Stream source converted....");
		
		convertedStream.filter(new CustomFilterFunction())
		.setParallelism(32)
		.addSink( new RedisSink<Message>(getJedisPoolConfig(), new CustomRedisMapper()) )
		.setParallelism(32)
		;
		
		environment.execute("Demo Stream Job");

	}

	private FlinkJedisConfigBase getJedisPoolConfig() {

		return new FlinkJedisPoolConfig.Builder().setHost("localhost")
				.setPort(6379)
				.setDatabase(0).setMaxTotal(20).setTimeout(60 * 1000)
				.build();
	
		
	}

	
	public TypeInformation<List<Message>> getProducedType() {
		return typeInformation;
	}

	private Properties getKafkaConsumerProperties() {

		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9096");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "BSE");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "2638400");

		return properties;
	
		
	}

}
