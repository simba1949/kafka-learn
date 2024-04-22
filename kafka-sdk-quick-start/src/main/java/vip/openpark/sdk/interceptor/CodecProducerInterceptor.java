package vip.openpark.sdk.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义生产者拦截器
 *
 * @author anthony
 * @version 2024/4/22
 * @since 2024/4/22 21:34
 */
public class CodecProducerInterceptor implements ProducerInterceptor<String, String> {
	/**
	 * 发送数据的时候，会调用此方法
	 *
	 * @param record ProducerRecord
	 * @return ProducerRecord
	 */
	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		System.out.println("拦截器拦截到数据：" + record.value());
		
		String value = record.value();
		value = value.toUpperCase();
		
		return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), value);
	}
	
	/**
	 * 发送成功或者失败的时候，都会调用此方法
	 *
	 * @param metadata  RecordMetadata
	 * @param exception Exception
	 */
	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
	
	}
	
	/**
	 * 生产者对象关闭的时候，会调用此方法
	 */
	@Override
	public void close() {
	
	}
	
	/**
	 * 配置，创建生产者对象的时候，会调用此方法
	 *
	 * @param configs 配置
	 */
	@Override
	public void configure(Map<String, ?> configs) {
	
	}
}