package vip.openpark.sdk.topic;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;

/**
 * @author anthony
 * @version 2024/4/21
 * @since 2024/4/21 21:28
 */
@Slf4j
public class CreateTopicApplication {
	public static void main(String[] args) {
		Properties properties = new Properties();
		// 配置 kafka 连接地址
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.8.43:9092");
		// 创建 admin
		Admin admin = Admin.create(properties);
		
		// 创建 topic：topic 名称，分区数，副本数
		NewTopic newTopic = new NewTopic("test-topic", 1, (short) 1);
		
		CreateTopicsResult createTopicsResult = admin.createTopics(Collections.singleton(newTopic));
		createTopicsResult
			.all()
			.whenComplete((result, e) -> {
				if (e != null) {
					log.error("create topic error", e);
				} else {
					log.info("create topic success");
				}
			});
		
		// 关闭 admin
		admin.close();
	}
}