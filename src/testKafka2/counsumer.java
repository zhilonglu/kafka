package testKafka2;
import java.util.HashMap;
import java.util.List;  
import java.util.Map;  
import java.util.Properties;  
import kafka.consumer.ConsumerConfig;  
import kafka.consumer.ConsumerIterator;  
import kafka.consumer.KafkaStream;  
import kafka.javaapi.consumer.ConsumerConnector;
public class counsumer extends Thread{
	private final ConsumerConnector consumer;  
	private final String topic;  
	public static void main(String[] args) {  
		counsumer consumerThread = new counsumer("DRIVERBASIC");  
		consumerThread.start();  
	}  
	public counsumer(String topic) {  
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());  
		this.topic = topic;  
	}  
	private static ConsumerConfig createConsumerConfig() {  
		Properties props = new Properties();  
		// 设置zookeeper的链接地址
		props.put("zookeeper.connect","redis1.hhdata.com:2181,redis2.hhdata.com:2181,sql1.hhdata.com:2181");  
		// 设置group id
		props.put("group.id", "1");  
		// kafka的group 消费记录是保存在zookeeper上的, 但这个信息在zookeeper上不是实时更新的, 需要有个间隔时间更新
		props.put("auto.commit.interval.ms", "1000");
		props.put("zookeeper.session.timeout.ms","10000");  
//		props.put("metadata.broker.list", "192.168.1.33:6667,192.168.1.36:6667");
		return new ConsumerConfig(props);  
	}  
	public void run(){  
		//设置Topic=>Thread Num映射关系, 构建具体的流
		Map<String,Integer> topickMap = new HashMap<String, Integer>();  
		topickMap.put(topic, 1);  
		Map<String, List<KafkaStream<byte[],byte[]>>>  streamMap=consumer.createMessageStreams(topickMap);  
		KafkaStream<byte[],byte[]>stream = streamMap.get(topic).get(0);  
		ConsumerIterator<byte[],byte[]> it =stream.iterator();  
		System.out.println("*********Results********");  
		while(it.hasNext()){  
			System.out.println("get data:" +new String(it.next().message()));  
			try {  
				Thread.sleep(1000);  
			} catch (InterruptedException e) {  
				e.printStackTrace();  
			}  
		}  
	}  
}