package testKafka2;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
public class consumer2 {
	private static final String TOPIC = "GPS0405";
	private static final int THREAD_AMOUNT = 1;
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zookeeper.connect", "redis1.hhdata.com:2181,redis2.hhdata.com:2181,sql1.hhdata.com:2181");
		props.put("group.id", "group1");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");;
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		//ÿ��topicʹ�ö��ٸ�kafkastream��ȡ, ���consumer
		topicCountMap.put(TOPIC, THREAD_AMOUNT);
		//���Զ�ȡ���topic
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		Map<String, List<KafkaStream<byte[], byte[]>>> msgStreams = consumer.createMessageStreams(topicCountMap );
		List<KafkaStream<byte[], byte[]>> msgStreamList = msgStreams.get(TOPIC);
		//ʹ��ExecutorService�������߳�
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_AMOUNT);
		for (int i = 0; i < msgStreamList.size(); i++) {
			KafkaStream<byte[], byte[]> kafkaStream = msgStreamList.get(i);
			executor.submit(new HanldMessageThread(kafkaStream, i));
		}
		//�ر�consumer
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (consumer != null) {
			consumer.shutdown();
		}
		if (executor != null) {
			executor.shutdown();
		}
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}
}
/**
 * ���崦��message���߳�
 * @author Administrator
 *
 */
class HanldMessageThread implements Runnable {
	private KafkaStream<byte[], byte[]> kafkaStream = null;
	private int num = 0;
	public HanldMessageThread(KafkaStream<byte[], byte[]> kafkaStream, int num) {
		super();
		this.kafkaStream = kafkaStream;
		this.num = num;
	}
	public void run() {
		ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
		while(iterator.hasNext()) {
			String message = new String(iterator.next().message());
			System.out.println("Thread no: " + num + ", message: " + message);
		}
	}
}
