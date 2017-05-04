package testKafka2;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.sf.json.JSONObject;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
/*
 * �����������ݣ��ɹ���ȡkafka�е�����
 */
public class consumer3 {
	public final ConsumerConnector consumer;
	public Connection conn = null;
	public Statement stmt = null;
	public int result = 0;
	public String sql = "";
	public consumer3() {
		Properties props = new Properties();
		//zookeeper ����
		props.put("zookeeper.connect", "redis1.hhdata.com:2181,redis2.hhdata.com:2181,sql1.hhdata.com:2181");
		//group ����һ��������
		props.put("group.id", "jd-group");
		//zk���ӳ�ʱ
		props.put("zookeeper.session.timeout.ms", "400000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		//���л���
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ConsumerConfig config = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			String databaseName = "gpsCount";// �Ѿ���MySQL���ݿ��д����õ����ݿ⡣
			String userName = "hhdata";// MySQLĬ�ϵ�root�˻���
			String password = "123456";// Ĭ�ϵ�root�˻�����Ϊ��
			conn = DriverManager.getConnection("jdbc:mysql://sql1.hhdata.com:3306/" + databaseName, userName, password);
			stmt = conn.createStatement();
			//			String sql = "CREATE TABLE gpsTest(uiCompanyId varchar(20),strVin varchar(20),strPositionTime varchar(20),strLongitude varchar(20),strLatitude varchar(20))";
			//			// �������ݿ��еı�
			//			result = stmt.executeUpdate(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void consume() 
	{
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put("DRIVERBASIC", new Integer(1));
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> consumerMap =
				consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
		KafkaStream<String, String> stream = consumerMap.get("DRIVERBASIC").get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		while (it.hasNext())
		{
//			String message = new String(it.next().message());
//			JSONObject js = JSONObject.fromObject(message);
//			String uiCompany = js.getString("uiCompanyId");
//			String strVin = js.getString("strVin");
//			String strPositionTime = js.getString("strPositionTime");
//			String strLongitude = js.getString("strLongitude");
//			String strLatitude = js.getString("strLatitude");
//			String mysqlValue = "VALUES('"+uiCompany+"\',\'"+strVin+"\','"
//					+strPositionTime+"\','"+strLongitude+"\','"+strLatitude+"')";
//			try {
//				sql = "INSERT INTO gpsTest(uiCompanyId,strVin,strPositionTime,strLongitude,strLatitude) "+mysqlValue;
//				result = stmt.executeUpdate(sql);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
			System.out.println(it.next().message());
		}
	}
	public static void main(String[] args) {
		new consumer3().consume();
	}
}
