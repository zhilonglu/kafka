package basicInfo;
//行政区代码详情表
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
public class regionalism_code_detail {
	public final ConsumerConnector consumer;
	public Connection conn = null;
	public Statement stmt = null;
	public int result = 0;
	public String sql = "";
	public regionalism_code_detail() {
		Properties props = new Properties();
		//zookeeper 配置
		props.put("zookeeper.connect", "redis1.hhdata.com:2181,redis2.hhdata.com:2181,sql1.hhdata.com:2181");
		//group 代表一个消费组
		props.put("group.id", "jd-group");
		//zk连接超时
		props.put("zookeeper.session.timeout.ms", "400000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		//序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ConsumerConfig config = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			String databaseName = "nacp_baseinfo_publish";// 已经在MySQL数据库中创建好的数据库。
			String userName = "hhdata";// MySQL默认的root账户名
			String password = "123456";// 默认的root账户密码为空
			conn = DriverManager.getConnection("jdbc:mysql://sql1.hhdata.com:3306/" + databaseName, userName, password);
			stmt = conn.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void consume() 
	{
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put("VEHGNSS", new Integer(1));
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> consumerMap =
				consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
		KafkaStream<String, String> stream = consumerMap.get("VEHGNSS").get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		while (it.hasNext())
		{
			String message = new String(it.next().message());
			JSONObject js = JSONObject.fromObject(message);
			String uiCompany = js.getString("uiCompanyId");
			String strVin = js.getString("strVin");
			String strPositionTime = js.getString("strPositionTime");
			String strLongitude = js.getString("strLongitude");
			String strLatitude = js.getString("strLatitude");
			String mysqlValue = "VALUES('"+uiCompany+"',\'"+strVin+"\','"
					+strPositionTime+"\','"+strLongitude+"\','"+strLatitude+"')";
			try {
				sql = "INSERT INTO gpsTest(uiCompanyId,strVin,strPositionTime,strLongitude,strLatitude) "+mysqlValue;
				result = stmt.executeUpdate(sql);
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println(it.next().message());
		}
	}
	public static void main(String[] args) {
		new regionalism_code_detail().consume();
	}
}








