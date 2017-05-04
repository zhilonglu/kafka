//驾驶员基础信息
package basicInfo;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.sf.json.JSONObject;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
//程序完成测试，并成功写入指定数据库20160919     v1
/*
 * @LZL 20160919
 */
public class base_info_register_passenger {
	static Statement stmt = null;
	static int register_passenger_id = 5;
	public static String combineToStr(JSONObject js){
		String output="";//输出的value值
		//解析json文件
		int uiComId = Integer.valueOf(js.getString("uiComId"));
		String strComName  = js.getString("strComName");
		int uiPassengerId = Integer.valueOf(js.getString("uiPassengerId"));
		String strPassengerName = js.getString("strPassengerName");
		String strRegisterDate = js.getString("strRegisterDate");
		String strPassengerPhone = js.getString("strPassengerPhone");
		String strCardType = js.getString("strCardType");
		String strCardId= js.getString("strCardId");
		int uiFlg = Integer.valueOf(js.getString("uiFlg"));
		//数据库中的所有字段
//		int register_passenger_id;
		int enterprise_id=uiComId;
		String enterprise_name=strComName;
		String register_time=strRegisterDate;
		String passenger_appellation="";
		String passenger_telephone=strPassengerPhone;
		String passenger_name=strPassengerName;
		String passenger_gender="";
		String id_type=strCardType;
		String id_no=strCardId;
		output =","+enterprise_id+",'"+enterprise_name+"','"+register_time+"','"+passenger_appellation+"','"+passenger_telephone+"','"
				+passenger_name+"','"+passenger_gender+"','"+id_type+"','"+id_no+"')";
		return output;
	}
	public static void consume() 
	{
		Properties props = new Properties();
		props.put("zookeeper.connect", "redis1.hhdata.com:2181,redis2.hhdata.com:2181,sql1.hhdata.com:2181");
		props.put("group.id", "1111");
		props.put("auto.offset.reset", "smallest");
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put("PASSENGERBASIC", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("PASSENGERBASIC");
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String databaseName = "nacp_baseinfo_publish";// 已经在MySQL数据库中创建好的数据库。
			String userName = "root";// MySQL默认的root账户名
			String password = "111111";// 默认的root账户密码为空
			conn = DriverManager.getConnection("jdbc:mysql://192.168.1.217:3306/" + databaseName, userName, password);
			stmt = conn.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
		for(final KafkaStream<byte[], byte[]> kafkaStream : streams){
			new Thread(new Runnable() {
				@Override
				public void run() {
					for(MessageAndMetadata<byte[], byte[]> mm : kafkaStream){
						String msg = null;
						try {
							msg = new String(mm.message(),"UTF-8");
						} catch (UnsupportedEncodingException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						System.out.println(msg);
						register_passenger_id ++;
						JSONObject js = JSONObject.fromObject(msg);
						String mysqlValue = combineToStr(js);
						String sql= "INSERT INTO base_info_register_passenger(register_passenger_id,enterprise_id,enterprise_name,"+
								"register_time,passenger_appellation,passenger_telephone"+
								",passenger_name,passenger_gender,id_type,id_no)"+"VALUES("+register_passenger_id+mysqlValue;
						try {
							stmt.execute(sql);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}).start();
		}
	}
	public static void main(String[] args) {
		consume();
	}
}
