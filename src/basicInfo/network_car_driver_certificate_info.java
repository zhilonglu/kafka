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
//程序完成测试，并成功写入指定数据库20160920     v1
/*
 * 网络预约出租汽车驾驶员证信息
 * @LZL 20160920
 */
public class network_car_driver_certificate_info {
	static Statement stmt = null;
	static int id = 0;
	public static String combineToStr(JSONObject js){
		String output="";//输出的value值
		//解析json文件
		int uiComId = Integer.valueOf(js.getString("uiComId"));
		String strComName  = js.getString("strComName");
		String uiDistId = js.getString("uiDistId");
		String uiFareType = js.getString("uiFareType");
		String strFareValidOn = js.getString("strFareValidOn");
		String strFareValidOff = js.getString("strFareValidOff");
		String uiStartFare = js.getString("uiStartFare");
		String strStartMile = js.getString("strStartMile");
		String uiUnitPrice = js.getString("uiUnitPrice");
		String uiUpPrice = js.getString("uiUpPrice");
		String strPeakTimeOn = js.getString("strPeakTimeOn");
		String strPeakTimeOff = js.getString("strPeakTimeOff");
		String uiPeakUnitPrice = js.getString("uiPeakUnitPrice");
		String uiPeakUpPrice = js.getString("uiPeakUpPrice");
		int uiFlg = Integer.valueOf(js.getString("uiFlg"));
		//数据库中的所有字段
//		int id;
		double province_regionalism_code=0;
		String province_regionalism_name="";
		String province_employee_id="";
		String certificate_type="";
		String nation="";
		String educational_level="";
		String contact_phone="";
		String contact_address="";
		String employee_photo_name="";
		String employed_qualification_type="";
		String employed_qualification_number="";
		String employed_qualification_beginning_time="";
		String employed_qualification_certificate_date="";
		String certificate_period_validity="";
		String certifying_authority_code="";
		String cerifying_authority_name="";
		double regionalism_code=0;
		String certificate_state="";
		String certificate_medium_type="";
		String ic_road_transport_certificate="";
		String transaction_type="";
		String create_time="";
		String update_time="";
		output = province_regionalism_code+",'"+province_regionalism_name+"','"+province_employee_id+"','"+certificate_type+"','"+nation+"','"+educational_level+
				"','"+contact_phone+"','"+contact_address+"','"+employee_photo_name+"','"+employed_qualification_type+"','"+employed_qualification_number+"','"+employed_qualification_beginning_time+
				"','"+employed_qualification_certificate_date+"','"+certificate_period_validity+"','"+certifying_authority_code+"','"+cerifying_authority_name+"',"+regionalism_code+
				",'"+certificate_state+"','"+certificate_medium_type+"','"+ic_road_transport_certificate+"','"+transaction_type+"','"+create_time+"','"+update_time+"');";
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
		topicCountMap.put("FAREBASIC", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("FAREBASIC");
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
						id ++;
						JSONObject js = JSONObject.fromObject(msg);
						String mysqlValue = combineToStr(js);
						String sql = "INSERT INTO network_car_driver_certificate_info(id,province_regionalism_code,province_regionalism_name,province_employee_id,certificate_type,nation,educational_level,contact_phone,contact_address,"
								+"employee_photo_name,employed_qualification_type,employed_qualification_number,employed_qualification_beginning_time,employed_qualification_certificate_date,"
								+"certificate_period_validity,certifying_authority_code,cerifying_authority_name,regionalism_code,certificate_state,certificate_ medium_type,"
								+"ic_road_transport_certificate,transaction_type,create_time,update_time) VALUES("+id+","+mysqlValue;
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
