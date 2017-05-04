//驾驶员基础信息
package kafkaToMysql;
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
//程序完成测试，并成功写入指定数据库20160919
/*
 * @LZL 20160919
 */
public class base_info_driver {
	static Statement stmt = null;
	static int driver_id = 5;
	public static String combineToStr(JSONObject js){
		String output="";//输出的value值
		//解析json文件
		int uiComId = Integer.valueOf(js.getString("uiComId"));
		String strComName  = js.getString("strComName");
		int uiDistId = Integer.valueOf(js.getString("uiDistId"));
		String strLicenseId = js.getString("strLicenseId");
		String strDriverName = js.getString("strDriverName");
		String strDirverType = js.getString("strDriverTye");
		String strRegisterDate = js.getString("strRegisterDate");
		String strDriverPhotoAddr = js.getString("strDriverPhotoAddr");
		String strDriverPhotoDesp = js.getString("strDriverPhotoDesp");
		String strDriverPhone= js.getString("strDriverPhone");
		String uiFlg = js.getString("uiFlg");
		//数据库中的所有字段
		//int driver_id = 20160918;
		int enterprise_id = uiComId;
		String enterprise_name= strComName;
		int vehicle_id=0;
		String vehicle_driver_number=strLicenseId;
		String vehicle_driver_name=strDriverName;
		String gender="";
		String nationality="";
		String household_register="";
		String address="";
		String birth_date="20160919";
		String driving_licence="";
		String driving_licence_first_date=strRegisterDate;
		String quasi_driving_type="";
		String driving_licence_validity_start="20160919";
		String driving_licence_validity_end="20160919";
		String net_driving_licence_number="";
		String driver_picture=strDriverPhotoAddr;
		String driver_nation="";
		String driver_marital_status="";
		String driver_foreign_language_ability="";
		String driver_telephone=strDriverPhone;
		String driver_address="";
		String driver_education="";
		String net_driver_licence_first_date="20160919";
		String driver_licence_lssuing_date="20160919";
		String driver_licence_validity_end="20160919";
		String driver_licence_org="20160919";
		String driver_training_course_name="";
		String training_course_date="20160919";
		String train_begin_time="20160919";
		String train_end_time="20160919";
		int train_long_hour=0;
		int complete_order_number=0;
		int traffic_violation_number=0;
		int driver_contract_sign_enterprise_id=0;
		String sigin_date="20160919";
		String contract_due_time="20160919";
		String contract_validity_date="20160919";
		String is_parade_taxi_driver="";
		String full_or_part_time_driver="";
		String is_in_blacklist="";
		String driver_phone_model="";
		String driver_phone_operator="";
		String driver_app_version_no="";
		String use_map_type="";
		String emergency_contact="";
		String emergency_contact_telephone="";
		String emergency_contact_postal_address="";
		String driver_service_quality_examination_result="";
		String traffic_accident_record="";
		int single_breach_record=0;
		int service_evaluation_score=0;
		int complaint_number=0;
		output =","+enterprise_id+",'"+enterprise_name+"',"+vehicle_id+",'"+vehicle_driver_number+"','"
				+ vehicle_driver_name+"','"+ gender+"','"+ nationality+"','"+ household_register+"','"+ address+"','"+ birth_date+"','"+ driving_licence+"','"
				+ driving_licence_first_date+"','"+ quasi_driving_type+"','"+ driving_licence_validity_start+"','"+ driving_licence_validity_end+"','"
				+ net_driving_licence_number+"','"+ driver_picture+"','"+ driver_nation+"','"+ driver_marital_status+"','"+ driver_foreign_language_ability+"','"
				+ driver_telephone+"','"+ driver_address+"','"+ driver_education+"','"+ net_driver_licence_first_date+"','"+ driver_licence_lssuing_date+"','"
				+ driver_licence_validity_end+"','"+ driver_licence_org+"','"+ driver_training_course_name+"','"+ training_course_date+"','"
				+ train_begin_time+"','"+ train_end_time+"',"+train_long_hour+","+complete_order_number+","+traffic_violation_number+","+
				driver_contract_sign_enterprise_id+",'"+ sigin_date+"','"+ contract_due_time+"','"+ contract_validity_date+"','"
				+ is_parade_taxi_driver+"','"+ full_or_part_time_driver+"','"+ is_in_blacklist+"','"+ driver_phone_model+"','"+ driver_phone_operator+"','"
				+ driver_app_version_no+"','"+ use_map_type+"','"+ emergency_contact+"','"+ emergency_contact_telephone+"','"+ emergency_contact_postal_address+"','"
				+ driver_service_quality_examination_result+"','"+ traffic_accident_record+"',"+single_breach_record+","+service_evaluation_score+","+complaint_number+")";
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
		topicCountMap.put("DRIVERBASIC", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("DRIVERBASIC");
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
						driver_id ++;
						JSONObject js = JSONObject.fromObject(msg);
						String mysqlValue = combineToStr(js);
						String sql= "INSERT INTO base_info_driver(driver_id,enterprise_id,enterprise_name,vehicle_id,vehicle_driver_number,vehicle_driver_name,gender,nationality,household_register,"+
								"address,birth_date,driving_licence,driving_licence_first_date,quasi_driving_type,driving_licence_validity_start,driving_licence_validity_end,"+
								"net_driving_licence_number,driver_picture,driver_nation,driver_marital_status,driver_foreign_language_ability,driver_telephone,driver_address,"+
								"driver_education,net_driver_licence_first_date,driver_licence_lssuing_date,driver_licence_validity_end,driver_licence_org,driver_training_course_name,"+
								"training_course_date,train_begin_time,train_end_time,train_long_hour,complete_order_number,traffic_violation_number,driver_contract_sign_enterprise_id,"+
								"sigin_date,contract_due_time,contract_validity_date,is_parade_taxi_driver,full_or_part_time_driver,is_in_blacklist,"+
								"driver_phone_model,driver_phone_operator,driver_app_version_no,use_map_type,emergency_contact,emergency_contact_telephone,emergency_contact_postal_address,"+
								"driver_service_quality_examination_result,traffic_accident_record,single_breach_record,service_evaluation_score,complaint_number)"+"VALUES("+driver_id+mysqlValue;
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
