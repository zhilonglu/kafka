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
 * 网约车平台公司服务公司所在地服务机构信息
 * @LZL 20160919
 */
public class base_info_enterprise_service_org {
	static Statement stmt = null;
	static int service_org_id = 0;
	public static String combineToStr(JSONObject js){
		String output="";//输出的value值
		//解析json文件
		int uiComId = Integer.valueOf(js.getString("uiComId"));
		String strComName  = js.getString("strComName");
		String strServiceDate = js.getString("strServiceDate");
		String strServiceName = js.getString("strServiceName");
		String uiServiceId = js.getString("uiServiceId");
		String strServiceAddress = js.getString("strServiceAddress");
		String strChaName = js.getString("strChaName");
		String strChaPhone = js.getString("strChaPhone");
		String strAdmName = js.getString("strAdmName");
		String strAdmPhone = js.getString("strAdmPhone");
		int uiFlg = Integer.valueOf(js.getString("uiFlg"));
		//数据库中的所有字段
//		int service_org_id;
		int enterprise_id=uiComId;
		String enterprise_name=strComName;
		double service_org_regionalism_code=0;
		String org_establishment_date=strServiceDate;
		String service_org_name=strServiceName;
		String service_org_code=uiServiceId;
		String service_org_address=strServiceAddress;
		String service_org_principal_name=strChaName;
		String principal_contact_infomation=strChaPhone;
		String principal_org_manager_name=strAdmName;
		String manager_contact_infomation=strAdmPhone;
		String administrative_document_post_way="";
		String administrative_document_mail_address="";
		output = enterprise_id+",'"+enterprise_name+"',"+service_org_regionalism_code+",'"+org_establishment_date+"','"+service_org_name+
				"','"+service_org_code+"','"+service_org_address+"','"+service_org_principal_name+"','"+principal_contact_infomation+"','"+principal_org_manager_name+
				"','"+manager_contact_infomation+"','"+administrative_document_post_way+"','"+administrative_document_mail_address+"')";
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
		topicCountMap.put("SERVICEBASIC", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("SERVICEBASIC");
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
						service_org_id ++;
						JSONObject js = JSONObject.fromObject(msg);
						String mysqlValue = combineToStr(js);
						String sql="INSERT INTO base_info_enterprise_service_org(service_org_id,enterprise_id,enterprise_name,service_org_regionalism_code,org_establishment_date,service_org_name,service_org_code,"+
								"service_org_address,service_org_principal_name,principal_contact_infomation,principal_org_manager_name,manager_contact_infomation,"+
								"administrative_document_post_way,administrative_document_mail_address) VALUES("+service_org_id+","+mysqlValue;
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
