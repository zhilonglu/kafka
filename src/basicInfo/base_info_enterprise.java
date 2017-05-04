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
 * 网约车平台公司基础信息
 * @LZL 20160919
 */
public class base_info_enterprise {
	static Statement stmt = null;
	static int enterprise_id = 10;
	public static String combineToStr(JSONObject js){
		String output="";//输出的value值
		//解析json文件
		int uiComId = Integer.valueOf(js.getString("uiComId"));
		String strComName  = js.getString("strComName");
		String strContactPhone = js.getString("strContactPhone");
		String strOCC = js.getString("strOCC");
		String strLicenseId = js.getString("strLicenseId");
		String strRegAddress = js.getString("strRegAddress");
		int uiFlg = Integer.valueOf(js.getString("uiFlg"));
		//数据库中的所有字段
		enterprise_id=uiComId;
		String enterprise_name=strComName;
		String business_register_numberr=strOCC;
		String organization_code_no="";
		String taxpayer_identifier="";
		String social_information_code="";
		String net_business_license_number=strLicenseId;
		String business_scope="";
		String operation_area="";
		String contact_address=strRegAddress;
		double regionalism_code=0;
		String business_industry_economic_type="";
		String business_license_validity_start="20160919";
		String business_license_validity_end="20160919";
		String business_license_issuing_authority="";
		String business_license_first_register_date="20160919";
		double register_capital=0;
		double net_register_vehicle_number=0;
		double register_driver_number=0;
		String legal_representative_name="";
		String legal_representative_id="";
		String legal_representative_id_picture="";
		String legal_representative_telephone=strContactPhone="";
		output = enterprise_name+"','"+business_register_numberr+"','"+organization_code_no+"','"+taxpayer_identifier+"','"+social_information_code+
				"','"+net_business_license_number+"','"+business_scope+"','"+operation_area+"','"+contact_address+"',"+regionalism_code+",'"+business_industry_economic_type+
				"','"+business_license_validity_start+"','"+business_license_validity_end+"','"+business_license_issuing_authority+"','"+business_license_first_register_date+"',"+register_capital+
				","+net_register_vehicle_number+","+register_driver_number+",'"+legal_representative_name+"','"+legal_representative_id+"','"+legal_representative_id_picture+"','"+legal_representative_telephone+"')";
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
		topicCountMap.put("COMPANYBASIC", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("COMPANYBASIC");
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
						enterprise_id ++;
						JSONObject js = JSONObject.fromObject(msg);
						String mysqlValue = combineToStr(js);
						String sql = "INSERT INTO base_info_enterprise(enterprise_id,enterprise_name,business_register_number,organization_code_no,taxpayer_identifier,social_information_code,net_business_license_number,business_scope,"
								+"operation_area,contact_address,regionalism_code,business_industry_economic_type,business_license_validity_start,business_license_validity_end,business_license_issuing_authority,"
								+"business_license_first_register_date,register_capital,net_register_vehicle_number,register_driver_number,legal_representative_name,legal_representative_id,legal_representative_id_picture,legal_representative_telephone) VALUES("+enterprise_id+",'"+mysqlValue;
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
