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
 * 运价信息
 * @LZL 20160919
 */
public class base_info_freight {
	static Statement stmt = null;
	static int freight_info_id = 0;
	public static String combineToStr(JSONObject js){
		String output="";//输出的value值
		//解析json文件
		int uiComId = Integer.valueOf(js.getString("uiComId"));
		String strComName  = js.getString("strComName");
		String uiDistId = js.getString("uiDistId");
		String uiFareType = js.getString("uiFareType");
		String strFareValidOn = js.getString("strFareValidOn");
		String strFareValidOff = js.getString("strFareValidOff");
		String strStartFare = js.getString("strStartFare");
		String strStartMile = js.getString("strStartMile");
		String strUnitPrice = js.getString("strUnitPrice");
		String strUpPrice = js.getString("strUpPrice");
		String strPeakTimeOn = js.getString("strPeakTimeOn");
		String strPeakTimeOff = js.getString("strPeakTimeOff");
		String strPeakUnitPrice = js.getString("strPeakUnitPrice");
		String strPeakUpPrice = js.getString("strPeakUpPrice");
		int uiFlg = Integer.valueOf(js.getString("uiFlg"));
		//数据库中的所有字段
//		int freight_info_id;
		int enterprise_id=uiComId;
		String enterprise_name=strComName;
		double regionalism_code=Integer.valueOf(uiDistId);
		int freight_type_id=0;
		String freight_type=uiFareType;
		String freight_validity_start_date=strFareValidOn;
		String freight_validity_end_date=strFareValidOff;
		double start_price=Double.valueOf(strStartFare);
		double start_mileage=Double.valueOf(strStartMile);
		double unit_price=Double.valueOf(strUnitPrice);
		double one_way_markup_unit_price=Double.valueOf(strUpPrice);
		double one_way_markup_km=0;
		String operate_peak_time_start="20160919"+strPeakTimeOn+"00";
		String operate_peak_time_end="20160919"+strPeakTimeOff+"00";
		double peak_period_markup_unit_price = Double.valueOf(strPeakUnitPrice);
		double peak_period_markup_km = Double.valueOf(strPeakUpPrice);
		output = enterprise_id+",'"+enterprise_name+"',"+regionalism_code+","+freight_type_id+",'"+freight_type+
				"','"+freight_validity_start_date+"','"+freight_validity_end_date+"',"+start_price+","+start_mileage+","+unit_price+","+one_way_markup_unit_price+
				","+one_way_markup_km+",'"+operate_peak_time_start+"','"+operate_peak_time_end+"',"+peak_period_markup_unit_price+","+peak_period_markup_km+");";
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
						freight_info_id ++;
						JSONObject js = JSONObject.fromObject(msg);
						String mysqlValue = combineToStr(js);
						String sql = "INSERT INTO base_info_freight(freight_info_id,enterprise_id,enterprise_name,regionalism_code,freight_type_id,freight_type,freight_validity_start_date,freight_validity_end_date,"+
								"start_price,start_mileage,unit_price,one_way_markup_unit_price,one_way_markup_km,operate_peak_time_start,operate_peak_time_end,peak_period_markup_unit_price,peak_period_markup_km) VALUES("+freight_info_id+","+mysqlValue;
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
