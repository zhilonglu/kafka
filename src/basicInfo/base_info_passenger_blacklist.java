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
//������ɲ��ԣ����ɹ�д��ָ�����ݿ�20160919     v1
/*
 * �˿ͺ�������Ϣ 
 * @LZL 20160920
 */
public class base_info_passenger_blacklist {
	static Statement stmt = null;
	static int blacklist_id = 0;
	public static String combineToStr(JSONObject js){
		String output="";//�����valueֵ
		//����json�ļ�
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
		//���ݿ��е������ֶ�
//		int blacklist_id;
		int passenger_id=0;
		int enterprise_id=0;
		String into_blacklist_date="";
		String out_blacklist_date="";
		String passenger_telephone="";
		String passenger_name="";
		String id_type="";
		String id_number="";
		String cause="";
		output = passenger_id+","+enterprise_id+",'"+into_blacklist_date+"','"+out_blacklist_date+
				"','"+passenger_telephone+"','"+passenger_name+"','"+id_type+"','"+id_number+"','"+cause+"');";
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
			String databaseName = "nacp_baseinfo_publish";// �Ѿ���MySQL���ݿ��д����õ����ݿ⡣
			String userName = "root";// MySQLĬ�ϵ�root�˻���
			String password = "111111";// Ĭ�ϵ�root�˻�����Ϊ��
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
						blacklist_id ++;
						JSONObject js = JSONObject.fromObject(msg);
						String mysqlValue = combineToStr(js);
						String sql = "INSERT INTO base_info_passenger_blacklist(blacklist_id,passenger_id,enterprise_id,into_blacklist_date,out_blacklist_date,passenger_telephone,passenger_name,id_type,id_number,cause)"
								+ " VALUES("+blacklist_id+","+mysqlValue;
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
