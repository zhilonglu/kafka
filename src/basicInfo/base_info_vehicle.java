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
 * 车辆基础信息
 * @LZL 20160919
 */
public class base_info_vehicle {
	static Statement stmt = null;
	static int vehicle_id = 0;
	public static String combineToStr(JSONObject js){
		String output="";//输出的value值
		//解析json文件
		int uiComId = Integer.valueOf(js.getString("uiComId"));
		String strComName  = js.getString("strComName");
		int uiDistId = Integer.valueOf(js.getString("uiDistId"));
		String strVin = js.getString("strVin");
		String uiVehCol = js.getString("uiVehCol");
		String strVehType = js.getString("strVehType");
		int uiPassengerNumber = Integer.valueOf(js.getString("uiPassengerNumber"));
		String strRegisterDate = js.getString("strRegisterDate");
		String strFareType= js.getString("strFareType");
		int uiFlg = Integer.valueOf(js.getString("uiFlg"));
		//数据库中的所有字段
//		int vehicle_id;
		int enterprise_id=uiComId;
		String enterprise_name=strComName;
		double regionalism_code=uiDistId;
		String regionalism_name="";
		String vehicle_belong_city="";
		String license_plate_number=strVin;
		int license_plate_color_id=0;
		String license_plate_color=uiVehCol;
		String vehicle_brand="";
		int vehicle_type_id=0;
		String vehicle_type=strVehType;
		String vehicle_model="";
		int driver_id=0;
		String vehicle_owner="";
		String vehicle_color="";
		String vehicle_picture="";
		String vehicle_engine_no="";
		String vehicle_identification_number="";
		String vehicle_fuel_type="";
		int approved_passenger_number=uiPassengerNumber;
		String vehicle_register_date=strRegisterDate;
		String annual_inspection_status_code="";
		int insurance_company_id=0;
		String insurance_company_name="";
		double insurance_number=0;
		String insurance_type="";
		double insurance_amount=0;
		String insurance_effective_time="20160919";
		String insurance_expiration_time="20160919";
		int freight_type_id=0;
		String freight_type=strFareType;
		String vehicle_engine_emissiont="";
		double vehicle_total_mileage=0;
		double obd_serial_number=0;
		String vehicle_overhaul_state="";
		String vehicle_annual_inspection_time="20160919";
		double net_transport_no=0;
		String net_transport_no_issuing_bodies="";
		String operation_area="";
		String net_transport_validity_start="20160919";
		String net_transport_validity_end="20160919";
		String vehicle_first_register_date="";
		double net_invoice_equipment_no=0;
		String satellite_locate_brand="";
		String satellite_locate_model="";
		String satellite_locate_install_time="";
		double vehicle_price=0;
		String buy_car_invoice_scanning_element="";
		String driving_license_scanning_element="";
		String insurance_policy_scanning_element="";
		String vehicle_affiliated_platform="";
		String vehicle_first_register_time="";
		String satellite_locate_terminal_model="";
		String vehicle_satellite_locate_terminal_name="";
		double no=0;
		String install_time="20160919";
		String vehicle_lwh="";
		int vehicle_wheelbase=0;
		String is_install_gps_emergency_alarm="";
		double complete_order_number=0;
		double service_evaluation_score=0;
		double complaint_number=0;
		double traffic_violation_number=0;
		output = enterprise_id+",'"+enterprise_name+"',"+regionalism_code+",'"+regionalism_name+"','"+vehicle_belong_city+
				"','"+license_plate_number+"',"+license_plate_color_id+",'"+license_plate_color+"','"+vehicle_brand+"',"+vehicle_type_id+
				",'"+vehicle_type+"','"+vehicle_model+"',"+driver_id+",'"+vehicle_owner+"','"+vehicle_color+"','"+vehicle_picture+"','"+vehicle_engine_no+
				"','"+vehicle_identification_number+"','"+vehicle_fuel_type+"',"+approved_passenger_number+",'"+vehicle_register_date+"','"+annual_inspection_status_code+
				"',"+insurance_company_id+",'"+insurance_company_name+"',"+insurance_number+",'"+insurance_type+"',"+insurance_amount+",'"+insurance_effective_time+"','"+insurance_expiration_time+
				"',"+freight_type_id+",'"+freight_type+"','"+vehicle_engine_emissiont+"',"+vehicle_total_mileage+","+obd_serial_number+
				",'"+vehicle_overhaul_state+"','"+vehicle_annual_inspection_time+"',"+net_transport_no+",'"+net_transport_no_issuing_bodies+"','"+operation_area+
				"','"+net_transport_validity_start+"','"+net_transport_validity_end+"','"+vehicle_first_register_date+"',"+net_invoice_equipment_no+",'"+satellite_locate_brand+
				"','"+satellite_locate_model+"','"+satellite_locate_install_time+"',"+vehicle_price+",'"+buy_car_invoice_scanning_element+"','"+driving_license_scanning_element+
				"','"+insurance_policy_scanning_element+"','"+vehicle_affiliated_platform+"','"+vehicle_first_register_time+"','"+satellite_locate_terminal_model+
				"','"+vehicle_satellite_locate_terminal_name+"',"+no+",'"+install_time+"','"+vehicle_lwh+"',"+vehicle_wheelbase+
				",'"+is_install_gps_emergency_alarm+"',"+complete_order_number+","+service_evaluation_score+","+complaint_number+","+traffic_violation_number+")";
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
		topicCountMap.put("VEHICLEBASIC", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("VEHICLEBASIC");
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
						vehicle_id ++;
						JSONObject js = JSONObject.fromObject(msg);
						String mysqlValue = combineToStr(js);
						String sql = "INSERT INTO base_info_vehicle(vehicle_id,enterprise_id,enterprise_name,regionalism_code,regionalism_name,vehicle_belong_city,"
								+"license_plate_number,license_plate_color_id,license_plate_color,vehicle_brand,vehicle_type_id,vehicle_type,vehicle_model,"
								+"driver_id,vehicle_owner,vehicle_color,vehicle_picture,vehicle_engine_no,vehicle_identification_number,vehicle_fuel_type,"
								+"approved_passenger_number,vehicle_register_date,annual_inspection_status_code,insurance_company_id,insurance_company_name,"
								+"insurance_number,insurance_type,insurance_amount,insurance_effective_time,insurance_expiration_time,freight_type_id,"
								+"freight_type,vehicle_engine_emissiont,vehicle_total_mileage,obd_serial_number,vehicle_overhaul_state,vehicle_annual_inspection_time,"
								+"net_transport_no,net_transport_no_issuing_bodies,operation_area,net_transport_validity_start,net_transport_validity_end,vehicle_first_register_date,"
								+"net_invoice_equipment_no,satellite_locate_brand,satellite_locate_model,satellite_locate_install_time,vehicle_price,buy_car_invoice_scanning_element,"
								+"driving_license_scanning_element,insurance_policy_scanning_element,vehicle_affiliated_platform,vehicle_first_register_time,satellite_locate_terminal_model,"
								+"vehicle_satellite_locate_terminal_name,no,install_time,vehicle_lwh,vehicle_wheelbase,is_install_gps_emergency_alarm,complete_order_number,service_evaluation_score,"
								+"complaint_number,traffic_violation_number) VALUES("+vehicle_id+","+mysqlValue;
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
