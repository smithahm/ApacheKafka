import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerApp {

	private static Producer<String, String> producer;

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		Connection con = DriverManager.getConnection(
				"jdbc:sqlserver://localhost:1434;databaseName=testdb;instanceName=SQLEXPRESS", "sa", "Smith1234");
		System.out.println("testing SQL server connection");
		intialize();
		Statement sa = con.createStatement();
		String sql = "select * from Product";
		ResultSet rs = sa.executeQuery(sql);
		while (rs.next()) {
			producer.send(
					new ProducerRecord<String, String>("sqlTest", Integer.toString(rs.getInt(1)), rs.getString(2)));

		}
		producer.close();
	}

	private static void intialize() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
	}
}
