package mapper;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.FileUtils;

public class Mapper {
	private static String baseLocalURL;
	private static void print(String s) {
		System.out.println(s);
	}
	public Mapper() {}
	public static void main(String[] args) {
		if (args.length != 1) {
			print("Invalid number of arguments\njava -jar Downloader.jar \"OUT_DIR\"");
			return;
		}
		baseLocalURL = args[0];
		Mapper mapper = new Mapper();
		mapper.map();
	}
	
	private void writeTableToFile(ResultSet res, File file) throws SQLException {
		if (file.exists()) {
			file.delete();
		}
		ResultSetMetaData resMeta = res.getMetaData();
		int numCols = resMeta.getColumnCount();
		StringBuilder sb = new StringBuilder();
		sb.append("{\"items\":[");
		int i = 0;
		while(res.next()) {
			if (i > 0) {
				sb.append(",");
			}
			if (i++ % 5 == 0) {
				try {
					FileUtils.writeStringToFile(file, sb.toString(), true);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				sb.setLength(0);
			}
			sb.append("{\"item\":{");
			for (int j = 2; j <= numCols; j++) {
				if (j > 1) {
					sb.append(",");
				}
				sb.append("\""+resMeta.getColumnName(j)+"\":");
				sb.append("\""+res.getString(j)+"\"");
			}
			sb.append("}}");
		}
		sb.append("]");
		try {
			FileUtils.writeStringToFile(file, sb.toString(), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void map() {
		
		try {
			DriverManager.registerDriver(new org.apache.derby.jdbc.EmbeddedDriver());
			String dbURL = "jdbc:derby:db/master;create=true";
			Connection conn = DriverManager.getConnection(dbURL);
			Statement stmt = conn.createStatement();
			ResultSet res = stmt.executeQuery("SELECT * FROM PITCHING");
			File file = new File(baseLocalURL+"\\pitching.json");
			writeTableToFile(res, file);
			res = stmt.executeQuery("SELECT * FROM BATTING");
			file = new File(baseLocalURL+"\\batting.json");
			writeTableToFile(res, file);
			res = stmt.executeQuery("SELECT * FROM PLAYERS");
			file = new File(baseLocalURL+"\\players.json");
			writeTableToFile(res, file);
			res.close();
			stmt.close();
			conn.close();
			print("done");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
