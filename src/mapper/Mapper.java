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
	private enum Table { BATTING, PITCHING, APPEARANCES, TEAMS};
	private static String baseLocalURL;
	private static void print(String s) {
		System.out.println(s);
	}
	public Mapper() {}
	public static void main(String[] args) {
		if (args.length == 0) {
			baseLocalURL = System.getProperty("user.dir")+"\\out";
		}
		else if (args.length != 1) {
			print("Invalid number of arguments\njava -jar Downloader.jar \"OUT_DIR\"");
			return;
		}
		else {
			baseLocalURL = args[0];
		}
		Mapper mapper = new Mapper();
		mapper.map();
	}
	
	private void writeTableToFile(ResultSet res, File file, Table table) throws SQLException {
		if (file.exists()) {
			file.delete();
		}
		ResultSetMetaData resMeta = res.getMetaData();
		int numCols = resMeta.getColumnCount();
		StringBuilder sb = new StringBuilder();
		if (table == Table.BATTING) {
			sb.append("{\"batters\":[");	
		} else if (table == Table.PITCHING) {
			sb.append("{\"pitchers\":[");
		} else if (table == Table.APPEARANCES) {
			sb.append("{\"appearances\":[");
		} else if (table == Table.TEAMS) {
			sb.append("{\"teams\":[");
		}
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
			if (table == Table.BATTING) {
				sb.append("{\"batter\":{");	
			} else if (table == Table.PITCHING) {
				sb.append("{\"pitcher\":{");
			} else if (table == Table.APPEARANCES) {
				sb.append("{\"appearance\":{");
			} else if (table == Table.TEAMS) {
				sb.append("{\"team\":{");
			}
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
			for (int year = 1955; year <= 2015; year++) {
//				ResultSet res = stmt.executeQuery("SELECT * FROM BATTING WHERE YEARID=" + year);
//				File file = new File(baseLocalURL+"\\"+year+"\\batting.json");
//				writeTableToFile(res, file, Table.BATTING);
//				res.close();	
//				res = stmt.executeQuery("SELECT * FROM PITCHING WHERE YEARID=" + year);
//				file = new File(baseLocalURL+"\\"+year+"\\pitching.json");
//				writeTableToFile(res, file, Table.PITCHING);
//				res.close();
//				res = stmt.executeQuery("SELECT * FROM BATTINGPOST WHERE YEARID=" + year);
//				file = new File(baseLocalURL+"\\"+year+"\\battingpost.json");
//				writeTableToFile(res, file, Table.BATTING);
//				res.close();	
//				res = stmt.executeQuery("SELECT * FROM PITCHINGPOST WHERE YEARID=" + year);
//				file = new File(baseLocalURL+"\\"+year+"\\pitchingpost.json");
//				writeTableToFile(res, file, Table.PITCHING);
//				res.close();
//				res = stmt.executeQuery("SELECT * FROM APPEARANCES WHERE YEARID=" + year);
//				file = new File(baseLocalURL+"\\"+year+"\\appearances.json");
//				writeTableToFile(res, file, Table.APPEARANCES);
				ResultSet res = stmt.executeQuery("SELECT * FROM TEAMS WHERE YEARID=" + year);
				File file = new File(baseLocalURL+"\\"+year+"\\teams.json");
				writeTableToFile(res, file, Table.TEAMS);
			}
			stmt.close();
			conn.close();
			print("done");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
