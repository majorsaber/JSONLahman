package mapper;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;

public class Mapper {
	private enum Table { BATTING, PITCHING, APPEARANCES, TEAMS, MASTER};
	private static String baseLocalURL;
	private static void print(String s) {
		System.out.println(s);
	}
	public Mapper() {}
	public static void main(String[] args) {
		if (args.length == 0) {
			baseLocalURL = getOSPath(System.getProperty("user.dir")+"\\out");
			print("No directory specified, output can be found at:\n" + baseLocalURL);
		}
		else if (args.length != 1) {
			print("Invalid number of arguments\njava -jar Mapper.jar \"OUT_DIR\"");
			return;
		}
		else {
			baseLocalURL = args[0];
			print("Output can be found at:\n" + baseLocalURL);
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
			sb.append("{\"batter\":[");	
		} else if (table == Table.PITCHING) {
			sb.append("{\"pitcher\":[");
		} else if (table == Table.APPEARANCES) {
			sb.append("{\"appearance\":[");
		} else if (table == Table.TEAMS) {
			sb.append("{\"team\":[");
		} else if (table == Table.MASTER) {
			sb.append("{\"player\":[");
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
			sb.append("{");
			for (int j = 1; j <= numCols; j++) {
				if (j > 1) {
					sb.append(",");
				}
				sb.append("\""+resMeta.getColumnName(j)+"\":");
				int type = resMeta.getColumnType(j);
				if (type == Types.INTEGER) {
					int val = res.getInt(j);
					sb.append(val==-1?"null":val);
				} else if (type == Types.DOUBLE){
					double d = res.getDouble(j);
					sb.append(d==-1?"null":d);
				} else {
					String s = res.getString(j);
					sb.append(s.isEmpty()?"null":"\""+res.getString(j)+"\"");
				}
			}
			sb.append("}");
		}
		sb.append("]}");
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
		} catch (SQLException e1) {
			print("Could not register JDBC driver.");
			return;
		}
		Connection conn = null;
		//if we're running from the bin directory, db is one level up
		String dbURL = "jdbc:derby:../db/master;create=false";
		try {
			conn = DriverManager.getConnection(dbURL);
		} catch (SQLException e1) {
			//dirty code to just retry if running in debug mode
			dbURL = "jdbc:derby:db/master;create=false";
			try {
				conn = DriverManager.getConnection(dbURL);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			Statement stmt = conn.createStatement();
			ResultSet res = stmt.executeQuery("SELECT * FROM MASTER");
			File file = new File(getOSPath(baseLocalURL+"\\master.json"));
			writeTableToFile(res, file, Table.MASTER);
			for (int year = 1871; year <= 2015; year++) {
				res = stmt.executeQuery("SELECT * FROM BATTING WHERE YEARID=" + year);
				file = new File(getOSPath(baseLocalURL+"\\"+year+"\\batting.json"));
				writeTableToFile(res, file, Table.BATTING);
				res.close();	
				res = stmt.executeQuery("SELECT * FROM PITCHING WHERE YEARID=" + year);
				file = new File(getOSPath(baseLocalURL+"\\"+year+"\\pitching.json"));
				writeTableToFile(res, file, Table.PITCHING);
				res.close();
				res = stmt.executeQuery("SELECT * FROM BATTINGPOST WHERE YEARID=" + year);
				file = new File(getOSPath(baseLocalURL+"\\"+year+"\\battingpost.json"));
				writeTableToFile(res, file, Table.BATTING);
				res.close();	
				res = stmt.executeQuery("SELECT * FROM PITCHINGPOST WHERE YEARID=" + year);
				file = new File(getOSPath(baseLocalURL+"\\"+year+"\\pitchingpost.json"));
				writeTableToFile(res, file, Table.PITCHING);
				res.close();
				res = stmt.executeQuery("SELECT * FROM APPEARANCES WHERE YEARID=" + year);
				file = new File(getOSPath(baseLocalURL+"\\"+year+"\\appearances.json"));
				writeTableToFile(res, file, Table.APPEARANCES);
				res.close();
				res = stmt.executeQuery("SELECT * FROM TEAMS WHERE YEARID=" + year);
				file = new File(getOSPath(baseLocalURL+"\\"+year+"\\teams.json"));
				writeTableToFile(res, file, Table.TEAMS);
				res.close();
			}
			stmt.close();
			conn.close();
			print("done");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static String getOSPath(String path) {
		if (SystemUtils.IS_OS_MAC) return path.replaceAll("\\", "/");
		else return path;
	}
}
