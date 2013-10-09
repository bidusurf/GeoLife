package br.ufsc.tcc.pedro;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
public class GeoLifeETL {

	private static PreparedStatement psSelectObject;
	private static PreparedStatement psInsertObject;

	@BeforeClass
	public static void setupDriver() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection("jdbc:mysql://192.168.1.145:3306/tcc", "pedro", "bidu1");
			psSelectObject = conn.prepareStatement("SELECT idObject FROM Object WHERE name = ?");
			psInsertObject = conn.prepareStatement("INSERT INTO Object SET name = ?");
		} catch (Exception e) {
			fail();
		}
	}
	
	public void processLabels(int objectId, File dataDir) {
		
	}
	
	public void processMainDir(String mainDirName) {
		File mainDir = new File(mainDirName);
		for (File dataDir : mainDir.listFiles()) {
			System.out.println(dataDir);
			if (dataDir.isDirectory() && Arrays.asList(dataDir.list()).contains("labels.txt")) {
				try {
					int objectId;
					psSelectObject.setString(1, dataDir.getName());
					ResultSet rsSelectObject = psSelectObject.executeQuery();
					if (rsSelectObject.first()) {
						objectId = rsSelectObject.getInt("idObject");
					} else {
						psInsertObject.setString(1, dataDir.getName());
						objectId = psInsertObject.executeUpdate();
					}
					processLabels(objectId, dataDir);
				} catch (SQLException se) {
					
				}
			}
		}
	}

	@Test
	public void testProcessDataDit() {
		processMainDir("C:\\Users\\pedro\\Ubuntu One\\TCC\\GeoLife\\Geolife Trajectories 1.2\\Data");
	}

}