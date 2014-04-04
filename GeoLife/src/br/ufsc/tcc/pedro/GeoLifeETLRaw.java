package br.ufsc.tcc.pedro;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
public class GeoLifeETLRaw {

	private static Connection conn;
	private static PreparedStatement psSelectObject;
	private static PreparedStatement psInsertObject;
	private static PreparedStatement psInsertRawTrajectory;
	private static PreparedStatement psInsertRawPoint;
	private static DateFormat dateFormat;
	private static int insPoints = 0;
	private static double maxLat = -99999990, minLat = 99999990, maxLon = -99999990, minLon = 99999990;
	static {
		dateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT + 0:00"));
	}

	@BeforeClass
	public static void setupDriver() {
		try {
//			Class.forName("com.mysql.jdbc.Driver");
			Class.forName("org.postgresql.Driver");
//				conn = DriverManager.getConnection("jdbc:mysql://192.168.1.145:3306/tcc", "pedro", "bidu1");
			conn = DriverManager.getConnection("jdbc:postgresql://192.168.1.105:5432/geolife", "postgres", "postgres");
			psSelectObject = conn.prepareStatement("SELECT \"idObject\" FROM \"Object\" WHERE name = ?");
			psInsertObject = conn.prepareStatement("INSERT INTO \"Object\" (name) values (?)", Statement.RETURN_GENERATED_KEYS);
			psInsertRawTrajectory = conn.prepareStatement("INSERT INTO \"RawTrajectory\" (\"idObject\") values (?)", 
					Statement.RETURN_GENERATED_KEYS);
			psInsertRawPoint = conn.prepareStatement(
					"INSERT INTO \"RawPoint\" (\"idRawTrajectory\", timestamp, the_geom) values (?, ?, ST_GeometryFromText(?, 4326))");
			conn.setAutoCommit(false);
		} catch (Exception e) {
			fail();
		}
	}
	
	@AfterClass
	public static void finishTransaction() {
		
	}
	
	private void processTrajectories(int idObject, File dataDir) 
			throws SQLException, IOException, ParseException {
		Path path = FileSystems.getDefault().getPath(dataDir.getAbsolutePath(), "Trajectory");
		for (File trajectoryFile : path.toFile().listFiles()) {
			BufferedReader reader = Files.newBufferedReader(trajectoryFile.toPath(), StandardCharsets.UTF_8);
			int lineCount = 0;
			String line;
			int rawTrajectoryId;
			psInsertRawTrajectory.setInt(1, idObject);
			if (psInsertRawTrajectory.executeUpdate() > 0) {
				ResultSet generatedKeys = psInsertRawTrajectory.getGeneratedKeys();
				if (generatedKeys.next()) {
					rawTrajectoryId = generatedKeys.getInt(1);
					generatedKeys.close();
				} else {
					throw new SQLException("Erro ao inserir em RawTrajectory");
				}
			} else {
				throw new SQLException("Erro ao inserir em RawTrajectory");
			}
			while ((line = reader.readLine()) != null) {
				if (lineCount < 6) {
					lineCount++;
					continue;
				}
				String[] fields = line.split(",");
				System.out.print("\r" + ++insPoints);
				double latitude = Double.parseDouble(fields[0]);
				double longitude = Double.parseDouble(fields[1]);
				Date timestamp = dateFormat.parse(fields[5].replace("-", "/") + " " + fields[6]);
				psInsertRawPoint.setInt(1, rawTrajectoryId);
				psInsertRawPoint.setTimestamp(2, new Timestamp(timestamp.getTime()));
				psInsertRawPoint.setString(3, String.format(Locale.ENGLISH, "POINT(%f %f)", longitude, latitude));
				psInsertRawPoint.executeUpdate();
			}
		}
		conn.commit();
	}

	private void processMainDir(String mainDirName) throws SQLException, IOException, ParseException {
		File mainDir = new File(mainDirName);
		for (File dataDir : mainDir.listFiles()) {
			int idObject = 0;
			psSelectObject.setString(1, dataDir.getName());
			ResultSet rsSelectObject = psSelectObject.executeQuery();
			if (rsSelectObject.next()) {
				idObject = rsSelectObject.getInt("idObject");
				rsSelectObject.close();
			} else {
				psInsertObject.setString(1, dataDir.getName());
				if (psInsertObject.executeUpdate() > 0) {
					ResultSet generatedKeys = psInsertObject.getGeneratedKeys();
					if (generatedKeys.next()) {
						idObject = generatedKeys.getInt(1);
						generatedKeys.close();
					} else {
						throw new SQLException("Erro ao inserir em Object");
					}
				} else {
					throw new SQLException("Erro ao inserir em Object");
				}
			}
			processTrajectories(idObject, dataDir);
			System.out.println(dataDir);
			System.out.println("Max Lat: " + maxLat);
			System.out.println("Min Lat: " + minLat);
			System.out.println("Max Lon: " + maxLon);
			System.out.println("Min Lon: " + minLon);
		}
	}

//	@Test
	public void testProcessDataDit() {
		try {
			processMainDir("C:\\Users\\pedro\\Ubuntu One\\TCC\\GeoLife\\Geolife Trajectories 1.2\\Data");
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException sqle) {
				throw new RuntimeException("Fatal error during program execution!", sqle);
			}
			fail();
		}
	}

	@Test
	public void populateSubtrajectoryLine() {
		try {
			PreparedStatement psSelectTrajectory = conn.prepareStatement("SELECT * FROM \"RawTrajectory\" ORDER BY \"idRawTrajectory\"");
			PreparedStatement psUpdateTrajectory = conn.prepareStatement("UPDATE \"RawTrajectory\" SET the_geom = ST_GeometryFromText(?, 4326) WHERE \"idRawTrajectory\" = ?");
			PreparedStatement psSelectPoints = conn.prepareStatement("SELECT ST_AsText(the_geom) FROM \"RawPoint\" WHERE \"idRawTrajectory\" = ? ORDER BY \"idRawPoint\"");
			ResultSet rsSelectSub = psSelectTrajectory.executeQuery();
			while (rsSelectSub.next()) {
				int idTrajectory = rsSelectSub.getInt("idRawTrajectory");
				System.out.println(idTrajectory);
				psSelectPoints.setInt(1, idTrajectory);
				ResultSet rsSelectPoints = psSelectPoints.executeQuery();
				StringBuffer pointsBuffer = new StringBuffer();
				while (rsSelectPoints.next()) {
					if (pointsBuffer.length() == 0) {
						pointsBuffer.append("LINESTRING(");
					} else {
						pointsBuffer.append(",");
					}
					pointsBuffer.append(rsSelectPoints.getString(1).replace("POINT(", "").replace(")", ""));
				}
				rsSelectPoints.close();
				if (pointsBuffer.length() > 0) {
					pointsBuffer.append(")");
					psUpdateTrajectory.setString(1, pointsBuffer.toString());
					psUpdateTrajectory.setInt(2, idTrajectory);
					try {
						psUpdateTrajectory.executeUpdate();
						conn.commit();
					} catch (Exception e) {
						e.printStackTrace();
						try {
							conn.rollback();
						} catch (SQLException sqle) {
							throw new RuntimeException("Fatal error during program execution!", sqle);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException sqle) {
				throw new RuntimeException("Fatal error during program execution!", sqle);
			}
			fail();
		}
	}
}