package br.ufsc.tcc.pedro;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Array;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.omg.PortableServer.IdUniquenessPolicyValue;

import static org.junit.Assert.*;
public class GeoLifeETL {

	private Map<String, Integer> transportationMeans = new HashMap<String, Integer>();
	private static Connection conn;
	private static PreparedStatement psSelectObject;
	private static PreparedStatement psInsertObject;
	private static PreparedStatement psInsertSemanticTrajectory;
	private static PreparedStatement psInsertSemanticSubTrajectory;
	private static PreparedStatement psInsertTransportationMean;
	private static PreparedStatement psSelectTransportationMean;
	private static PreparedStatement psInsertSemanticPoint;
	private static DateFormat dateFormat;
	private static Integer idSemanticTrajectory;
	private static int insPoints = 0;
	static {
		dateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT + 0:00"));
	}

	@BeforeClass
	public static void setupDriver() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://192.168.1.145:3306/tcc", "pedro", "bidu1");
			psSelectObject = conn.prepareStatement("SELECT idObject FROM Object WHERE name = ?");
			psInsertObject = conn.prepareStatement("INSERT INTO Object SET name = ?", Statement.RETURN_GENERATED_KEYS);
			psInsertSemanticTrajectory = conn.prepareStatement("INSERT INTO SemanticTrajectory SET idObject = ?", 
					Statement.RETURN_GENERATED_KEYS);
			psInsertSemanticSubTrajectory = conn.prepareStatement(
					"INSERT INTO SemanticSubTrajectory SET idSemanticTrajectory = ?, startTime = ?, endTime = ?, idTransportationMean = ?",
					Statement.RETURN_GENERATED_KEYS);
			psInsertTransportationMean = conn.prepareStatement("INSERT INTO TransportationMean SET description = ?", 
					Statement.RETURN_GENERATED_KEYS);
			psSelectTransportationMean = conn.prepareStatement("SELECT idTransportationMean FROM TransportationMean WHERE description = ?");
			psInsertSemanticPoint = conn.prepareStatement(
					"INSERT INTO SemanticPoint SET idSemanticSubTrajectory = ?, timestamp = ?, the_geom = GeomFromText(?)",
					Statement.RETURN_GENERATED_KEYS);
			conn.setAutoCommit(false);
		} catch (Exception e) {
			fail();
		}
	}
	
	@AfterClass
	public static void finishTransaction() {
		
	}
	
	private void processLabels(int idObject, File dataDir) throws SQLException, IOException, ParseException {
		Path path = FileSystems.getDefault().getPath(dataDir.getAbsolutePath(), "labels.txt");
		BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
		String line;
		List<SemanticSubTrajectory> semanticSubTrajectoryCandidates = new ArrayList<SemanticSubTrajectory>();
		while ((line = reader.readLine()) != null) {
			if (line.contains("Start")) {
				continue;
			}
			String[] fields = line.split("\t");
			SemanticSubTrajectory semanticSubTrajectory = new SemanticSubTrajectory();
			semanticSubTrajectory.startTime = dateFormat. parse(fields[0]);
			semanticSubTrajectory.endTime = dateFormat.parse(fields[1]);
			semanticSubTrajectory.transportationMean = fields[2];
			semanticSubTrajectoryCandidates.add(semanticSubTrajectory);
		}

		processTrajectories(idObject, dataDir, semanticSubTrajectoryCandidates);
		conn.commit();
	}
	
	private void processTrajectories(int idObject, File dataDir, List<SemanticSubTrajectory> semanticSubTrajectoryCandidates) 
			throws SQLException, IOException, ParseException {
		Path path = FileSystems.getDefault().getPath(dataDir.getAbsolutePath(), "Trajectory");
		idSemanticTrajectory = null;
		for (File trajectoryFile : path.toFile().listFiles()) {
			System.out.println(trajectoryFile.getAbsolutePath());
			BufferedReader reader = Files.newBufferedReader(trajectoryFile.toPath(), StandardCharsets.UTF_8);
			int lineCount = 0;
			String line;
			while ((line = reader.readLine()) != null) {
				if (lineCount < 6) {
					lineCount++;
					continue;
				}
				String[] fields = line.split(",");
				double latitude = Double.parseDouble(fields[0]);
				double longitude = Double.parseDouble(fields[1]);
				double altitude = Double.parseDouble(fields[2]);
				Date timestamp = dateFormat.parse(fields[5].replace("-", "/") + " " + fields[6]);
				int subTrajectoryId = findSubTrajectory(idObject, timestamp, semanticSubTrajectoryCandidates);
				if (subTrajectoryId == 0) {
					continue;
				}
				psInsertSemanticPoint.setInt(1, subTrajectoryId);
				psInsertSemanticPoint.setTimestamp(2, new Timestamp(timestamp.getTime()));
				psInsertSemanticPoint.setString(3, String.format("POINT(%f %f)", latitude, longitude));
				psInsertSemanticPoint.executeUpdate();
			}
		}
	}

	private int findSubTrajectory(int idObject, Date timestamp,
			List<SemanticSubTrajectory> semanticSubTrajectoryCandidates) throws SQLException {
		for (SemanticSubTrajectory subTrajectory : semanticSubTrajectoryCandidates) {
			if (timestamp.equals(subTrajectory.startTime) || 
					timestamp.equals(subTrajectory.endTime) || 
					(timestamp.after(subTrajectory.startTime) && timestamp.before(subTrajectory.endTime))) {
				if (subTrajectory.id == 0) {
					if (idSemanticTrajectory == null) {
						psInsertSemanticTrajectory.setInt(1, idObject);
						if (psInsertSemanticTrajectory.executeUpdate() > 0) {
							ResultSet generatedKeys = psInsertSemanticTrajectory.getGeneratedKeys();
							if (generatedKeys.next()) {
								idSemanticTrajectory = generatedKeys.getInt(1);
								generatedKeys.close();
							} else {
								throw new SQLException("Erro ao inserir em SemanticTrajectory");
							}
						} else {
							throw new SQLException("Erro ao inserir em SemanticTrajectory");
						}
					}

					int idTransportationMean = findTransportationMean(subTrajectory.transportationMean);

					psInsertSemanticSubTrajectory.setInt(1, idSemanticTrajectory.intValue());
					psInsertSemanticSubTrajectory.setTimestamp(2, new Timestamp(subTrajectory.startTime.getTime()));
					psInsertSemanticSubTrajectory.setTimestamp(3, new Timestamp(subTrajectory.endTime.getTime()));
					psInsertSemanticSubTrajectory.setInt(4, idTransportationMean);
					if (psInsertSemanticSubTrajectory.executeUpdate() > 0) {
						ResultSet generatedKeys = psInsertSemanticSubTrajectory.getGeneratedKeys();
						if (generatedKeys.next()) {
							subTrajectory.id = generatedKeys.getInt(1);
							generatedKeys.close();
						} else {
							throw new SQLException("Erro ao inserir em SemanticSubTrajectory");
						}
					} else {
						throw new SQLException("Erro ao inserir em SemanticSubTrajectory");
					}
				}
				return subTrajectory.id;
			}
		}
		return 0;
	}

	private int findTransportationMean(String transportationMean) throws SQLException {
		if (transportationMeans.containsKey(transportationMean)) {
			return transportationMeans.get(transportationMean);
		}
		
		psSelectTransportationMean.setString(1, transportationMean);
		ResultSet rsTransportationMean = psSelectTransportationMean.executeQuery();
		if (rsTransportationMean.next()) {
			int retVal = rsTransportationMean.getInt(1) ;
			rsTransportationMean.close();
			return retVal;
		}
		
		psInsertTransportationMean.setString(1, transportationMean);
		if (psInsertTransportationMean.executeUpdate() > 0) {
			ResultSet generatedKeys = psInsertTransportationMean.getGeneratedKeys();
			if (generatedKeys.next()) {
				int retVal = generatedKeys.getInt(1);
				transportationMeans.put(transportationMean, retVal);
				generatedKeys.close();
				return retVal;
			} else {
				throw new SQLException("Erro ao inserir em TransportationMean");
			}
		} else {
			throw new SQLException("Erro ao inserir em TransportationMean");
		}
	}

	private void processMainDir(String mainDirName) throws SQLException, IOException, ParseException {
		File mainDir = new File(mainDirName);
		for (File dataDir : mainDir.listFiles()) {
			if (dataDir.isDirectory() && Arrays.asList(dataDir.list()).contains("labels.txt")) {
				int idObject;
				psSelectObject.setString(1, dataDir.getName());
				ResultSet rsSelectObject = psSelectObject.executeQuery();
				if (rsSelectObject.first()) {
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
				processLabels(idObject, dataDir);
			}
		}
	}

	@Test
	public void testProcessDataDit() {
		try {
			processMainDir("C:\\Users\\pedro\\Ubuntu One\\TCC\\GeoLife\\Geolife Trajectories 1.2\\Data");
			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException sqle) {
				throw new RuntimeException("Fatal error during program execution!", sqle);
			}
			fail();
		}
	}

	private class SemanticSubTrajectory {
		
		int id = 0;
		
		Date startTime;
		
		Date endTime;
		
		String transportationMean;
	}
}