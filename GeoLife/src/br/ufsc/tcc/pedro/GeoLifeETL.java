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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
public class GeoLifeETL {

	private static Connection conn;
	private static PreparedStatement psSelectObject;
	private static PreparedStatement psInsertObject;
	private static PreparedStatement psInsertSemanticTrajectory;
	private static PreparedStatement psInsertSemanticSubTrajectory;
	private static PreparedStatement psInsertTransportationMean;
	private static DateFormat dateFormat;
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
			psInsertObject = conn.prepareStatement("INSERT INTO Object SET name = ?");
			psInsertSemanticTrajectory = conn.prepareStatement("INSERT INTO SemanticTrajectory SET idObject = ?");
			psInsertSemanticSubTrajectory = conn.prepareStatement("INSERT INTO SemanticSubTrajectory SET idSemanticTrajectory = ?, start = ?, finish = ?, idTransportationMean = ?");
			psInsertTransportationMean = conn.prepareStatement("INSERT INTO TransportationMean SET description = ?");
			conn.setAutoCommit(false);
		} catch (Exception e) {
			fail();
		}
	}
	
	@AfterClass
	public static void finishTransaction() {
		
	}
	
	private void processLabels(int objectId, File dataDir) throws SQLException, IOException, ParseException {
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

		processTrajectories(objectId, dataDir, semanticSubTrajectoryCandidates);
	}
	
	private void processTrajectories(int objectId, File dataDir, List<SemanticSubTrajectory> semanticSubTrajectoryCandidates) throws IOException, ParseException {
		Path path = FileSystems.getDefault().getPath(dataDir.getAbsolutePath(), "Trajectory");
		for (File trajectoryFile : path.toFile().listFiles()) {
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
				int subTrajectoryId = findSubTrajectory(timestamp, semanticSubTrajectoryCandidates);
			}
		}
	}

	private int findSubTrajectory(Date timestamp, List<SemanticSubTrajectory> semanticSubTrajectoryCandidates) {
		for (SemanticSubTrajectory subTrajectory : semanticSubTrajectoryCandidates) {
			if (timestamp.equals(subTrajectory.startTime) || 
					timestamp.equals(subTrajectory.endTime) || 
					(timestamp.after(subTrajectory.startTime) && timestamp.before(subTrajectory.endTime))) {
				if (subTrajectory.id > 0) {
					return subTrajectory.id;
				}
				
				// TODO: insert semantic sub trajectory
				
				return 0;
			}
		}
		return 0;
	}

	private void processMainDir(String mainDirName) throws SQLException, IOException, ParseException {
		File mainDir = new File(mainDirName);
		for (File dataDir : mainDir.listFiles()) {
			System.out.println(dataDir);
			if (dataDir.isDirectory() && Arrays.asList(dataDir.list()).contains("labels.txt")) {
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
		
		int id;
		
		Date startTime;
		
		Date endTime;
		
		String transportationMean;
	}
}