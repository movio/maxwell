package com.zendesk.maxwell;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.ConnectionPool;

import java.io.Console;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

public class MaxwellBootstrap {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBootstrap.class);
	static final int PROGRESS_COUNT_MODULO = 100;

	private MaxwellBootstrapConfig config;
	private Console console = System.console();

	private class ProgressReporter {

		private Thread thread = null;
		private boolean isRunning = true;
		private boolean seenStartRow = false;
		private long startedTimeMillis;

		public ProgressReporter(final int total) {
			thread = new Thread(new Runnable() {
				@Override
				public void run( ) {
					try {
						KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config.kafkaProperties);
						consumer.subscribe(new ArrayList<>(Arrays.asList("maxwell")));
						consumeReplicationStream(consumer, total);
						consumer.close();
					} catch ( Exception e ) {
						e.printStackTrace();
						System.exit(1);
					}
				}
			});
		}

		private void consumeReplicationStream(KafkaConsumer<String, String> consumer, int total) throws IOException {
			ObjectMapper mapper = new ObjectMapper();
			int count = 0;
			while( isRunning ) {
				ConsumerRecords<String, String> records = consumer.poll(0);
				for ( ConsumerRecord<String, String> record: records.records("maxwell") ) {
					JsonNode json = mapper.readTree(record.value());
					if ( !seenStartRow &&	tableMatches(json) && isStartRow(json) ) {
						seenStartRow = true;
						LOGGER.info("bootstrapping started");
					} else if ( seenStartRow && tableMatches(json) && isCompletedRow(json) ) {
						isRunning = false;
						LOGGER.info("bootstrapping complete");
					} else if ( seenStartRow && tableMatches(json) ) {
						++count;
						displayProgress(total, count);
					}
				}
			}
		}

		private boolean tableMatches(JsonNode json) {
			String databaseName = json.path("database").asText();
			String tableName = json.path("table").asText();
			return databaseName.equals(config.databaseName) && tableName.equals(config.tableName);
		}

		private boolean isCompletedRow(JsonNode json) {
			return json.path("type").asText().equals("bootstrap-complete");
		}

		private boolean isStartRow(JsonNode json) {
			return json.path("type").asText().equals("bootstrap-start");
		}

		public void start() {
			thread.start();
			startedTimeMillis = System.currentTimeMillis();
		}

		public void join( ) throws InterruptedException {
			thread.join();
		}

		private void displayProgress(int total, int count) {
			if ( count % PROGRESS_COUNT_MODULO == 0 || count == total ) {
				long currentTimeMillis = System.currentTimeMillis();
				long elapsedMillis = currentTimeMillis - startedTimeMillis;
				long predictedTotalMillis = ( long ) ((elapsedMillis / ( float ) count) * total);
				long remainingMillis = predictedTotalMillis - elapsedMillis;
				String duration = prettyDuration(remainingMillis);
				displayLine(String.format("%d / %d (%.2f%%) - %s remaining", count, total, ( count * 100.0 ) / total, duration));
			}
			if ( count == total ) {
				System.out.println();
			}
		}

		private String prettyDuration(long millis) {
			long d = millis / (1000 * 60 * 60 * 24);
			long h = millis / (1000 * 60 * 60);
			long m = millis / (1000 * 60);
			long s = millis / 1000;
			if ( d > 0 ) {
				return String.format("%d days %02dh %02dm %02ds", d, h, m, s);
			} else if ( h > 0 ) {
				return String.format("%02dh %02dm %02ds", h, m, s);
			} else if ( m > 0 ) {
				return String.format("%02dm %02ds", m, s);
			} else if ( s > 0 ) {
				return String.format("%02ds", s);
			} else {
				return "";
			}
		}

		private void displayLine(String line) {
			if ( console != null ) {
				System.out.print("\u001b[2K\u001b[G" + line);
				System.out.flush();
			}
		}
	}

	private void run(String[] argv) throws Exception {
		this.config = new MaxwellBootstrapConfig(argv);
		MaxwellBootstrapConfig config = this.config;
		if ( config.log_level != null ) {
			MaxwellLogging.setLevel(config.log_level);
		}
		ConnectionPool connectionPool = getConnectionPool(config);
		try ( Connection connection = connectionPool.getConnection() ) {
			ProgressReporter progressReporter = new ProgressReporter(getRowCount(connection, config));
			progressReporter.start();
			insertBootstrapStartRow(connection, config);
			progressReporter.join();
		} catch ( SQLException e ) {
			LOGGER.error("Failed to connect to mysql server @ " + config.getConnectionURI());
			LOGGER.error(e.getLocalizedMessage());
			System.exit(1);
		}
	}

	private ConnectionPool getConnectionPool(MaxwellBootstrapConfig config) {
		String name = "MaxwellBootstrapConnectionPool";
		int maxPool = 10;
		int maxSize = 0;
		int idleTimeout = 10;
		String connectionURI = config.getConnectionURI();
		String mysqlUser = config.mysqlUser;
		String mysqlPassword = config.mysqlPassword;
		return new ConnectionPool(name, maxPool, maxSize, idleTimeout, connectionURI, mysqlUser, mysqlPassword);
	}

	private int getRowCount(Connection connection, MaxwellBootstrapConfig config) throws SQLException {
		LOGGER.info("counting rows");
		String sql = String.format("select count(*) from %s.%s", config.databaseName, config.tableName);
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		ResultSet resultSet = preparedStatement.executeQuery();
		resultSet.next();
		return resultSet.getInt(1);
	}

	private void insertBootstrapStartRow(Connection connection, MaxwellBootstrapConfig config) throws SQLException {
		LOGGER.info("inserting bootstrap start row");
		String sql = "insert into maxwell.bootstrap (database_name, table_name) values(?, ?)";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, config.databaseName);
		preparedStatement.setString(2, config.tableName);
		preparedStatement.execute();
	}

	public static void main(String[] args) {
		try {
			new MaxwellBootstrap().run(args);
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
