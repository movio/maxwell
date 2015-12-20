package com.zendesk.maxwell;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStore;

public class AbstractMaxwellTest {
	protected static MysqlIsolatedServer server;
	protected Schema schema;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		server = new MysqlIsolatedServer();
		server.boot();

		SchemaStore.ensureMaxwellSchema(server.getConnection());
	}

	@AfterClass
	public static void teardownServer() {
		server.shutDown();
	}

	public static String removeTimeStampsAndIds(String json) {
		json = json.replaceAll("\"ts\":[0-9]+", "\"ts\":0");
		json = json.replaceAll("\"started_at\":\"[^\"]+\"", "\"started_at\":\"\"");
		json = json.replaceAll("\"completed_at\":\"[^\"]+\"", "\"completed_at\":\"\"");
		json = json.replaceAll("\"id\":[0-9]+", "\"id\":0");
		json = json.replaceAll("\"xid\":[0-9]+", "\"xid\":0");
		return json;
	}

	public String getSQLDir() {
		 final String dir = System.getProperty("user.dir");
		 return dir + "/src/test/resources/sql/";
	}


	private void resetMaster() throws SQLException, IOException {
		List<String> queries = new ArrayList<String>(Arrays.asList(
				"CREATE DATABASE if not exists shard_2",
				"DROP DATABASE if exists shard_1",
				"CREATE DATABASE shard_1",
				"USE shard_1"
		));

		for ( File file: new File(getSQLDir() + "/schema").listFiles()) {
			if ( !file.getName().endsWith(".sql"))
				continue;

			byte[] sql = Files.readAllBytes(file.toPath());
			String s = new String(sql);
			if ( s != null ) {
				queries.add(s);
			}
		}

		queries.add("RESET MASTER");

		server.executeList(queries);
	}

	private void generateBinlogEvents() throws IOException, SQLException {
		Path p = Paths.get(getSQLDir() + "/rows/rows.sql");
		List<String>sql = Files.readAllLines(p, Charset.forName("UTF-8"));

		server.executeList(sql);
	}

	@Before
	public void setupMysql() throws SQLException, IOException, InterruptedException {
		resetMaster();
		generateBinlogEvents();
	}

	protected MaxwellContext buildContext() {
		MaxwellConfig config = new MaxwellConfig();

		config.mysqlHost = "127.0.0.1";
		config.mysqlPort = server.getPort();
		config.mysqlUser = "maxwell";
		config.mysqlPassword = "maxwell";
		config.bootstrapperBatchFetchSize = 64;

		return new MaxwellContext(config);
	}

	protected List<RowMap>getRowsForSQL(MaxwellFilter filter, String queries[], String before[], final boolean excludeMaxwellRows) throws Exception {
		BinlogPosition start = BinlogPosition.capture(server.getConnection());
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection());

		if ( before != null ) {
			server.executeList(Arrays.asList(before));
		}

		MaxwellContext context = buildContext();

		Schema initialSchema = capturer.capture();

		server.executeList(Arrays.asList(queries));

		BinlogPosition endPosition = BinlogPosition.capture(server.getConnection());

		final ArrayList<RowMap> list = new ArrayList<>();

		AbstractProducer producer = new AbstractProducer(context) {
			@Override
			public void push(RowMap r) {
				if ( !excludeMaxwellRows || !r.getDatabase().equals("maxwell") ) {
					list.add(r);
				}
			}
		};

		AsynchronousBootstrapper bootstrapper = new AsynchronousBootstrapper(context) {
			@Override
			protected SynchronousBootstrapper getSynchronousBootstrapper() {
				return new SynchronousBootstrapper(context) {
					@Override
					protected Connection getConnection() throws SQLException {
						return server.getNewConnection();
					}
				};
			}
		};

		TestMaxwellReplicator p = new TestMaxwellReplicator(initialSchema, producer, bootstrapper, context, start, endPosition);

		p.setFilter(filter);

		p.getEvents(producer);

		schema = p.schema;

		bootstrapper.join();

		context.terminate();

		return list;
	}

	protected List<RowMap>getRowsForSQL(MaxwellFilter filter, String queries[], String before[]) throws Exception {
		return getRowsForSQL(filter, queries, before, true);
	}

	protected List<RowMap>getRowsForSQL(MaxwellFilter filter, String queries[]) throws Exception {
		return getRowsForSQL(filter, queries, null, true);
	}

	@After
	public void tearDown() throws Exception {
	}
}
