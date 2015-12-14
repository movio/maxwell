package com.zendesk.maxwell;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class MaxwellBootstrapConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBootstrapConfig.class);

	public String  mysqlHost;
	public Integer mysqlPort;
	public String  mysqlUser;
	public String  mysqlPassword;
	public String  databaseName;
	public String  tableName;
	public final Properties kafkaProperties;
	public String kafkaTopic;

	public String log_level;

	public MaxwellBootstrapConfig( ) {
		this.kafkaProperties = new Properties();
	}

	public MaxwellBootstrapConfig(String argv[]) {
		this();
		this.parse(argv);
		this.setDefaults();
	}

	public String getConnectionURI( ) {
		return "jdbc:mysql://" + mysqlHost + ":" + mysqlPort;
	}

	private OptionParser getOptionParser() {
		OptionParser parser = new OptionParser();
		parser.accepts( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR" ).withRequiredArg();
		parser.accepts( "host", "mysql host" ).withRequiredArg();
		parser.accepts( "user", "mysql username" ).withRequiredArg();
		parser.accepts( "password", "mysql password" ).withRequiredArg();
		parser.accepts( "port", "mysql port" ).withRequiredArg();

		parser.accepts( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" ).withRequiredArg();
		parser.accepts( "kafka_topic", "optionally provide a topic name to push to. default: maxwell").withRequiredArg();

		parser.accepts( "database", "include these databases, formatted as include_dbs=db1,db2").withRequiredArg();
		parser.accepts( "table", "exclude these databases, formatted as exclude_dbs=db1,db2").withRequiredArg();
		parser.accepts( "help", "display help").forHelp();
		parser.formatHelpWith(new BuiltinHelpFormatter(160, 4));
		return parser;
	}

	private String parseLogLevel(String level) {
		level = level.toLowerCase();
		if ( !( level.equals("debug") || level.equals("info") || level.equals("warn") || level.equals("error")))
			usage("unknown log level: " + level);
		return level;
	}

	private void parse(String [] argv) {
		OptionSet options = getOptionParser().parse(argv);

		if ( options.has("help") )
			usage("Help for Maxwell Bootstrap Utility:");

		if ( options.has("log_level")) {
			this.log_level = parseLogLevel((String) options.valueOf("log_level"));
		}
		if ( options.has("host"))
			this.mysqlHost = (String) options.valueOf("host");
		if ( options.has("password"))
			this.mysqlPassword = (String) options.valueOf("password");
		if ( options.has("user"))
			this.mysqlUser = (String) options.valueOf("user");
		if ( options.has("port"))
			this.mysqlPort = Integer.valueOf((String) options.valueOf("port"));

		if ( options.has("kafka.bootstrap.servers"))
			this.kafkaProperties.setProperty("bootstrap.servers", (String) options.valueOf("kafka.bootstrap.servers"));

		this.kafkaProperties.setProperty("group.id", "default");
		this.kafkaProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		this.kafkaProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		if ( options.has("kafka_topic"))
			this.kafkaTopic = (String) options.valueOf("kafka_topic");

		if ( options.has("database"))
			this.databaseName = (String) options.valueOf("database");

		if ( options.has("table"))
			this.tableName = (String) options.valueOf("table");

	}

	private void setDefaults() {

		if ( this.mysqlPort == null )
			this.mysqlPort = 3306;

		if ( this.mysqlHost == null ) {
			LOGGER.warn("mysql host not specified, defaulting to localhost");
			this.mysqlHost = "localhost";
		}

		if ( this.mysqlPassword == null ) {
			usage("mysql password not given!");
		}

		if ( this.kafkaTopic == null ) {
			this.kafkaTopic = "maxwell";
		}

	}

	private void usage(String string) {
		System.err.println(string);
		System.err.println();
		try {
			getOptionParser().printHelpOn(System.err);
			System.exit(1);
		} catch (IOException e) {
		}
	}

}
