package com.zendesk.maxwell;

import com.google.code.or.OpenReplicator;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.DateTimeColumnDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Iterator;

public class SynchronousBootstrapper extends AbstractBootstrapper {

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplicator.class);

	public SynchronousBootstrapper( MaxwellContext context ) { super(context); }

	@Override
	public boolean isStartBootstrapRow(RowMap row) {
		return isBootstrapRow(row) &&
			row.getData("started_at") == null &&
			row.getData("completed_at") == null &&
			( long ) row.getData("is_complete") == 0;
	}

	@Override
	public boolean isCompleteBootstrapRow(RowMap row) {
		return isBootstrapRow(row) &&
			row.getData("started_at") != null &&
			row.getData("completed_at") != null &&
			( long ) row.getData("is_complete") == 1;
	}

	@Override
	public boolean isBootstrapRow(RowMap row) {
		return row.getDatabase().equals("maxwell") &&
			row.getTable().equals("bootstrap");
	}

	@Override
	public boolean shouldSkip(RowMap row) {
		return false;
	}

	@Override
	public void startBootstrap(RowMap startBootstrapRow, Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {
		String databaseName = ( String ) startBootstrapRow.getData("database_name");
		String tableName = ( String ) startBootstrapRow.getData("table_name");
		LOGGER.debug(String.format("bootstrapping request for %s.%s", databaseName, tableName));
		Database database = findDatabase(schema, databaseName);
		Table table = findTable(tableName, database);
		BinlogPosition position = new BinlogPosition(replicator.getBinlogPosition(), replicator.getBinlogFileName());
		producer.push(startBootstrapRow);
		producer.push(replicationStreamBootstrapStartRow(table, position));
		LOGGER.info(String.format("bootstrapping started for %s.%s, binlog position is %s", databaseName, tableName, position.toString()));
		try ( Connection connection = context.getConnectionPool().getConnection() ) {
			setBootstrapRowToStarted(startBootstrapRow, connection);
			ResultSet resultSet = getAllRows(databaseName, tableName, connection);
			while ( resultSet.next() ) {
				RowMap row = new RowMap(
						"insert",
						databaseName,
						tableName,
						System.currentTimeMillis(),
						table.getPKList(),
						position);
				setRowValues(row, resultSet, table);
				producer.push(row);
			}
			setBootstrapRowToCompleted(startBootstrapRow, connection);
		}
	}

	private RowMap replicationStreamBootstrapStartRow(Table table, BinlogPosition position) {
		return replicationStreamBootstrapRow("bootstrap-start", table, position);
	}

	private RowMap replicationStreamBootstrapCompletedRow(Table table, BinlogPosition position) {
		return replicationStreamBootstrapRow("bootstrap-complete", table, position);
	}

	private RowMap replicationStreamBootstrapRow(String type, Table table, BinlogPosition position) {
		return new RowMap(
				type,
				table.getDatabase().getName(),
				table.getName(),
				System.currentTimeMillis(),
				table.getPKList(),
				position);
	}

	@Override
	public void completeBootstrap(RowMap completeBootstrapRow, Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {
		String databaseName = ( String ) completeBootstrapRow.getData("database_name");
		String tableName = ( String ) completeBootstrapRow.getData("table_name");
		Database database = findDatabase(schema, databaseName);
		ensureTable(tableName, findDatabase(schema, databaseName));
		Table table = findTable(tableName, database);
		BinlogPosition position = new BinlogPosition(replicator.getBinlogPosition(), replicator.getBinlogFileName());
		producer.push(completeBootstrapRow);
		producer.push(replicationStreamBootstrapCompletedRow(table, position));
		LOGGER.info(String.format("bootstrapping ended for %s.%s", databaseName, tableName));
	}

	@Override
	public void resume(Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {
		try ( Connection connection = context.getConnectionPool().getConnection() ) {
			String sql = "update maxwell.bootstrap set started_at = NULL where is_complete = 0 and started_at is not NULL";
			connection.prepareStatement(sql).execute();
		}
	}

	private Table findTable(String tableName, Database database) {
		Table table = database.findTable(tableName);
		if ( table == null )
			throw new RuntimeException("Couldn't find table " + tableName);
		return table;
	}

	private Database findDatabase(Schema schema, String databaseName) {
		Database database = schema.findDatabase(databaseName);
		if ( database == null )
			throw new RuntimeException("Couldn't find database " + databaseName);
		return database;
	}

	private void ensureTable(String tableName, Database database) {
		findTable(tableName, database);
	}

	private ResultSet getAllRows(String databaseName, String tableName, Connection connection) throws SQLException {
		Statement statement = createBatchStatement(connection);
		return statement.executeQuery(String.format("select * from %s.%s", databaseName, tableName));
	}

	private Statement createBatchStatement(Connection connection) throws SQLException {
		Statement statement = connection.createStatement();
		statement.setFetchSize(context.getConfig().bootstrapperBatchFetchSize);
		return statement;
	}

	private void setBootstrapRowToStarted(RowMap startBootstrapRow, Connection connection) throws SQLException {
		String sql = "update maxwell.bootstrap set started_at=NOW() where id = ?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setLong(1, ( Long ) startBootstrapRow.getData("id"));
		preparedStatement.execute();
	}
	
	private void setBootstrapRowToCompleted(RowMap startBootstrapRow, Connection connection) throws SQLException {
		String sql = "update maxwell.bootstrap set is_complete=1, completed_at=NOW() where id = ?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setLong(1, ( Long ) startBootstrapRow.getData("id"));
		preparedStatement.execute();
	}

	private void setRowValues(RowMap row, ResultSet resultSet, Table table) throws SQLException, IOException {
		Iterator<ColumnDef> columnDefinitions = table.getColumnList().iterator();
		int columnIndex = 1;
		while ( columnDefinitions.hasNext() ) {
			ColumnDef columnDefinition = columnDefinitions.next();
			row.putData(columnDefinition.getName(), getObject(resultSet, columnIndex, columnDefinition.getType()));
			++columnIndex;
		}
	}

	private Object getObject(ResultSet resultSet, int columnIndex, String type) throws SQLException {
		switch ( type ) {
			case "bool":
			case "boolean":
			case "tinyint":
			case "smallint":
			case "mediumint":
			case "int":
				return resultSet.getInt(columnIndex);
			case "bigint":
				return resultSet.getLong(columnIndex);
			case "tinytext":
			case "text":
			case "mediumtext":
			case "longtext":
			case "varchar":
			case "char":
				return resultSet.getString(columnIndex);
			case "tinyblob":
			case "blob":
			case "mediumblob":
			case "longblob":
			case "binary":
			case "varbinary":
				return resultSet.getString(columnIndex);
			case "real":
			case "numeric":
			case "double":
				return resultSet.getDouble(columnIndex);
			case "float":
				return resultSet.getFloat(columnIndex);
			case "decimal":
				return resultSet.getDouble(columnIndex);
			case "date":
				return resultSet.getString(columnIndex);
			case "datetime":
			case "timestamp":
				return resultSet.getString(columnIndex);
			case "year":
				return resultSet.getInt(columnIndex);
			case "time":
				return resultSet.getString(columnIndex);
			case "enum":
				return resultSet.getString(columnIndex);
			case "set":
				return resultSet.getString(columnIndex);
			case "bit":
				return resultSet.getString(columnIndex);
			default:
				throw new IllegalArgumentException("unsupported column type " + type);
		}
	}

}
