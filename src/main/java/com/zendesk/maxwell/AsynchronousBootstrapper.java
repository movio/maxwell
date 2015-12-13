package com.zendesk.maxwell;

import com.google.code.or.OpenReplicator;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class AsynchronousBootstrapper extends AbstractBootstrapper {

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplicator.class);

	private Thread thread = null;
	private Queue<RowMap> queue = new LinkedList<>();
	private RowMap bootstrappedRow = null;
	private SynchronousBootstrapper synchronousBootstrapper = new SynchronousBootstrapper(context);

	public AsynchronousBootstrapper( MaxwellContext context ) {	super(context); }

	@Override
	public boolean isStartBootstrapRow(RowMap row) {
		return synchronousBootstrapper.isStartBootstrapRow(row);
	}

	@Override
	public boolean isCompleteBootstrapRow(RowMap row) {
		return synchronousBootstrapper.isCompleteBootstrapRow(row);
	}

	@Override
	public boolean isBootstrapRow(RowMap row) {
		return synchronousBootstrapper.isBootstrapRow(row);
	}

	@Override
	public boolean shouldSkip(RowMap row) throws SQLException {
		if ( !isBootstrapRow(row) ) {
			return false;
		}
		if ( bootstrappedRow != null && haveSameTable(row, bootstrappedRow) ) {
			return true;
		}
		for ( RowMap queuedRow : queue ) {
			if ( haveSameTable(row, queuedRow) ) {
				return true;
			}
		}
		return false;
	}

	private boolean haveSameTable(RowMap table1, RowMap table2) {
		return table1.getDatabase().equals(table2.getDatabase()) && table1.getTable().equals(table2.getTable());
	}

	@Override
	public void startBootstrap(final RowMap startBootstrapRow, final Schema schema, final AbstractProducer producer, final OpenReplicator replicator) throws Exception {
		queueRow(startBootstrapRow);
		if (thread == null) {
			final RowMap row = bootstrappedRow = queue.remove();
			thread = new Thread(new Runnable() {
				@Override
				public void run( ) {
					try {
						synchronousBootstrapper.startBootstrap(row, schema, producer, replicator);
					} catch ( Exception e ) {
						e.printStackTrace();
						System.exit(1);
					}
				}
			});
			thread.start();
		}
	}

	private void queueRow(RowMap row) {
		String databaseName = ( String ) row.getData("database_name");
		String tableName = ( String ) row.getData("table_name");
		queue.add(row);
		LOGGER.info(String.format("async bootstrapping: queued table %s.%s for bootstrapping", databaseName, tableName));
	}

	@Override
	public void completeBootstrap(RowMap completeBootstrapRow, Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {
		String databaseName = ( String ) completeBootstrapRow.getData("database_name");
		String tableName = ( String ) completeBootstrapRow.getData("table_name");
		long position = ( long ) completeBootstrapRow.getData("binlog_position");
		String file = ( String ) completeBootstrapRow.getData("binlog_file");
		BinlogPosition startPosition = new BinlogPosition(position, file);
		BinlogPosition endPosition = new BinlogPosition(replicator.getBinlogPosition(), replicator.getBinlogFileName());
		try {
			// FIXME: should replay events of table between startPosition and endPosition!
			synchronousBootstrapper.completeBootstrap(completeBootstrapRow, schema, producer, replicator);
			LOGGER.info(String.format("async bootstrapping ended for %s.%s", databaseName, tableName));
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			thread = null;
			bootstrappedRow = null;
		}
		if ( !queue.isEmpty() ) {
			startBootstrap(queue.remove(), schema, producer, replicator);
		}
	}

	@Override
	public void resume(Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {
		synchronousBootstrapper.resume(schema, producer, replicator);
	}
}

