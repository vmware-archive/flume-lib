package com.gopivotal.flume.sink;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresSink extends AbstractSink implements Configurable {

	private String table = null;
	private String user = null;
	private String password = null;
	private String url = null;
	private String delimiter = null;
	private Charset encoding = null;
	private Connection client = null;
	private CounterGroup counterGroup = new CounterGroup();
	private PreparedStatement insert = null;
	private String sqlStatement = null;

	private int batchSize = 0;
	private int numColumns = 0;
	private int[] types = null;

	private static final Logger LOG = LoggerFactory
			.getLogger(PostgresSink.class);

	@Override
	public void configure(Context context) {

		try {
			// Load JDBC driver for Postgres
			Class.forName("org.postgresql.Driver");
			LOG.info("Loaded postgres JDBC driver");
		} catch (ClassNotFoundException e) {
			throw new FlumeException("Postgres JDBC driver not on classpath");
		}

		// Get all configuration variables
		String hostname = context.getString("hostname");
		int port = context.getInteger("port");
		String database = context.getString("database");
		table = context.getString("table");
		user = context.getString("user", null);
		password = context.getString("password", null);
		numColumns = context.getInteger("num.columns");
		batchSize = context.getInteger("batch.size", 100);
		encoding = Charset.forName(context.getString("encoding", "UTF8"));
		delimiter = context.getString("delimiter", "\\|");

		// Parse the types from the configuration
		types = new int[numColumns];

		for (int i = 1; i <= numColumns; ++i) {
			types[i - 1] = getSqlType(context.getString("columntype." + i));
			if (types[i - 1] == Types.NULL) {
				throw new FlumeException("Type "
						+ context.getString("columntype." + i)
						+ " not supported");
			}
		}

		// Log the properties
		LOG.info("Properties: {}", context.getParameters());

		// Create the connect string and SQL statement
		url = "jdbc:postgresql://" + hostname + ":" + port + "/" + database;

		sqlStatement = "INSERT INTO " + table + " VALUES (";

		for (int i = 0; i < numColumns; ++i) {
			sqlStatement += "?,";
		}

		sqlStatement = sqlStatement.substring(0, sqlStatement.length() - 1);
		sqlStatement += ");";

		LOG.info("Statement: {}", sqlStatement);
	}

	@Override
	public Status process() throws EventDeliveryException {

		// Get the channel and transaction

		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();

		try {
			// Start the transaction and get all events
			transaction.begin();

			List<Event> batch = new ArrayList<Event>();

			for (int i = 0; i < batchSize; ++i) {
				Event event = channel.take();

				if (event == null) {
					counterGroup.incrementAndGet("batch.underflow");
					break;
				}

				batch.add(event);
			}

			if (batch.isEmpty()) {
				counterGroup.incrementAndGet("batch.empty");
				status = Status.BACKOFF;
			} else {

				// Verify we have a connection
				verifyConnection();

				// For each event in the batch
				for (Event e : batch) {
					String line = new String(e.getBody(), encoding);
					String[] tokens;

					// If we have only one column, set the token to the line
					if (numColumns == 1) {
						tokens = new String[] { line };
					} else {
						// Else, we split on our delimiter
						tokens = line.split(delimiter);
					}

					// if we have the right number of tokens (columns)
					if (tokens.length == numColumns) {

						// Set our insert statement based on the types
						for (int i = 1; i <= tokens.length; ++i) {
							switch (types[i - 1]) {
							case Types.VARCHAR:
							case Types.CHAR:
								insert.setString(i, tokens[i - 1]);
								break;
							case Types.NUMERIC:
								insert.setBigDecimal(i, new BigDecimal(
										tokens[i - 1]));
								break;
							case Types.INTEGER:
								insert.setInt(i,
										Integer.parseInt(tokens[i - 1]));
								break;
							case Types.SMALLINT:
								insert.setShort(i,
										Short.parseShort(tokens[i - 1]));
								break;
							case Types.BIGINT:
								insert.setLong(i, Long.parseLong(tokens[i - 1]));
								break;
							case Types.REAL:
							case Types.FLOAT:
								insert.setFloat(i,
										Float.parseFloat(tokens[i - 1]));
								break;
							case Types.DOUBLE:
								insert.setDouble(i,
										Double.parseDouble(tokens[i - 1]));
								break;
							case Types.DATE:
								insert.setDate(i, Date.valueOf(tokens[i - 1]));
								break;
							case Types.TIME:
								insert.setTime(i, Time.valueOf(tokens[i - 1]));
								break;
							case Types.TIMESTAMP:
								insert.setTimestamp(i,
										Timestamp.valueOf(tokens[i - 1]));
								break;
							default:
								throw new FlumeException("Type for "
										+ tokens[i - 1]
										+ " is not yet implemented");
							}
						}

						// Execute the update and increment the counter
						insert.executeUpdate();

						counterGroup.incrementAndGet("statements.commit");
					} else {

						// Log that we have the wrong number of columns for this
						// record
						counterGroup
								.incrementAndGet("statements.wrong.num.columns");
					}
				}

				// Commit to Postgres
				client.commit();

				// Increment that this was a successful batch
				counterGroup.incrementAndGet("batch.success");
			}

			// Commit the transmission
			transaction.commit();

			LOG.debug("Counters: {}", counterGroup);
		} catch (Exception e) {
			transaction.rollback();
			throw new EventDeliveryException(e);
		} finally {
			transaction.close();
		}

		return status;
	}

	@Override
	public synchronized void start() {

		try {
			openConnection();
		} catch (SQLException e) {
			throw new FlumeException(e);
		}

		super.start();
		LOG.info("Postgres Sink started");
	}

	@Override
	public synchronized void stop() {

		try {
			destroyConnection();
		} catch (SQLException e) {
			throw new FlumeException(e);
		}

		super.stop();
		LOG.info("Postgres Sink stopped");
	}

	/**
	 * Verifies that a connection is valid and will open one if needed.
	 * 
	 * @throws SQLException
	 */
	private void verifyConnection() throws SQLException {
		if (client == null) {
			openConnection();
		} else if (client.isClosed()) {
			destroyConnection();
			openConnection();
		}
	}

	/**
	 * Opens the given connection and creates the prepared statement
	 * 
	 * @throws SQLException
	 */
	private void openConnection() throws SQLException {
		Properties props = new Properties();

		if (user != null && password != null) {
			props.setProperty("user", user);
			props.setProperty("password", password);
		} else if (user != null ^ password != null) {
			LOG.warn("User or password is set without the other. Continuing with no login auth");
		}

		client = DriverManager.getConnection(url, props);
		client.setAutoCommit(false);
		insert = client.prepareStatement(sqlStatement);
		LOG.info("Opened client connection and prepared insert statement");
	}

	/**
	 * Destroys the current JDBC connection, if any.
	 * 
	 * @throws SQLException
	 */
	private void destroyConnection() throws SQLException {
		if (client != null) {
			client.close();
			client = null;
			LOG.info("Closed client connection");
			LOG.info("Counters: {}", counterGroup);
		}
	}

	/**
	 * Gets the SQL type based on a string
	 * 
	 * @param String
	 *            The string of a SQL type
	 * @return The {@link Types} integer value
	 */
	private int getSqlType(String string) {

		if (string.equalsIgnoreCase("VARCHAR")) {
			return Types.VARCHAR;
		} else if (string.equalsIgnoreCase("TEXT")) {
			return Types.VARCHAR;
		} else if (string.equalsIgnoreCase("CHAR")) {
			return Types.CHAR;
		} else if (string.equalsIgnoreCase("NUMERIC")) {
			return Types.NUMERIC;
		} else if (string.equalsIgnoreCase("INTEGER")) {
			return Types.INTEGER;
		} else if (string.equalsIgnoreCase("INT")) {
			return Types.INTEGER;
		} else if (string.equalsIgnoreCase("SMALLINT")) {
			return Types.BIGINT;
		} else if (string.equalsIgnoreCase("BIGINT")) {
			return Types.REAL;
		} else if (string.equalsIgnoreCase("REAL")) {
			return Types.FLOAT;
		} else if (string.equalsIgnoreCase("FLOAT")) {
			return Types.FLOAT;
		} else if (string.equalsIgnoreCase("DOUBLE")) {
			return Types.DOUBLE;
		} else if (string.equalsIgnoreCase("DOUBLE PRECISION")) {
			return Types.DOUBLE;
		} else if (string.equalsIgnoreCase("DATE")) {
			return Types.DATE;
		} else if (string.equalsIgnoreCase("TIME")) {
			return Types.TIME;
		} else if (string.equalsIgnoreCase("TIMESTAMP")) {
			return Types.TIMESTAMP;
		}

		return Types.NULL;
	}

}
