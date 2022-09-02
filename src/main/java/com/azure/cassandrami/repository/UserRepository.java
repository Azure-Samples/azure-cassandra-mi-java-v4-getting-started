package com.azure.cassandrami.repository;

import java.io.IOException;
import java.util.List;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.azure.cassandrami.util.Configurations;
import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class gives implementations of create, delete table on Cassandra database
 * Insert & select data from the table
 */
public class UserRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserRepository.class);
    private CqlSession session;
    private String keyspace;
    private String table;
    Configurations config;

    public UserRepository(CqlSession session, String keyspace, String table) {
        this.session = session;
        this.keyspace = keyspace;
        this.table = table;
        this.config = new Configurations();

    }


    /**
     * Create keyspace uprofile in cassandra DB
     */
    public void dropKeyspace() {
        String query = "DROP KEYSPACE IF EXISTS "+keyspace+"";
        session.execute(query);
        LOGGER.info("dropped keyspace '"+keyspace+"'");
    }    

    /**
     * Create keyspace uprofile in cassandra DB
     * @throws IOException
     */
    public void createKeyspace() throws IOException {        
        String dc = config.getProperty("DC");
        String query = "CREATE KEYSPACE "+keyspace+" WITH REPLICATION = {'class':'NetworkTopologyStrategy', '"+dc+"' :3}";
        session.execute(query);
        LOGGER.info("Created keyspace '"+keyspace+"'");
    }

    /**
     * Create user table in cassandra DB
     */
    public void createTable() {
        String query = "CREATE TABLE "+keyspace+"."+table+" (user_id int PRIMARY KEY, user_name text, user_bcity text)";
        session.execute(query);
        LOGGER.info("Created table '"+table+"'");
    }

    /**
     * Select all rows from user table
     */
    public void selectAllUsers() {

        final String query = "SELECT * FROM "+keyspace+"."+table+"";
        List<Row> rows = session.execute(query).all();

        for (Row row : rows) {
            LOGGER.info("Obtained row: {} | {} | {} ", row.getInt("user_id"), row.getString("user_name"), row.getString("user_bcity"));
        }
    }

    /**
     * Select a row from user table
     *
     * @param id user_id
     */
    public void selectUser(int id) {
        final String query = "SELECT * FROM "+keyspace+"."+table+" where user_id = 3";
        Row row = session.execute(query).one();

        LOGGER.info("Obtained row: {} | {} | {} ", row.getInt("user_id"), row.getString("user_name"), row.getString("user_bcity"));
    }

    /**
     * Insert a row into user table
     *
     * @param id   user_id
     * @param name user_name
     * @param city user_bcity
     */
    public void insertUser(String preparedStatement, int id, String name, String city) {
        PreparedStatement prepared = session.prepare(preparedStatement);
        BoundStatement bound = prepared.bind(id, city, name).setIdempotent(true);
        session.execute(bound);
    }

    /**
     * Create a PrepareStatement to insert a row to user table
     *
     * @return PreparedStatement
     */
    public String prepareInsertStatement() {
        final String insertStatement = "INSERT INTO  "+keyspace+"."+table+" (user_id, user_name , user_bcity) VALUES (?,?,?)";
        return insertStatement;
    }
}