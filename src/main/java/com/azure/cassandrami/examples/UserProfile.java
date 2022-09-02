package com.azure.cassandrami.examples;

import com.azure.cassandrami.repository.UserRepository;
import com.azure.cassandrami.util.Configurations;
import com.datastax.oss.driver.api.core.CqlSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example class which will demonstrate following operations on Azure Managed Instance for Apache Cassandra
 * - Create Keyspace - Create Table - Insert Rows - Select all data from a table - Select a row from a table
 */
public class UserProfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserProfile.class);

    public static void main(String[] s) throws Exception {

        Configurations config = new Configurations();
        String dc = config.getProperty("DC");
        LOGGER.info("Creating Cassandra session...");
        CqlSession cassandraSession = CqlSession.builder().withLocalDatacenter(dc).build();

        final String keyspace = "uprofile";
        final String table = "user";

        try {
            UserRepository repository = new UserRepository(cassandraSession, keyspace, table);

            //Drop keyspace in cassandra database if exists
            repository.dropKeyspace();      
            Thread.sleep(2000);

            //Create keyspace in cassandra database
            repository.createKeyspace();
            Thread.sleep(2000);
            
            //Create table in cassandra database
            repository.createTable();
            Thread.sleep(2000);

            //Insert rows into user table
            String preparedStatement = repository.prepareInsertStatement();
            repository.insertUser(preparedStatement, 1, "LyubovK", "Bangalore");
            repository.insertUser(preparedStatement, 2, "JiriK", "Mumbai");
            repository.insertUser(preparedStatement, 3, "IvanH", "Belgum");
            repository.insertUser(preparedStatement, 4, "YuliaT", "Gurgaon");
            repository.insertUser(preparedStatement, 5, "IvanaV", "Dubai");

            LOGGER.info("Select all users");
            repository.selectAllUsers();

            LOGGER.info("Select a user by id (3)");
            repository.selectUser(3);
        }
        finally {
            cassandraSession.close();
            LOGGER.info("Please delete your table after verifying the presence of the data from CQL");
        }
    }
}
