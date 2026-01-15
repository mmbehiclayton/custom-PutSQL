package com.acme.nifi.processors;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SsePutSQLTest {

    private TestRunner runner;
    private SsePutSQL processor;
    private DBCPService mockSourceDbcp;
    private DBCPService mockTargetDbcp;
    private DBCPService mockEngineDbcp;
    private DBCPService mockOperationDbcp;

    @Before
    public void setUp() {
        processor = new SsePutSQL();
        runner = TestRunners.newTestRunner(processor);

        // Mock DBCP services
        mockSourceDbcp = mock(DBCPService.class);
        mockTargetDbcp = mock(DBCPService.class);
        mockEngineDbcp = mock(DBCPService.class);
        mockOperationDbcp = mock(DBCPService.class);

        try {
            // Mock Connection and DatabaseMetaData for source
            Connection mockSourceConnection = mock(Connection.class);
            DatabaseMetaData mockSourceMetaData = mock(DatabaseMetaData.class);
            when(mockSourceConnection.getMetaData()).thenReturn(mockSourceMetaData);
            when(mockSourceMetaData.getURL()).thenReturn("jdbc:mysql://localhost:3306/testdb");
            when(mockSourceMetaData.getDriverName()).thenReturn("com.mysql.cj.jdbc.Driver");
            when(mockSourceDbcp.getConnection()).thenReturn(mockSourceConnection);
            when(mockSourceDbcp.getConnection(Mockito.anyMap())).thenReturn(mockSourceConnection);
            when(mockSourceDbcp.getIdentifier()).thenReturn("sourceDbcp");

            // Mock Connection and DatabaseMetaData for target
            Connection mockTargetConnection = mock(Connection.class);
            DatabaseMetaData mockTargetMetaData = mock(DatabaseMetaData.class);
            when(mockTargetConnection.getMetaData()).thenReturn(mockTargetMetaData);
            when(mockTargetMetaData.getURL()).thenReturn("jdbc:mysql://localhost:3306/testdb");
            when(mockTargetMetaData.getDriverName()).thenReturn("com.mysql.cj.jdbc.Driver");
            when(mockTargetDbcp.getConnection()).thenReturn(mockTargetConnection);
            when(mockTargetDbcp.getConnection(Mockito.anyMap())).thenReturn(mockTargetConnection);
            when(mockTargetDbcp.getIdentifier()).thenReturn("targetDbcp");

            // Mock Connection for engine
            Connection mockEngineConnection = mock(Connection.class);
            when(mockEngineConnection.getAutoCommit()).thenReturn(true);
            doNothing().when(mockEngineConnection).setAutoCommit(false);
            doNothing().when(mockEngineConnection).commit();
            doNothing().when(mockEngineConnection).rollback();
            when(mockEngineConnection.createStatement()).thenReturn(mock(java.sql.Statement.class));
            when(mockEngineDbcp.getConnection()).thenReturn(mockEngineConnection);
            when(mockEngineDbcp.getConnection(Mockito.anyMap())).thenReturn(mockEngineConnection);
            when(mockEngineDbcp.getIdentifier()).thenReturn("engineDbcp");

            // Mock Connection for operation DBCP
            Connection mockOperationConnection = mock(Connection.class);
            DatabaseMetaData mockOperationMetaData = mock(DatabaseMetaData.class);
            when(mockOperationConnection.getMetaData()).thenReturn(mockOperationMetaData);
            when(mockOperationMetaData.getURL()).thenReturn("jdbc:mysql://localhost:3306/testdb");
            when(mockOperationConnection.getAutoCommit()).thenReturn(true);
            doNothing().when(mockOperationConnection).setAutoCommit(true);
            when(mockOperationDbcp.getConnection()).thenReturn(mockOperationConnection);
            when(mockOperationDbcp.getConnection(Mockito.anyMap())).thenReturn(mockOperationConnection);
            when(mockOperationDbcp.getIdentifier()).thenReturn("operationDbcp");

            // Register and enable mock services
            runner.addControllerService("sourceDbcp", mockSourceDbcp);
            runner.addControllerService("targetDbcp", mockTargetDbcp);
            runner.addControllerService("engineDbcp", mockEngineDbcp);
            runner.addControllerService("operationDbcp", mockOperationDbcp);

            runner.enableControllerService(mockSourceDbcp);
            runner.enableControllerService(mockTargetDbcp);
            runner.enableControllerService(mockEngineDbcp);
            runner.enableControllerService(mockOperationDbcp);

            // Set processor properties
            runner.setProperty(SsePutSQL.ENABLE_VALIDATION, "true");
            runner.setProperty(SsePutSQL.SOURCE_DBCP_SERVICE, "sourceDbcp");
            runner.setProperty(SsePutSQL.TARGET_DBCP_SERVICE, "targetDbcp");
            runner.setProperty(SsePutSQL.ENGINE_DBCP_SERVICE, "engineDbcp");
            runner.setProperty(SsePutSQL.CONNECTION_POOL, "operationDbcp");
            runner.setProperty(SsePutSQL.OPERATION_ID, "testOpId");
            runner.setProperty(SsePutSQL.VALIDATION_MODE, "STRICT");
            runner.setProperty(SsePutSQL.AUTO_COMMIT, "true");
            runner.setProperty(SsePutSQL.SUPPORT_TRANSACTIONS, "false");
            runner.setProperty(SsePutSQL.BATCH_SIZE, "100");
            runner.setProperty(SsePutSQL.OBTAIN_GENERATED_KEYS, "false");
            runner.setProperty(org.apache.nifi.processor.util.pattern.RollbackOnFailure.ROLLBACK_ON_FAILURE, "false");
        } catch (InitializationException e) {
            fail("Failed to initialize controller services: " + e.getMessage());
        } catch (SQLException e) {
            fail("Failed to set up mocked database connections: " + e.getMessage());
        }
    }

    @Test
    public void testProcessorWithValidSQL() {
        try {
            // Mock engine connection for environment configuration query
            Connection mockEngineConnection = mockEngineDbcp.getConnection();
            PreparedStatement mockEnvStmt = mock(PreparedStatement.class);
            ResultSet mockEnvRs = mock(ResultSet.class);

            // Mock the environment configuration query
            when(mockEngineConnection.prepareStatement(Mockito.contains("SELECT")))
                    .thenReturn(mockEnvStmt);
            doNothing().when(mockEnvStmt).setString(1, "testOpId");
            when(mockEnvStmt.executeQuery()).thenReturn(mockEnvRs);
            when(mockEnvRs.next()).thenReturn(true);

            // Mock environment configuration results
            when(mockEnvRs.getLong("environment_id")).thenReturn(1L);
            when(mockEnvRs.getString("environment_name")).thenReturn("Test Environment");
            when(mockEnvRs.getString("source_db_type")).thenReturn("mysql");
            when(mockEnvRs.getString("source_host")).thenReturn("localhost");
            when(mockEnvRs.getInt("source_port")).thenReturn(3306);
            when(mockEnvRs.getString("source_database")).thenReturn("testdb");
            when(mockEnvRs.getString("source_name")).thenReturn("Test Source");
            when(mockEnvRs.getString("target_db_type")).thenReturn("mysql");
            when(mockEnvRs.getString("target_host")).thenReturn("localhost");
            when(mockEnvRs.getInt("target_port")).thenReturn(3306);
            when(mockEnvRs.getString("target_database")).thenReturn("testdb");
            when(mockEnvRs.getString("target_name")).thenReturn("Test Target");

            // Mock operation DBCP connection
            Connection mockOperationConnection = mockOperationDbcp.getConnection();
            PreparedStatement mockStmt = mock(PreparedStatement.class);
            when(mockOperationConnection.prepareStatement("INSERT INTO test_table (id) VALUES (?)"))
                    .thenReturn(mockStmt);
            when(mockStmt.executeBatch()).thenReturn(new int[] { 1 });
            doNothing().when(mockStmt).setInt(1, 123);

            // Create FlowFile with SQL and parameters
            Map<String, String> attributes = new HashMap<>();
            attributes.put("sql.args.1.type", String.valueOf(java.sql.Types.INTEGER)); // INTEGER
            attributes.put("sql.args.1.value", "123");
            runner.enqueue("INSERT INTO test_table (id) VALUES (?)", attributes);

            // Run processor
            runner.run();

            // Verify results
            runner.assertTransferCount(SsePutSQL.REL_SUCCESS, 1);
            runner.assertTransferCount(SsePutSQL.REL_FAILURE, 0);
            runner.assertTransferCount(SsePutSQL.REL_RETRY, 0);
            runner.assertTransferCount(SsePutSQL.REL_VALIDATION_FAILED, 0);

            MockFlowFile flowFile = runner.getFlowFilesForRelationship(SsePutSQL.REL_SUCCESS).get(0);
            flowFile.assertAttributeEquals("validation.passed", "true");
            flowFile.assertAttributeEquals("validation.environment.id", "1");
            flowFile.assertAttributeEquals("validation.environment.name", "Test Environment");
        } catch (SQLException e) {
            fail("Failed to set up mocked database connections: " + e.getMessage());
        }
    }

    @Test
    public void testInvalidOperationId() {
        try {
            // Mock engine connection for invalid operation
            Connection mockEngineConnection = mockEngineDbcp.getConnection();
            PreparedStatement mockStmt = mock(PreparedStatement.class);
            ResultSet mockRs = mock(ResultSet.class);
            when(mockEngineConnection.prepareStatement(Mockito.contains("SELECT")))
                    .thenReturn(mockStmt);
            doNothing().when(mockStmt).setString(1, "testOpId");
            when(mockStmt.executeQuery()).thenReturn(mockRs);
            when(mockRs.next()).thenReturn(false); // No environment found

            // Create FlowFile
            Map<String, String> attributes = new HashMap<>();
            runner.enqueue("INSERT INTO test_table (id) VALUES (123)", attributes);

            // Run processor
            runner.run();

            // Verify results
            runner.assertTransferCount(SsePutSQL.REL_SUCCESS, 0);
            runner.assertTransferCount(SsePutSQL.REL_SUCCESS, 0);
            runner.assertTransferCount(SsePutSQL.REL_FAILURE, 0);
            runner.assertTransferCount(SsePutSQL.REL_RETRY, 0);
            runner.assertTransferCount(SsePutSQL.REL_VALIDATION_FAILED, 1);

            MockFlowFile flowFile = runner.getFlowFilesForRelationship(SsePutSQL.REL_VALIDATION_FAILED).get(0);
            flowFile.assertAttributeEquals("validation.passed", "false");
            flowFile.assertAttributeEquals("validation.error",
                    "No environment configuration found for operation_id: testOpId");
        } catch (SQLException e) {
            fail("Failed to set up mocked database connections in testInvalidOperationId: " + e.getMessage());
        }
    }

    @Test
    public void testValidationFailure() {
        try {
            // Mock engine connection for environment configuration query
            Connection mockEngineConnection = mockEngineDbcp.getConnection();
            PreparedStatement mockEnvStmt = mock(PreparedStatement.class);
            ResultSet mockEnvRs = mock(ResultSet.class);

            // Mock the environment configuration query
            when(mockEngineConnection.prepareStatement(Mockito.contains("SELECT")))
                    .thenReturn(mockEnvStmt);
            doNothing().when(mockEnvStmt).setString(1, "testOpId");
            when(mockEnvStmt.executeQuery()).thenReturn(mockEnvRs);
            when(mockEnvRs.next()).thenReturn(true);

            // Mock environment configuration results with different URLs
            when(mockEnvRs.getLong("environment_id")).thenReturn(1L);
            when(mockEnvRs.getString("environment_name")).thenReturn("Test Environment");
            when(mockEnvRs.getString("source_db_type")).thenReturn("mysql");
            when(mockEnvRs.getString("source_host")).thenReturn("different-host");
            when(mockEnvRs.getInt("source_port")).thenReturn(3306);
            when(mockEnvRs.getString("source_database")).thenReturn("testdb");
            when(mockEnvRs.getString("source_name")).thenReturn("Test Source");
            when(mockEnvRs.getString("target_db_type")).thenReturn("mysql");
            when(mockEnvRs.getString("target_host")).thenReturn("different-host");
            when(mockEnvRs.getInt("target_port")).thenReturn(3306);
            when(mockEnvRs.getString("target_database")).thenReturn("testdb");
            when(mockEnvRs.getString("target_name")).thenReturn("Test Target");

            // Create FlowFile
            Map<String, String> attributes = new HashMap<>();
            runner.enqueue("INSERT INTO test_table (id) VALUES (123)", attributes);

            // Run processor
            runner.run();

            // Verify results - should route to validation_failed in STRICT mode
            runner.assertTransferCount(SsePutSQL.REL_SUCCESS, 0);
            runner.assertTransferCount(SsePutSQL.REL_FAILURE, 0);
            runner.assertTransferCount(SsePutSQL.REL_RETRY, 0);
            runner.assertTransferCount(SsePutSQL.REL_VALIDATION_FAILED, 1);

            MockFlowFile flowFile = runner.getFlowFilesForRelationship(SsePutSQL.REL_VALIDATION_FAILED).get(0);
            flowFile.assertAttributeEquals("validation.error",
                    "Controller service mismatch for environment 'Test Environment' (ID: 1). Source match: false, Target match: false. Expected: Source=jdbc:mysql://different-host:3306/testdb, Target=jdbc:mysql://different-host:3306/testdb. Actual: Source=jdbc:mysql://localhost:3306/testdb, Target=jdbc:mysql://localhost:3306/testdb");
        } catch (SQLException e) {
            fail("Failed to set up mocked database connections in testValidationFailure: " + e.getMessage());
        }
    }

    @Test
    public void testValidationFailureWithStatusUpdate() {
        try {
            // Note: FAILURE_ACTION property was removed - validation failures route to
            // validation_failed relationship

            // Mock engine connection for environment configuration query
            Connection mockEngineConnection = mockEngineDbcp.getConnection();
            PreparedStatement mockEnvStmt = mock(PreparedStatement.class);
            ResultSet mockEnvRs = mock(ResultSet.class);

            // Mock the environment configuration query
            when(mockEngineConnection.prepareStatement(Mockito.contains("SELECT")))
                    .thenReturn(mockEnvStmt);
            doNothing().when(mockEnvStmt).setString(1, "testOpId");
            when(mockEnvStmt.executeQuery()).thenReturn(mockEnvRs);
            when(mockEnvRs.next()).thenReturn(true);

            // Mock environment configuration results with different URLs
            when(mockEnvRs.getLong("environment_id")).thenReturn(1L);
            when(mockEnvRs.getString("environment_name")).thenReturn("Test Environment");
            when(mockEnvRs.getString("source_db_type")).thenReturn("mysql");
            when(mockEnvRs.getString("source_host")).thenReturn("different-host");
            when(mockEnvRs.getInt("source_port")).thenReturn(3306);
            when(mockEnvRs.getString("source_database")).thenReturn("testdb");
            when(mockEnvRs.getString("source_name")).thenReturn("Test Source");
            when(mockEnvRs.getString("target_db_type")).thenReturn("mysql");
            when(mockEnvRs.getString("target_host")).thenReturn("different-host");
            when(mockEnvRs.getInt("target_port")).thenReturn(3306);
            when(mockEnvRs.getString("target_database")).thenReturn("testdb");
            when(mockEnvRs.getString("target_name")).thenReturn("Test Target");

            // Mock operation status update
            PreparedStatement mockUpdateStmt = mock(PreparedStatement.class);
            when(mockEngineConnection.prepareStatement(Mockito.contains("UPDATE")))
                    .thenReturn(mockUpdateStmt);
            doNothing().when(mockUpdateStmt).setString(1, "FAILED");
            doNothing().when(mockUpdateStmt).setString(2, "Environment validation failed");
            doNothing().when(mockUpdateStmt).setLong(3, 1L);
            when(mockUpdateStmt.executeUpdate()).thenReturn(1);

            // Create FlowFile
            Map<String, String> attributes = new HashMap<>();
            runner.enqueue("INSERT INTO test_table (id) VALUES (123)", attributes);

            // Run processor
            runner.run();

            // Verify results - validation failure should route to validation_failed in
            // STRICT mode
            runner.assertTransferCount(SsePutSQL.REL_SUCCESS, 0);
            runner.assertTransferCount(SsePutSQL.REL_FAILURE, 0);
            runner.assertTransferCount(SsePutSQL.REL_RETRY, 0);
            runner.assertTransferCount(SsePutSQL.REL_VALIDATION_FAILED, 1);

            MockFlowFile flowFile = runner.getFlowFilesForRelationship(SsePutSQL.REL_VALIDATION_FAILED).get(0);
            flowFile.assertAttributeEquals("validation.passed", "false");
            flowFile.assertAttributeExists("validation.error");
        } catch (SQLException e) {
            fail("Failed to set up mocked database connections in testValidationFailureWithStatusUpdate: "
                    + e.getMessage());
        }
    }
}