/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.acme.nifi.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
// DBCPService import removed - using reflection to avoid NoClassDefFoundError
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.ErrorTypes;
import org.apache.nifi.processor.util.pattern.ExceptionHandler;
import org.apache.nifi.processor.util.pattern.PartialFunctions;
import org.apache.nifi.processor.util.pattern.PartialFunctions.FetchFlowFiles;
import org.apache.nifi.processor.util.pattern.PartialFunctions.FlowFileGroup;
import org.apache.nifi.processor.util.pattern.PutGroup;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processor.util.pattern.RoutingResult;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.db.JdbcCommon;

import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.apache.nifi.processor.util.pattern.ExceptionHandler.createOnError;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "put", "rdbms", "database", "update", "insert", "relational"})
@CapabilityDescription("Executes a SQL UPDATE or INSERT command with enhanced environment validation. Validates that source and target controller service connection pools match the expected environment configuration before executing database operations. "
        + "to execute. The SQL command may use the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes "
        + "with the naming convention sql.args.N.type and sql.args.N.value, where N is a positive integer. The sql.args.N.type is expected to be "
        + "a number indicating the JDBC Type. The content of the FlowFile is expected to be in UTF-8 format.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "fragment.identifier", description = "If the <Support Fragment Transactions> property is true, this attribute is used to determine whether or "
                + "not two FlowFiles belong to the same transaction."),
        @ReadsAttribute(attribute = "fragment.count", description = "If the <Support Fragment Transactions> property is true, this attribute is used to determine how many FlowFiles "
                + "are needed to complete the transaction."),
        @ReadsAttribute(attribute = "fragment.index", description = "If the <Support Fragment Transactions> property is true, this attribute is used to determine the order that the FlowFiles "
                + "in a transaction should be evaluated."),
        @ReadsAttribute(attribute = "sql.args.N.type", description = "Incoming FlowFiles are expected to be parametrized SQL statements. The type of each Parameter is specified as an integer "
                + "that represents the JDBC Type of the parameter."),
        @ReadsAttribute(attribute = "sql.args.N.value", description = "Incoming FlowFiles are expected to be parametrized SQL statements. The value of the Parameters are specified as "
                + "sql.args.1.value, sql.args.2.value, sql.args.3.value, and so on. The type of the sql.args.1.value Parameter is specified by the sql.args.1.type attribute."),
        @ReadsAttribute(attribute = "sql.args.N.format", description = "This attribute is always optional, but default options may not always work for your data. "
                + "Incoming FlowFiles are expected to be parametrized SQL statements. In some cases "
                + "a format option needs to be specified, currently this is only applicable for binary data types, dates, times and timestamps. Binary Data Types (defaults to 'ascii') - "
                + "ascii: each string character in your attribute value represents a single byte. This is the format provided by Avro Processors. "
                + "base64: the string is a Base64 encoded string that can be decoded to bytes. "
                + "hex: the string is hex encoded with all letters in upper case and no '0x' at the beginning. "
                + "Dates/Times/Timestamps - "
                + "Date, Time and Timestamp formats all support both custom formats or named format ('yyyy-MM-dd','ISO_OFFSET_DATE_TIME') "
                + "as specified according to java.time.format.DateTimeFormatter. "
                + "If not specified, a long value input is expected to be an unix epoch (milli seconds from 1970/1/1), or a string value in "
                + "'yyyy-MM-dd' format for Date, 'HH:mm:ss.SSS' for Time (some database engines e.g. Derby or MySQL do not support milliseconds and will truncate milliseconds), "
                + "'yyyy-MM-dd HH:mm:ss.SSS' for Timestamp is used.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "sql.generated.key", description = "If the database generated a key for an INSERT statement and the Obtain Generated Keys property is set to true, "
                + "this attribute will be added to indicate the generated key, if possible. This feature is not supported by all database vendors.")
})
public class EnhancedPutSQL extends AbstractSessionFactoryProcessor {

    // Master Switch for Validation
    public static final PropertyDescriptor ENABLE_VALIDATION = new PropertyDescriptor.Builder()
            .name("enable-validation")
            .displayName("Enable Validation")
            .description("Enable or disable environment validation. Set to false only for testing or emergency bypass. When false, processor behaves exactly like standard PutSQL.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    // Validation Properties (only shown when ENABLE_VALIDATION is true)
    public static final PropertyDescriptor OPERATION_ID = new PropertyDescriptor.Builder()
            .name("operation-id")
            .displayName("Operation ID")
            .description("The operation ID from FlowFile attributes. Used to look up environment configuration.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${operation.id}")
            .dependsOn(ENABLE_VALIDATION, "true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ENGINE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("engine-dbcp-service")
            .displayName("SSE Engine Database Connection Pool")
            .description("The DBCP Controller Service for connecting to the SSE Engine database to query environment configurations")
            .required(false)
            .identifiesControllerService(ControllerService.class)
            .dependsOn(ENABLE_VALIDATION, "true")
            .build();

    public static final PropertyDescriptor SOURCE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("source-dbcp-service")
            .displayName("Source Database Connection Pool")
            .description("The DBCP Controller Service to validate against the environment's source configuration")
            .required(false)
            .identifiesControllerService(ControllerService.class)
            .dependsOn(ENABLE_VALIDATION, "true")
            .build();

    public static final PropertyDescriptor TARGET_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("target-dbcp-service")
            .displayName("Target Database Connection Pool")
            .description("The DBCP Controller Service to validate against the environment's target configuration")
            .required(false)
            .identifiesControllerService(ControllerService.class)
            .dependsOn(ENABLE_VALIDATION, "true")
            .build();

    public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor.Builder()
            .name("validation-mode")
            .displayName("Validation Mode")
            .description("Determines validation behavior: STRICT (terminate on mismatch) or WARNING (log and continue)")
            .required(false)
            .defaultValue("STRICT")
            .allowableValues("STRICT", "WARNING")
            .dependsOn(ENABLE_VALIDATION, "true")
            .build();

    // Standard PutSQL Properties
    static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool")
            .description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
                    + "The Connection Pool is necessary in order to determine the appropriate database column types.")
            .identifiesControllerService(ControllerService.class)
            .required(true)
            .build();

    static final PropertyDescriptor SQL_STATEMENT = new PropertyDescriptor.Builder()
            .name("putsql-sql-statement")
            .displayName("SQL Statement")
            .description("The SQL statement to execute. The statement can be empty, a constant value, or built from attributes "
                    + "using Expression Language. If this property is specified, it will be used regardless of the content of "
                    + "incoming FlowFiles. If this property is empty, the content of the incoming FlowFile is expected "
                    + "to contain a valid SQL statement, to be issued by the processor to the database.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
            .name("database-session-autocommit")
            .displayName("Database Session AutoCommit")
            .description("The autocommit mode to set on the database connection being used. If set to false, the operation(s) will be explicitly committed or rolled back "
                    + "(based on success or failure respectively), if set to true the driver/database handles the commit/rollback.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor SUPPORT_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("Support Fragmented Transactions")
            .description("If true, when a FlowFile is consumed by this Processor, the Processor will first check the fragment.identifier and fragment.count attributes of that FlowFile. "
                    + "If the fragment.count value is greater than 1, the Processor will not process any FlowFile with that fragment.identifier until all are available; "
                    + "at that point, it will process all FlowFiles with that fragment.identifier as a single transaction, in the order specified by the FlowFiles' fragment.index attributes. "
                    + "This Provides atomicity of those SQL statements. Once any statement of this transaction throws exception when executing, this transaction will be rolled back. When "
                    + "transaction rollback happened, none of these FlowFiles would be routed to 'success'. If the <Rollback On Failure> is set true, these FlowFiles will stay in the input "
                    + "relationship. When the <Rollback On Failure> is set false,, if any of these FlowFiles will be routed to 'retry', all of these FlowFiles will be routed to 'retry'.Otherwise, "
                    + "they will be routed to 'failure'. If this value is false, these attributes will be ignored and the updates will occur independent of one another.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    static final PropertyDescriptor TRANSACTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Transaction Timeout")
            .description("If the <Support Fragmented Transactions> property is set to true, specifies how long to wait for all FlowFiles for a particular fragment.identifier attribute "
                    + "to arrive before just transferring all of the FlowFiles with that identifier to the 'failure' relationship")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();
    static final PropertyDescriptor OBTAIN_GENERATED_KEYS = new PropertyDescriptor.Builder()
            .name("Obtain Generated Keys")
            .description("If true, any key that is automatically generated by the database will be added to the FlowFile that generated it using the sql.generate.key attribute. "
                    + "This may result in slightly slower performance and is not supported by all databases.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ENABLE_VALIDATION,
            OPERATION_ID,
            ENGINE_DBCP_SERVICE,
            SOURCE_DBCP_SERVICE,
            TARGET_DBCP_SERVICE,
            VALIDATION_MODE,
            CONNECTION_POOL,
            SQL_STATEMENT,
            SUPPORT_TRANSACTIONS,
            AUTO_COMMIT,
            TRANSACTION_TIMEOUT,
            BATCH_SIZE,
            OBTAIN_GENERATED_KEYS,
            RollbackOnFailure.ROLLBACK_ON_FAILURE
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();
    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();

    static final Relationship REL_VALIDATION_FAILED = new Relationship.Builder()
            .name("validation_failed")
            .description("FlowFiles that fail environment validation are routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_RETRY,
            REL_FAILURE,
            REL_VALIDATION_FAILED
    );

    private static final String FRAGMENT_ID_ATTR = FragmentAttributes.FRAGMENT_ID.key();
    private static final String FRAGMENT_INDEX_ATTR = FragmentAttributes.FRAGMENT_INDEX.key();
    private static final String FRAGMENT_COUNT_ATTR = FragmentAttributes.FRAGMENT_COUNT.key();

    private static final String ERROR_MESSAGE_ATTR = "error.message";
    private static final String ERROR_CODE_ATTR = "error.code";
    private static final String ERROR_SQL_STATE_ATTR = "error.sql.state";

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        // Property migration handled by RollbackOnFailure utility
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected final Collection<ValidationResult> customValidate(ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();
        final String support_transactions = context.getProperty(SUPPORT_TRANSACTIONS).getValue();
        final String rollback_on_failure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).getValue();
        final String auto_commit = context.getProperty(AUTO_COMMIT).getValue();

        if (auto_commit.equalsIgnoreCase("true")) {
            if (support_transactions.equalsIgnoreCase("true")) {
                results.add(new ValidationResult.Builder()
                                .subject(SUPPORT_TRANSACTIONS.getDisplayName())
                                .explanation(format("'%s' cannot be set to 'true' when '%s' is also set to 'true'."
                                        + "Transactions for batch updates cannot be supported when auto commit is set to 'true'",
                                        SUPPORT_TRANSACTIONS.getDisplayName(), AUTO_COMMIT.getDisplayName()))
                                .build());
            }
            if (rollback_on_failure.equalsIgnoreCase("true")) {
                results.add(new ValidationResult.Builder()
                        .subject(RollbackOnFailure.ROLLBACK_ON_FAILURE.getDisplayName())
                        .explanation(format("'%s' cannot be set to 'true' when '%s' is also set to 'true'."
                                + "Transaction rollbacks for batch updates cannot be supported when auto commit is set to 'true'",
                                RollbackOnFailure.ROLLBACK_ON_FAILURE.getDisplayName(), AUTO_COMMIT.getDisplayName()))
                        .build());
            }
        }
        return results;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private static class FunctionContext extends RollbackOnFailure {
        private boolean obtainKeys = false;
        private boolean fragmentedTransaction = false;
        private boolean originalAutoCommit = false;
        private final long startNanos = System.nanoTime();

        private FunctionContext(boolean rollbackOnFailure) {
            super(rollbackOnFailure, true);
        }

        private boolean isSupportBatching() {
            return !obtainKeys && !fragmentedTransaction;
        }
    }

    private PutGroup<FunctionContext, Connection, StatementFlowFileEnclosure> process;
    private BiFunction<FunctionContext, ErrorTypes, ErrorTypes.Result> adjustError;
    private ExceptionHandler<FunctionContext> exceptionHandler;


    private final FetchFlowFiles<FunctionContext> fetchFlowFiles = (c, s, fc, r) -> {
        final FlowFilePoll poll = pollFlowFiles(c, s, fc, r);
        if (poll == null) {
            return null;
        }
        fc.fragmentedTransaction = poll.isFragmentedTransaction();
        return poll.getFlowFiles();
    };

    private final PartialFunctions.InitConnection<FunctionContext, Connection> initConnection = (c, s, fc, ffs) -> {
        final ControllerService service = c.getProperty(CONNECTION_POOL).asControllerService();
        final Object dbcpService = asDBCPService(service);
        if (dbcpService == null) {
            throw new ProcessException("DBCP Service class not available. Ensure nifi-dbcp-service-api is available in NiFi runtime.");
        }
        Connection connection = null;
        try {
            connection = getConnectionFromDBCPService(dbcpService, 
                    ffs == null || ffs.isEmpty() ? emptyMap() : ffs.get(0).getAttributes());
            fc.originalAutoCommit = connection.getAutoCommit();
            final boolean autocommit = c.getProperty(AUTO_COMMIT).asBoolean();
            if (fc.originalAutoCommit != autocommit) {
                try {
                    connection.setAutoCommit(autocommit);
                } catch (SQLFeatureNotSupportedException sfnse) {
                    getLogger().debug("setAutoCommit({}) not supported by this driver", autocommit);
                }
            }
        } catch (SQLException e) {
            throw new ProcessException("Failed to disable auto commit due to " + e, e);
        } catch (Exception e) {
            throw new ProcessException("Failed to get connection from DBCP service: " + e.getMessage(), e);
        }
        return connection;
    };


    @FunctionalInterface
    private interface GroupingFunction {
        void apply(final ProcessContext context, final ProcessSession session, final FunctionContext fc,
                   final Connection conn, final List<FlowFile> flowFiles,
                   final List<StatementFlowFileEnclosure> groups,
                   final RoutingResult result);
    }

    private final GroupingFunction groupFragmentedTransaction = (context, session, fc, conn, flowFiles, groups, result) -> {
        final FragmentedEnclosure fragmentedEnclosure = new FragmentedEnclosure();
        groups.add(fragmentedEnclosure);

        final Map<String, StatementFlowFileEnclosure> sqlToEnclosure = new HashMap<>();

        for (final FlowFile flowFile : flowFiles) {
            final String sql = context.getProperty(EnhancedPutSQL.SQL_STATEMENT).isSet()
                    ? context.getProperty(EnhancedPutSQL.SQL_STATEMENT).evaluateAttributeExpressions(flowFile).getValue()
                    : getSQL(session, flowFile);

            final StatementFlowFileEnclosure enclosure = sqlToEnclosure
                    .computeIfAbsent(sql, k -> new StatementFlowFileEnclosure(sql));

            fragmentedEnclosure.addFlowFile(flowFile, enclosure);
        }
    };

    private final GroupingFunction groupFlowFilesBySQLBatch = (context, session, fc, conn, flowFiles, groups, result) -> {
        for (final FlowFile flowFile : flowFiles) {
            final String sql = context.getProperty(EnhancedPutSQL.SQL_STATEMENT).isSet()
                    ? context.getProperty(EnhancedPutSQL.SQL_STATEMENT).evaluateAttributeExpressions(flowFile).getValue()
                    : getSQL(session, flowFile);

            // Create a new PreparedStatement or reuse the one from the last group if that is the same.
            final StatementFlowFileEnclosure enclosure;
            final StatementFlowFileEnclosure lastEnclosure = groups.isEmpty() ? null : groups.get(groups.size() - 1);

            if (lastEnclosure == null || !lastEnclosure.getSql().equals(sql)) {
                enclosure = new StatementFlowFileEnclosure(sql);
                groups.add(enclosure);
            } else {
                enclosure = lastEnclosure;
            }

            if (!exceptionHandler.execute(fc, flowFile, input -> {
                final PreparedStatement stmt = enclosure.getCachedStatement(conn);
                JdbcCommon.setParameters(stmt, flowFile.getAttributes());
                stmt.addBatch();
            }, onFlowFileError(context, session, result))) {
                continue;
            }

            enclosure.addFlowFile(flowFile);
        }
    };

    private final GroupingFunction groupFlowFilesBySQL = (context, session, fc, conn, flowFiles, groups, result) -> {
        for (final FlowFile flowFile : flowFiles) {
            final String sql = context.getProperty(EnhancedPutSQL.SQL_STATEMENT).isSet()
                    ? context.getProperty(EnhancedPutSQL.SQL_STATEMENT).evaluateAttributeExpressions(flowFile).getValue()
                    : getSQL(session, flowFile);

            // Create a new PreparedStatement or reuse the one from the last group if that is the same.
            final StatementFlowFileEnclosure enclosure;
            final StatementFlowFileEnclosure lastEnclosure = groups.isEmpty() ? null : groups.get(groups.size() - 1);

            if (lastEnclosure == null || !lastEnclosure.getSql().equals(sql)) {
                enclosure = new StatementFlowFileEnclosure(sql);
                groups.add(enclosure);
            } else {
                enclosure = lastEnclosure;
            }

            enclosure.addFlowFile(flowFile);
        }
    };

    final PutGroup.GroupFlowFiles<FunctionContext, Connection, StatementFlowFileEnclosure> groupFlowFiles = (context, session, fc, conn, flowFiles, result) -> {
        final List<StatementFlowFileEnclosure> groups = new ArrayList<>();

        // There are three patterns:
        // 1. Support batching: An enclosure has multiple FlowFiles being executed in a batch operation
        // 2. Obtain keys: An enclosure has multiple FlowFiles, and each FlowFile is executed separately
        // 3. Fragmented transaction: One FlowFile per Enclosure?
        if (fc.obtainKeys) {
            groupFlowFilesBySQL.apply(context, session, fc, conn, flowFiles, groups, result);
        } else if (fc.fragmentedTransaction) {
            groupFragmentedTransaction.apply(context, session, fc, conn, flowFiles, groups, result);
        } else {
            groupFlowFilesBySQLBatch.apply(context, session, fc, conn, flowFiles, groups, result);
        }

        return groups;
    };

    final PutGroup.PutFlowFiles<FunctionContext, Connection, StatementFlowFileEnclosure> putFlowFiles = (context, session, fc, conn, enclosure, result) -> {

        final List<FlowFile> sentFlowFiles = new ArrayList<>();

        if (fc.isSupportBatching()) {

            // We have PreparedStatement that have batches added to them.
            // We need to execute each batch and close the PreparedStatement.
            exceptionHandler.execute(fc, enclosure, input -> {
                try (final PreparedStatement stmt = enclosure.getCachedStatement(conn)) {
                    stmt.executeBatch();
                    sentFlowFiles.addAll(enclosure.getFlowFiles());
                    result.routeTo(enclosure.getFlowFiles(), REL_SUCCESS);
                }
            }, onBatchUpdateError(context, session, result));

        } else {
            for (final FlowFile flowFile : enclosure.getFlowFiles()) {

                final StatementFlowFileEnclosure targetEnclosure
                        = enclosure instanceof FragmentedEnclosure
                        ? ((FragmentedEnclosure) enclosure).getTargetEnclosure(flowFile)
                        : enclosure;

                // Execute update one by one.
                exceptionHandler.execute(fc, flowFile, input -> {
                    try (final PreparedStatement stmt = targetEnclosure.getNewStatement(conn, fc.obtainKeys)) {

                        // set the appropriate parameters on the statement.
                        JdbcCommon.setParameters(stmt, flowFile.getAttributes());

                        stmt.executeUpdate();

                        // attempt to determine the key that was generated, if any. This is not supported by all
                        // database vendors, so if we cannot determine the generated key (or if the statement is not an INSERT),
                        // we will just move on without setting the attribute.
                        FlowFile sentFlowFile = flowFile;
                        final String generatedKey = determineGeneratedKey(stmt);
                        if (generatedKey != null) {
                            sentFlowFile = session.putAttribute(sentFlowFile, "sql.generated.key", generatedKey);
                        }

                        sentFlowFiles.add(sentFlowFile);
                        result.routeTo(sentFlowFile, REL_SUCCESS);

                    }
                }, onFlowFileError(context, session, result));
            }
        }

        if (!sentFlowFiles.isEmpty()) {
            // Determine the database URL
            String url = "jdbc://unknown-host";
            try {
                url = conn.getMetaData().getURL();
            } catch (final SQLException ignored) {
            }

            // Emit a Provenance SEND event
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fc.startNanos);
            for (final FlowFile flowFile : sentFlowFiles) {
                session.getProvenanceReporter().send(flowFile, url, transmissionMillis, true);
            }
        }
    };

    private ExceptionHandler.OnError<FunctionContext, FlowFile> onFlowFileError(final ProcessContext context, final ProcessSession session, final RoutingResult result) {
        ExceptionHandler.OnError<FunctionContext, FlowFile> onFlowFileError = createOnError(context, session, result, REL_FAILURE, REL_RETRY);
        onFlowFileError = onFlowFileError.andThen((ctx, flowFile, errorTypesResult, exception) -> {

            switch (errorTypesResult.destination()) {
                case Failure:
                    getLogger().error("Failed to update database for {} due to {}; routing to failure", flowFile, exception, exception);
                    addErrorAttributesToFlowFile(session, flowFile, exception);
                    break;
                case Retry:
                    getLogger().error("Failed to update database for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry",
                           flowFile, exception, exception);
                    addErrorAttributesToFlowFile(session, flowFile, exception);
                    break;
                case Self:
                    getLogger().error("Failed to update database for {} due to {};",  flowFile, exception, exception);
                    break;
            }
        });
        return RollbackOnFailure.createOnError(onFlowFileError);
    }

    private ExceptionHandler.OnError<RollbackOnFailure, FlowFileGroup> onGroupError(final ProcessContext context, final ProcessSession session, final RoutingResult result) {
        ExceptionHandler.OnError<RollbackOnFailure, FlowFileGroup> onGroupError =
                ExceptionHandler.createOnGroupError(context, session, result, REL_FAILURE, REL_RETRY);

        onGroupError = onGroupError.andThen((ctx, flowFileGroup, errorTypesResult, exception) -> {
            switch (errorTypesResult.destination()) {
                case Failure:
                    List<FlowFile> flowFilesToFailure = getFlowFilesOnRelationship(result, REL_FAILURE);
                    result.getRoutedFlowFiles().put(REL_FAILURE, addErrorAttributesToFlowFilesInGroup(session, flowFilesToFailure, flowFileGroup.getFlowFiles(), exception));
                    break;
                case Retry:
                    List<FlowFile> flowFilesToRetry = getFlowFilesOnRelationship(result, REL_RETRY);
                    result.getRoutedFlowFiles().put(REL_RETRY, addErrorAttributesToFlowFilesInGroup(session, flowFilesToRetry, flowFileGroup.getFlowFiles(), exception));
                    break;
            }
        });

        return onGroupError;
    }

    private List<FlowFile> getFlowFilesOnRelationship(RoutingResult result, final Relationship relationship) {
        return Optional.ofNullable(result.getRoutedFlowFiles().get(relationship))
                .orElse(emptyList());
    }

    private List<FlowFile> addErrorAttributesToFlowFilesInGroup(ProcessSession session, List<FlowFile> flowFilesOnRelationship, List<FlowFile> flowFilesInGroup, Exception exception) {
        return flowFilesOnRelationship.stream()
                    .map(ff ->  flowFilesInGroup.contains(ff) ? addErrorAttributesToFlowFile(session, ff, exception) : ff)
                    .collect(toList());
    }

    private ExceptionHandler.OnError<FunctionContext, StatementFlowFileEnclosure> onBatchUpdateError(
            final ProcessContext context, final ProcessSession session, final RoutingResult result) {
        return RollbackOnFailure.createOnError((c, enclosure, r, e) -> {

            // If rollbackOnFailure is enabled, the error will be thrown as ProcessException instead.
            if (e instanceof BatchUpdateException && !c.isRollbackOnFailure()) {

                // If we get a BatchUpdateException, then we want to determine which FlowFile caused the failure,
                // and route that FlowFile to failure while routing those that finished processing to success and those
                // that have not yet been executed to retry.
                // Currently fragmented transaction does not use batch update.
                final int[] updateCounts = ((BatchUpdateException) e).getUpdateCounts();
                final List<FlowFile> batchFlowFiles = enclosure.getFlowFiles();

                // In the presence of a BatchUpdateException, the driver has the option of either stopping when an error
                // occurs, or continuing. If it continues, then it must account for all statements in the batch and for
                // those that fail return a Statement.EXECUTE_FAILED for the number of rows updated.
                // So we will iterate over all of the update counts returned. If any is equal to Statement.EXECUTE_FAILED,
                // we will route the corresponding FlowFile to failure. Otherwise, the FlowFile will go to success
                // unless it has not yet been processed (its index in the List > updateCounts.length).
                int failureCount = 0;
                int successCount = 0;
                int retryCount = 0;
                for (int i = 0; i < updateCounts.length; i++) {
                    final int updateCount = updateCounts[i];
                    final FlowFile flowFile = batchFlowFiles.get(i);
                    if (updateCount == Statement.EXECUTE_FAILED) {
                        result.routeTo(addErrorAttributesToFlowFile(session, flowFile, e), REL_FAILURE);
                        failureCount++;
                    } else {
                        result.routeTo(flowFile, REL_SUCCESS);
                        successCount++;
                    }
                }

                if (failureCount == 0) {
                    // if no failures found, the driver decided not to execute the statements after the
                    // failure, so route the last one to failure.
                    final FlowFile failedFlowFile = batchFlowFiles.get(updateCounts.length);
                    result.routeTo(addErrorAttributesToFlowFile(session, failedFlowFile, e), REL_FAILURE);
                    failureCount++;
                }

                if (updateCounts.length < batchFlowFiles.size()) {
                    final List<FlowFile> unexecuted = batchFlowFiles.subList(updateCounts.length + 1, batchFlowFiles.size());
                    for (final FlowFile flowFile : unexecuted) {
                        result.routeTo(flowFile, REL_RETRY);
                        retryCount++;
                    }
                }

                getLogger().error("Failed to update database due to a failed batch update, {}. There were a total of {} FlowFiles that failed, {} that succeeded, "
                        + "and {} that were not execute and will be routed to retry; ", e, failureCount, successCount, retryCount, e);

                return;

            }

            // Apply default error handling and logging for other Exceptions.
            ExceptionHandler.OnError<RollbackOnFailure, FlowFileGroup> onGroupError = onGroupError(context, session, result);
            onGroupError = onGroupError.andThen((cl, il, rl, el) -> {
                switch (r.destination()) {
                    case Failure:
                        getLogger().error("Failed to update database for {} due to {}; routing to failure", il.getFlowFiles(), e, e);
                        break;
                    case Retry:
                        getLogger().error("Failed to update database for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry",
                                il.getFlowFiles(), e, e);
                        break;
                }
            });
            onGroupError.apply(c, enclosure, r, e);
        });
    }

    @OnScheduled
    public void constructProcess() {
        process = new PutGroup<>();

        process.setLogger(getLogger());
        process.fetchFlowFiles(fetchFlowFiles);
        process.initConnection(initConnection);
        process.groupFetchedFlowFiles(groupFlowFiles);
        process.putFlowFiles(putFlowFiles);
        process.adjustRoute(RollbackOnFailure.createAdjustRoute(REL_FAILURE, REL_RETRY));

        process.onCompleted((c, s, fc, conn) -> {
            try {
                // Only call commit() if auto-commit is false, per the JDBC spec (see java.sql.Connection)
                if (!conn.getAutoCommit()) {
                    conn.commit();
                }
            } catch (SQLException e) {
                // Throw ProcessException to rollback process session.
                throw new ProcessException("Failed to commit database connection due to " + e, e);
            }
        });

        process.onFailed((c, s, fc, conn, e) -> {
            try {
                // Only call rollback() if auto-commit is false, per the JDBC spec (see java.sql.Connection)
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (SQLException re) {
                // Just log the fact that rollback failed.
                // ProcessSession will be rollback by the thrown Exception so don't have to do anything here.
                getLogger().warn("Failed to rollback database connection due to {}", re, re);
            }
        });

        process.cleanup((c, s, fc, conn) -> {
            // make sure that we try to set the auto commit back to whatever it was.
            final boolean autocommit = c.getProperty(AUTO_COMMIT).asBoolean();
            if (fc.originalAutoCommit != autocommit) {
                try {
                    conn.setAutoCommit(fc.originalAutoCommit);
                } catch (final SQLException se) {
                    getLogger().warn("Failed to reset autocommit due to {}", se);
                }
            }
        });

        process.adjustFailed((c, r) -> {
            if (c.getProperty(SUPPORT_TRANSACTIONS).asBoolean()) {
                if (r.contains(REL_RETRY) || r.contains(REL_FAILURE)) {
                    final List<FlowFile> transferredFlowFiles = r.getRoutedFlowFiles().values().stream()
                            .flatMap(List::stream).collect(toList());

                    Relationship rerouteShip = r.contains(REL_RETRY) ? REL_RETRY : REL_FAILURE;
                    r.getRoutedFlowFiles().clear();
                    r.routeTo(transferredFlowFiles, rerouteShip);
                    return true;
                }
            }
            return false;
        });

        exceptionHandler = new ExceptionHandler<>();
        exceptionHandler.mapException(e -> {
            if (e instanceof SQLNonTransientException) {
                return ErrorTypes.InvalidInput;
            } else if (e instanceof SQLException) {
                return ErrorTypes.TemporalFailure;
            } else {
                return ErrorTypes.UnknownFailure;
            }
        });
        adjustError = RollbackOnFailure.createAdjustError(getLogger());
        exceptionHandler.adjustError(adjustError);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        // Step 1: Check master switch
        boolean validationEnabled = context.getProperty(ENABLE_VALIDATION).asBoolean();
        
        if (!validationEnabled) {
            // No validation - proceed with standard PutSQL using RollbackOnFailure pattern
            final Boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
            final FunctionContext functionContext = new FunctionContext(rollbackOnFailure);
            functionContext.obtainKeys = context.getProperty(OBTAIN_GENERATED_KEYS).asBoolean();
            RollbackOnFailure.onTrigger(context, sessionFactory, functionContext, getLogger(), 
                session -> process.onTrigger(context, session, functionContext));
            return;
        }
        
        // Validation enabled - perform validation first, then execute SQL
        final Boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        final FunctionContext functionContext = new FunctionContext(rollbackOnFailure);
        functionContext.obtainKeys = context.getProperty(OBTAIN_GENERATED_KEYS).asBoolean();
        
        RollbackOnFailure.onTrigger(context, sessionFactory, functionContext, getLogger(), session -> {
            FlowFile flowFile = session.get();
            if (flowFile == null) {
                return;
            }
            
            // Step 2: Validate required properties
            String operationId = context.getProperty(OPERATION_ID)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
            
            if (operationId == null || operationId.trim().isEmpty()) {
                flowFile = session.putAttribute(flowFile, "validation.error", 
                    "Validation enabled but operation-id property is not set");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            
            // Get DBCP services (cast from ControllerService to avoid NoClassDefFoundError)
            ControllerService engineService = context.getProperty(ENGINE_DBCP_SERVICE).asControllerService();
            ControllerService sourceService = context.getProperty(SOURCE_DBCP_SERVICE).asControllerService();
            ControllerService targetService = context.getProperty(TARGET_DBCP_SERVICE).asControllerService();
            
            // Safely cast to DBCPService using helper method
            Object engineDbcp = asDBCPService(engineService);
            Object sourceDbcp = asDBCPService(sourceService);
            Object targetDbcp = asDBCPService(targetService);
            
            if (engineDbcp == null || sourceDbcp == null || targetDbcp == null) {
                flowFile = session.putAttribute(flowFile, "validation.error", 
                    "Validation enabled but required DBCP services are not configured");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            
            // Step 4: Perform validation
            try {
                ValidationResultData validationResult = performEnvironmentValidation(
                    context, flowFile, engineDbcp, sourceDbcp, targetDbcp, operationId);
                
                if (!validationResult.isValid()) {
                    String validationMode = context.getProperty(VALIDATION_MODE).getValue();
                    
                    if ("STRICT".equals(validationMode)) {
                        // BLOCK execution - route to validation_failed
                        flowFile = session.putAttribute(flowFile, "validation.passed", "false");
                        flowFile = session.putAttribute(flowFile, "validation.error", 
                            validationResult.getErrorMessage());
                        flowFile = session.putAttribute(flowFile, "validation.timestamp", 
                            String.valueOf(System.currentTimeMillis()));
                        session.transfer(flowFile, REL_VALIDATION_FAILED);
                        return; // CRITICAL: Don't call process.onTrigger()
                    } else {
                        // WARNING mode: Log but continue
                        getLogger().warn("Environment validation warning: {} - Proceeding anyway", 
                            validationResult.getErrorMessage());
                    }
                } else {
                    // Validation passed - add success attributes
                    flowFile = session.putAttribute(flowFile, "validation.passed", "true");
                    flowFile = session.putAttribute(flowFile, "validation.environment.id", 
                        String.valueOf(validationResult.getEnvironmentId()));
                    flowFile = session.putAttribute(flowFile, "validation.environment.name", 
                        validationResult.getEnvironmentName());
                    flowFile = session.putAttribute(flowFile, "validation.timestamp", 
                        String.valueOf(System.currentTimeMillis()));
                }
            } catch (Exception e) {
                // If validation itself fails, route to failure
                getLogger().error("Failed to perform validation: {}", e.getMessage(), e);
                flowFile = session.putAttribute(flowFile, "validation.passed", "false");
                flowFile = session.putAttribute(flowFile, "validation.error", 
                    "Validation failed with exception: " + e.getMessage());
                flowFile = session.putAttribute(flowFile, "validation.timestamp", 
                    String.valueOf(System.currentTimeMillis()));
                String validationMode = context.getProperty(VALIDATION_MODE).getValue();
                if ("STRICT".equals(validationMode)) {
                    session.transfer(flowFile, REL_VALIDATION_FAILED);
                    return;
                } else {
                    getLogger().warn("Validation exception in WARNING mode - proceeding anyway: {}", e.getMessage());
                }
            }
            
            // Step 5: Proceed with standard PutSQL execution
            // Note: Uses RollbackOnFailure pattern and PutGroup for batch processing
            process.onTrigger(context, session, functionContext);
        });
    }

    /**
     * Pulls a batch of FlowFiles from the incoming queues. If no FlowFiles are available, returns <code>null</code>.
     * Otherwise, a List of FlowFiles will be returned.
     * <p>
     * If all FlowFiles pulled are not eligible to be processed, the FlowFiles will be penalized and transferred back
     * to the input queue and an empty List will be returned.
     * <p>
     * Otherwise, if the Support Fragmented Transactions property is true, all FlowFiles that belong to the same
     * transaction will be sorted in the order that they should be evaluated.
     *
     * @param context the process context for determining properties
     * @param session the process session for pulling FlowFiles
     * @return a FlowFilePoll containing a List of FlowFiles to process, or <code>null</code> if there are no FlowFiles to process
     */
    private FlowFilePoll pollFlowFiles(final ProcessContext context, final ProcessSession session,
                                       final FunctionContext functionContext, final RoutingResult result) {
        // Determine which FlowFile Filter to use in order to obtain FlowFiles.
        final boolean useTransactions = context.getProperty(SUPPORT_TRANSACTIONS).asBoolean();
        boolean fragmentedTransaction = false;

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final ControllerService service = context.getProperty(CONNECTION_POOL).asControllerService();
        final Object dbcpService = asDBCPService(service);
        if (dbcpService == null) {
            throw new ProcessException("DBCP Service class not available. Ensure nifi-dbcp-service-api is available in NiFi runtime.");
        }
        FlowFileFilter dbcpServiceFlowFileFilter;
        try {
            dbcpServiceFlowFileFilter = getFlowFileFilterFromDBCPService(dbcpService, batchSize);
        } catch (Exception e) {
            throw new ProcessException("Failed to get FlowFileFilter from DBCP service: " + e.getMessage(), e);
        }
        final List<FlowFile> selectedFlowFiles;
        if (useTransactions) {
            final TransactionalFlowFileFilter filter = new TransactionalFlowFileFilter(dbcpServiceFlowFileFilter);
            selectedFlowFiles = session.get(filter);
            fragmentedTransaction = filter.isFragmentedTransaction();
        } else {
            if (dbcpServiceFlowFileFilter == null) {
                selectedFlowFiles = session.get(batchSize);
            } else {
                selectedFlowFiles = session.get(dbcpServiceFlowFileFilter);
            }
        }

        final List<FlowFile> validFlowFiles;
        // The ConnectionPool service has a FlowFileFilter when, and only when it is a DBCPConnectionPoolLookup.
        // As such, it requires incoming FileFiles to have a 'database.name' attribute
        if (
                dbcpServiceFlowFileFilter != null
                        && !selectedFlowFiles.isEmpty()
                        // The filtered FlowFiles are homogenous. If one lacks 'database.name', all do.
                        && selectedFlowFiles.get(0).getAttribute("database.name") == null
        ) {
            selectedFlowFiles.forEach(flowFile -> getLogger().warn(
                    "{} missing attribute named [database.name] Routing to [{}]",
                    flowFile,
                    REL_FAILURE.getName()
            ));

            result.routeTo(selectedFlowFiles, REL_FAILURE);
            validFlowFiles = List.of();
        } else {
            validFlowFiles = selectedFlowFiles;
        }

        if (validFlowFiles.isEmpty()) {
            return null;
        }

        // If we are supporting fragmented transactions, verify that all FlowFiles are correct
        if (fragmentedTransaction) {
            try {
                if (!isFragmentedTransactionReady(validFlowFiles, context.getProperty(TRANSACTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS))) {
                    // Not ready, penalize FlowFiles and put it back to self.
                    validFlowFiles.forEach(f -> result.routeTo(session.penalize(f), Relationship.SELF));
                    return null;
                }

            } catch (IllegalArgumentException e) {
                // Map relationship based on context, and then let default handler to handle.
                final ErrorTypes.Result adjustedRoute = adjustError.apply(functionContext, ErrorTypes.InvalidInput);
                onGroupError(context, session, result)
                        .apply(functionContext, () -> validFlowFiles, adjustedRoute, e);

                return null;
            }

            // sort by fragment index.
            validFlowFiles.sort(Comparator.comparing(o -> Integer.parseInt(o.getAttribute(FRAGMENT_INDEX_ATTR))));
        }

        return new FlowFilePoll(validFlowFiles, fragmentedTransaction);
    }


    /**
     * Returns the key that was generated from the given statement, or <code>null</code> if no key
     * was generated or it could not be determined.
     *
     * @param stmt the statement that generated a key
     * @return the key that was generated from the given statement, or <code>null</code> if no key
     *         was generated, or it could not be determined.
     */
    private String determineGeneratedKey(final PreparedStatement stmt) {
        try {
            final ResultSet generatedKeys = stmt.getGeneratedKeys();
            if (generatedKeys != null && generatedKeys.next()) {
                return generatedKeys.getString(1);
            }
        } catch (final SQLException ignored) {
            // This is not supported by all vendors. This is a best-effort approach.
        }

        return null;
    }

    /**
     * Determines the SQL statement that should be executed for the given FlowFile
     *
     * @param session the session that can be used to access the given FlowFile
     * @param flowFile the FlowFile whose SQL statement should be executed
     *
     * @return the SQL that is associated with the given FlowFile
     */
    private String getSQL(final ProcessSession session, final FlowFile flowFile) {
        // Read the SQL from the FlowFile's content
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));

        // Create the PreparedStatement to use for this FlowFile.
        return new String(buffer, StandardCharsets.UTF_8);
    }

    /**
     * Determines which relationship the given FlowFiles should go to, based on a transaction timing out or
     * transaction information not being present. If the FlowFiles should be processed and not transferred
     * to any particular relationship yet, will return <code>null</code>
     *
     * @param flowFiles the FlowFiles whose relationship is to be determined
     * @param transactionTimeoutMillis the maximum amount of time (in milliseconds) that we should wait
     *            for all FlowFiles in a transaction to be present before routing to failure
     * @return the appropriate relationship to route the FlowFiles to, or <code>null</code> if the FlowFiles
     *         should instead be processed
     */
    boolean isFragmentedTransactionReady(final List<FlowFile> flowFiles, final Long transactionTimeoutMillis) throws IllegalArgumentException {
        int selectedNumFragments = 0;
        final BitSet bitSet = new BitSet();

        BiFunction<String, Object[], IllegalArgumentException> illegal = (s, objects) -> new IllegalArgumentException(format(s, objects));

        for (final FlowFile flowFile : flowFiles) {
            final String fragmentCount = flowFile.getAttribute(FRAGMENT_COUNT_ATTR);
            if (fragmentCount == null && flowFiles.size() == 1) {
                return true;
            } else if (fragmentCount == null) {
                throw illegal.apply("Cannot process %s because there are %d FlowFiles with the same fragment.identifier "
                        + "attribute but not all FlowFiles have a fragment.count attribute", new Object[] {flowFile, flowFiles.size()});
            }

            final int numFragments;
            try {
                numFragments = Integer.parseInt(fragmentCount);
            } catch (final NumberFormatException nfe) {
                throw illegal.apply("Cannot process %s because the fragment.count attribute has a value of '%s', which is not an integer",
                        new Object[] {flowFile, fragmentCount});
            }

            if (numFragments < 1) {
                throw illegal.apply("Cannot process %s because the fragment.count attribute has a value of '%s', which is not a positive integer",
                        new Object[] {flowFile, fragmentCount});
            }

            if (selectedNumFragments == 0) {
                selectedNumFragments = numFragments;
            } else if (numFragments != selectedNumFragments) {
                throw illegal.apply("Cannot process %s because the fragment.count attribute has different values for different FlowFiles with the same fragment.identifier",
                        new Object[] {flowFile});
            }

            final String fragmentIndex = flowFile.getAttribute(FRAGMENT_INDEX_ATTR);
            if (fragmentIndex == null) {
                throw illegal.apply("Cannot process %s because the fragment.index attribute is missing", new Object[] {flowFile});
            }

            final int idx;
            try {
                idx = Integer.parseInt(fragmentIndex);
            } catch (final NumberFormatException nfe) {
                throw illegal.apply("Cannot process %s because the fragment.index attribute has a value of '%s', which is not an integer",
                        new Object[] {flowFile, fragmentIndex});
            }

            if (idx < 0) {
                throw illegal.apply("Cannot process %s because the fragment.index attribute has a value of '%s', which is not a positive integer",
                        new Object[] {flowFile, fragmentIndex});
            }

            if (bitSet.get(idx)) {
                throw illegal.apply("Cannot process %s because it has the same value for the fragment.index attribute as another FlowFile with the same fragment.identifier",
                        new Object[] {flowFile});
            }

            bitSet.set(idx);
        }

        if (selectedNumFragments == flowFiles.size()) {
            return true; // no relationship to route FlowFiles to yet - process the FlowFiles.
        }

        long latestQueueTime = 0L;
        for (final FlowFile flowFile : flowFiles) {
            if (flowFile.getLastQueueDate() != null && flowFile.getLastQueueDate() > latestQueueTime) {
                latestQueueTime = flowFile.getLastQueueDate();
            }
        }

        if (transactionTimeoutMillis != null) {
            if (latestQueueTime > 0L && System.currentTimeMillis() - latestQueueTime > transactionTimeoutMillis) {
                throw illegal.apply("The transaction timeout has expired for the following FlowFiles; they will be routed to failure: %s", new Object[] {flowFiles});
            }
        }

        getLogger().debug("Not enough FlowFiles for transaction. Returning all FlowFiles to queue");
        return false;  // not enough FlowFiles for this transaction. Return them all to queue.
    }

    private FlowFile addErrorAttributesToFlowFile(final ProcessSession session, FlowFile flowFile, final Exception exception) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ERROR_MESSAGE_ATTR, exception.getMessage());

        if (exception instanceof SQLException) {
            int errorCode = ((SQLException) exception).getErrorCode();
            String sqlState = ((SQLException) exception).getSQLState();

            if (errorCode > 0) {
                attributes.put(ERROR_CODE_ATTR, valueOf(errorCode));
            }

            if (sqlState != null) {
                attributes.put(ERROR_SQL_STATE_ATTR, sqlState);
            }
        }

        return session.putAllAttributes(flowFile, attributes);
    }

    /**
     * A FlowFileFilter that is responsible for ensuring that the FlowFiles returned either belong
     * to the same "fragmented transaction" (i.e., 1 transaction whose information is fragmented
     * across multiple FlowFiles) or that none of the FlowFiles belongs to a fragmented transaction
     */
    static class TransactionalFlowFileFilter implements FlowFileFilter {
        private final FlowFileFilter nonFragmentedTransactionFilter;
        private String selectedId = null;
        private int numSelected = 0;
        private boolean ignoreFragmentIdentifiers = false;

        public TransactionalFlowFileFilter(FlowFileFilter nonFragmentedTransactionFilter) {
            this.nonFragmentedTransactionFilter = nonFragmentedTransactionFilter;
        }

        public boolean isFragmentedTransaction() {
            return !ignoreFragmentIdentifiers;
        }

        private FlowFileFilterResult filterNonFragmentedTransaction(final FlowFile flowFile) {
            if (nonFragmentedTransactionFilter == null) {
                return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
            } else {
                // Use non-fragmented tx filter for further filtering.
                return nonFragmentedTransactionFilter.filter(flowFile);
            }
        }

        @Override
        public FlowFileFilterResult filter(final FlowFile flowFile) {
            final String fragmentId = flowFile.getAttribute(FRAGMENT_ID_ATTR);
            final String fragCount = flowFile.getAttribute(FRAGMENT_COUNT_ATTR);

            // if first FlowFile selected is not part of a fragmented transaction, then
            // we accept any FlowFile that is also not part of a fragmented transaction.
            if (ignoreFragmentIdentifiers) {
                if (fragmentId == null || "1".equals(fragCount)) {
                    return filterNonFragmentedTransaction(flowFile);
                } else {
                    return FlowFileFilterResult.REJECT_AND_CONTINUE;
                }
            }

            if (fragmentId == null || "1".equals(fragCount)) {
                if (selectedId == null) {
                    // Only one FlowFile in the transaction.
                    ignoreFragmentIdentifiers = true;
                    return filterNonFragmentedTransaction(flowFile);
                } else {
                    // we've already selected 1 FlowFile, and this one doesn't match.
                    return FlowFileFilterResult.REJECT_AND_CONTINUE;
                }
            }

            if (selectedId == null) {
                // select this fragment id as the chosen one.
                selectedId = fragmentId;
                numSelected++;
                return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
            }

            if (selectedId.equals(fragmentId)) {
                // fragment id's match. Find out if we have all of the necessary fragments or not.
                final int numFragments;
                if (fragCount != null && JdbcCommon.NUMBER_PATTERN.matcher(fragCount).matches()) {
                    numFragments = Integer.parseInt(fragCount);
                } else {
                    numFragments = Integer.MAX_VALUE;
                }

                if (numSelected >= numFragments - 1) {
                    // We have all of the fragments we need for this transaction.
                    return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                } else {
                    // We still need more fragments for this transaction, so accept this one and continue.
                    numSelected++;
                    return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                }
            } else {
                return FlowFileFilterResult.REJECT_AND_CONTINUE;
            }
        }
    }


    /**
     * A simple, immutable data structure to hold a List of FlowFiles and an indicator as to whether
     * or not those FlowFiles represent a "fragmented transaction" - that is, a collection of FlowFiles
     * that all must be executed as a single transaction (we refer to it as a fragment transaction
     * because the information for that transaction, including SQL and the parameters, is fragmented
     * across multiple FlowFiles).
     */
    private static class FlowFilePoll {
        private final List<FlowFile> flowFiles;
        private final boolean fragmentedTransaction;

        public FlowFilePoll(final List<FlowFile> flowFiles, final boolean fragmentedTransaction) {
            this.flowFiles = flowFiles;
            this.fragmentedTransaction = fragmentedTransaction;
        }

        public List<FlowFile> getFlowFiles() {
            return flowFiles;
        }

        public boolean isFragmentedTransaction() {
            return fragmentedTransaction;
        }
    }


    private static class FragmentedEnclosure extends StatementFlowFileEnclosure {

        private final Map<FlowFile, StatementFlowFileEnclosure> flowFileToEnclosure = new HashMap<>();

        public FragmentedEnclosure() {
            super(null);
        }

        public void addFlowFile(final FlowFile flowFile, final StatementFlowFileEnclosure enclosure) {
            addFlowFile(flowFile);
            flowFileToEnclosure.put(flowFile, enclosure);
        }

        public StatementFlowFileEnclosure getTargetEnclosure(final FlowFile flowFile) {
            return flowFileToEnclosure.get(flowFile);
        }
    }

    /**
     * A simple, immutable data structure to hold a Prepared Statement and a List of FlowFiles
     * for which that statement should be evaluated.
     */
    private static class StatementFlowFileEnclosure implements FlowFileGroup {
        private final String sql;
        private PreparedStatement statement;
        private final List<FlowFile> flowFiles = new ArrayList<>();

        public StatementFlowFileEnclosure(String sql) {
            this.sql = sql;
        }

        public String getSql() {
            return sql;
        }

        public PreparedStatement getNewStatement(final Connection conn, final boolean obtainKeys) throws SQLException {
            if (obtainKeys) {
                // Create a new Prepared Statement, requesting that it return the generated keys.
                PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

                if (stmt == null) {
                    // since we are passing Statement.RETURN_GENERATED_KEYS, calls to conn.prepareStatement will
                    // in some cases (at least for DerbyDB) return null.
                    // We will attempt to recompile the statement without the generated keys being returned.
                    stmt = conn.prepareStatement(sql);
                }

                // If we need to obtain keys, then we cannot do a Batch Update. In this case,
                // we don't need to store the PreparedStatement in the Map because we aren't
                // doing an addBatch/executeBatch. Instead, we will use the statement once
                // and close it.
                return stmt;
            }

            return conn.prepareStatement(sql);
        }

        public PreparedStatement getCachedStatement(final Connection conn) throws SQLException {
            if (statement != null) {
                return statement;
            }

            statement = conn.prepareStatement(sql);
            return statement;
        }

        @Override
        public List<FlowFile> getFlowFiles() {
            return flowFiles;
        }

        public void addFlowFile(final FlowFile flowFile) {
            this.flowFiles.add(flowFile);
        }

        @Override
        public int hashCode() {
            return sql.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return false;
            }
            if (!(obj instanceof StatementFlowFileEnclosure)) {
                return false;
            }
            StatementFlowFileEnclosure other = (StatementFlowFileEnclosure) obj;

            return sql.equals(other.sql);
        }
    }

    // ========== Helper Methods ==========

    /**
     * Safely cast ControllerService to DBCPService using reflection
     * Returns Object to avoid NoClassDefFoundError when DBCPService class is not available
     */
    private Object asDBCPService(ControllerService service) {
        if (service == null) {
            return null;
        }
        try {
            // Use reflection to check if service implements DBCPService interface
            // This avoids NoClassDefFoundError when DBCPService class is not available
            Class<?> dbcpServiceClass = Class.forName("org.apache.nifi.dbcp.DBCPService");
            if (dbcpServiceClass.isInstance(service)) {
                return dbcpServiceClass.cast(service);
            }
            return null;
        } catch (ClassNotFoundException e) {
            // DBCPService interface not available - this is expected in some NiFi setups
            getLogger().debug("DBCPService interface not available: {}", e.getMessage());
            return null;
        } catch (NoClassDefFoundError e) {
            // DBCPService class not available at runtime
            getLogger().debug("DBCPService class not available: {}", e.getMessage());
            return null;
        } catch (Exception e) {
            getLogger().error("Unexpected error casting to DBCPService: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Get connection from DBCPService using reflection
     */
    private Connection getConnectionFromDBCPService(Object dbcpService, java.util.Map<String, String> attributes) throws Exception {
        if (dbcpService == null) {
            return null;
        }
        Class<?> dbcpServiceClass = dbcpService.getClass();
        java.lang.reflect.Method getConnectionMethod = dbcpServiceClass.getMethod("getConnection", java.util.Map.class);
        return (Connection) getConnectionMethod.invoke(dbcpService, attributes);
    }
    
    /**
     * Get FlowFileFilter from DBCPService using reflection
     */
    private FlowFileFilter getFlowFileFilterFromDBCPService(Object dbcpService, int batchSize) throws Exception {
        if (dbcpService == null) {
            return null;
        }
        Class<?> dbcpServiceClass = dbcpService.getClass();
        java.lang.reflect.Method getFlowFileFilterMethod = dbcpServiceClass.getMethod("getFlowFileFilter", int.class);
        return (FlowFileFilter) getFlowFileFilterMethod.invoke(dbcpService, batchSize);
    }

    // ========== Validation Helper Classes and Methods ==========

    /**
     * Validation result container
     */
    private static class ValidationResultData {
        private final boolean valid;
        private final String operationId;
        private final Long environmentId;
        private final String environmentName;
        private final String errorMessage;
        
        private ValidationResultData(boolean valid, String operationId, Long environmentId, String environmentName, String errorMessage) {
            this.valid = valid;
            this.operationId = operationId;
            this.environmentId = environmentId;
            this.environmentName = environmentName;
            this.errorMessage = errorMessage;
        }
        
        public static ValidationResultData success(String operationId, Long environmentId, String environmentName) {
            return new ValidationResultData(true, operationId, environmentId, environmentName, null);
        }
        
        public static ValidationResultData failure(String operationId, String errorMessage) {
            return new ValidationResultData(false, operationId, null, null, errorMessage);
        }
        
        public boolean isValid() {
            return valid;
        }
        
        public String getOperationId() {
            return operationId;
        }
        
        public Long getEnvironmentId() {
            return environmentId;
        }
        
        public String getEnvironmentName() {
            return environmentName;
        }
        
        public String getErrorMessage() {
            return errorMessage;
        }
    }

    /**
     * Environment configuration container
     */
    private static class EnvironmentConfig {
        Long environmentId;
        String environmentName;
        String sourceDbType, sourceHost, sourceDatabase;
        Integer sourcePort;
        Boolean sourceIsSid;  // Oracle SID flag
        String targetDbType, targetHost, targetDatabase;
        Integer targetPort;
        Boolean targetIsSid;  // Oracle SID flag
    }

    /**
     * Main validation method that performs environment validation.
     * This method orchestrates the entire validation process:
     * 1. Extract operation_id from FlowFile attributes
     * 2. Query SSE Engine database for environment configuration
     * 3. Extract JDBC URLs from controller services
     * 4. Compare actual URLs with expected URLs
     * 5. Return validation result
     */
    private ValidationResultData performEnvironmentValidation(ProcessContext context, FlowFile flowFile,
            Object engineDbcp, Object sourceDbcp, Object targetDbcp,
            String operationId) {
        
        // Step 1: Extract operation_id from FlowFile attributes
        if (operationId == null || operationId.trim().isEmpty()) {
            return ValidationResultData.failure(operationId, "operation_id is empty or not set");
        }
        
        getLogger().debug("Validating environment for operation_id: {}", operationId);
        
        // Step 2: Query environment configuration from SSE Engine database
        EnvironmentConfig envConfig;
        try {
            envConfig = queryEnvironmentConfig(engineDbcp, operationId);
        } catch (SQLException e) {
            return ValidationResultData.failure(operationId, 
                "Failed to query environment configuration: " + e.getMessage());
        }
        
        if (envConfig == null) {
            return ValidationResultData.failure(operationId, 
                "No environment configuration found for operation_id: " + operationId);
        }
        
        // Step 3: Extract JDBC URLs from controller services
        String sourceJdbcUrl;
        String targetJdbcUrl;
        try {
            sourceJdbcUrl = extractJdbcUrlFromService(sourceDbcp);
            targetJdbcUrl = extractJdbcUrlFromService(targetDbcp);
        } catch (SQLException e) {
            return ValidationResultData.failure(operationId, 
                "Failed to extract JDBC URLs from controller services: " + e.getMessage());
        }
        
        // Step 4: Build expected JDBC URLs from environment configuration
        String expectedSourceUrl = buildJdbcUrl(
            envConfig.sourceDbType,
            envConfig.sourceHost,
            envConfig.sourcePort,
            envConfig.sourceDatabase,
            envConfig.sourceIsSid != null ? envConfig.sourceIsSid : false
        );
        
        String expectedTargetUrl = buildJdbcUrl(
            envConfig.targetDbType,
            envConfig.targetHost,
            envConfig.targetPort,
            envConfig.targetDatabase,
            envConfig.targetIsSid != null ? envConfig.targetIsSid : false
        );
        
        // Log URLs for debugging (at debug level to avoid cluttering logs)
        getLogger().debug("Expected Source URL: {}", expectedSourceUrl);
        getLogger().debug("Actual Source URL: {}", sourceJdbcUrl);
        getLogger().debug("Expected Target URL: {}", expectedTargetUrl);
        getLogger().debug("Actual Target URL: {}", targetJdbcUrl);
        
        // Step 5: Compare URLs
        boolean sourceMatches = compareJdbcUrls(sourceJdbcUrl, expectedSourceUrl);
        boolean targetMatches = compareJdbcUrls(targetJdbcUrl, expectedTargetUrl);
        
        if (!sourceMatches || !targetMatches) {
            // Validation failed - create detailed error message matching SseExecuteSQL
            String errorMsg = format(
                "Controller service mismatch for environment '%s' (ID: %d). " +
                "Source match: %s, Target match: %s. " +
                "Expected: Source=%s, Target=%s. " +
                "Actual: Source=%s, Target=%s",
                envConfig.environmentName,
                envConfig.environmentId,
                sourceMatches,
                targetMatches,
                expectedSourceUrl,
                expectedTargetUrl,
                sourceJdbcUrl,
                targetJdbcUrl
            );
            return ValidationResultData.failure(operationId, errorMsg);
        }
        
        // Validation passed
        return ValidationResultData.success(operationId, envConfig.environmentId, envConfig.environmentName);
    }

    /**
     * Query environment configuration from SSE Engine database
     */
    private EnvironmentConfig queryEnvironmentConfig(Object engineDbcp, String operationId) 
            throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            try {
                conn = getConnectionFromDBCPService(engineDbcp, emptyMap());
            } catch (Exception e) {
                throw new SQLException("Failed to get connection from DBCP service: " + e.getMessage(), e);
            }
            
            // Query by operation ID - matches SseExecuteSQL query exactly
            String sql = "SELECT DISTINCT " +
                  "o.id as operation_id, " +
                  "o.environment_id, " +
                  "e.name as environment_name, " +
                  "e.status as environment_status, " +
                  "-- Source configuration details " +
                  "sc.id as source_config_id, " +
                  "sc.database_type as source_db_type, " +
                  "sc.host as source_host, " +
                  "sc.port as source_port, " +
                  "sc.db_name as source_database, " +
                  "sc.name as source_name, " +
                  "sc.is_sid as source_is_sid, " +
                  "-- Target configuration details " +
                  "tc.id as target_config_id, " +
                  "tc.database_type as target_db_type, " +
                  "tc.host as target_host, " +
                  "tc.port as target_port, " +
                  "tc.db_name as target_database, " +
                  "tc.name as target_name, " +
                  "tc.is_sid as target_is_sid " +
                  "FROM operations o " +
                  "INNER JOIN environment_configurations e ON o.environment_id = e.id " +
                  "INNER JOIN database_configurations sc ON e.source_config_id = sc.id AND sc.deleted = FALSE " +
                  "INNER JOIN database_configurations tc ON e.target_config_id = tc.id AND tc.deleted = FALSE " +
                  "WHERE o.id = ? " +
                  "LIMIT 1";
            
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, operationId);
            
            rs = stmt.executeQuery();
            
            if (!rs.next()) {
                return null;
            }
            
            EnvironmentConfig config = new EnvironmentConfig();
            
            // Environment details
            config.environmentId = rs.getLong("environment_id");
            config.environmentName = rs.getString("environment_name");
            
            // Source database configuration
            config.sourceDbType = rs.getString("source_db_type");
            config.sourceHost = rs.getString("source_host");
            config.sourcePort = rs.getInt("source_port");
            config.sourceDatabase = rs.getString("source_database");
            // Handle is_sid - can be null, so check for null and default to false
            int sourceIsSidInt = rs.getInt("source_is_sid");
            config.sourceIsSid = rs.wasNull() ? false : (sourceIsSidInt != 0);
            
            // Target database configuration
            config.targetDbType = rs.getString("target_db_type");
            config.targetHost = rs.getString("target_host");
            config.targetPort = rs.getInt("target_port");
            config.targetDatabase = rs.getString("target_database");
            // Handle is_sid - can be null, so check for null and default to false
            int targetIsSidInt = rs.getInt("target_is_sid");
            config.targetIsSid = rs.wasNull() ? false : (targetIsSidInt != 0);
            
            getLogger().info("Retrieved environment configuration: {} (ID: {})", 
                config.environmentName, config.environmentId);
            
            return config;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignored) {}
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ignored) {}
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ignored) {}
            }
        }
    }

    /**
     * Extracts JDBC URL from a DBCP service by getting connection metadata.
     * 
     * This method:
     * 1. Gets a connection from the DBCP service
     * 2. Gets the database metadata from the connection
     * 3. Extracts the URL from the metadata
     * 4. Closes the connection
     * 
     * @param dbcpService The DBCP service to extract URL from
     * @return The JDBC URL string
     * @throws SQLException if there's a database error
     */
    private String extractJdbcUrlFromService(Object dbcpService) throws SQLException {
        Connection conn = null;
        try {
            // Get a connection from the DBCP service
            conn = getConnectionFromDBCPService(dbcpService, emptyMap());
            
            // Get database metadata from the connection
            DatabaseMetaData metadata = conn.getMetaData();
            
            // Extract the URL from the metadata
            String url = metadata.getURL();
            return url;
        } catch (Exception e) {
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw new SQLException("Failed to get connection from DBCP service: " + e.getMessage(), e);
        } finally {
            // Always close the connection
            if (conn != null) {
                try { 
                    conn.close(); 
                } catch (SQLException e) { 
                    getLogger().debug("Error closing connection", e); 
                }
            }
        }
    }

    /**
     * Builds JDBC URL using the same logic as EnvironmentConnectionPoolController.
     * 
     * This method creates JDBC URLs in the standard format for different database types:
     * - Oracle: jdbc:oracle:thin:@host:port/database (service name) or jdbc:oracle:thin:@host:port:database (SID)
     * - PostgreSQL: jdbc:postgresql://host:port/database
     * - MySQL: jdbc:mysql://host:port/database
     * - SQL Server: jdbc:sqlserver://host:port;databaseName=database
     * 
     * @param dbType Database type (oracle, postgresql, mysql, sqlserver)
     * @param host Hostname or IP address
     * @param port Port number
     * @param database Database name
     * @param isSid Whether the database uses SID (Oracle only, true = use :, false = use /)
     * @return Formatted JDBC URL
     * @throws IllegalArgumentException if database type is not supported
     */
    private String buildJdbcUrl(String dbType, String host, int port, String database, boolean isSid) {
        // Build URL based on database type (Java 11 compatible)
        String baseUrl;
        String dbTypeLower = dbType.toLowerCase();
        
        if ("oracle".equals(dbTypeLower)) {
            // Oracle: use ":" for SID, "/" for service name
            if (isSid) {
                baseUrl = "jdbc:oracle:thin:@%s:%d:%s";
            } else {
                baseUrl = "jdbc:oracle:thin:@%s:%d/%s";
            }
        } else if ("postgresql".equals(dbTypeLower)) {
            baseUrl = "jdbc:postgresql://%s:%d/%s";
        } else if ("mysql".equals(dbTypeLower)) {
            baseUrl = "jdbc:mysql://%s:%d/%s";
        } else if ("sqlserver".equals(dbTypeLower)) {
            baseUrl = "jdbc:sqlserver://%s:%d;databaseName=%s";
        } else {
            throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
        
        // Format the URL with the provided parameters
        return format(baseUrl, host, port, database);
    }

    /**
     * Compare JDBC URLs (normalized, case-insensitive)
     */
    private boolean compareJdbcUrls(String url1, String url2) {
        if (url1 == null || url2 == null) {
            return false;
        }
        return normalizeJdbcUrl(url1).equalsIgnoreCase(normalizeJdbcUrl(url2));
    }

    /**
     * Normalizes JDBC URL for comparison by:
     * 1. Removing query parameters (everything after ?)
     * 2. Removing trailing slashes
     * 3. Trimming whitespace
     * 4. Converting to lowercase
     * 
     * This ensures that URLs like:
     * - "jdbc:postgresql://host:5432/db?param=value" 
     * - "jdbc:postgresql://host:5432/db/"
     * - "jdbc:postgresql://HOST:5432/DB"
     * 
     * All become: "jdbc:postgresql://host:5432/db"
     * 
     * @param jdbcUrl The JDBC URL to normalize
     * @return Normalized JDBC URL
     */
    private String normalizeJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return "";
        }
        
        // Remove query parameters (everything after ?)
        int queryIndex = jdbcUrl.indexOf('?');
        if (queryIndex > 0) {
            jdbcUrl = jdbcUrl.substring(0, queryIndex);
        }
        
        // Remove trailing slashes using regex
        jdbcUrl = jdbcUrl.replaceAll("/+$", "");
        
        // Normalize whitespace and convert to lowercase
        return jdbcUrl.trim().toLowerCase();
    }
}