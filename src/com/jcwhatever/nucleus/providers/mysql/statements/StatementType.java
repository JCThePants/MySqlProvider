package com.jcwhatever.nucleus.providers.mysql.statements;

/**
 * Types of sql statements.
 */
public enum StatementType {
    QUERY,
    UPDATE,
    TRANSACTION_START,
    TRANSACTION_COMMIT
}
