// errors --------------------------------------------------------------------

// Error classes - TODO - add these to errors.ts in @craigmcc/ts-database.

// External Modules ----------------------------------------------------------

import {
    DatabaseError,
} from "@craigmcc/ts-database";
import {Source} from "@craigmcc/ts-database/dist/errors";

// Specific Error Subclasses -------------------------------------------------

/**
 * The specified column does not exist.
 */
export class ColumnNotFoundError extends DatabaseError {
    constructor(source: Source, context?: string) {
        super(source, context);
    }
    error = "ColumnNotFoundError";
}

/**
 * The specified table does not exist.
 */
export class TableNotFoundError extends DatabaseError {
    constructor(source: Source, context?: string) {
        super(source, context);
    }
    error = "TableNotFoundError";
}

