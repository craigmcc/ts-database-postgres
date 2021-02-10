// ddl.test ------------------------------------------------------------------

// Data Definition Language (DDL) tests for @craigmcc/ts-database-postgres

// External Modules ----------------------------------------------------------

const chai = require("chai");
require("custom-env").env(true);
const expect = chai.expect;
import {
    ColumnAttributes,
    ColumnNotFoundError,
    DataObject,
    DataType,
    DuplicateColumnError,
    DuplicateIndexError,
    DuplicateTableError,
    IndexNotFoundError,
    TableAttributes,
    TableNotFoundError,
} from "@craigmcc/ts-database";

// Internal Modules ----------------------------------------------------------

import ConnectionImpl from "../ConnectionImpl";

const CONNECTION_URI = process.env.CONNECTION_URI ? process.env.CONNECTION_URI : "UNKNOWN";
const db = new ConnectionImpl(CONNECTION_URI);

const CHILD_COLUMNS: ColumnAttributes[] = [
    { name: "id", primaryKey: true, type: DataType.INTEGER, allowNull: false },
    { name: "test_id", type: DataType.INTEGER, allowNull: false }, // FK to TEST_TABLE
    { name: "comments", type: DataType.STRING, allowNull: true },
];

const CHILD_COLUMN = "test_id";
const CHILD_TABLE = "child_table";

const TEST_COLUMNS: ColumnAttributes[] = [
    { name: "id", primaryKey: true, type: DataType.INTEGER, allowNull: false },
    { name: "first_name", type: DataType.STRING, allowNull: false },
    { name: "last_name", type: DataType.STRING, allowNull: false },
    { name: "active", type: DataType.BOOLEAN, allowNull: false, defaultValue: "true" },
    { name: "age", type: DataType.SMALLINT, allowNull: true },
    { name: "comments", type: DataType.STRING, allowNull: true },
    { name: "created", type: DataType.DATETIME, allowNull: true },
    { name: "started_date", type: DataType.DATE, allowNull: true },
    { name: "started_time", type: DataType.TIME, allowNull: true },
];
const TEST_COLUMNS_ADD: ColumnAttributes[] = [
    { name: "first_add", type: DataType.BIGINT, allowNull: true },
    { name: "second_add", type: DataType.STRING, allowNull: false, defaultValue: "Second" },
];
const TEST_TABLE = "test_table";

const INSERT_ROWS: DataObject[] = [
    {
        first_name: "Barney",
        last_name: "Rubble",
        comments: "This is Barney",
    },
    {
        first_name: "Fred",
        last_name: "Flintstone",
        comments: "This is Fred",
    },
    {
        first_name: "Bam Bam",
        last_name: "Rubble",
        active: false,
    }
]

// Private Functions ---------------------------------------------------------

const doAfter = async () => {
    try {
        await db.disconnect();
        expect(db.connected).equals(false);
    } catch (error) {
        expect.fail(`doAfter/ddl: Should not have thrown '${error.message}'`);
    }
}

const doBefore = async () => {
    try {
        await db.connect();
        expect(db.connected).equals(true);
    } catch (error) {
        expect.fail(`doBefore/ddl: Should not have thrown '${error.message}'`);
    }
}

const doBeforeEachCreateChild = async () => {
    try {
        await db.dropTable(CHILD_TABLE, {ifExists: true});
    } catch (error) {
        // Ignore any errors from dropping table that might not be there
    }
    try {
        await db.addTable(CHILD_TABLE, CHILD_COLUMNS);
    } catch (error) {
        expect.fail(`beforeEachCreateChild/ddl: addTable should not have thrown '${error.message}'`);
    }
}

const doBeforeEachCreateTable = async () => {
    try {
        await db.dropTable(CHILD_TABLE, { ifExists: true });
    } catch (error) {
        // Ignore any errors from dropping the table that might not be there
    }
    try {
        await db.dropTable(TEST_TABLE, { ifExists: true });
    } catch (error) {
        // Ignore any errors from dropping the table that might not be there
    }
    try {
        await db.addTable(TEST_TABLE, TEST_COLUMNS);
    } catch (error) {
        expect.fail(`beforeEachCreateTable/ddl: addTable should not have thrown '${error.message}'`);
    }
}

const doBeforeEachDropChild = async () => {
    try {
        await db.dropTable(CHILD_TABLE, { ifExists: true });
    } catch (error) {
        // Ignore any errors from dropping the table that might not be there
    }
}

const doBeforeEachDropTable = async () => {
    try {
        await db.dropTable(TEST_TABLE, { ifExists: true });
    } catch (error) {
        // Ignore any errors from dropping the table that might not be there
    }
}

const doBeforeEachInsertRows = async () => {
    await doBeforeEachCreateTable();
    try {
        await db.insert(TEST_TABLE, INSERT_ROWS);
    } catch (error) {
        expect.fail(`beforeEachInsertRows/ddl: insert should not have thrown '${error.message}'`);
    }
}

// Test Suite ----------------------------------------------------------------

describe("ddlTests", () => {

    // Lifecycle Hooks -------------------------------------------------------

    before("before", async () => {
        await doBefore();
    })

    after("after", async () => {
        await doAfter();
    })

    // Test Modules ----------------------------------------------------------

    describe("addColumn", () => {

        beforeEach("beforeEach/addColumn", async () => {
            await doBeforeEachCreateTable();
            await doBeforeEachInsertRows();
        })

        it("should fail on duplicate column", async () => {
            try {
                await db.addColumn(TEST_TABLE, TEST_COLUMNS[2]);
                expect.fail("Should have thrown error for duplicate column");
            } catch (error) {
                if (error instanceof DuplicateColumnError) {
                    expect(error.message).includes(TEST_COLUMNS[2].name);
                } else {
                    expect.fail(`Should not have thrown ${error.message} (${error.code})`);
                }
            }
        })

        it("should pass on one valid column", async () => {
            await db.addColumn(TEST_TABLE, TEST_COLUMNS_ADD[0]);
        })

        it("should pass on two valid columns", async () => {
            await db.addColumn(TEST_TABLE, TEST_COLUMNS_ADD);
        })

    })

    describe("addForeignKey", () => {

        beforeEach("beforeEach/addForeignKey", async () => {
            await doBeforeEachCreateTable();
            await doBeforeEachCreateChild();
        })

        it("should fail on incorrect foreign column", async () => {
            const INCORRECT_COLUMN = "comments";
            try {
                await db.addForeignKey(CHILD_TABLE, CHILD_COLUMN, {
                    columnName: INCORRECT_COLUMN,
                    tableName: CHILD_TABLE,
                });
                expect.fail("Should have thrown error on incorrect column");
            } catch (error) {
                if (error.code && (error.code === "42830")) {
                    // expected result
                } else {
                    expect.fail(`Should not have thrown '${error.message}' (${error.code})`);
                }
            }
        })

        it("should fail on invalid foreign column", async () => {
            const INVALID_COLUMN = "invalid_column";
            try {
                await db.addForeignKey(CHILD_TABLE, CHILD_COLUMN, {
                    columnName: INVALID_COLUMN,
                    tableName: CHILD_TABLE,
                });
                expect.fail("Should have thrown error on invalid column");
            } catch (error) {
                if (error instanceof ColumnNotFoundError) {
                    expect(error.message).includes(INVALID_COLUMN);
                } else {
                    expect.fail(`Should not have thrown '${error.message}' (${error.code})`);
                }
            }
        })

        it("should fail on invalid foreign table", async() => {
            const INVALID_TABLE = "invalid_table";
            try {
                await db.addForeignKey(CHILD_TABLE, CHILD_COLUMN, {
                    columnName: "id",
                    tableName: INVALID_TABLE
                });
                expect.fail("Should have thrown error on invalid table");
            } catch (error) {
                if (error instanceof TableNotFoundError) {
                    expect(error.message).includes(INVALID_TABLE);
                } else {
                    expect.fail(`Should not have thrown '${error.message}' (${error.code})`);
                }
            }
        })

        it("should pass on valid parameters", async () => {
            try {
                const name = await db.addForeignKey(CHILD_TABLE, CHILD_COLUMN, {
                    columnName: "id",
                    tableName: TEST_TABLE,
                });
                expect(name).equals(CHILD_TABLE + "_" + CHILD_COLUMN + "_fkey");
            } catch (error) {
                expect.fail(`Should not have thrown '${error.message}'`);
            }
        })

    })

    describe("addIndex", () => {

        beforeEach("beforeEach/dropColumn", async () => {
            await doBeforeEachCreateTable();
        })

        it("should fail on duplicate index", async () => {
            try {
                const result = await db.addIndex(TEST_TABLE, {
                    columnName: ["last_name", "first_name"],
                    name: TEST_TABLE + "_last_name_first_name_index",
                });
            } catch (error) {
                expect.fail(`Should not have thrown '${error.message}'`);
            }
            try {
                const result = await db.addIndex(TEST_TABLE, {
                    columnName: ["last_name", "first_name"],
                    name: TEST_TABLE + "_last_name_first_name_index",
                });
                expect.fail("Should have thrown error on second index");
            } catch (error) {
                if (error instanceof DuplicateIndexError) {
                    expect(error.message).includes(TEST_TABLE);
                } else {
                    expect.fail(`Should not have thrown '${error.message}' (${error.code})`);
                }
            }
        })

        it("should pass on valid non-unique index", async () => {
            try {
                const result = await db.addIndex(TEST_TABLE, {
                    columnName: ["last_name", "first_name"],
                });
            } catch (error) {
                expect.fail(`Should not have thrown '${error.message}'`);
            }
        })

        it("should pass on valid unique index", async () => {
            try {
                const result = await db.addIndex(TEST_TABLE, {
                    columnName: ["last_name", "first_name"],
                    unique: true,
                });
            } catch (error) {
                expect.fail(`Should not have thrown '${error.message}'`);
            }
        })

    })

    describe("addTable", () => {

        beforeEach("beforeEach/addTable", async () => {
            await doBeforeEachDropTable();
        })

        it("should fail on duplicate table", async () => {
            await db.addTable(TEST_TABLE, TEST_COLUMNS);
            try {
                await db.addTable(TEST_TABLE, TEST_COLUMNS);
                expect.fail("Should have thrown error on duplicate table");
            } catch (error) {
                if (error instanceof DuplicateTableError) {
                    expect(error.message).includes(TEST_TABLE);
                } else {
                    expect.fail(`Should not have thrown ${error.message} (${error.code})`);
                }
            }
        })

        it("should pass on valid table", async () => {
            await db.addTable(TEST_TABLE, TEST_COLUMNS);
        })

    });

    describe("describeTable", () => {

        beforeEach("beforeEach/describeTable", async () => {
            await doBeforeEachCreateTable();
        })

        it("should fail on non-existing table", async () => {
            const NON_EXISTING_TABLE = "non_existing_table";
            try {
                await db.describeTable(NON_EXISTING_TABLE);
                expect.fail("Should have thrown error on missing table");
            } catch (error) {
                if (error instanceof TableNotFoundError) {
                    expect(error.message).includes(NON_EXISTING_TABLE);
                } else {
                    expect.fail(`Should not have thrown ${error.message} (${error.code})`);
                }
            }
        })

        it ("should pass on existing table", async () => {
            const tableAttributes = await db.describeTable(TEST_TABLE);
            expect(tableAttributes.name).equals(TEST_TABLE);
            TEST_COLUMNS.forEach(testColumn => {
                const tableColumn = lookupColumn(tableAttributes, testColumn.name);
                expect(tableColumn.allowNull).equals(testColumn.allowNull);
                // TODO - Ignoring autoIncrement
                if (testColumn.defaultValue !== undefined) {
                    expect(tableColumn.defaultValue).equals(testColumn.defaultValue);
                }
                if (testColumn.primaryKey !== undefined) {
                    expect(tableColumn.primaryKey).equals(testColumn.primaryKey);
                }
                expect(tableColumn.type).equals(testColumn.type);
            })
        })

        const lookupColumn
            = (tableAttributes: TableAttributes, columnName: string)
            : ColumnAttributes =>
        {
            let foundColumn: ColumnAttributes | null = null;
            tableAttributes.columns.forEach(tableColumn => {
                if (tableColumn.name === columnName) {
                    foundColumn = tableColumn;
                }
            });
            if (foundColumn) {
                return foundColumn;
            }
            throw new Error(`columnName: Missing column '${columnName}'`);
        }

    })

    describe("dropColumn", () => {

        beforeEach("beforeEach/dropColumn", async () => {
            await doBeforeEachCreateTable();
        })

        it("should fail on invalid column", async () => {
            try {
                await db.dropColumn(TEST_TABLE, TEST_COLUMNS_ADD[0].name);
                expect.fail("Should have thrown error on missing column");
            } catch (error) {
                if (error instanceof ColumnNotFoundError) {
                    expect(error.message).includes(TEST_COLUMNS_ADD[0].name);
                } else {
                    expect.fail(`Should not have thrown ${error.message} (${error.code})`);
                }
            }
        })

        it("should pass on valid column", async () => {
            await db.dropColumn(TEST_TABLE, TEST_COLUMNS[4].name);
        })

    })

    describe("dropIndex", () => {

        beforeEach("beforeEach/dropIndex", async () => {
            await doBeforeEachCreateTable();
        })

        it("should fail on invalid index", async () => {
            const INVALID_INDEX = "invalid_index_name";
            try {
                await db.dropIndex(TEST_TABLE, INVALID_INDEX);
                expect.fail("Should have thrown error on missing index");
            } catch (error) {
                if (error instanceof IndexNotFoundError) {
                    expect(error.message).includes(INVALID_INDEX);
                } else {
                    expect.fail(`Should not have thrown ${error.message} (${error.code})`);
                }
            }
        })

        it("should pass on invalid index with ifExists option", async () => {
            const INVALID_INDEX = "invalid_index_name";
            try {
                await db.dropIndex
                (TEST_TABLE, INVALID_INDEX, {ifExists: true});
            } catch (error) {
                expect.fail(`Should not have thrown '${error.message}'`);
            }
        })

        it("should pass on valid index", async () => {
            let indexName: string = "";
            try {
                indexName = await db.addIndex(TEST_TABLE, {
                    columnName: ["last_name", "first_name"]
                });
            } catch (error) {
                expect.fail(`addIndex should not have thrown '${error.message}' (${error.code})`);
            }
            try {
                await db.dropIndex(TEST_TABLE, indexName);
            } catch (error) {
                expect.fail(`dropIndex should not have thrown '${error.message}' (${error.code})}`);
            }
        })

    })

    describe("dropTable", () => {

        beforeEach("beforeEach/dropTable", async () => {
            await doBeforeEachCreateTable();
        })

        it("should fail on non-existing table", async () => {
            const NON_EXISTING_TABLE = "non_existing_table";
            try {
                await db.dropTable(NON_EXISTING_TABLE);
                expect.fail("Should have thrown error on missing table");
            } catch (error) {
                if (error instanceof TableNotFoundError) {
                    expect(error.message).includes(NON_EXISTING_TABLE);
                } else {
                    expect.fail(`Should not have thrown ${error.message} (${error.code})`);
                }
            }
        })

        it ("should pass on existing table", async () => {
            await db.dropTable(TEST_TABLE);
        })

    })

})
