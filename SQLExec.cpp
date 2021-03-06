/**
 * Team Antelope, Sprint Otono
 *
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @author Lili Hao
 * @author Jara Lindsay
 * @author Bryn Lasher
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

/**
 * Makes the query result printable
 * @param out Ostream
 * @param qres the QueryResult to print
 * @return out Ostream
 */
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

/**
 * Destructor
 */
QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}

/**
* Execute the given SQL statement.
* @param statement   the Hyrise AST of the SQL statement to execute
* @returns           the query result (freed by caller)
*/
QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/**
* Pull out column name and attributes from AST's column definition clause.
* @param col                AST column definition
* @param column_name        returned by reference
* @param column_attributes  returned by reference
*/
void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

/**
 * Create a new table or index.
 * @param statement     The table or index to create
 * @return QueryResult  The result of the create query
 */
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

/**
 * Create a new table.
 * @param statement     The table to create
 * @return QueryResult  The result of the query
 */
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (exception &e) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

/**
 * Create a new index.
 * @param statement     The statement with a table to create the index for
 * @return QueryResult  The result of the create index query
 */
QueryResult *SQLExec::create_index(const CreateStatement *statement) {

    // get the table name and index name from statement
    Identifier table_name = statement->tableName;
    Identifier index_name = statement->indexName;
    Identifier index_type = statement->indexType;

    //ColumnNames column_names;
    //ColumnAttributes column_attributes;
    Identifier column_name;
    //ColumnAttribute column_attribute;

    // Add to schema: _indices
    ValueDict row;
    row["table_name"] = table_name;
    row["index_name"] = index_name;
    row["index_type"] = index_type;

    if (index_type == "BTREE") {
        row["is_unique"] = Value(true);
    } else {
        row["is_unique"] = Value(false);
    }

    uint16_t idx = 0;
    row["seq_in_index"] = Value(idx);

    DbRelation &indices = SQLExec::tables->get_table(Indices::TABLE_NAME);

    // check if the index on given table already created
    // DbIndex *index = SQLExec::indices->get_index(table_name, index_name);
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);

    Handles *handles = indices.select(&where);

    // the number of returned matching rows
    u_long n = handles->size();
    if (n > 0) {
        delete handles;
        //delete &where;
        //delete &row;
        return new QueryResult("Error: DbRelationError: duplicate index " + table_name);
    }

    //Handles i_handles;
    for (auto const *col : *statement->indexColumns) {

        row["column_name"] = Value(col);

        // increase idx
        idx += 1;
        row["seq_in_index"] = Value(idx);

        handles->push_back(indices.insert(&row));  // Insert into _indices

    }

    delete handles;
    return new QueryResult("created index " + index_name);

}

/**
 * Drop the specified table or index.
 * @param statement     The table or index to drop
 * @return QueryResult  The result of the drop query
 */
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and DROP INDEX are implemented");
    }
}

/**
 * Drop the specified table.
 * @param statement     Table to drop
 * @return QueryResult  The result of the drop query
 */
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

/**
 *  Drop the index from the table.
 * @param statement     Index to drop
 * @return QueryResult  The result of the drop index query
 */
QueryResult *SQLExec::drop_index(const DropStatement *statement) {

    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME || table_name == Indices::TABLE_NAME)
        throw SQLExecError("cannot drop index for schema table");

    // the query filter
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);

    // remove from _indices schema
    DbRelation &indices = SQLExec::tables->get_table(Indices::TABLE_NAME);
    Handles *i_handles = indices.select(&where);
    for (auto const &handle: *i_handles)
        indices.del(handle);
    delete i_handles;

    return new QueryResult(string("dropped index ") + index_name);
}

/**
 * Show tables or columns.
 * @param statement     Item to show
 * @return QueryResult  The result of the show query
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

/**
 * Show the index from the table.
 * @param statement     Index to show
 * @return QueryResult  The result of the show index query
 */
QueryResult *SQLExec::show_index(const ShowStatement *statement) {


    Identifier table_name = statement->tableName;

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    DbRelation &indices = SQLExec::tables->get_table(Indices::TABLE_NAME);

    ValueDict where;
    where["table_name"] = Value(table_name);
    Handles *i_handles = indices.select(&where);

    u_long n = i_handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *i_handles) {
        ValueDict *row = indices.project(handle, column_names);
        rows->push_back(row);
    }

    delete i_handles;

    string ret("successfully returned ");
    ret += to_string(n);
    ret += " rows";

    return new QueryResult(column_names, column_attributes, rows, ret);
}

/**
 * Show all current tables.
 * @return QueryResult  The result of the show tables query
 */
QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

/**
 * Shows all current columns.
 * @param statement     The columns to show
 * @return QueryResult  The result of the show columns query
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

