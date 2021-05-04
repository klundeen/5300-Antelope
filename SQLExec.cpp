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

// Static tables data
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

    if (column_names != nullptr) {
        delete column_names;
    }
    if (column_attributes != nullptr) {
        delete column_attributes;
    }
    if (rows != nullptr) {
        for (auto row : *rows) {
            delete row;
        }
        delete rows;
    }
}

/**
* Execute the given SQL statement.
* @param statement   the Hyrise AST of the SQL statement to execute
* @returns           the query result (freed by caller)
*/
QueryResult *SQLExec::execute(const SQLStatement *statement) {

    // FIXME: initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement); // FIXME Part of Mem leak
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
    column_name = string(col->name);
    switch (col->type) {
        case hsql::ColumnDefinition::INT: {
            column_attribute = ColumnAttribute(ColumnAttribute::INT);
            break;
        }
        case hsql::ColumnDefinition::TEXT: {
            column_attribute = ColumnAttribute(ColumnAttribute::TEXT);
            break;
        }
        default:
            throw SQLExecError("not implemented");  // FIXME
            break;
    }

}

/**
 *
 * @param statement
 * @return
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
 *
 * @param statement
 * @return
 */
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    return new QueryResult("create index not implemented");  // FIXME
}

/**
 * Create a new table.
 * @param statement     The table to create
 * @return QueryResult  The result of the query
 */
QueryResult *SQLExec::create_table(const CreateStatement *statement) {

    string ret;
    Identifier table_name = statement->tableName;

    ValueDict where;
    where["table_name"] = table_name;

    ColumnNames column_names;
    ColumnAttributes column_attributes;

    Identifier cn;
    ColumnAttribute ca;

    for (ColumnDefinition *col : *statement->columns) {

        SQLExec::column_definition(col, cn, ca);
        column_names.push_back(cn);
        column_attributes.push_back(ca);

    }

    // add to schema Tables::TABLE_NAME if not exist
    Handles *t_handles = SQLExec::tables->select(&where);

    if (t_handles->size() > 0) {
        throw DbRelationError(table_name + " already exists");
    }

    delete t_handles;

    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);

    try {

        // add to schema Columns::TABLE_NAME
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        for (uint i = 0; i < column_names.size(); i++) {
            row["column_name"] = column_names[i];
            row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
            c_handles.push_back(columns.insert(&row));
        }

        // create the table in Berkeley DB
        HeapTable table(statement->tableName, column_names, column_attributes);

        try {
            table.create();
        } catch (DbException &e) {
            ret += "Error: ";
            ret += e.what();

            //remove from _columes schema
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            return new QueryResult(ret);
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables schema
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }

    ret += "created ";
    ret += statement->tableName;
    return new QueryResult(ret);

}

/**
 *
 * @param statement
 * @return
 */
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

/**
 *
 * @param statement
 * @return
 */
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    return new QueryResult("drop index not implemented");  // FIXME
}

/**
 * Drop the specified table.
 * @param statement     Table to drop
 * @return QueryResult  The result of the drop query
 */
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    // FIXME : drop table
    switch (statement->type) {
        case ShowStatement::kTables:
            break;
        default:
            return new QueryResult("only drop table is supported");
    }

    Identifier table_name = statement->name;

    // do not drop schema tables
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME) {
        return new QueryResult("cannot drop schema tables");
    }

    string ret;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    ValueDict where;

    // drop from _columns schema
    Handles *c_handles;
    where["table_name"] = table_name;

    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    c_handles = columns.select(&where);

    for (auto const &handle: *c_handles) {
        columns.del(handle);
    }

    delete c_handles;

    // drop from _tables schema
    // look up the handle for the dropping table
    // SELECT * FROM _tables WHERE table_name = <table_name>

    Handles *t_handles = SQLExec::tables->select(&where);

    if (t_handles->size() == 0) {
        throw DbRelationError(table_name + " does not exist");
    }

    for (auto const &handle: *t_handles) {
        SQLExec::tables->del(handle);
    }
    delete t_handles;

    // drop the table from Berkeley DB
    HeapTable table(statement->name, column_names, column_attributes);

    try {
        table.drop();
    } catch (DbException &e) {
        ret += "Error: ";
        ret += e.what();
        return new QueryResult(ret);
    }

    ret += "dropped ";
    ret += statement->name;
    return new QueryResult(ret);
}

/**
 * Show tables or columns.
 * @param statement     Item to show
 * @return QueryResult  The result of the show query
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables(); // FIXME Part of Mem leak
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

/**
 *
 * @param statement
 * @return
 */
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

/**
 * Show all current tables.
 * @return QueryResult  The result of the show tables query
 */
QueryResult *SQLExec::show_tables() {
    // SELECT table_name FROM _tables WHERE table_name NOT IN ("_tables", "_columns");

    ColumnNames *column_names = new ColumnNames();
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes();
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDicts *rows = new ValueDicts;
    ValueDict *row;
    Handles *t_handles = SQLExec::tables->select();

    for (auto const &handle: *t_handles) {
        row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = (*row)["table_name"].s;

        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME) {
            rows->push_back(row);
        }
    }
    delete t_handles;

    string ret("successfully returned ");
    ret += to_string(rows->size());
    ret += " rows";

    return new QueryResult(column_names, column_attributes, rows, ret);
}

/**
 * Shows all current columns.
 * @param statement     The columns to show
 * @return QueryResult  The result of the show columns query
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    // SELECT table_name, column_name, data_type FROM _columns WHERE table_name = <table_name>;

    // given table name
    Identifier table_name = statement->tableName;

    // query filter
    ValueDict where;
    where["table_name"] = table_name;

    // query _cloumes schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *c_handles = columns.select(&where);

    ColumnNames *column_names = new ColumnNames();
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes();
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDicts *rows = new ValueDicts;

    for (auto const &handle: *c_handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete c_handles;

    string ret("successfully returned ");
    ret += to_string(rows->size());
    ret += " rows";

    return new QueryResult(column_names, column_attributes, rows, ret);
}
