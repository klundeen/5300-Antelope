/**
 *CPSC5300 Antelope
 *Milestone 1
 *Parse Tree Skeleton
 *Members: Stephan, Harsha Thulasi
 */


#include <stdio.h>                                                                                                                                           
#include <stdlib.h>                                                                                                                                          
#include <string.h>                                                                                                                                          
#include <db_cxx.h>                                                                                                                                          
#include <SQLParser.h>                                                                                                                                       
#include <sqlhelper.h>                                                                                                                                       
#include <cassert>                                                                                                                                           
                                                                                                                                                             
using namespace std;                                                                                                                                         
using namespace hsql;


string operatorToString(const Expr *expr);                                                                                                                   
string expresionToString(const Expr *expr);                                                                                                                  
string tableRefInfoToString(const TableRef *table);                                                                                                          
string colDefToString(const ColumnDefinition *col);

/**                                                                             
 *Convert experssion to equivalent sql                                          
 *@param: input experssion to unparse                                           
 *@return: sql string statement equivalent to the expression                    
 */

string expressionToString (const Expr *expr)
{
  string sqlStr;

  switch(expr->type)
    {

    case kExprStar:
      sqlStr += "*";
      break;

    case kExprColumnRef:
      if(expr ->table != NULL)
        sqlStr += string(expr->table) + ".";
      break;

    case kExprLiteralString:
      sqlStr += expr->name;
      break;
      
    case kExprLiteralInt:
      sqlStr += to_string(expr->ival);
      break;
      
    case kExprLiteralFloat:
      sqlStr += to_string(expr->fval);
      break;

    case kExprFunctionRef:
      sqlStr += string(expr->name) + "?" + expr->expr->name;
      break;
      
    case kExprOperator:
      sqlStr += operatorToString(expr);
      break;

    default:
      sqlStr += "Unknown??";
      break;
    }

  //check for alias                                                            

  if (expr->alias != NULL)
    sqlStr += string("AS") + expr->alias;


  return sqlStr;
}


/**                                                                             
 *convert oeprator expression to equivalent sql                                 
 *@param: expression oeprator                                                   
 *@return: equivalent sql                                                       
 */

string operatorToString(const Expr * expr)
{

  string sqlStr;

  return sqlStr;
  
}

/**                                                                             
 *convert hyrise tableref AST to equivalent sql                                 
 *@param table reference ast                                                    
 *@return equivalent sql table                                                  
 */
string tableRefInfoToString(const TableRef *table)
{
  string tableRef;

  return tableRef;
}


/**                                                                             
 *convert hyrise columnDefinition AST to equivalent sql                         
 *@param column definition                                                      
 *@return equivalent sql column definition                                      
 */
string colDefToString(const ColumnDefinition *col)
{
  string colDef = col->name;

  return colDef;
}

                                
/**                                                                             
 *print sql select statement                                                    
 *@param hyrise AST select stmt                                                 
 *@return select stmt string (temporarily)                                      
 */
string executeSelect(const SelectStatement *stmt)
{
  string sqlStmt = "SELECT ";

  bool addComma = false;
  for(Expr * expr : *stmt->selectList){
    if(addComma)
      sqlStmt += ", ";
    sqlStmt += expressionToString(expr);
    addComma = true;

  }

  sqlStmt += "FROM " + tableRefInfoToString(stmt->fromTable);

  if(stmt->whereClause != NULL)
    sqlStmt += " WHERE " + expressionToString(stmt->whereClause);

  return sqlStmt;

}


/**                                                                             
 *print sql insert statement                                                    
 *@param hyrise AST insert stmt                                                 
 *@reeturn insert stmt string (temporarily)                                     
 */
string executeInsert(const InsertStatement *stmt)
{
  string sqlStmt = "INSERT ...";
  //fix later, not required for milestone1                                      
  return sqlStmt;

}


/**                                                                             
 *print sql create statement                                                    
 *@parm hyrise AST create stmt                                                  
 *@return create stmt string (termporarily)                                     
 */
string executeCreate(const CreateStatement *stmt)
{
  string sqlStmt = "CREATE TABLE ";

  if(stmt->type != CreateStatement::kTable)
    return sqlStmt + "...";

  if(stmt->ifNotExists)
    sqlStmt += "IF NOT EXISTS ";

  sqlStmt += string(stmt->tableName) + " (";

  bool addComma = false;
  for(ColumnDefinition *col : *stmt->columns){
    if(addComma)
      sqlStmt += ", ";

    sqlStmt += colDefToString(col);
    addComma = true;
  }
  sqlStmt += ")";

  return sqlStmt;
  
}

/**                                                                             
 *print sql statements                                                          
 *@param hyrise AST statements                                                  
 *@return string (temporarily)                                                  
 */
string execute(const SQLStatement *stmt)
{
  switch(stmt->type())
    {
    case kStmtSelect:
      return executeSelect((const SelectStatement*)stmt);

    case kStmtInsert:
      return executeInsert((const InsertStatement*)stmt);

    case kStmtCreate:
      return executeCreate((const CreateStatement*)stmt);

    default:
      return "Not yet implemented";

    }

}

/**                                                                             
 *main entry point                                                              
 *                                                                              
 *@args path to the BerkeleyDB database env                                     
 */
int main(int argNum, char **argv)
{
  if (argNum != 2){
    cout<<"Usage: cpsc5300: dbenvPath"<<endl;
    return 1;
  }

  char * envHome = argv[1];

  cout<<"sql5300: running with database environemnt at:"<<envHome<<endl;

  DbEnv env(0U);

  try {
    env.open(envHome, DB_CREATE | DB_INIT_MPOOL,0);
  }catch(DbException &exc){
    cout<<"Error opening database environment"<<endl;
    cout<<"(sql5300: "<<exc.what()<<")"<<endl;
    exit(1);
  }catch(std::exception &exc){
    cout<<"Error opening database environment"<<endl;
    cout<<"(sql5300: "<<exc.what()<<")"<<endl;
    exit(1);
  }
  
  while(true){
    cout<< "SQL> ";
    string inputQuery;
    
    getline(cin, inputQuery);
    
    if(inputQuery.length() == 0)
      continue;
    if(inputQuery == "quit")
      break;
    
    SQLParserResult *output = SQLParser::parseSQLString(inputQuery);
    if(!output->isValid()){
      cout<<"Invalid SQL: "<<inputQuery<<endl;
      continue;
    }

    for (uint i = 0; i<output->size(); i++)
      cout << execute(output->getStatement(i))<<endl;
 
  }

  return EXIT_SUCCESS;


}
