namespace java gridool.sqlet.api

enum CommandType { MAP_SHUFFLE, MAP_NO_COMPILE, REDUCE, EXT_SCRIPT }

struct SqletCommand {
  1: CommandType cmdType,
  2: string command,
  3: optional string catalogName = "default",
  4: optional map<string, string> properties,
  5: optional string comment
}

const string PROP_OUTPUT_TYPE = "output_type";
const string PROP_OUTPUT_TBLNAME = "output_table_name";
const string PROP_REDUCE_PE = "reduce_processor_element";

enum ErrorType { PARSE, EXECUTION, UNSUPPORTED }

exception SqletServiceException {
  1: ErrorType type,
  2: string message
}

enum InputOutputType { CSV, JSON, XML }

service ThriftSqletService {
  # get partitioning information of the given catalog name in the specified format
  string getPartitions(1:string catalogName, 2:InputOutputType outType) throws(1:SqletServiceException ex)

  # delete catalog of the given name (clears catalog if catalog name is default)
  bool deleteCatalog(1:string catalogName) throws(1:SqletServiceException ex)

  # execute the given query. Takes sequence of Sqlet queries.
  void executeQuery(1:string query) throws(1:SqletServiceException ex)
  
  # execute the given command.
  void executeCommand(1:SqletCommand cmd) throws(1:SqletServiceException ex)
}