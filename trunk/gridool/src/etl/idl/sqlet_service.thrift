namespace java gridool.sqlet.api

enum CommandType { MAPSHUFFLE, MAPONLY }

struct SqletCommand {
  1: CommandType cmdType,
  2: string query,
  3: optional string comment,
}

service ThriftSqletService {
  # execute the given query. Takes sequence of Sqlet queries.
  void execute(1:string query) throws(1:SqletServiceException ex)
  
  # execute the given command.
  void execute(1:SqletCommand cmd) throws(1:SqletServiceException ex)
}

enum ErrorType { PARSE, EXECUTION }

exception SqletServiceException {
  1: ErrorType type,
  2: string message,
}