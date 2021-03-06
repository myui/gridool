#
# Autogenerated by Thrift Compiler (0.7.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

require 'thrift'
require 'sqlet_service_types'

module ThriftSqletService
  class Client
    include ::Thrift::Client

    def getPartitions(catalogName, outType)
      send_getPartitions(catalogName, outType)
      return recv_getPartitions()
    end

    def send_getPartitions(catalogName, outType)
      send_message('getPartitions', GetPartitions_args, :catalogName => catalogName, :outType => outType)
    end

    def recv_getPartitions()
      result = receive_message(GetPartitions_result)
      return result.success unless result.success.nil?
      raise result.ex unless result.ex.nil?
      raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'getPartitions failed: unknown result')
    end

    def deleteCatalog(catalogName)
      send_deleteCatalog(catalogName)
      return recv_deleteCatalog()
    end

    def send_deleteCatalog(catalogName)
      send_message('deleteCatalog', DeleteCatalog_args, :catalogName => catalogName)
    end

    def recv_deleteCatalog()
      result = receive_message(DeleteCatalog_result)
      return result.success unless result.success.nil?
      raise result.ex unless result.ex.nil?
      raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'deleteCatalog failed: unknown result')
    end

    def executeQuery(query)
      send_executeQuery(query)
      recv_executeQuery()
    end

    def send_executeQuery(query)
      send_message('executeQuery', ExecuteQuery_args, :query => query)
    end

    def recv_executeQuery()
      result = receive_message(ExecuteQuery_result)
      raise result.ex unless result.ex.nil?
      return
    end

    def executeCommand(cmd)
      send_executeCommand(cmd)
      recv_executeCommand()
    end

    def send_executeCommand(cmd)
      send_message('executeCommand', ExecuteCommand_args, :cmd => cmd)
    end

    def recv_executeCommand()
      result = receive_message(ExecuteCommand_result)
      raise result.ex unless result.ex.nil?
      return
    end

  end

  class Processor
    include ::Thrift::Processor

    def process_getPartitions(seqid, iprot, oprot)
      args = read_args(iprot, GetPartitions_args)
      result = GetPartitions_result.new()
      begin
        result.success = @handler.getPartitions(args.catalogName, args.outType)
      rescue SqletServiceException => ex
        result.ex = ex
      end
      write_result(result, oprot, 'getPartitions', seqid)
    end

    def process_deleteCatalog(seqid, iprot, oprot)
      args = read_args(iprot, DeleteCatalog_args)
      result = DeleteCatalog_result.new()
      begin
        result.success = @handler.deleteCatalog(args.catalogName)
      rescue SqletServiceException => ex
        result.ex = ex
      end
      write_result(result, oprot, 'deleteCatalog', seqid)
    end

    def process_executeQuery(seqid, iprot, oprot)
      args = read_args(iprot, ExecuteQuery_args)
      result = ExecuteQuery_result.new()
      begin
        @handler.executeQuery(args.query)
      rescue SqletServiceException => ex
        result.ex = ex
      end
      write_result(result, oprot, 'executeQuery', seqid)
    end

    def process_executeCommand(seqid, iprot, oprot)
      args = read_args(iprot, ExecuteCommand_args)
      result = ExecuteCommand_result.new()
      begin
        @handler.executeCommand(args.cmd)
      rescue SqletServiceException => ex
        result.ex = ex
      end
      write_result(result, oprot, 'executeCommand', seqid)
    end

  end

  # HELPER FUNCTIONS AND STRUCTURES

  class GetPartitions_args
    include ::Thrift::Struct, ::Thrift::Struct_Union
    CATALOGNAME = 1
    OUTTYPE = 2

    FIELDS = {
      CATALOGNAME => {:type => ::Thrift::Types::STRING, :name => 'catalogName'},
      OUTTYPE => {:type => ::Thrift::Types::I32, :name => 'outType', :enum_class => InputOutputType}
    }

    def struct_fields; FIELDS; end

    def validate
      unless @outType.nil? || InputOutputType::VALID_VALUES.include?(@outType)
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Invalid value of field outType!')
      end
    end

    ::Thrift::Struct.generate_accessors self
  end

  class GetPartitions_result
    include ::Thrift::Struct, ::Thrift::Struct_Union
    SUCCESS = 0
    EX = 1

    FIELDS = {
      SUCCESS => {:type => ::Thrift::Types::STRING, :name => 'success'},
      EX => {:type => ::Thrift::Types::STRUCT, :name => 'ex', :class => SqletServiceException}
    }

    def struct_fields; FIELDS; end

    def validate
    end

    ::Thrift::Struct.generate_accessors self
  end

  class DeleteCatalog_args
    include ::Thrift::Struct, ::Thrift::Struct_Union
    CATALOGNAME = 1

    FIELDS = {
      CATALOGNAME => {:type => ::Thrift::Types::STRING, :name => 'catalogName'}
    }

    def struct_fields; FIELDS; end

    def validate
    end

    ::Thrift::Struct.generate_accessors self
  end

  class DeleteCatalog_result
    include ::Thrift::Struct, ::Thrift::Struct_Union
    SUCCESS = 0
    EX = 1

    FIELDS = {
      SUCCESS => {:type => ::Thrift::Types::BOOL, :name => 'success'},
      EX => {:type => ::Thrift::Types::STRUCT, :name => 'ex', :class => SqletServiceException}
    }

    def struct_fields; FIELDS; end

    def validate
    end

    ::Thrift::Struct.generate_accessors self
  end

  class ExecuteQuery_args
    include ::Thrift::Struct, ::Thrift::Struct_Union
    QUERY = 1

    FIELDS = {
      QUERY => {:type => ::Thrift::Types::STRING, :name => 'query'}
    }

    def struct_fields; FIELDS; end

    def validate
    end

    ::Thrift::Struct.generate_accessors self
  end

  class ExecuteQuery_result
    include ::Thrift::Struct, ::Thrift::Struct_Union
    EX = 1

    FIELDS = {
      EX => {:type => ::Thrift::Types::STRUCT, :name => 'ex', :class => SqletServiceException}
    }

    def struct_fields; FIELDS; end

    def validate
    end

    ::Thrift::Struct.generate_accessors self
  end

  class ExecuteCommand_args
    include ::Thrift::Struct, ::Thrift::Struct_Union
    CMD = 1

    FIELDS = {
      CMD => {:type => ::Thrift::Types::STRUCT, :name => 'cmd', :class => SqletCommand}
    }

    def struct_fields; FIELDS; end

    def validate
    end

    ::Thrift::Struct.generate_accessors self
  end

  class ExecuteCommand_result
    include ::Thrift::Struct, ::Thrift::Struct_Union
    EX = 1

    FIELDS = {
      EX => {:type => ::Thrift::Types::STRUCT, :name => 'ex', :class => SqletServiceException}
    }

    def struct_fields; FIELDS; end

    def validate
    end

    ::Thrift::Struct.generate_accessors self
  end

end

