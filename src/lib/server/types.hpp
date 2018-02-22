#pragma once

namespace opossum {

enum class NetworkMessageType : unsigned char {
  // Important: The character '0' is treated as a null message
  // That means we cannot have an invalid type
  NullCommand = '0',

  // Responses
  ParseComplete = '1',
  BindComplete = '2',
  CloseComplete = '3',
  CommandComplete = 'C',
  ParameterStatus = 'S',
  AuthenticationRequest = 'R',
  ErrorResponse = 'E',
  EmptyQueryResponse = 'I',
  NoDataResponse = 'n',
  ReadyForQuery = 'Z',
  RowDescription = 'T',
  DataRow = 'D',

  // Errors
  HumanReadableError = 'M',
  SqlstateCodeError = 'C',

  // Commands
  ExecuteCommand = 'E',
  SyncCommand = 'S',
  FlushCommand = 'H',
  TerminateCommand = 'X',
  DescribeCommand = 'D',
  BindCommand = 'B',
  ParseCommand = 'P',
  SimpleQueryCommand = 'Q',
  CloseCommand = 'C',

  // SSL willingness
  SslYes = 'S',
  SslNo = 'N',
  Notice = 'N',
};

enum class TransactionStatusIndicator : unsigned char {
  Idle = 'I',
  InTransactionBlock = 'T',
  InFailedTransactionBlock = 'e'
};

}  // namespace opossum
