#pragma once

namespace opossum {

// Each message contains a field (4 bytes) indicating the packet's size including itself. Using extra variable here to
// avoid magic numbers.
static constexpr auto LENGTH_FIELD_SIZE = 4u;

// Documentation of the message types can be found here:
// https://www.postgresql.org/docs/12/protocol-message-formats.html
enum class PostgresMessageType : unsigned char {
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

  // Selection of error and notice message fields. All possible fields are documented at:
  // https://www.postgresql.org/docs/12/protocol-error-fields.html
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

// SQL error codes
constexpr char TRANSACTION_CONFLICT[] = "40001";

}  // namespace opossum
