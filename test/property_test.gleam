import gleam/io
import gleam/int
import gleam/iterator
import gleam/list
import gleam/result
import gleam/string
import postgresql_protocol as pg
import prng/random
import prng/seed
import showtime
import showtime/tests/should

const iterations = 2000

pub fn main() {
  showtime.main()
}

fn make_seed() {
  let n = int.random(4_294_967_296)
  let s = seed.new(n)
  io.println("Seed for this test is: " <> int.to_string(n))
  s
}

fn random_bitarray() -> random.Generator(BitArray) {
  use size <- random.then(random.int(0, 10))
  random.fixed_size_list(from: random.int(0, 255), of: size)
  |> random.then(fn(bytes) {
    bytes
    |> list.fold(<<>>, fn(sum, byte) { <<sum:bits, byte:8>> })
    |> random.constant()
  })
}

fn random_ascii() -> random.Generator(String) {
  use size <- random.then(random.int(0, 10))
  random.fixed_size_list(from: random.int(32, 126), of: size)
  |> random.then(fn(bytes) {
    bytes
    |> list.map(fn(byte) { string.utf_codepoint(byte) })
    |> result.values()
    |> string.from_utf_codepoints()
    |> random.constant()
  })
}

fn random_string() -> random.Generator(String) {
  random_ascii()
}

fn random_int32() -> random.Generator(Int) {
  random.int(0, 2_147_483_647)
}

fn random_int16() -> random.Generator(Int) {
  random.int(0, 65_535)
}

fn random_command() -> random.Generator(pg.Command) {
  random.uniform(pg.Insert, [
    pg.Delete,
    pg.Update,
    pg.Merge,
    pg.Select,
    pg.Move,
    pg.Fetch,
    pg.Copy,
  ])
}

fn random_transaction_status() -> random.Generator(pg.TransactionStatus) {
  random.uniform(pg.TransactionStatusIdle, [
    pg.TransactionStatusInTransaction,
    pg.TransactionStatusFailed,
  ])
}

fn random_row_description_field() {
  use name <- random.then(random_string())
  use table_oid <- random.then(random_int32())
  use attr_number <- random.then(random_int16())
  use data_type_oid <- random.then(random_int32())
  use data_type_size <- random.then(random_int16())
  use type_modifier <- random.then(random_int32())
  use format_code <- random.then(random_int16())
  random.constant(pg.RowDescriptionField(
    name: name,
    table_oid: table_oid,
    attr_number: attr_number,
    data_type_oid: data_type_oid,
    data_type_size: data_type_size,
    type_modifier: type_modifier,
    format_code: format_code,
  ))
}

fn random_copy_direction() {
  random.uniform(pg.In, [pg.Out, pg.Both])
}

fn random_copy_format() {
  random.uniform(pg.Binary, [pg.Text])
}

fn random_error_or_notice_fields() {
  use size <- random.then(random.int(0, 32))
  use code <- random.then(random_string())
  use detail <- random.then(random_string())
  use file <- random.then(random_string())
  use hint <- random.then(random_string())
  use line <- random.then(random_string())
  use message <- random.then(random_string())
  use position <- random.then(random_string())
  use routine <- random.then(random_string())
  use severity_localized <- random.then(random_string())
  use severity <- random.then(random_string())
  use where <- random.then(random_string())
  use column <- random.then(random_string())
  use data_type <- random.then(random_string())
  use constraint <- random.then(random_string())
  use internal_position <- random.then(random_string())
  use internal_query <- random.then(random_string())
  use schema <- random.then(random_string())
  use table <- random.then(random_string())
  random.uniform(pg.Code(code), [
    pg.Detail(detail),
    pg.File(file),
    pg.Hint(hint),
    pg.Line(line),
    pg.Message(message),
    pg.Position(position),
    pg.Routine(routine),
    pg.SeverityLocalized(severity_localized),
    pg.Severity(severity),
    pg.Where(where),
    pg.Column(column),
    pg.DataType(data_type),
    pg.Constraint(constraint),
    pg.InternalPosition(internal_position),
    pg.InternalQuery(internal_query),
    pg.Schema(schema),
    pg.Table(table),
  ])
  |> random.fixed_size_set(size)
}

fn random_backend_message() -> List(random.Generator(pg.BackendMessage)) {
  [
    random.constant(pg.BeBindComplete),
    random.constant(pg.BeCloseComplete),
    {
      use command <- random.then(random_command())
      use columns <- random.then(random_int32())
      random.constant(pg.BeCommandComplete(command, columns))
    },
    {
      use data <- random.then(random_bitarray())
      random.constant(pg.BeCopyData(data: data))
    },
    random.constant(pg.BeCopyDone),
    random.constant(pg.BeAuthenticationOk),
    random.constant(pg.BeAuthenticationKerberosV5),
    random.constant(pg.BeAuthenticationCleartextPassword),
    {
      use salt <- random.then(random_bitarray())
      random.constant(pg.BeAuthenticationMD5Password(salt: salt))
    },
    random.constant(pg.BeAuthenticationGSS),
    {
      use auth_data <- random.then(random_bitarray())
      random.constant(pg.BeAuthenticationGSSContinue(auth_data: auth_data))
    },
    random.constant(pg.BeAuthenticationSSPI),
    {
      use mechanisms <- random.then(random.list(random_string()))
      random.constant(pg.BeAuthenticationSASL(mechanisms: mechanisms))
    },
    {
      use data <- random.then(random_bitarray())
      random.constant(pg.BeAuthenticationSASLContinue(data: data))
    },
    {
      use data <- random.then(random_bitarray())
      random.constant(pg.BeAuthenticationSASLFinal(data: data))
    },
    {
      use status <- random.then(random_transaction_status())
      random.constant(pg.BeReadyForQuery(status))
    },
    {
      use fields <- random.then(random.list(random_row_description_field()))
      random.constant(pg.BeRowDescriptions(fields))
    },
    {
      use columns <- random.then(random.list(random_bitarray()))
      random.constant(pg.BeMessageDataRow(columns))
    },
    {
      use process_id <- random.then(random_int32())
      use secret_key <- random.then(random_int32())
      random.constant(pg.BeBackendKeyData(
        process_id: process_id,
        secret_key: secret_key,
      ))
    },
    {
      use name <- random.then(random_string())
      use value <- random.then(random_string())
      random.constant(pg.BeParameterStatus(name: name, value: value))
    },
    {
      use direction <- random.then(random_copy_direction())
      use overall_format <- random.then(random_copy_format())
      use codes <- random.then(case overall_format {
        pg.Text -> random.list(random.constant(pg.Text))
        pg.Binary -> random.list(random_copy_format())
      })
      random.constant(pg.BeCopyResponse(
        direction: direction,
        overall_format: overall_format,
        codes: codes,
      ))
    },
    {
      use newest_minor <- random.then(random_int32())
      use unrecognized_options <- random.then(random.list(random_string()))
      random.constant(pg.BeNegotiateProtocolVersion(
        newest_minor: newest_minor,
        unrecognized_options: unrecognized_options,
      ))
    },
    random.constant(pg.BeNoData),
    {
      use fields <- random.then(random_error_or_notice_fields())
      random.constant(pg.BeNoticeResponse(fields))
    },
    {
      use process_id <- random.then(random_int32())
      use channel <- random.then(random_string())
      use payload <- random.then(random_string())
      random.constant(pg.BeNotificationResponse(
        process_id: process_id,
        channel: channel,
        payload: payload,
      ))
    },
    {
      use descriptions <- random.then(random.list(random_int32()))
      random.constant(pg.BeParameterDescription(descriptions))
    },
    random.constant(pg.BeParseComplete),
    random.constant(pg.BePortalSuspended),
    {
      use fields <- random.then(random_error_or_notice_fields())
      random.constant(pg.BeErrorResponse(fields))
    },
  ]
}

fn random_format() {
  random.choose(pg.Text, pg.Binary)
}

fn random_format_value() -> random.Generator(pg.FormatValue) {
  use format <- random.then(random_format())
  use size <- random.then(random.int(2, 10))
  use formats <- random.then(random.fixed_size_list(random_format(), size))

  random.uniform(pg.FormatAllText, [
    { pg.FormatAll(format) },
    { pg.Formats(formats) },
  ])
}

fn random_value() {
  use value <- random.then(random_bitarray())

  random.uniform(pg.Parameter(value), [pg.Null])
}

fn random_parameters(size) -> random.Generator(pg.ParameterValues) {
  random.fixed_size_list(random_value(), size)
}

fn random_what() {
  random.choose(pg.PreparedStatement, pg.Portal)
}

fn random_frontend_message() -> List(random.Generator(pg.FrontendMessage)) {
  [
    {
      use portal <- random.then(random_string())
      use statement_name <- random.then(random_string())
      use parameter_format <- random.then(random_format_value())
      use parameters <- random.then(case parameter_format {
        pg.FormatAllText -> {
          use size <- random.then(random.int(0, 10))
          random_parameters(size)
        }
        pg.FormatAll(_) -> {
          use size <- random.then(random.int(0, 10))
          random_parameters(size)
        }
        pg.Formats(formats) -> random_parameters(list.length(formats))
      })
      use result_format <- random.then(random_format_value())

      random.constant(pg.FeBind(
        portal: portal,
        statement_name: statement_name,
        parameter_format: parameter_format,
        parameters: parameters,
        result_format: result_format,
      ))
    },
    {
      use process_id <- random.then(random_int32())
      use secret_key <- random.then(random_int32())
      random.constant(pg.FeCancelRequest(process_id, secret_key))
    },
    {
      use what <- random.then(random_what())
      use name <- random.then(random_string())
      random.constant(pg.FeClose(what: what, name: name))
    },
    {
      use data <- random.then(random_bitarray())
      random.constant(pg.FeCopyData(data: data))
    },
    random.constant(pg.FeCopyDone),
    {
      use error <- random.then(random_string())
      random.constant(pg.FeCopyFail(error: error))
    },
    {
      use what <- random.then(random_what())
      use name <- random.then(random_string())
      random.constant(pg.FeDescribe(what: what, name: name))
    },
    {
      use portal <- random.then(random_string())
      use return_row_count <- random.then(random_int32())
      random.constant(pg.FeExecute(
        portal: portal,
        return_row_count: return_row_count,
      ))
    },
    random.constant(pg.FeFlush),
    {
      use object_id <- random.then(random_int32())
      use argument_format <- random.then(random_format_value())
      use arguments <- random.then(case argument_format {
        pg.FormatAllText -> {
          use size <- random.then(random.int(0, 10))
          random_parameters(size)
        }
        pg.FormatAll(_) -> {
          use size <- random.then(random.int(0, 10))
          random_parameters(size)
        }
        pg.Formats(formats) -> random_parameters(list.length(formats))
      })
      use result_format <- random.then(random_format())

      random.constant(pg.FeFunctionCall(
        object_id: object_id,
        argument_format: argument_format,
        arguments: arguments,
        result_format: result_format,
      ))
    },
    random.constant(pg.FeGssEncRequest),
    {
      use data <- random.then(random_bitarray())
      random.constant(pg.FeAmbigous(pg.FeGssResponse(data: data)))
    },
    {
      use name <- random.then(random_string())
      use query <- random.then(random_string())
      use parameter_object_ids <- random.then(random.list(random_int32()))
      random.constant(pg.FeParse(
        name: name,
        query: query,
        parameter_object_ids: parameter_object_ids,
      ))
    },
    {
      use password <- random.then(random_string())
      random.constant(pg.FeAmbigous(pg.FePasswordMessage(password: password)))
    },
    {
      use query <- random.then(random_string())
      random.constant(pg.FeQuery(query: query))
    },
    {
      use name <- random.then(random_string())
      use data <- random.then(random_bitarray())
      random.constant(
        pg.FeAmbigous(pg.FeSaslInitialResponse(name: name, data: data)),
      )
    },
    {
      use data <- random.then(random_bitarray())
      random.constant(pg.FeAmbigous(pg.FeSaslResponse(data: data)))
    },
    {
      use params <- random.then(
        random.list(random.pair(random_string(), random_string())),
      )
      random.constant(pg.FeStartupMessage(params: params))
    },
    random.constant(pg.FeSslRequest),
    random.constant(pg.FeTerminate),
    random.constant(pg.FeSync),
  ]
}

fn test_properties(properties, callback) {
  let seed = make_seed()
  list.each(properties, fn(property) {
    property
    |> random.to_iterator(seed)
    |> iterator.take(iterations)
    |> iterator.map(callback)
    |> iterator.run()
  })
}

pub fn decode_backend_property_test() {
  random_backend_message()
  |> test_properties(compare_backend)
}

fn compare_backend(message: pg.BackendMessage) {
  let binary = pg.encode_backend_message(message)
  let decoded = pg.decode_backend_packet(binary)
  case message {
    _ ->
      decoded
      |> should.equal(Ok(#(message, <<>>)))
  }
}

pub fn decode_frontend_property_test() {
  random_frontend_message()
  |> test_properties(compare_frontend)
}

fn compare_frontend(message: pg.FrontendMessage) {
  case message {
    // TODO: introduce state machine?
    pg.FeAmbigous(_) -> Nil
    //   pg.encode_frontend_message(message)
    //   |> pg.decode_frontend_packet()
    //   |> should.be_ok()
    //   |> should.equal(#(pg.FeAmbigous(pg.FeGssResponse(data)), <<>>))
    // }
    // pg.FeAmbigous(pg.FePasswordMessage(data)) -> {
    //   pg.encode_frontend_message(message)
    //   |> pg.decode_frontend_packet()
    //   |> should.be_ok()
    //   |> should.equal(
    //     #(
    //       pg.FeAmbigous(
    //         pg.FeGssResponse(<<bit_array.from_string(data):bits, 0>>),
    //       ),
    //       <<>>,
    //     ),
    //   )
    // }
    // pg.FeAmbigous(pg.FeSaslInitialResponse(name: name, data: data)) -> {
    //   pg.encode_frontend_message(message)
    //   |> pg.decode_frontend_packet()
    //   |> should.be_ok()
    //   |> should.equal(
    //     #(
    //       pg.FeAmbigous(
    //         pg.FeGssResponse(<<bit_array.from_string(name):bits, 0, data:bits>>),
    //       ),
    //       <<>>,
    //     ),
    //   )
    // }
    _ -> {
      pg.encode_frontend_message(message)
      |> pg.decode_frontend_packet()
      |> should.be_ok()
      |> should.equal(#(message, <<>>))
    }
  }
}
