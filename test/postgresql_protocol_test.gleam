import gleam/bit_array
import gleam/list
import gleam/set
import postgresql_protocol as pg
import showtime
import showtime/tests/should

pub fn main() {
  showtime.main()
}

fn bequal(a: BitArray, b: BitArray) -> Nil {
  bit_array.inspect(a)
  |> should.equal(bit_array.inspect(b))
}

fn check_multiple(conn, multiple) {
  list.fold(multiple, conn, check_one)
}

fn check_one(conn, one) {
  let #(conn, message) = should.be_ok(pg.receive(conn))
  should.equal(message, one)
  conn
}

fn decode_message_ok(input) -> pg.BackendMessage {
  input
  |> pg.decode_backend_message()
  |> should.be_ok()
}

fn decode_message_error(input) -> pg.MessageDecodingError {
  input
  |> pg.decode_backend_message()
  |> should.be_error()
}

pub fn decode_error_response_test() {
  decode_message_error(<<"E":utf8, 1>>)
  |> should.equal(pg.MessageDecodingError(
    "invalid field separator; data: <<1>>",
  ))

  decode_message_ok(<<"E":utf8, "S":utf8, "Severity":utf8, 0>>)
  |> should.equal(pg.BeErrorResponse(
    [pg.Severity("Severity")]
    |> set.from_list(),
  ))
}

pub fn decode_packet_test() {
  pg.decode_backend_packet(<<"2":utf8, 4:32>>)
  |> should.equal(Ok(#(pg.BeBindComplete, <<>>)))
}

pub fn decode_close_complete_test() {
  decode_message_ok(<<"3":utf8>>)
  |> should.equal(pg.BeCloseComplete)
}

pub fn decode_bind_complete_test() {
  decode_message_ok(<<"2":utf8>>)
  |> should.equal(pg.BeBindComplete)
}

pub fn decode_command_complete_test() {
  decode_message_ok(<<"C":utf8, "INSERT 0 1":utf8, 0>>)
  |> should.equal(pg.BeCommandComplete(pg.Insert, 1))
}

pub fn decode_copy_data_test() {
  decode_message_ok(<<"d":utf8, 1, 2, 3>>)
  |> should.equal(pg.BeCopyData(<<1, 2, 3>>))
}

pub fn decode_copy_done_test() {
  decode_message_ok(<<"c":utf8>>)
  |> should.equal(pg.BeCopyDone)
}

pub fn decode_copy_in_response_test() {
  decode_message_ok(<<"G":utf8, 1:8, 0:16>>)
  |> should.equal(pg.BeCopyResponse(pg.In, pg.Binary, []))

  decode_message_ok(<<"G":utf8, 1:8, 1:16, 0, 0>>)
  |> should.equal(pg.BeCopyResponse(pg.In, pg.Binary, [pg.Text]))

  decode_message_ok(<<"G":utf8, 1:8, 1:16, 0, 1>>)
  |> should.equal(pg.BeCopyResponse(pg.In, pg.Binary, [pg.Binary]))

  decode_message_error(<<"G":utf8, 0:8, 1:16, 0, 1>>)
  |> should.equal(pg.MessageDecodingError(
    "invalid copy response format; data: <<0, 1>>",
  ))

  decode_message_error(<<"G":utf8, 1:8, 1:16, 1, 2>>)
  |> should.equal(pg.MessageDecodingError(
    "invalid format code: 258; data: <<>>",
  ))
}

pub fn decode_copy_out_response_test() {
  decode_message_ok(<<"H":utf8, 1:8, 0:16>>)
  |> should.equal(pg.BeCopyResponse(pg.Out, pg.Binary, []))
  decode_message_ok(<<"H":utf8, 1:8, 1:16, 0, 0>>)
  |> should.equal(pg.BeCopyResponse(pg.Out, pg.Binary, [pg.Text]))

  decode_message_ok(<<"H":utf8, 1:8, 1:16, 0, 1>>)
  |> should.equal(pg.BeCopyResponse(pg.Out, pg.Binary, [pg.Binary]))

  decode_message_error(<<"H":utf8, 0:8, 1:16, 0, 1>>)
  |> should.equal(pg.MessageDecodingError(
    "invalid copy response format; data: <<0, 1>>",
  ))

  decode_message_error(<<"H":utf8, 1:8, 1:16, 1, 2>>)
  |> should.equal(pg.MessageDecodingError(
    "invalid format code: 258; data: <<>>",
  ))

  decode_message_error(<<"H":utf8, 1:8, 1:16, 1, 2, 3>>)
  |> should.equal(pg.MessageDecodingError(
    "size must be count * 2; data: <<1, 2, 3>>",
  ))
}

pub fn decode_copy_both_response_test() {
  decode_message_ok(<<"W":utf8, 1:8, 0:16>>)
  |> should.equal(pg.BeCopyResponse(pg.Both, pg.Binary, []))

  decode_message_ok(<<"W":utf8, 1:8, 1:16, 0, 0>>)
  |> should.equal(pg.BeCopyResponse(pg.Both, pg.Binary, [pg.Text]))

  decode_message_ok(<<"W":utf8, 1:8, 1:16, 0, 1>>)
  |> should.equal(pg.BeCopyResponse(pg.Both, pg.Binary, [pg.Binary]))

  decode_message_error(<<"W":utf8, 0:8, 1:16, 0, 1>>)
  |> should.equal(pg.MessageDecodingError(
    "invalid copy response format; data: <<0, 1>>",
  ))

  decode_message_error(<<"W":utf8, 1:8, 1:16, 1, 2>>)
  |> should.equal(pg.MessageDecodingError(
    "invalid format code: 258; data: <<>>",
  ))

  decode_message_error(<<"W":utf8, 1:8, 1:16, 1, 2, 3>>)
  |> should.equal(pg.MessageDecodingError(
    "size must be count * 2; data: <<1, 2, 3>>",
  ))
}

pub fn decode_message_data_row_test() {
  decode_message_ok(<<"D":utf8, 1:16, 1:32, "a":utf8>>)
  |> should.equal(pg.BeMessageDataRow([<<"a":utf8>>]))

  decode_message_error(<<"D":utf8, 1:8, 1:16, 1:32, "a":utf8>>)
  |> should.equal(pg.MessageDecodingError(
    "failed to parse data row at count 256; data: <<1, 0, 0, 0, 1, 97>>",
  ))
}

pub fn decode_notice_resonse_test() {
  decode_message_ok(<<
    "N":utf8, "C":utf8, "Stuff":utf8, 0, "S":utf8, "NOTICE":utf8, 0,
  >>)
  |> should.equal(pg.BeNoticeResponse(
    [pg.Code("Stuff"), pg.Severity("NOTICE")]
    |> set.from_list(),
  ))
}

pub fn decode_notification_response_test() {
  decode_message_ok(<<"A":utf8, 42:32, "channel":utf8, 0, "payload":utf8, 0>>)
  |> should.equal(pg.BeNotificationResponse(
    process_id: 42,
    channel: "channel",
    payload: "payload",
  ))
}

pub fn decode_parameter_description_test() {
  decode_message_ok(<<"t":utf8, 1:16, 42:32>>)
  |> should.equal(pg.BeParameterDescription([42]))

  decode_message_error(<<"t":utf8, 1:16, 42:32, 1024:32>>)
  |> should.equal(pg.MessageDecodingError(
    "invalid parameter description; data: <<>>",
  ))

  decode_message_ok(<<"t":utf8, 2:16, 42:32, 1024:32>>)
  |> should.equal(pg.BeParameterDescription([42, 1024]))

  decode_message_error(<<"t":utf8, 2:16, 42:32>>)
  |> should.equal(pg.MessageDecodingError(
    "invalid parameter description; data: <<>>",
  ))
}

pub fn decode_parameter_status_test() {
  decode_message_ok(<<"S":utf8, "foo":utf8, 0, "bar":utf8, 0>>)
  |> should.equal(pg.BeParameterStatus("foo", "bar"))
}

pub fn decode_parse_complete_test() {
  decode_message_ok(<<"1":utf8>>)
  |> should.equal(pg.BeParseComplete)
}

pub fn decode_parse_portal_suspended_test() {
  decode_message_ok(<<"s":utf8>>)
  |> should.equal(pg.BePortalSuspended)
}

pub fn decode_ready_for_query_test() {
  decode_message_ok(<<"Z":utf8, "I":utf8>>)
  |> should.equal(pg.BeReadyForQuery(pg.TransactionStatusIdle))

  decode_message_ok(<<"Z":utf8, "T":utf8>>)
  |> should.equal(pg.BeReadyForQuery(pg.TransactionStatusInTransaction))

  decode_message_ok(<<"Z":utf8, "E":utf8>>)
  |> should.equal(pg.BeReadyForQuery(pg.TransactionStatusFailed))
}

pub fn decode_row_descriptions_test() {
  decode_message_ok(<<
    "T":utf8, 1:16, "name":utf8, 0, 1:32, 2:16, 3:32, 4:16, 5:32, 6:16,
  >>)
  |> should.equal(
    pg.BeRowDescriptions([pg.RowDescriptionField("name", 1, 2, 3, 4, 5, 6)]),
  )

  decode_message_error(<<
    "T":utf8, 1:16, "name1":utf8, 0, 1:32, 2:16, 3:32, 4:16, 5:32, 6:16,
    "name2":utf8, 0, 1:32, 2:16, 3:32, 4:16, 5:32, 6:16,
  >>)
  |> should.equal(pg.MessageDecodingError(
    "row description count mismatch; data: <<>>",
  ))
}

pub fn encode_startup_message_test() {
  pg.encode_frontend_message(
    pg.FeStartupMessage([
      #("user", "postgres"),
      #("database", "que"),
      #("application_name", "que_lib"),
    ]),
  )
  |> bequal(<<
    61:32,
    pg.protocol_version:bits,
    "user":utf8,
    0,
    "postgres":utf8,
    0,
    "database":utf8,
    0,
    "que":utf8,
    0,
    "application_name":utf8,
    0,
    "que_lib":utf8,
    0,
    0,
  >>)
}

pub fn encode_bind_text_test() {
  pg.encode_frontend_message(pg.FeBind(
    statement_name: "name",
    portal: "portal",
    parameter_format: pg.FormatAllText,
    parameters: [pg.Parameter(<<"hey":utf8, 0>>)],
    result_format: pg.FormatAllText,
  ))
  |> bequal(<<
    "B":utf8, 30:32, "portal":utf8, 0, "name":utf8, 0, 0:16, 1:16, 4:32,
    "hey":utf8, 0, 0:16,
  >>)
}

pub fn encode_bind_binary_test() {
  pg.encode_frontend_message(pg.FeBind(
    statement_name: "name",
    portal: "portal",
    parameter_format: pg.FormatAll(pg.Binary),
    parameters: [pg.Parameter(<<"hey":utf8, 0>>)],
    result_format: pg.FormatAll(pg.Binary),
  ))
  |> bequal(<<
    "B":utf8, 34:32, "portal":utf8, 0, "name":utf8, 0, 1:16, 1:16, 1:16, 4:32,
    "hey":utf8, 0, 1:16, 1:16,
  >>)
}

pub fn encode_cancel_request_test() {
  pg.encode_frontend_message(pg.FeCancelRequest(1, 2))
  |> bequal(<<16:32, 80_877_102:32, 1:32, 2:32>>)
}

pub fn encode_close_test() {
  pg.encode_frontend_message(pg.FeClose(pg.Portal, "foo"))
  |> bequal(<<"C":utf8, 9:32, "P":utf8, "foo":utf8, 0>>)

  pg.encode_frontend_message(pg.FeClose(pg.PreparedStatement, "foo"))
  |> bequal(<<"C":utf8, 9:32, "S":utf8, "foo":utf8, 0>>)
}

pub fn encode_copy_data_test() {
  pg.encode_frontend_message(pg.FeCopyData(<<1, 2, 3>>))
  |> bequal(<<"d":utf8, 7:32, 1, 2, 3>>)
}

pub fn encode_copy_done_test() {
  pg.encode_frontend_message(pg.FeCopyDone)
  |> bequal(<<"c":utf8, 4:32>>)
}

pub fn encode_copy_fail_test() {
  pg.encode_frontend_message(pg.FeCopyFail("reason"))
  |> bequal(<<"f":utf8, 11:32, "reason":utf8, 0>>)
}

pub fn encode_describe_test() {
  pg.encode_frontend_message(pg.FeDescribe(pg.Portal, "portal"))
  |> bequal(<<"D":utf8, 12:32, "P":utf8, "portal":utf8, 0>>)
}

pub fn encode_execute_test() {
  pg.encode_frontend_message(pg.FeExecute("portal", 42))
  |> bequal(<<"E":utf8, 15:32, "portal":utf8, 0, 42:32>>)
}

pub fn encode_flush_test() {
  pg.encode_frontend_message(pg.FeFlush)
  |> bequal(<<"H":utf8, 4:32>>)
}

pub fn encode_function_call_test() {
  let size = 24
  let object_id = 123
  let format = <<1:16, 0:16>>
  let arguments = <<1:16, 4:32, "hey":utf8, 0>>
  let result_format = pg.Binary
  pg.encode_frontend_message(pg.FeFunctionCall(
    object_id: object_id,
    argument_format: pg.FormatAll(pg.Text),
    arguments: [pg.Parameter(<<"hey":utf8, 0>>)],
    result_format: result_format,
  ))
  |> bequal(<<
    "F":utf8,
    size:32,
    object_id:32,
    format:bits,
    arguments:bits,
    1:16,
  >>)
}

pub fn encode_gssenc_request_test() {
  pg.encode_frontend_message(pg.FeGssEncRequest)
  |> bequal(<<8:32, 80_877_104:32>>)
}

pub fn encode_gss_response_test() {
  pg.encode_frontend_message(pg.FeAmbigous(pg.FeGssResponse(<<1, 2, 3>>)))
  |> bequal(<<"p":utf8, 7:32, 1, 2, 3>>)
}

pub fn encode_parse_test() {
  pg.encode_frontend_message(pg.FeParse("dest", "query", [42]))
  |> bequal(<<"P":utf8, 21:32, "dest":utf8, 0, "query":utf8, 0, 1:16, 42:32>>)
}

pub fn encode_password_message_test() {
  pg.encode_frontend_message(pg.FeAmbigous(pg.FePasswordMessage("hunter2")))
  |> bequal(<<"p":utf8, 12:32, "hunter2":utf8, 0>>)
}

pub fn encode_query_test() {
  pg.encode_frontend_message(pg.FeQuery("SELECT 1;"))
  |> bequal(<<"Q":utf8, 14:32, "SELECT 1;":utf8, 0>>)
}

pub fn encode_sasl_initial_response_test() {
  pg.encode_frontend_message(
    pg.FeAmbigous(pg.FeSaslInitialResponse(name: "foo", data: <<"bar":utf8>>)),
  )
  |> bequal(<<"p":utf8, 15:32, "foo":utf8, 0, 3:32, "bar":utf8>>)
}

pub fn encode_sasl_response_test() {
  pg.encode_frontend_message(
    pg.FeAmbigous(pg.FeSaslResponse(data: <<"bar":utf8>>)),
  )
  |> bequal(<<"p":utf8, 7:32, "bar":utf8>>)
}

pub fn encode_ssl_request_test() {
  pg.encode_frontend_message(pg.FeSslRequest)
  |> bequal(<<8:32, 80_877_103:32>>)
}

pub fn encode_sync_test() {
  pg.encode_frontend_message(pg.FeSync)
  |> bequal(<<"S":utf8, 4:32>>)
}

pub fn encode_terminate_test() {
  pg.encode_frontend_message(pg.FeTerminate)
  |> bequal(<<"X":utf8, 4:32>>)
}
// pub fn new_connection_test() {
//   let #(conn, _state) =
//     pg.connect("127.0.0.1", 5432, 1000)
//     |> pg.start([
//       #("user", "postgres"),
//       #("database", "que"),
//       #("application_name", "pg"),
//     ])

//   conn
//   |> pg.send(
//     pg.encode_frontend_message(pg.FeQuery(
//       "SELECT 1 + 1;",
//     )),
//   )
//   |> should.be_ok()
//   |> check_multiple([
//     pg.BeRowDescriptions([
//       pg.RowDescriptionField(
//         "?column?",
//         0,
//         0,
//         23,
//         4,
//         4_294_967_295,
//         0,
//       ),
//     ]),
//     pg.BeMessageDataRow([<<"2":utf8>>]),
//     pg.BeCommandComplete(pg.Select, 1),
//     pg.BeReadyForQuery(
//       pg.TransactionStatusIdle,
//     ),
//   ])
// }
