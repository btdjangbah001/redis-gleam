import gleeunit/should
import parser
import gleam/option.{Some}
import gleam/set


pub fn integer_decoding_works_when_resp_is_valid_test(){
  let resp = ":12\r\n"

  parser.decode(resp)
  |> should.equal(parser.Integer(12)) 

}

pub fn integer_decoding_yields_simpleerror_when_resp_is_invalid_test(){
  let resp = ":a\r\n"

  parser.decode(resp) 
  |> should.equal(parser.SimpleError("Expected a number after ':'\r\n")) 
}

pub fn double_decoding_works_when_resp_is_valid_test(){
  let resp = ",12.17\r\n"

  parser.decode(resp)
  |> should.equal(parser.Double(12.17)) 
}

pub fn double_decoding_yields_simpleerror_when_resp_is_invalid_test(){
  let resp = ",a\r\n"

  parser.decode(resp) 
  |> should.equal(parser.SimpleError("Expected a floating point number after ','\r\n")) 
}

pub fn big_number_decoding_works_when_resp_is_valid_test(){
  let resp = "(1234562354532\r\n"

  parser.decode(resp)
  |> should.equal(parser.BigNumber(1234562354532)) 
}

pub fn bug_number_decoding_yields_simpleerror_when_resp_is_invalid_test(){
  let resp = "(a\r\n"

  parser.decode(resp) 
  |> should.equal(parser.SimpleError("Expected a number after '('\r\n")) 
}

pub fn simple_string_decoding_works_when_resp_is_valid_test(){
  let resp = "+bernard\r\n"

  parser.decode(resp)
  |> should.equal(parser.SimpleString("bernard")) 

}

pub fn simple_string_decoding_works_with_empty_string_when_resp_is_valid_test(){
  let resp = "+\r\n"

  parser.decode(resp)
  |> should.equal(parser.SimpleString("")) 

}

pub fn simple_string_decoding_fails_when_resp_is_invalid_test(){
  let resp = "+"

  parser.decode(resp)
  |> should.equal(parser.SimpleError("Protocol error. Expected a string after '+'")) 

}

pub fn array_decoding_works_when_resp_is_valid_test(){
  let resp = "*2\r\n$4\r\necho\r\n$3\r\nhey\r\n"

  parser.decode(resp)
  |> should.equal(parser.Array(Some([parser.BulkString(Some("hey")), parser.BulkString(Some("echo"))]))) 

}

pub fn array_decoding_fails_when_array_size_greater_than_actual_size_test(){
  let resp = "*3\r\n$4\r\necho\r\n$3\r\nhey\r\n"

  parser.decode(resp)
  |> should.equal(parser.Array(Some([parser.SimpleError("Protocol Error. Invalid type "), parser.BulkString(Some("hey")), parser.BulkString(Some("echo"))]))) 
}

//todo: test less than as well

pub fn push_decoding_works_when_resp_is_valid_test(){
  let resp = ">2\r\n$4\r\necho\r\n$3\r\nhey\r\n"

  parser.decode(resp)
  |> should.equal(parser.Push(Some([parser.BulkString(Some("hey")), parser.BulkString(Some("echo"))]))) 

}

pub fn push_decoding_fails_when_array_size_greater_than_actual_size_test(){
  let resp = ">3\r\n$4\r\necho\r\n$3\r\nhey\r\n"

  parser.decode(resp)
  |> should.equal(parser.Push(Some([parser.SimpleError("Protocol Error. Invalid type "), parser.BulkString(Some("hey")), parser.BulkString(Some("echo"))]))) 
}

pub fn set_decoding_works_when_resp_is_valid_test(){
  let resp = "~2\r\n$4\r\necho\r\n$3\r\nhey\r\n"

  parser.decode(resp)
  |> should.equal(parser.Set(Some(set.from_list([parser.BulkString(Some("hey")), parser.BulkString(Some("echo"))]))))

}

pub fn set_decoding_fails_when_array_size_greater_than_actual_size_test(){
  let resp = "~3\r\n$4\r\necho\r\n$3\r\nhey\r\n"

  parser.decode(resp)
  |> should.equal(parser.Set(Some(set.from_list([parser.BulkString(Some("hey")), parser.BulkString(Some("echo")), parser.SimpleError("Protocol Error. Invalid type ")])))) 
}

