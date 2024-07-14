import gleam/dict.{type Dict}
import gleam/float
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/set
import gleam/string
import gleam/result

pub type RedisValue {
  SimpleString(String)
  BulkString(Option(String))
  //BulkStringNull
  Integer(Int)
  Array(Option(List(RedisValue)))
  //ArrayNull
  Null
  Boolean(Bool)
  Double(Float)
  BigNumber(Int)
  BulkError(Option(String))
  //BulkErrorNull
  VerbatimString(Option(String))
  //VerbatimStringNull
  Map(Option(Dict(RedisValue, RedisValue)))
  //MapNull
  Set(Option(set.Set(RedisValue)))
  //SetNull
  Push(Option(List(RedisValue)))
  //PushNull
  SimpleError(String)
}

type DecodeResult =
  #(RedisValue, String)

pub fn decode(input: String) -> RedisValue {
  decode_acc(input).0
  //extra logic to check for input not done should be here i guess
}

pub fn decode_acc(input: String) -> DecodeResult {
  case input {
    "*" <> rest -> {
      let int_decode_result = decode_integer(rest)
      let redisvalue = int_decode_result.0
      let size_option = case redisvalue {
        Integer(num) -> Some(num)
        _ -> None
      }
      case size_option {
        Some(num) -> {
          let input = int_decode_result.1
          decode_with_size(input, num, Array(Some([])))
        }
        None -> #(redisvalue, input)
      }
    }
    "+" <> rest -> decode_simple_string(rest)
    ":" <> rest -> decode_integer(rest)
    "$" <> rest -> {
      let int_decode_result = decode_integer(rest)
      let redisvalue = int_decode_result.0
      let size_option = case redisvalue {
        Integer(num) -> Some(num)
        _ -> None
      }
      case size_option {
        Some(num) -> {
          let input = int_decode_result.1
          decode_with_size(input, num, BulkString(Some("")))
        }
        None -> #(redisvalue, input)
      }
    }
    "_" <> rest -> {
      case skip_separator(rest) {
        Ok(inp) -> #(Null, inp)
        Error(_) -> #(
          SimpleError("Protocol error. Expected delimeter when decoding null"),
          rest,
        )
      }
    }
    "#" <> rest -> {
      let flag = string.first(rest)
      case flag {
        Ok(f) -> {
          case f {
            "t" -> #(Boolean(True), string.slice(rest, 1, string.length(rest)))
            "f" -> #(Boolean(False), string.slice(rest, 1, string.length(rest)))
            letter -> #(
              SimpleError(
                "Protocol error. Expect boolean to be either 't' or 'f' but found '"
                <> letter
                <> "'",
              ),
              rest,
            )
          }
        }
        Error(_) -> #(
          SimpleError("Expected 't' or 'f' after # but was empty"),
          rest,
        )
      }
    }
    "," <> rest -> decode_float(rest)
    "(" <> rest -> decode_big_number(rest)
    "!" <> rest -> {
      let int_decode_result = decode_integer(rest)
      let redisvalue = int_decode_result.0
      let size_option = case redisvalue {
        Integer(num) -> Some(num)
        _ -> None
      }
      case size_option {
        Some(num) -> {
          let input = int_decode_result.1
          decode_with_size(input, num, VerbatimString(Some("")))
        }
        None -> #(redisvalue, input)
      }
    }
    "=" <> rest -> {
      let int_decode_result = decode_integer(rest)
      let redisvalue = int_decode_result.0
      let size_option = case redisvalue {
        Integer(num) -> Some(num)
        _ -> None
      }
      case size_option {
        Some(num) -> {
          let input = int_decode_result.1
          decode_with_size(input, num, VerbatimString(Some("")))
        }
        None -> #(redisvalue, input)
      }
    }
    "~" <> rest -> {
      let int_decode_result = decode_integer(rest)
      let redisvalue = int_decode_result.0
      let size_option = case redisvalue {
        Integer(num) -> Some(num)
        _ -> None
      }
      case size_option {
        Some(num) -> {
          let input = int_decode_result.1
          decode_with_size(input, num, Set(Some(set.new())))
        }
        None -> #(redisvalue, input)
      }
    }
    ">" <> rest -> {
      let int_decode_result = decode_integer(rest)
      let redisvalue = int_decode_result.0
      let size_option = case redisvalue {
        Integer(num) -> Some(num)
        _ -> None
      }
      case size_option {
        Some(num) -> {
          let input = int_decode_result.1
          decode_with_size(input, num, Push(Some([])))
        }
        None -> #(redisvalue, input)
      }
    }
    _ -> #(SimpleError("Protocol Error. Invalid type " <> result.unwrap(string.first(input), "")), input)
  }
}

fn decode_simple_string(input: String) -> DecodeResult {
  let parts = string.split_once(input, "\r\n")
  case parts {
    Ok(p) -> {
      let str = p.0
      #(SimpleString(str), p.1)
    }
    Error(_) -> #(SimpleError("Protocol error. Expected a string after '+'"), input)
  }
}

pub fn decode_with_size(input: String, size: Int, acc: RedisValue) -> DecodeResult {
  case size {
    0 -> {
      case acc {
        Map(_) -> #(acc, input)
        Set(_) -> #(acc, input)
        Push(_) -> #(acc, input)
        Array(_) -> #(acc, input)
        _ -> {
          case skip_separator(input) {
            Ok(input) -> #(acc, input)
            _ -> #(SimpleError("Protocol Error. Expected a delimeter but found none"), input)
          }
        }
      }
    }
    -1 -> {
      case skip_separator(input) {
        Ok(inp) -> {
          case acc {
            Map(_) -> #(Map(None), inp)
            BulkError(_) -> #(BulkError(None), inp)
            VerbatimString(_) -> #(VerbatimString(None), inp)
            BulkString(_) -> #(BulkString(None), inp)
            Set(_) -> #(Set(None), inp)
            Push(_) -> #(Push(None), inp)
            Array(_) -> #(Array(None), inp)
            value -> panic as {"decode with size received an accumlator that is " <> encode(value)} 
          }
        }
        Error(_) -> {
          #(
            SimpleError(
              "Protocol error. Expected delimeter when decoding null value",
            ),
            input,
          )
        }
      }
    }
    _ -> { 
      let #(inp, acc) = case acc {
        Map(Some(map)) -> {
          let key = decode_acc(input)
          let value = decode_acc(key.1)
          #(value.1, Map(Some(dict.insert(map, key.0, value.0))))
        }
        BulkError(Some(data)) -> {
          case string.first(input) {
            Ok(letter) -> {
              #(string.slice(input, 1, string.length(input)), BulkError(Some(string.append(data, letter))))
            }
            Error(_) -> #(input, SimpleError("String length did not match when decoding bulk error"))
          }
        }
        VerbatimString(Some(data)) -> {
          case string.first(input) {
            Ok(letter) -> {
              #(string.slice(input, 1, string.length(input)), VerbatimString(Some(string.append(data, letter))))
            }
            Error(_) -> #(input, SimpleError("String length did not match when decoding verbatim string"))
          }
        }
        BulkString(Some(data)) -> {
          case string.first(input) {
            Ok(letter) -> {
              #(string.slice(input, 1, string.length(input)), BulkString(Some(string.append(data, letter))))
            }
            Error(_) -> #(input, SimpleError("String length did not match when decoding bulk string"))
          }
        }
        Set(Some(set)) -> {
          let value = decode_acc(input)
          #(value.1, Set(Some(set.insert(set, value.0))))
        }
        Push(Some(list)) -> {
          let value = decode_acc(input)
          #(value.1, Push(Some([value.0, ..list])))
        }
        Array(Some(list)) -> {
          let value = decode_acc(input)
          #(value.1, Array(Some([value.0, ..list])))
        }
        value -> panic as {"deocde with size received an accumlator that is " <> encode(value)} 
      }
      decode_with_size(inp, size - 1, acc)
    }
  } 
}

fn decode_integer(inp: String) -> DecodeResult {
  let parts = string.split_once(inp, "\r\n")

  case parts {
    Ok(p) -> {
      case int.parse(p.0) {
        Ok(num) -> {
          let rest = string.append("\r\n", p.1)
          case skip_separator(rest) {
            Ok(inp) -> {
              #(Integer(num), inp)
            }
            Error(_) -> #(
              SimpleError(
                "Protocol error. Expected delimeter when decoding integer",
              ),
              p.1,
            )
          }
        }
        Error(_) -> #(SimpleError("Expected a number after ':'\r\n"), inp)
      }
    }
    Error(_) -> #(SimpleError("Protocol error\r\n"), inp)
  }
}

fn decode_big_number(inp: String) -> DecodeResult {
  let parts = string.split_once(inp, "\r\n")

  case parts {
    Ok(p) -> {
      case int.parse(p.0) {
        Ok(num) -> {
          let rest = string.append("\r\n", p.1)
          case skip_separator(rest) {
            Ok(inp) -> {
              #(BigNumber(num), inp)
            }
            Error(_) -> #(
              SimpleError(
                "Protocol error. Expected delimeter when decoding integer",
              ),
              p.1,
            )
          }
        }
        Error(_) -> #(SimpleError("Expected a number after '('\r\n"), inp)
      }
    }
    Error(_) -> #(SimpleError("Protocol error\r\n"), inp)
  }
}

fn decode_float(inp: String) -> DecodeResult {
  let parts = string.split_once(inp, "\r\n")

  case parts {
    Ok(p) -> {
      case float.parse(p.0) {
        Ok(num) -> {
          let rest = string.append("\r\n", p.1)
          case skip_separator(rest) {
            Ok(inp) -> {
              #(Double(num), inp)
            }
            Error(_) -> #(
              SimpleError(
                "Protocol error. Expected delimeter when decoding integer",
              ),
              p.1,
            )
          }
        }
        Error(_) -> #(SimpleError("Expected a floating point number after ','\r\n"), inp)
      }
    }
    Error(_) -> #(SimpleError("Protocol error\r\n"), inp)
  }
}

fn skip_separator(input: String) -> Result(String, Nil) {
  case input {
    "\r\n" <> rest -> Ok(rest)
    _ -> Error(Nil)
  }
}

pub fn encode(value: RedisValue) {
  let string = encode_acc(value, "")
  case value {
    Array(Some(value)) -> {
      let size = list.length(value)
      "*" <> int.to_string(size) <> "\r\n" <> string
    }
    Map(Some(value)) -> {
      let size = dict.size(value)
      "%" <> int.to_string(size) <> "\r\n" <> string
    }
    Set(Some(value)) -> {
      let size = set.size(value)
      "~" <> int.to_string(size) <> "\r\n" <> string
    }
    Push(Some(value)) -> {
      let size = list.length(value)
      ">" <> int.to_string(size) <> "\r\n" <> string
    }
    _ -> string
  }
}

fn encode_acc(value: RedisValue, acc: String) -> String {
  case value {
    SimpleString(value) -> string.append(acc, "+" <> value <> "\r\n")
    BulkString(Some(value)) ->
      string.append(
        acc,
        "$" <> int.to_string(string.byte_size(value)) <> "\r\n" <> value <> "\r\n",
      )
    BulkString(None) -> string.append(acc, "$-1\r\n")
    Integer(value) -> string.append(acc, ":" <> int.to_string(value) <> "\r\n")
    Array(Some(value)) -> {
      case value {
        [] -> acc
        [head, ..tail] -> {
          let acc = acc <> encode(head)
          encode_acc(Array(Some(tail)), acc)
        }
      }
    }
    Array(None) -> string.append(acc, "*-1\r\n")
    SimpleError(error) -> "-ERR " <> error <> "\r\n"
    Null -> acc <> "_\r\n"
    Boolean(flag) -> {
      acc
      <> case flag {
        True -> "#t\r\n"
        False -> "#f\r\n"
      }
    }
    Double(value) -> string.append(acc, "," <> float.to_string(value) <> "\r\n")
    BigNumber(value) ->
      string.append(acc, "(" <> int.to_string(value) <> "\r\n")
    BulkError(Some(value)) ->
      string.append(
        acc,
        "!" <> int.to_string(string.byte_size(value)) <> "\r\n" <> value <> "\r\n",
      )
    BulkError(None) -> string.append(acc, "!-1\r\n")
    VerbatimString(Some(value)) ->
      string.append(
        acc,
        "=" <> int.to_string(string.byte_size(value)) <> "\r\n" <> value <> "\r\n",
      )
    VerbatimString(None) -> string.append(acc, "=-1\r\n")
    Map(Some(map)) -> {
      let entries = dict.to_list(map)
      case entries {
        [] -> acc
        [head, ..tail] -> {
          let acc = acc <> encode(head.0) <> encode(head.1)
          encode_acc(Map(Some(dict.from_list(tail))), acc)
        }
      }
    }
    Map(None) -> string.append(acc, "%-1\r\n")
    Set(Some(value)) -> {
      case set.to_list(value) {
        [] -> acc
        [head, ..tail] -> {
          let acc = acc <> encode(head)
          encode_acc(Set(Some(set.from_list(tail))), acc)
        }
      }
    }
    Set(None) -> string.append(acc, "~-1\r\n")
    Push(Some(value)) -> {
      case value {
        [] -> acc
        [head, ..tail] -> {
          let acc = acc <> encode(head)
          encode_acc(Push(Some(tail)), acc)
        }
      }
    }
    Push(None) -> string.append(acc, ">-1\r\n")
  }
}
