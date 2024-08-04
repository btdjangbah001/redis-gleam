import mug
import birl
import cache.{type Cache}
import configuration.{type Config}
import connection.{type Connection, Mug}
import gleam/bit_array
import gleam/bytes_builder.{type BytesBuilder}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import glisten
import parser.{type RedisValue, Array, BulkString, SimpleError, SimpleString, Integer}
import replication

pub fn ping() -> BytesBuilder {
  build_response(SimpleString("PONG"))
}

pub fn echo_cmd(args: List(RedisValue)) -> BytesBuilder {
  case args {
    [BulkString(Some(arg))] -> build_response(BulkString(Some(arg)))
    [BulkString(None)] -> build_response(BulkString(None))
    _ -> simple_error("incorrect number of arguments for 'echo' command")
  }
}

pub fn info(args: List(RedisValue), config: Config) -> BytesBuilder {
  case args {
    [] -> build_response(BulkString(Some("# Replication\\r\\nrole:master")))
    [BulkString(Some(value))] -> {
      case string.lowercase(value) {
        "replication" -> {
          let role = case config.master {
            True -> "master"
            False -> "slave"
          }
          let master_repl = case config.master {
            True ->
              "\r\nmaster_replid:"
              <> config.replication_id
              <> "\r\nmaster_repl_offset:"
              <> int.to_string(config.replication_offset)
            False -> ""
          }
          build_response(
            BulkString(Some("# Replication\r\nrole:" <> role <> master_repl)),
          )
        }
        _ ->
          simple_error(
            "incorrect arguments for 'info' command '" <> value <> "'",
          )
      }
    }
    _ -> simple_error("incorrect number of arguments for 'info' command")
  }
}

pub fn get(args: List(RedisValue), store: Cache) -> BytesBuilder {
  case args {
    [BulkString(Some(arg))] -> {
      let value = cache.get(store, arg)
      case value {
        Ok(val) -> {
          bytes_builder.from_string(val)
        }
        Error(_) -> {
          build_response(BulkString(None))
        }
      }
    }
    [BulkString(None)] -> simple_error("key for 'get' command cannot be null")
    _ -> simple_error("incorrect number of arguments for 'get' command")
  }
}

pub fn mget(args: List(RedisValue), store: Cache) -> BytesBuilder {
  let responses = args
  |> list.map(fn(arg){single_get(arg, store)})

  responses 
  |> list.fold("*" <> args |> list.length() |> int.to_string() <> "\r\n", fn (acc, cur){acc <> cur})
  |> bytes_builder.from_string()
}

fn single_get(arg: RedisValue, store: Cache) -> String {
  case arg {
    BulkString(Some(arg)) -> {
      let value = cache.get(store, arg)
      case value {
        Ok(val) -> {
          val
        }
        Error(_) -> {
          parser.encode(BulkString(None))
        }
      }
    }
    BulkString(None) -> parser.encode(SimpleError("args for 'mget' command cannot be null"))
    _ -> parser.encode(SimpleError("expected $ but found " <> parser.encode(arg)))
  }
}

pub fn set(
  args: List(RedisValue),
  store: Cache,
  replicas: Option(replication.Replica(a)),
) -> BytesBuilder {
  case args {
    [BulkString(Some(key)), value] -> {
      set_to_cache(store, key, parser.encode(value), -1)
      case replicas {
        Some(replicas) ->
          replication.send(
            replicas,
            parser.encode(Array(Some([BulkString(Some("set")), ..args]))),
          )
        None -> Nil
      }
      build_response(SimpleString("OK"))
    }
    [
      BulkString(Some(key)),
      value,
      BulkString(Some(px)),
      BulkString(Some(expiry)),
    ] -> {
      case string.lowercase(px), int.parse(expiry) {
        "px", Ok(exp) -> {
          case exp > 0 {
            True -> {
              let now = birl.to_unix_milli(birl.utc_now())
              let expiry_time = now + exp
              set_to_cache(store, key, parser.encode(value), expiry_time)
              case replicas {
                Some(replicas) ->
                  replication.send(
                    replicas,
                    parser.encode(Array(Some([BulkString(Some("set")), ..args]))),
                  )
                None -> Nil
              }
              build_response(SimpleString("OK"))
            }
            False ->
              simple_error(
                "expiry cannot be a negative number. Found '" <> expiry <> "'",
              )
          }
        }
        "px", Error(_) ->
          simple_error(
            "px command nedds a number encoded as bulk string as argument. Found '"
            <> expiry
            <> "'",
          )
        _, _ ->
          simple_error(
            "expected third arg to 'set' command to be 'px' but found '"
            <> px
            <> "'",
          )
      }
    }
    [BulkString(None), _] ->
      simple_error("key for 'set' command cannot be null")
    _ -> simple_error("incorrect number of arguments for 'set' command")
  }
}

pub fn del(
  args: List(RedisValue), 
  store: Cache,
  replicas: Option(replication.Replica(a))) -> BytesBuilder {
  case args {
    [BulkString(Some(arg))] -> {
      let value = cache.delete(store, arg)
      case value {
        Ok(value) -> {
          case replicas {
            Some(replicas) ->
              replication.send(
                replicas,
                parser.encode(Array(Some([BulkString(Some("del")), ..args]))),
              )
            None -> Nil
          }
          bytes_builder.from_string(value)
        }
        Error(_) -> build_response(Integer(0))
      }
    }
    [BulkString(None)] -> simple_error("key for 'get' command cannot be null")
    _ -> simple_error("incorrect number of arguments for 'get' command")
  }
}

pub fn incr(
  args: List(RedisValue),
  store: Cache,
  replicas: Option(replication.Replica(a)),
) -> BytesBuilder {
  case args {
    [BulkString(Some(key))] -> {
      let value = cache.get(store, key)
      case value {
        Ok(val) -> {
          case parser.decode(val) {
            [BulkString(Some(val))] -> {
              case int.parse(val) {
                Ok(num) -> {
                  case replicas {
                    Some(replicas) ->
                      replication.send(
                        replicas,
                        parser.encode(Array(Some([BulkString(Some("incr")), ..args]))),
                      )
                    None -> Nil
                  }
                  let value = num + 1
                  set_to_cache(store, key, parser.encode(BulkString(Some(int.to_string(value)))), -1)
                  build_response(Integer(value))
                }
                Error(_) -> simple_error("value is not an integer or out of range")
              }
            }
            [] -> simple_error("'incr' command requires exactly one argument but found none")
            [_] -> simple_error("value is not an integer or out of range")
            _ -> simple_error("'incr' command requires exactly one argument but found none")
          }
        }
        Error(_) -> {
          case replicas {
            Some(replicas) ->
              replication.send(
                replicas,
                parser.encode(Array(Some([BulkString(Some("incr")), ..args]))),
              )
            None -> Nil
          }
          let value = BulkString(Some("1"))
          set_to_cache(store, key, parser.encode(value), -1)
          build_response(Integer(1))
        }
      }
    }
    [BulkString(None)] -> simple_error("key for 'incr' command cannot be null")
    _ -> simple_error("incorrect number of arguments for 'incr' command")
  }
}

pub fn replconf(args: List(RedisValue), conn: Connection(a)) -> BytesBuilder {
  case args {
    [BulkString(Some("listening-port")), BulkString(Some(_port))] -> {
      build_response(SimpleString("OK"))
    }
    [BulkString(Some("capa")), BulkString(Some(_capa))] -> {
      build_response(SimpleString("OK"))
    }
    [BulkString(Some("getack")), BulkString(Some("*"))] -> {
      let response = parser.encode(Array(Some([BulkString(Some("replconf")), BulkString(Some("ack")), BulkString(Some("0"))])))
      let assert Ok(_) = case conn {
        Mug(socket) -> mug.send(socket, bit_array.from_string(response), )
        _-> Ok(Nil)
      }
      response |> bytes_builder.from_string()
    }
    [BulkString(Some("ack")), BulkString(Some(_num))] -> {
      simple_error("received ack 0")
    }
    _ -> simple_error("incorrect arguments for 'replconf' command")
  }
}

pub fn psync(
  conn: Option(glisten.Connection(a)),
  args: List(RedisValue),
  config: Config,
  replicas: Option(replication.Replica(a)),
) -> BytesBuilder {
  case conn {
    Some(conn) -> {
      case args {
        [BulkString(Some(_repl_id)), BulkString(Some(_repl_offset))] -> {
          let response =
            build_response(SimpleString(
              "FULLRESYNC " <> config.replication_id <> " 0",
            ))
          let assert Ok(_) = glisten.send(conn, response)

           case replicas {
            Some(replicas) -> replication.add(replicas, conn)
            None -> panic as { "master must have replicas" }
          }

          // make sure conn is ready to accept batch commands then continue from here.. maybe task?
          let empty_file_base64 =
            bit_array.base64_decode(
              "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==",
            )

          case empty_file_base64 {
            Ok(empty) -> {
              let begin_encoding =
                bit_array.from_string(
                  "$" <> int.to_string(bit_array.byte_size(empty)) <> "\r\n",
                )
              bytes_builder.from_bit_array(bit_array.append(
                begin_encoding,
                empty,
              ))
            }
            Error(_) -> panic as { "could not decode rdb file as base64" }
          }
        }
        _ -> simple_error("incorrect arguments for 'psync' command")
      }
    }
    None -> simple_error("replica received psync")
  }
}

pub fn simple_error(message: String) -> BytesBuilder {
  build_response(SimpleError(message))
}

pub fn set_to_cache(store: Cache, key: String, value: String, expiry: Int) -> Nil {
  cache.set(store, key, value, expiry)
}

fn build_response(response: RedisValue) -> BytesBuilder {
  response
  |> parser.encode()
  |> bytes_builder.from_string()
}
