import birl
import cache.{type Cache}
import configuration.{type Config}
import gleam/bytes_builder
import gleam/int
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/string
import glisten
import parser.{type RedisValue, BulkString, SimpleError, SimpleString}

pub fn handle_ping(conn: glisten.Connection(a), state: Nil) {
  let assert Ok(_) =
    glisten.send(
      conn,
      bytes_builder.from_string(parser.encode(SimpleString("PONG"))),
    )
  actor.continue(state)
}

pub fn handle_echo(
  conn: glisten.Connection(a),
  state: Nil,
  args: List(RedisValue),
) {
  case args {
    [BulkString(Some(arg))] -> {
      let assert Ok(_) =
        glisten.send(
          conn,
          bytes_builder.from_string(parser.encode(BulkString(Some(arg)))),
        )
      actor.continue(state)
    }
    [BulkString(None)] -> {
      let assert Ok(_) =
        glisten.send(
          conn,
          bytes_builder.from_string(parser.encode(BulkString(None))),
        )
      actor.continue(state)
    }
    _ ->
      handle_simple_error(
        conn,
        state,
        "incorrect number of arguments for 'echo' command",
      )
  }
}

pub fn handle_info(
  conn: glisten.Connection(a),
  state: Nil,
  args: List(RedisValue),
  config: Config
) {
  case args {
    [] -> {
      let assert Ok(_) =
        glisten.send(
          conn,
          bytes_builder.from_string(
            parser.encode(BulkString(Some("# Replication\\r\\nrole:master"))),
          ),
        )
      actor.continue(state)
    }
    [BulkString(Some(value))] -> {
      case string.lowercase(value) {
        "replication" -> {
          let role = case config.master {
            True -> "master"
            False -> "slave"
          }
          let master_repl = case config.master {
            True -> "\r\nmaster_replid:" <> config.replication_id <> "\r\nmaster_repl_offset:" <> int.to_string(config.replication_offset)
            False -> ""
          }
          let assert Ok(_) =
            glisten.send(
              conn,
              bytes_builder.from_string(
                parser.encode(BulkString(Some("# Replication\r\nrole:" <> role <> master_repl))),
              ),
            )
          actor.continue(state)
        }
        _ ->
          handle_simple_error(
            conn,
            state,
            "incorrect arguments for 'info' command '" <> value <> "'",
          )
      }
    }
    _ ->
      handle_simple_error(
        conn,
        state,
        "incorrect number of arguments for 'info' command",
      )
  }
}

pub fn handle_get(
  conn: glisten.Connection(a),
  state: Nil,
  args: List(RedisValue),
  store: Cache,
) {
  case args {
    [BulkString(Some(arg))] -> {
      let value = cache.get(store, arg)
      case value {
        Ok(val) -> {
          let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(val))
          actor.continue(state)
        }
        Error(_) -> {
          let assert Ok(_) =
            glisten.send(
              conn,
              bytes_builder.from_string(parser.encode(BulkString(None))),
            )
          actor.continue(state)
        }
      }
    }
    [BulkString(None)] ->
      handle_simple_error(conn, state, "key for 'get' command cannot be null")
    _ ->
      handle_simple_error(
        conn,
        state,
        "incorrect number of arguments for 'get' command",
      )
  }
}


pub fn handle_set(
  conn: glisten.Connection(a),
  state: Nil,
  args: List(RedisValue),
  store: Cache,
) {
  case args {
    [BulkString(Some(key)), value] -> {
      set_to_cache(store, key, parser.encode(value), -1)
      let assert Ok(_) =
        glisten.send(
          conn,
          bytes_builder.from_string(parser.encode(parser.SimpleString("OK"))),
        )
      actor.continue(state)
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
              let assert Ok(_) =
                glisten.send(
                  conn,
                  bytes_builder.from_string(
                    parser.encode(parser.SimpleString("OK")),
                  ),
                )
              actor.continue(state)
            }
            False ->
              handle_simple_error(
                conn,
                state,
                "expiry cannot be a negative number. Found '" <> expiry <> "'",
              )
          }
        }
        "px", Error(_) ->
          handle_simple_error(
            conn,
            state,
            "px command nedds a number encoded as bulk string as argument. Found '"
              <> expiry
              <> "'",
          )
        _, _ ->
          handle_simple_error(
            conn,
            state,
            "expected third arg to 'set' command to be 'px' but found '"
              <> px
              <> "'",
          )
      }
    }
    [BulkString(None), _] ->
      handle_simple_error(conn, state, "key for 'set' command cannot be null")
    _ ->
      handle_simple_error(
        conn,
        state,
        "incorrect number of arguments for 'set' command",
      )
  }
}

pub fn handle_replconf(
  conn: glisten.Connection(a),
  state: Nil,
  args: List(RedisValue),
) {
  case args {
    [BulkString(Some("listening-port")), BulkString(Some(_port))] -> {
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(SimpleString("OK"))))
      actor.continue(state)
    }
    [BulkString(Some("capa")), BulkString(Some(_capa))] -> {
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(SimpleString("OK"))))
      actor.continue(state)
    }
    _ ->
      handle_simple_error(
        conn,
        state,
        "incorrect arguments for 'replconf' command",
      )
  }
}

pub fn handle_psync(
  conn: glisten.Connection(a),
  state: Nil,
  args: List(RedisValue),
) {
  case args {
    [BulkString(Some(repl_id)), BulkString(Some(repl_offset))] -> {
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(SimpleString("FULLRESYNC " <> repl_id <> " 0"))))
      actor.continue(state)
    }
    _ ->
      handle_simple_error(
        conn,
        state,
        "incorrect arguments for 'psync' command",
      )
  }
}


pub fn handle_simple_error(
  conn: glisten.Connection(a),
  state: Nil,
  message: String,
) {
  let assert Ok(_) =
    glisten.send(
      conn,
      bytes_builder.from_string(parser.encode(SimpleError(message))),
    )
  actor.continue(state)
}

fn set_to_cache(store: Cache, key: String, value: String, expiry: Int) {
  cache.set(store, key, value, expiry)
}
