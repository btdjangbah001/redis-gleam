import cache.{type Cache}
import configuration
import gleam/bit_array
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import glisten.{Packet}
import handlers
import parser.{Array, BulkString, SimpleError}

fn init_store() -> Cache {
  let assert Ok(cache) = cache.new()
  cache
}

pub fn main() {
  let config = configuration.load_configuration()
  case config.replicaof {
    Some(replica_deets) -> configuration.begin_hanshake(replica_deets)
    None -> Nil
  }
  io.debug(config)

  let store = init_store()

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      let assert Packet(msg) = msg
      let message = clean_msg(msg)
      case string.lowercase(string.trim(message)) {
        "ping" -> handlers.handle_ping(conn, state)
        _ -> {
          let redisvalue = parser.decode(message)
          io.debug(redisvalue)
          case redisvalue {
            Array(Some(list)) -> {
              case list.reverse(list) {
                [BulkString(Some(command)), ..args] -> {
                  case string.lowercase(command) {
                    "ping" -> handlers.handle_ping(conn, state)
                    "echo" -> handlers.handle_echo(conn, state, args)
                    "info" -> handlers.handle_info(conn, state, args, config)
                    "set" -> handlers.handle_set(conn, state, args, store)
                    "get" -> handlers.handle_get(conn, state, args, store)
                    _ ->
                      handlers.handle_simple_error(
                        conn,
                        state,
                        "unknown command '" <> command <> "'",
                      )
                  }
                }
                _ -> {
                  handlers.handle_simple_error(conn, state, "unknown command")
                }
              }
            }
            SimpleError(text) -> handlers.handle_simple_error(conn, state, text)
            _ ->
              handlers.handle_simple_error(
                conn,
                state,
                "unknown command",
              )
          }
        }
      }
    })
    |> glisten.serve(config.port)

  io.println("Redis server is running on port " <> int.to_string(config.port))

  process.sleep_forever()
}

fn clean_msg(msg: BitArray) -> String {
  bit_array.to_string(msg)
  |> result.unwrap("")
}
