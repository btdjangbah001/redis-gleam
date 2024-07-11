import gleam/bit_array
import gleam/bytes_builder
import gleam/erlang/process
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import glisten.{Packet}
import parser.{Array, BulkString, ErrorValue}
import cache.{type Cache}
import handlers

fn init_store() -> Cache {
  let assert Ok(cache) = cache.new()
  cache
}

pub fn main() {
  let store = init_store()

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      let assert Packet(msg) = msg
      let message = clean_msg(msg)
      case string.lowercase(string.trim(message)){
        "ping" -> handlers.handle_ping(conn, state)
        _ -> {
          let redisvalue = parser.decode(message)
          case redisvalue.0 {
            Array(Some(list)) -> {
              case list.reverse(list) {
                [BulkString(Some(command)), ..args] -> {
                  case string.lowercase(command) {
                    "ping" -> handlers.handle_ping(conn, state)
                    "echo" -> handlers.handle_echo(conn, state, args)
                    "set" -> handlers.handle_set(conn, state, args, store)
                    "get" -> handlers.handle_get(conn, state, args, store)
                    _ -> {
                      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(ErrorValue("ERR unknown command '" <> command <> "'"), "")))
                      actor.continue(state)
                    }
                  }
                }
                _ -> {
                  let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(ErrorValue("ERR commands should be encoded as bulk strings"), "")))
                      actor.continue(state)
                }
              }
            }
            ErrorValue(text) -> {
              let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(ErrorValue(text), "")))
              actor.continue(state)
            }
            _ -> {
              let assert Ok(_) = glisten.send(conn, bytes_builder.from_string("Nothing worked"))
              actor.continue(state)
            }
          }
        }
      }
    })
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn clean_msg(msg: BitArray) -> String {
  bit_array.to_string(msg)
  |> result.unwrap("")
}

