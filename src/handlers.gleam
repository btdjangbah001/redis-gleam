import glisten
import gleam/bytes_builder
import gleam/otp/actor
import gleam/option.{None, Some}
import cache.{type Cache}
import parser.{type RedisValue, SimpleString, BulkString, ErrorValue}
import gleam/string
import gleam/int
import birl
import birl/duration

pub fn handle_ping(conn: glisten.Connection(a), state: Nil){
    let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(SimpleString("PONG"), "")))
    actor.continue(state)
}

pub fn handle_echo(conn: glisten.Connection(a), state: Nil, args: List(RedisValue)){
    case args {
        [BulkString(Some(arg))] -> {
            let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(BulkString(Some(arg)), "")))
            actor.continue(state)
        }
        [BulkString(None)] -> {
            let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(BulkString(None), "")))
            actor.continue(state)
        }
        _ -> handle_simple_error(conn, state, "incorrect number of arguments for 'echo' command")
    } 
}

pub fn handle_get(conn: glisten.Connection(a), state: Nil, args: List(RedisValue), store: Cache){
    case args {
        [BulkString(Some(arg))] -> {
            let value = cache.get(store, arg)
            case value {
                Ok(val) -> {
                    let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(val))
                    actor.continue(state)
                }
                Error(_) -> {
                    let assert Ok(_) = glisten.send(conn,bytes_builder.from_string(parser.encode(BulkString(None), "")))
                    actor.continue(state)
                } 
            }
        }
        [BulkString(None)] -> {
            let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(ErrorValue("ERR key for 'get' command cannot be null"), "")))
            actor.continue(state)
        }
        _ -> {
            let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(ErrorValue("ERR incorrect number of arguments for 'get' command"), "")))
            actor.continue(state)
        }
    } 
}

pub fn handle_set(conn: glisten.Connection(a), state: Nil, args: List(RedisValue), store: Cache){
    case args {
        [BulkString(Some(key)), value] -> {
            set_to_cache(store, key, parser.encode(value, ""), -1)
            let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(parser.SimpleString("OK"), ""),))
            actor.continue(state)
        }
        [BulkString(Some(key)), value, BulkString(Some(px)), BulkString(Some(expiry))] -> {
            case string.lowercase(px), int.parse(expiry) {
                "px", Ok(expiry) -> {
                    case expiry > 0 {
                        True -> {
                            let now = birl.to_unix(birl.utc_now()) * 1000
                            let expiry_time = now + expiry 
                            set_to_cache(store, key, parser.encode(value, ""), expiry_time)
                            let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(parser.SimpleString("OK"), ""),))
                            actor.continue(state)
                        }
                        False -> handle_simple_error(conn, state, "expiry cannot be a negative number. Found '" <> int.to_string(expiry) <> "'")
                    }
                }
                "px", Error(_) -> handle_simple_error(conn, state, "px command nedds a number encoded as bulk string as argument. Found '" <> expiry <> "'")
                _, _-> handle_simple_error(conn, state, "expected third arg to 'set' command to be 'px' but found '" <> px <> "'")
            }     
        }
        [BulkString(None), _] -> handle_simple_error(conn, state, "key for 'set' command cannot be null")
        _ -> handle_simple_error(conn, state, "incorrect number of arguments for 'set' command")
    } 
}

pub fn handle_simple_error(conn: glisten.Connection(a), state: Nil, message: String) {
    let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(ErrorValue(message), "")))
    actor.continue(state) 
}

fn set_to_cache(store: Cache, key: String, value: String, expiry: Int){
    cache.set(store, key, value, expiry)
}