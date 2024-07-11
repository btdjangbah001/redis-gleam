import glisten
import gleam/bytes_builder
import gleam/otp/actor
import gleam/option.{None, Some}
import cache.{type Cache}
import parser.{type RedisValue, SimpleString, BulkString, ErrorValue}

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
        _ -> {
            let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(ErrorValue("ERR incorrect number of arguments for 'echo' command"), "")))
            actor.continue(state)
        }
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
            cache.set(store, key, parser.encode(value, ""))
            let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(parser.SimpleString("OK"), ""),))
            actor.continue(state)
        }
        [BulkString(None), _] -> {
            let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(ErrorValue("ERR key for 'set' command cannot be null"), "")))
            actor.continue(state)
        }
        _ -> {
            let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(parser.encode(ErrorValue("ERR incorrect number of arguments for 'set' command"), "")))
            actor.continue(state)
        }
    } 
}