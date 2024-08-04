import cache.{type Cache}
import configuration
import connection.{type Connection, Glisten}
import gleam/bit_array
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import glisten.{Packet}
import handlers
import parser.{Array, BulkString, SimpleError}
import replication

fn init_store() -> Cache {
  let assert Ok(cache) = cache.new()
  cache
}

fn init_replication() -> replication.Replica(a) {
  let assert Ok(replicas) = replication.new()
  replicas
}

pub fn main() {
  let config = configuration.load_configuration()
  let store = init_store()

  let replicas = case config.replicaof {
    Some(master_deets) -> {
      replication.send_hanshake(
        master_deets,
        config.port,
        process_message(store, None, config),
      )
      None
    }
    None -> {
      Some(init_replication())
    }
  }
  io.debug(config)

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      let assert Packet(msg) = msg
      exec_commands(msg, Glisten(conn), store, replicas, config)
      |> list.map(fn(response) {
        let assert Ok(_) = glisten.send(conn, response)
      })
      actor.continue(state)
    })
    |> glisten.serve(config.port)

  io.println("Redis server is running on port " <> int.to_string(config.port))

  process.sleep_forever()
}

fn clean_msg(msg: BitArray) -> String {
  bit_array.to_string(msg)
  |> result.unwrap("")
}

pub fn process_message(
  store: Cache,
  replicas: Option(replication.Replica(a)),
  config: configuration.Config,
) {
  fn(msg: BitArray, conn: Connection(a)) {
    let _ = exec_commands(msg, conn, store, replicas, config)
    Nil
  }
}


fn exec_commands(
  request: BitArray,
  conn: Connection(a),
  store: Cache,
  replicas: Option(replication.Replica(a)),
  config: configuration.Config,
) {
  let message = clean_msg(request)
  case string.lowercase(string.trim(message)) {
    "ping" -> [handlers.ping()]
    _ -> {
      parser.decode(message)
      |> list.map(fn(redisvalue) {
        case redisvalue {
          Array(Some(list)) -> {
            case list.reverse(list) {
              [BulkString(Some(command)), ..args] -> {
                case string.lowercase(command) {
                  "ping" -> handlers.ping()
                  "echo" -> handlers.echo_cmd(args)
                  "info" -> handlers.info(args, config)
                  "set" -> handlers.set(args, store, replicas)
                  "del" -> handlers.del(args, store, replicas)
                  "get" -> handlers.get(args, store)
                  "mget"-> handlers.mget(args, store)
                  "incr" -> handlers.incr(args, store, replicas)
                  "replconf" -> handlers.replconf(args, conn)
                  "psync" -> {
                    let conn = case conn {
                      Glisten(conn) -> Some(conn)
                      _ -> None
                    }
                    handlers.psync(conn, args, config, replicas)
                  }
                  _ -> {
                    let _ =
                      handlers.simple_error(
                        "unknown command '" <> command <> "'",
                      )
                  }
                }
              }
              _ -> {
                let _ = handlers.simple_error("unknown command")
              }
            }
          }
          SimpleError(text) -> {
            let _ = handlers.simple_error(text)
          }
          _ -> {
            let _ = handlers.simple_error("unknown command")
          }
        }
      })
    }
  }
}
