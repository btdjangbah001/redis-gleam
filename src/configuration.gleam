import argv
import gleam/bit_array
import gleam/int
import mug
import gleam/option.{type Option, None, Some}
import gleam/list
import gleam/string
import parser

pub type ReplicaDeets {
  ReplicaDeets(master_host: String, master_port: Int)
}

pub type Config {
  Config(port: Int, master: Bool, replicaof: Option(ReplicaDeets), replication_id: String, replication_offset: Int)
}

pub fn load_configuration() -> Config {
  let args = list.sized_chunk(argv.load().arguments, 2)

  let config = Config(port: 6379, master: True, replicaof: None, replication_id: "", replication_offset: 0)

  let config = build_config(args, config)

  case config.master {
    True -> Config(..config, replication_id: generate_replication_id(), replication_offset: 0)
    False -> config
  } 
}

pub fn begin_hanshake(replica_deets: ReplicaDeets, port: Int) -> Nil {
  let assert Ok(socket) = 
    mug.new(replica_deets.master_host, port: replica_deets.master_port)
    |> mug.timeout(milliseconds: 30000)
    |> mug.connect()

  let assert Ok(_) = send_ping(socket)
  let assert Ok(_) = send_first_replconf(socket, port)
  let assert Ok(_) = send_second_replconf(socket)
  let assert Ok(_) = send_psync(socket)
  Nil
}

fn send_ping(socket: mug.Socket){
  let assert Ok(Nil) = mug.send(socket, bit_array.from_string(parser.encode(parser.Array(Some([parser.BulkString(Some("PING"))])))))
  mug.receive(socket, timeout_milliseconds: 100)
}

fn send_first_replconf(socket: mug.Socket, port: Int){
  let assert Ok(Nil) = mug.send(socket, bit_array.from_string(parser.encode(parser.Array(Some([parser.BulkString(Some("REPLCONF")), parser.BulkString(Some("listening-port")), parser.BulkString(Some(int.to_string(port)))])))))
  mug.receive(socket, timeout_milliseconds: 100)
}

fn send_second_replconf(socket: mug.Socket){
  let assert Ok(Nil) = mug.send(socket, bit_array.from_string(parser.encode(parser.Array(Some([parser.BulkString(Some("REPLCONF")), parser.BulkString(Some("capa")), parser.BulkString(Some("psync2"))])))))
  mug.receive(socket, timeout_milliseconds: 100)
}

fn send_ping(socket: mug.Socket){
  let assert Ok(Nil) = mug.send(socket, bit_array.from_string(parser.encode(parser.Array(Some([parser.BulkString(Some("PSYNC")), parser.BulkString(Some("-1")), parser.BulkString(None)])))))
  mug.receive(socket, timeout_milliseconds: 100)
}

fn build_config(args, acc: Config) {
  case args {
    [] -> acc
    [head, ..tail] -> {
      case head {
        ["--port", port] -> {
          let p = int.parse(port)
          case p {
            Ok(p) -> build_config(tail, Config(..acc, port: p))
            Error(_) ->panic as {"Invalid port provided " <> port <> ". Port needs to be a number"}
          }
        }
        ["-p", port] -> {
          let p = int.parse(port)
          case p {
            Ok(p) -> build_config(tail, Config(..acc, port: p))
            Error(_) ->panic as {"Invalid port provided " <> port <> ". Port needs to be a number"}
          }
        }
        ["--replicaof", replica] -> build_config(tail, Config(..acc, master: False, replicaof: Some(build_replica_deets(replica))))
        [arg, _] -> panic as {"unknown arg provided " <> arg}
        _ -> panic as {"invalid option"}
      }
    }
  }
}

fn generate_replication_id() -> String {
  "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

fn build_replica_deets(arg: String) -> ReplicaDeets {
  case string.split(arg, " ") {
    [host, port] -> {
      let p = int.parse(port)
      case p {
        Ok(p) -> ReplicaDeets(master_host: host, master_port: p)
        Error(_) -> panic as {"Invalid port provided for --replicaof arg " <> port <> ". Port needs to be a number"}
      }
    }
    [value] -> panic as {value <> " is an invalid arg to --replicaof. it must be in the format '<MASTER_HOST> <MASTER_PORT>'"}
    _ -> panic as {"--replicaof must be in the format '<MASTER_HOST> <MASTER_PORT>'"}
  }
}