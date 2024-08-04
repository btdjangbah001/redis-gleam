import configuration
import connection.{type Connection, Mug}
import gleam/bit_array
import gleam/bytes_builder.{type BytesBuilder}
import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/option.{Some}
import gleam/otp/actor
import gleam/otp/task
import glisten
import mug
import parser

pub type ReplicaConnections(a) =
  dict.Dict(glisten.Connection(a), List(BytesBuilder))

pub type Message(a) {
  Add(glisten.Connection(a))
  Batch(glisten.Connection(a), BytesBuilder)
  Flush(glisten.Connection(a))
  Delete(glisten.Connection(a))
  Send(String)
  GetAck(glisten.Connection(a))
}

pub type Replica(a) =
  Subject(Message(a))

fn handle_repl_commands(
  message: Message(a),
  replicas: ReplicaConnections(a),
) -> actor.Next(Message(a), ReplicaConnections(a)) {
  case message {
    Add(conn) -> {
      dict.insert(replicas, conn, [])
      |> actor.continue()
    }
    Delete(_) ->
      panic as { "we are not deleting connections from replicas yet" }
    Batch(conn, resp) -> {
      let value = dict.get(replicas, conn)
      let value = case value {
        Ok(list) -> [resp, ..list]
        Error(_) -> [resp]
      }
      dict.insert(replicas, conn, value)
      |> actor.continue()
    }
    Flush(conn) -> {
      let value = dict.get(replicas, conn)
      case value {
        Ok(list) -> handle_flush(conn, list)
        Error(_) -> Nil
      }
      dict.insert(replicas, conn, [])
      |> actor.continue()
    }
    Send(resp) -> {
      let disconnected = handle_send(replicas, resp, [])
      let replicas = remove_disconnected(disconnected, replicas)
      actor.continue(replicas)
    }
    GetAck(conn) -> {
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string("*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n"))
      actor.continue(replicas)
    }
  }
}

fn remove_disconnected(
  conns: List(glisten.Connection(a)),
  replicas: ReplicaConnections(a),
) {
  case conns {
    [] -> replicas
    [head, ..tail] -> {
      dict.delete(replicas, head)
      |> remove_disconnected(tail, _)
    }
  }
}

fn handle_send(
  replicas: ReplicaConnections(a),
  resp: String,
  acc: List(glisten.Connection(a)),
) {
  case dict.to_list(replicas) {
    [] -> acc
    [conn, ..conns] -> {
      case conn.1 {
        [] -> {
          let acc = case glisten.send(conn.0, bytes_builder.from_string(resp)) {
            Ok(_) -> acc
            Error(_) -> [conn.0, ..acc]
          }
          handle_send(dict.from_list(conns), resp, acc)
        }
        _ -> {
          handle_send(dict.from_list(conns), resp, acc)
        }
      }
    }
  }
}

fn handle_flush(conn: glisten.Connection(a), list: List(BytesBuilder)) {
  case list {
    [] -> Nil
    [head, ..tail] -> {
      let _ = glisten.send(conn, head)
      // what if connection dies? we don't care
      handle_flush(conn, tail)
    }
  }
}

pub fn new() -> Result(Replica(a), actor.StartError) {
  actor.start(dict.new(), handle_repl_commands)
}

pub fn add(replica: Replica(a), conn: glisten.Connection(a)) -> Nil {
  process.send(replica, Add(conn))
}

pub fn batch(
  replica: Replica(a),
  conn: glisten.Connection(a),
  resp: BytesBuilder,
) -> Nil {
  process.send(replica, Batch(conn, resp))
}

pub fn delete(replica: Replica(a), conn: glisten.Connection(a)) -> Nil {
  process.send(replica, Delete(conn))
}

pub fn send(replica: Replica(a), resp: String) -> Nil {
  process.send(replica, Send(resp))
}

pub fn send_hanshake(
  master_deets: configuration.MasterDeets,
  port: Int,
  func: fn(BitArray, Connection(a)) -> Nil,
) {
  let assert Ok(socket) =
    mug.new(master_deets.master_host, port: master_deets.master_port)
    |> mug.timeout(milliseconds: 100)
    |> mug.connect()

  let assert Ok(_) = send_ping(socket)
  let assert Ok(_) = send_first_replconf(socket, port)
  let assert Ok(_) = send_second_replconf(socket)
  let assert Ok(_) = send_psync(socket)
  task.async(fn() { receive_commands(socket, func) })
}

fn receive_commands(socket: mug.Socket, func: fn(BitArray, Connection(a)) -> Nil) {
  case mug.receive(socket, timeout_milliseconds: 1000) {
    Ok(packet) -> {
      func(packet, Mug(socket))
      receive_commands(socket, func)
    }
    _ -> receive_commands(socket, func)
  }
}

fn send_ping(socket: mug.Socket) {
  let assert Ok(_) =
    Some([parser.BulkString(Some("PING"))])
    |> parser.Array()
    |> parser.encode()
    |> bit_array.from_string()
    |> mug.send(socket, _)

  mug.receive(socket, timeout_milliseconds: 1000)
}

fn send_first_replconf(socket: mug.Socket, port: Int) {
  let assert Ok(_) =
    Some([
      parser.BulkString(Some("REPLCONF")),
      parser.BulkString(Some("listening-port")),
      parser.BulkString(Some(int.to_string(port))),
    ])
    |> parser.Array()
    |> parser.encode()
    |> bit_array.from_string()
    |> mug.send(socket, _)

  mug.receive(socket, timeout_milliseconds: 1000)
}

fn send_second_replconf(socket: mug.Socket) {
  let assert Ok(_) =
    Some([
      parser.BulkString(Some("REPLCONF")),
      parser.BulkString(Some("capa")),
      parser.BulkString(Some("psync2")),
    ])
    |> parser.Array()
    |> parser.encode()
    |> bit_array.from_string()
    |> mug.send(socket, _)

  mug.receive(socket, timeout_milliseconds: 1000)
}

fn send_psync(socket: mug.Socket) {
  let assert Ok(_) =
    Some([
      parser.BulkString(Some("PSYNC")),
      parser.BulkString(Some("?")),
      parser.BulkString(Some("-1")),
    ])
    |> parser.Array()
    |> parser.encode()
    |> bit_array.from_string()
    |> mug.send(socket, _)

  mug.receive(socket, timeout_milliseconds: 1000)
}
