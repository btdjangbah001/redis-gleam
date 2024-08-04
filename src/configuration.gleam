import argv
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

pub type MasterDeets {
  MasterDeets(master_host: String, master_port: Int)
}

pub type Config {
  Config(
    port: Int,
    master: Bool,
    replicaof: Option(MasterDeets),
    replication_id: String,
    replication_offset: Int,
  )
}

pub fn load_configuration() -> Config {
  let args = list.sized_chunk(argv.load().arguments, 2)

  let config =
    Config(
      port: 6379,
      master: True,
      replicaof: None,
      replication_id: "",
      replication_offset: 0,
    )

  let config = build_config(args, config)

  case config.replicaof {
    None ->
      Config(
        ..config,
        replication_id: generate_replication_id(),
        replication_offset: 0,
      )
    Some(_) -> config
  }
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
            Error(_) ->
              panic as {
                "Invalid port provided "
                <> port
                <> ". Port needs to be a number"
              }
          }
        }
        ["-p", port] -> {
          let p = int.parse(port)
          case p {
            Ok(p) -> build_config(tail, Config(..acc, port: p))
            Error(_) ->
              panic as {
                "Invalid port provided "
                <> port
                <> ". Port needs to be a number"
              }
          }
        }
        ["--replicaof", replica] ->
          build_config(
            tail,
            Config(
              ..acc,
              master: False,
              replicaof: Some(build_master_deets(replica)),
            ),
          )
        [arg, _] -> panic as { "unknown arg provided " <> arg }
        _ -> panic as { "invalid option" }
      }
    }
  }
}

fn generate_replication_id() -> String {
  "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

fn build_master_deets(arg: String) -> MasterDeets {
  case string.split(arg, " ") {
    [host, port] -> {
      let p = int.parse(port)
      case p {
        Ok(p) -> MasterDeets(master_host: host, master_port: p)
        Error(_) ->
          panic as {
            "Invalid port provided for --replicaof arg "
            <> port
            <> ". Port needs to be a number"
          }
      }
    }
    [value] ->
      panic as {
        value
        <> " is an invalid arg to --replicaof. it must be in the format '<MASTER_HOST> <MASTER_PORT>'"
      }
    _ ->
      panic as {
        "--replicaof must be in the format '<MASTER_HOST> <MASTER_PORT>'"
      }
  }
}
