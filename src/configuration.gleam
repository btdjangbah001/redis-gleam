import argv
import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/list

pub type Config {
  Config(port: Int, master: Bool, replicaof: Option(String))
}

pub fn load_configuration() -> Config {
  let args = list.sized_chunk(argv.load().arguments, 2)

  let config = Config(port: 6379, master: True, replicaof: None)

  build_config(args, config)
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
        ["--replicaof", replica] -> {
          //validate replica to be in format "host port"
          build_config(tail, Config(..acc, master: False, replicaof: Some(replica)))
        }
        [arg, _] -> panic as {"unknown arg provided " <> arg}
        _ -> panic as {"invalid option"}
      }
    }
  }
}