import argv
import gleam/int

pub type Config {
  Config(port: Int)
}

pub fn load_configuration() -> Config {
  let port = case argv.load().arguments {
    ["--port", port] -> {
      let p = int.parse(port)
      case p {
        Ok(p) -> p
        Error(_) ->
          panic as {
            "Invalid port provided " <> port <> ". Port needs to be a number"
          }
      }
    }
    ["-p", port] -> {
      let p = int.parse(port)
      case p {
        Ok(p) -> p
        Error(_) ->
          panic as {
            "Invalid port provided " <> port <> ". Port needs to be a number"
          }
      }
    }
    _ -> 6379
  }

  Config(port: port)
}
