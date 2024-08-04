import birl
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/otp/actor

const timeout = 3000

pub type Store =
  Dict(String, #(String, Int))

pub type Message {
  Get(Subject(Result(String, Nil)), String)
  Set(String, #(String, Int))
  Delete(Subject(Result(String, Nil)), String)
}


pub type Cache =
  Subject(Message)

fn handle_commands(message: Message, store: Store) -> actor.Next(Message, Store) {
  case message {
    Set(key, value) -> {
      dict.insert(store, key, value)
      |> actor.continue()
    }
    Get(client, key) -> {
      let value = dict.get(store, key)
      case value {
        Ok(value) -> {
          case value.1 {
            -1 -> {
              process.send(client, Ok(value.0))
              actor.continue(store)
            }
            _ -> {
              let current_ts = birl.to_unix_milli(birl.utc_now())
              case current_ts < value.1 {
                True -> {
                  process.send(client, Ok(value.0))
                  actor.continue(store)
                }
                False -> {
                  process.send(client, Error(Nil))
                  actor.continue(dict.delete(store, key))
                }
              }
            }
          }
        }
        Error(_) -> {
          process.send(client, Error(Nil))
          actor.continue(store)
        }
      }
    }
    Delete(client, key) -> {
      let value = dict.get(store, key)
      case value {
        Ok(_) -> {
          let store = dict.delete(store, key)
          process.send(client, Ok(":1\r\n"))
          actor.continue(store)
        }
        Error(_) -> {
          process.send(client, Ok(":0\r\n"))
          actor.continue(store)
        }
      }
    }
  }
}

pub fn new() -> Result(Cache, actor.StartError) {
  actor.start(dict.new(), handle_commands)
}

pub fn set(cache: Cache, key: String, value: String, expiry: Int) -> Nil {
  process.send(cache, Set(key, #(value, expiry)))
}

pub fn get(cache: Cache, key: String) -> Result(String, Nil) {
  actor.call(cache, Get(_, key), timeout)
  //process.try_call maybe?
}

pub fn delete(cache: Cache, key: String) -> Result(String, Nil) {
  actor.call(cache, Delete(_, key), timeout)
}
