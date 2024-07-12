import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import birl
import gleam/io
import gleam/int

const timeout = 3000

pub type Store =
  Dict(String, #(String, Int))

pub type Message {
  Get(Subject(Result(String, Nil)), String)
  Set(String, #(String, Int))
  Delete(String)
}

pub type Cache =
  Subject(Message)

fn handle_commands(message: Message, store: Store) -> actor.Next(Message, Store) {
  case message {
    Set(key, value) -> {
      let store = dict.insert(store, key, value)
      actor.continue(store)
    }
    Get(client, key) -> {
      let value = dict.get(store, key)
      let value = case value {
        Ok(value) -> {
          case value.1 {
            -1 -> Ok(value.0)
            _ -> {
              let current_ts = birl.to_unix(birl.utc_now()) * 1000
              io.debug("Accessing time is: " <> int.to_string(current_ts))
              io.debug(value)
              case current_ts < value.1 {
                True -> Ok(value.0)
                False -> Error(Nil)
              }
            }
          }
        }
        Error(_) -> Error(Nil)
      }
      process.send(client, value)
      actor.continue(store)
    }
    Delete(key) -> {
      let store = dict.delete(store, key)
      actor.continue(store)
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
  actor.call(cache, Get(_, key), timeout) //process.try_call maybe?
}

pub fn delete(cache: Cache, key: String) -> Nil {
  process.send(cache, Delete(key))
}
