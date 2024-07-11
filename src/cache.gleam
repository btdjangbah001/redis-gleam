import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/otp/actor

const timeout = 3000

pub type Store =
  Dict(String, String)

pub type Message {
  Get(Subject(Result(String, Nil)), String)
  Set(String, String)
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
      process.send(client, dict.get(store, key))
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

pub fn set(cache: Cache, key: String, value: String) -> Nil {
  process.send(cache, Set(key, value))
}

pub fn get(cache: Cache, key: String) -> Result(String, Nil) {
  actor.call(cache, Get(_, key), timeout) //process.try_call maybe?
}

pub fn delete(cache: Cache, key: String) -> Nil {
  process.send(cache, Delete(key))
}
