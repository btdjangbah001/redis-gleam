import glisten
import mug

pub type Connection(a) {
  Glisten(glisten.Connection(a))
  Mug(mug.Socket)
}