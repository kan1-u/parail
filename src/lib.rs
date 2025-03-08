#![doc = include_str!("../README.md")]

#[cfg(feature = "iter")]
pub mod iter;
pub mod prelude;
#[cfg(feature = "stream")]
pub mod stream;
mod utils;

#[cfg(feature = "futures")]
pub extern crate futures;
#[cfg(feature = "rayon")]
pub extern crate rayon;
#[cfg(feature = "tokio")]
pub extern crate tokio;
