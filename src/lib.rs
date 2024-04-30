mod client;
mod config;
mod error;
pub mod profile;
mod request;
mod response;

pub use error::DeltaSharingError;

pub use client::DeltaSharingClient;
pub use profile::Profile;

pub type Error = DeltaSharingError;
pub type Result<T> = std::result::Result<T, Error>;
