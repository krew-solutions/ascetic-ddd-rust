//! The shape of a URI on the bus: `scheme://channel[/key]`.
//!
//! The scheme picks the adapter, the channel is the topic or queue, and what
//! follows the channel is a key: messages with one key stay in order where
//! the transport partitions. `kafka://orders/order-7` is topic `orders`, key
//! `order-7`; `in-memory://orders` is the same channel with no key.

use crate::error::Error;

/// The scheme: what comes before the first `:`.
pub fn scheme(uri: &str) -> Result<&str, Error> {
    uri.split_once(':')
        .map(|(scheme, _)| scheme)
        .ok_or_else(|| Error::UnknownScheme(uri.to_owned()))
}

/// The channel: what follows `://`, up to the first `/`.
pub fn channel(uri: &str) -> Result<&str, Error> {
    let rest = uri
        .split_once("://")
        .map(|(_, rest)| rest)
        .ok_or_else(|| Error::UnknownScheme(uri.to_owned()))?;
    let channel = rest.split('/').next().unwrap_or_default();
    if channel.is_empty() {
        return Err(Error::Transport(format!("`{uri}` names no channel").into()));
    }
    Ok(channel)
}

/// The key: what follows the channel, if anything.
pub fn key(uri: &str) -> Option<&str> {
    uri.split_once("://")
        .and_then(|(_, rest)| rest.split_once('/'))
        .map(|(_, key)| key)
        .filter(|key| !key.is_empty())
}

/// The URI without its key: the channel a consumer subscribes to.
pub fn without_key(uri: &str) -> &str {
    match uri.split_once("://").and_then(|(_, rest)| rest.find('/')) {
        Some(slash) => &uri[..uri.find("://").unwrap_or(0) + 3 + slash],
        None => uri,
    }
}
