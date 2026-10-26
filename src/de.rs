//! Tolerant deserializers used by the generated models.

use serde::{Deserialize, Deserializer};

/// `null` (or a missing field, together with `#[serde(default)]`) becomes `T::default()`.
pub(crate) fn null_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Flex {
    Num(serde_json::Number),
    Str(String),
}

fn flex<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    Ok(match Option::<Flex>::deserialize(d)? {
        None => None,
        Some(Flex::Num(n)) => n.as_f64(),
        Some(Flex::Str(s)) => {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.parse::<f64>().map_err(serde::de::Error::custom)?)
            }
        }
    })
}

fn flex_int<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    Ok(match Option::<Flex>::deserialize(d)? {
        None => None,
        Some(Flex::Num(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Some(Flex::Str(s)) => {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.parse::<i64>().map_err(serde::de::Error::custom)?)
            }
        }
    })
}

/// Number or numeric string → `f64` (`null` → 0).
#[allow(dead_code)] // used by generated models when a flexible number field is required
pub(crate) fn flex_f64<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    Ok(flex(d)?.unwrap_or_default())
}

/// Number or numeric string → `Option<f64>`.
pub(crate) fn opt_flex_f64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    flex(d)
}

/// Number or numeric string → `i64` (`null` → 0).
#[allow(dead_code)] // used by generated models when a flexible number field is required
pub(crate) fn flex_i64<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    Ok(flex_int(d)?.unwrap_or_default())
}

/// Number or numeric string → `Option<i64>`.
pub(crate) fn opt_flex_i64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    flex_int(d)
}
