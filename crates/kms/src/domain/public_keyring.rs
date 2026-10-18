//! The public halves of a keyring: what a verifier outside the KMS holds.

use std::collections::BTreeMap;

use super::signature::SignatureAlgorithm;
use super::signing_key::PublicKey;
use crate::error::Error;

/// The public halves of the versions of a tenant's key of one algorithm
/// that verify, by version: what a JWKS publishes and a device's key
/// directory holds. Never empty, of one algorithm, its versions from one.
///
/// It names no version as the one that signs — the signer's configuration
/// does (ADR-0019) — and answers no newest, so that pinning a signature to
/// whatever the KMS made last is not the easy path. It does not verify: a
/// key the KMS holds is verified in the KMS (ADR-0016 point 7); a verifier
/// elsewhere verifies with its own library, and a test with
/// [`PublicKey::verify`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicKeyring {
    algorithm: SignatureAlgorithm,
    keys: BTreeMap<u32, PublicKey>,
}

impl PublicKeyring {
    /// The keyring of `algorithm` from its versions and their public
    /// halves, in any order.
    ///
    /// # Errors
    /// [`Error::Malformed`] when there is no version, a version is zero or
    /// named twice, or a key is of another algorithm.
    pub fn new(
        algorithm: SignatureAlgorithm,
        keys: impl IntoIterator<Item = (u32, PublicKey)>,
    ) -> Result<Self, Error> {
        let mut ring = BTreeMap::new();
        for (version, key) in keys {
            if version == 0 {
                return Err(Error::Malformed(
                    "a keyring's versions count from one".into(),
                ));
            }
            if key.algorithm() != algorithm {
                return Err(Error::Malformed(format!(
                    "version {version} is a {} key in a {algorithm} keyring",
                    key.algorithm()
                )));
            }
            if ring.insert(version, key).is_some() {
                return Err(Error::Malformed(format!(
                    "version {version} is named twice in a keyring"
                )));
            }
        }
        if ring.is_empty() {
            return Err(Error::Malformed(
                "a keyring has at least one version that verifies".into(),
            ));
        }
        Ok(PublicKeyring {
            algorithm,
            keys: ring,
        })
    }

    /// The algorithm.
    #[must_use]
    pub fn algorithm(&self) -> SignatureAlgorithm {
        self.algorithm
    }

    /// The public half of `version`, if that version verifies.
    #[must_use]
    pub fn get(&self, version: u32) -> Option<&PublicKey> {
        self.keys.get(&version)
    }

    /// The versions and their public halves, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &PublicKey)> + '_ {
        self.keys.iter().map(|(version, key)| (*version, key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::SigningKey;

    fn public(algorithm: SignatureAlgorithm, seed: u8) -> PublicKey {
        SigningKey::from_seed("t", algorithm, 1, &[seed; 32])
            .unwrap()
            .public()
            .clone()
    }

    #[test]
    fn a_keyring_is_its_versions_oldest_first_whatever_order_they_came_in() {
        let ring = PublicKeyring::new(
            SignatureAlgorithm::Ed25519,
            [
                (3, public(SignatureAlgorithm::Ed25519, 3)),
                (2, public(SignatureAlgorithm::Ed25519, 2)),
            ],
        )
        .unwrap();
        assert_eq!(ring.algorithm(), SignatureAlgorithm::Ed25519);
        assert_eq!(ring.iter().map(|(v, _)| v).collect::<Vec<_>>(), [2, 3]);
        assert_eq!(ring.get(2), Some(&public(SignatureAlgorithm::Ed25519, 2)));
        assert_eq!(ring.get(1), None);
    }

    #[test]
    fn a_keyring_is_never_empty_counts_from_one_names_a_version_once_and_holds_one_algorithm() {
        let ed = SignatureAlgorithm::Ed25519;
        for keys in [
            vec![],
            vec![(0, public(ed, 1))],
            vec![(1, public(ed, 1)), (1, public(ed, 2))],
            vec![(1, public(SignatureAlgorithm::EcdsaP256, 1))],
        ] {
            assert!(
                matches!(PublicKeyring::new(ed, keys), Err(Error::Malformed(_))),
                "accepted"
            );
        }
    }
}
