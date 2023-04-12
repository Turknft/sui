// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::AuxInputs;
use crate::openid_authenticator::JWTHeader;
use crate::{
    base_types::SuiAddress,
    crypto::{get_key_pair_from_rng, DefaultHash, Signature, SignatureScheme, SuiKeyPair},
    openid_authenticator::{
        MaskedContent, OAuthProviderContent, OpenIdAuthenticator, ProofPoints, PublicInputs,
        SerializedVerifyingKey,
    },
    signature::{AuthenticatorTrait, GenericSignature},
    utils::make_transaction,
};
use fastcrypto::rsa::{Base64UrlUnpadded, Encoding as OtherEncoding};
use fastcrypto::{
    encoding::{Base64, Encoding},
    hash::HashFunction,
};
use once_cell::sync::OnceCell;
use rand::{rngs::StdRng, SeedableRng};
use serde_json::Value;
use shared_crypto::intent::{Intent, IntentMessage, IntentScope};
pub fn keys() -> Vec<SuiKeyPair> {
    let mut seed = StdRng::from_seed([0; 32]);
    let kp1: SuiKeyPair = SuiKeyPair::Ed25519(get_key_pair_from_rng(&mut seed).1);
    let kp2: SuiKeyPair = SuiKeyPair::Secp256k1(get_key_pair_from_rng(&mut seed).1);
    let kp3: SuiKeyPair = SuiKeyPair::Secp256r1(get_key_pair_from_rng(&mut seed).1);
    vec![kp1, kp2, kp3]
}

#[test]
fn openid_authenticator_scenarios() {
    let keys = keys();
    let foundation_key = &keys[0];
    let user_key = &keys[0];

    let vk = SerializedVerifyingKey::from_fp("./src/unit_tests/google.vkey");
    let public_inputs = PublicInputs::from_fp("./src/unit_tests/public.json");
    let proof_points = ProofPoints::from_fp("./src/unit_tests/google.proof");
    let aux_inputs = AuxInputs::from_fp("./src/unit_tests/aux.json");

    let mut hasher = DefaultHash::default();
    hasher.update([SignatureScheme::OpenIdAuthenticator.flag()]);
    hasher.update(&vk.vk_gamma_abc_g1);
    hasher.update(&vk.alpha_g1_beta_g2);
    hasher.update(&vk.gamma_g2_neg_pc);
    hasher.update(&vk.delta_g2_neg_pc);
    let user_address = SuiAddress::from_bytes(hasher.finalize().digest).unwrap();

    // Create an example bulletin with 2 keys from Google.
    let example_bulletin = vec![
        OAuthProviderContent {
            iss: "https://accounts.google.com".to_string(),
            kty: "RSA".to_string(),
            kid: "acda360fb36cd15ff83af83e173f47ffc36d111c".to_string(),
            e: "AQAB".to_string(),
            n: "r54td3hTv87IwUNhdc-bYLIny4tBVcasvdSd7lbJILg58C4DJ0RJPczXd_rlfzzYGvgpt3Okf_anJd5aah196P3bqwVDdelcDYAhuajBzn40QjOBPefvdD5zSo18i7OtG7nhAhRSEGe6Pjzpck3wAogqYcDgkF1BzTsRB-DkxprsYhp5pmL5RnX-6EYP5t2m9jJ-_oP9v1yvZkT5UPb2IwOk5GDllRPbvp-aJW_RM18ITU3qIbkwSTs1gJGFWO7jwnxT0QBaFD8a8aev1tmR50ehK-Sz2ORtvuWBxbzTqXXL39qgNJaYwZyW-2040vvuZnaGribcxT83t3cJlQdMxw".to_string(),
            alg: "RS256".to_string(),
        }
    ];

    // Sign the bulletin content with the sui foundation key as a personal message.
    let bulletin_sig = Signature::new_secure(
        &IntentMessage::new(
            Intent::sui_app(IntentScope::PersonalMessage),
            example_bulletin.clone(),
        ),
        foundation_key,
    );
    println!("bulletin sig: {:?}", Base64::encode(bulletin_sig.as_ref()));

    // Sign the user transaction with the user's ephemeral key.
    let tx = make_transaction(user_address, user_key, Intent::sui_transaction());
    let s = match tx.inner().tx_signatures.first().unwrap() {
        GenericSignature::Signature(s) => s,
        _ => panic!("Expected a signature"),
    };

    let authenticator = OpenIdAuthenticator {
        vk,
        public_inputs: public_inputs.clone(),
        proof_points,
        masked_content: MaskedContent::new(
            &aux_inputs.masked_content,
            public_inputs.payload_index as usize,
            public_inputs.masked_content_hash,
        )
        .unwrap(),
        jwt_signature: Base64UrlUnpadded::decode_vec(&aux_inputs.jwt_signature).unwrap(),
        user_signature: s.clone(),
        bulletin_signature: bulletin_sig,
        bulletin: example_bulletin,
        bytes: OnceCell::new(),
    };

    assert!(authenticator
        .verify_secure_generic(
            &IntentMessage::new(
                Intent::sui_transaction(),
                tx.into_data().transaction_data().clone()
            ),
            user_address,
            Some(0)
        )
        .is_ok());
}

#[test]
fn test_authenticator_failure() {
    let aux = AuxInputs::from_fp("./src/unit_tests/aux.json");
    println!("aux: {:?}", aux);
    let res = Base64UrlUnpadded::decode_vec("eyJhbGciOiJSUzI1NiIsImtpZCI6ImFjZGEzNjBmYjM2Y2QxNWZmODNhZjgzZTE3M2Y0N2ZmYzM2ZDExMWMiLCJ0eXAiOiJKV1QifQ");
    println!("res = {:?}", res);
    let decoded_header = res.unwrap();
    let json_header: Value = serde_json::from_slice(&decoded_header).unwrap();
    let header: JWTHeader = serde_json::from_value(json_header).unwrap();
    println!("header = {:?}", header);
}

#[test]
fn test_serde_roundtrip() {}

#[test]
fn test_open_id_authenticator_address() {}
