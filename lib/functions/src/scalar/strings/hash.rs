use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use md5::{Digest, Md5};
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, StringFamily, TypedFamily,
};
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, EncodingName, RdfFusionEncodings,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Defines the specific hash algorithm to apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HashAlgorithm {
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,
}

/// Factory function for SPARQL hash functions (e.g., md5, sha1, sha256).
///
/// # Relevant Resources
/// - [SPARQL 1.1 - MD5](https://www.w3.org/TR/sparql11-query/#func-md5)
pub fn md5_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(HashSparqlUdf::new(
        encodings,
        BuiltinName::Md5.to_string(),
        HashAlgorithm::Md5,
    )))
}

/// Returns the SHA1 checksum of a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - SHA1](https://www.w3.org/TR/sparql11-query/#func-sha1)
pub fn sha1_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(HashSparqlUdf::new(
        encodings,
        BuiltinName::Sha1.to_string(),
        HashAlgorithm::Sha1,
    )))
}

/// Returns the SHA256 checksum of a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - SHA256](https://www.w3.org/TR/sparql11-query/#func-sha256)
pub fn sha256_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(HashSparqlUdf::new(
        encodings,
        BuiltinName::Sha256.to_string(),
        HashAlgorithm::Sha256,
    )))
}

/// Returns the SHA384 checksum of a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - SHA384](https://www.w3.org/TR/sparql11-query/#func-sha384)
pub fn sha384_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(HashSparqlUdf::new(
        encodings,
        BuiltinName::Sha384.to_string(),
        HashAlgorithm::Sha384,
    )))
}

/// Returns the SHA512 checksum of a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - SHA512](https://www.w3.org/TR/sparql11-query/#func-sha512)
pub fn sha512_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(HashSparqlUdf::new(
        encodings,
        BuiltinName::Sha512.to_string(),
        HashAlgorithm::Sha512,
    )))
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct HashSparqlUdf {
    encodings: RdfFusionEncodings,
    name: String,
    algorithm: HashAlgorithm,
    signature: Signature,
}

impl Debug for HashSparqlUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashSparqlUdf")
            .field("name", &self.name)
            .field("algorithm", &self.algorithm)
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl HashSparqlUdf {
    /// Create a new [`HashSparqlUdf`].
    pub fn new(
        encodings: RdfFusionEncodings,
        name: String,
        algorithm: HashAlgorithm,
    ) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name,
            algorithm,
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for HashSparqlUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let encoding_name = detect_encoding_from_types(&self.encodings, arg_types)?;

        match encoding_name {
            Some(EncodingName::TypedFamily) => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            _ => exec_err!("Unsupported encoding for hash return type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArrays::TypedFamily(tf_args)) => tf_args
                .map_children_tf_unary(|child| match child.downcast() {
                    DowncastTypedFamilyArray::String(array) => {
                        let values = array.value_array();

                        // Create hash mappings. We skip `apply_unary` from StringFamilyArray
                        // here because SPARQL hashes MUST strip language tags.
                        let hashed_values: StringArray = (0..values.len())
                            .map(|i| {
                                if values.is_null(i) {
                                    return None;
                                }
                                let val = values.value(i);
                                let hex = match self.algorithm {
                                    HashAlgorithm::Md5 => {
                                        let mut hasher = Md5::new();
                                        hasher.update(val);
                                        let result = hasher.finalize();
                                        base16ct::lower::encode_string(result.as_slice())
                                    }
                                    HashAlgorithm::Sha1 => {
                                        let mut hasher = Sha1::new();
                                        hasher.update(val);
                                        let result = hasher.finalize();
                                        base16ct::lower::encode_string(result.as_slice())
                                    }
                                    HashAlgorithm::Sha256 => {
                                        let mut hasher = Sha256::new();
                                        hasher.update(val);
                                        let result = hasher.finalize();
                                        base16ct::lower::encode_string(result.as_slice())
                                    }
                                    HashAlgorithm::Sha384 => {
                                        let mut hasher = Sha384::new();
                                        hasher.update(val);
                                        let result = hasher.finalize();
                                        base16ct::lower::encode_string(result.as_slice())
                                    }
                                    HashAlgorithm::Sha512 => {
                                        let mut hasher = Sha512::new();
                                        hasher.update(val);
                                        let result = hasher.finalize();
                                        base16ct::lower::encode_string(result.as_slice())
                                    }
                                };

                                Some(hex)
                            })
                            .collect();

                        let string_array = StringFamily::create_simple_strings_array(
                            Arc::new(hashed_values) as ArrayRef,
                        );

                        tf_encoding.create_array_with_single_family(
                            StringFamily::FAMILY_ID,
                            string_array,
                        )
                    }
                    _ => tf_encoding.create_null_array(child.array().len()),
                })?
                .into_array_ref(),
            _ => exec_err!("Hash function is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_default_encodings, create_standard_test_vector, evaluate_function_for_test,
    };
    use insta::assert_snapshot;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_md5_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(md5_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------+
        | input                                                                                        | MD5(?table?.input)                                                         |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                         |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                                         |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                                         |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                                         |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                                                         |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                                                         |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                                                         |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                                                         |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                                                         |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                                                         |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: edbab45572c72a5d9440b40bcc0500c0, language: }} |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: d6cc43dc2c5114286a76e24191db2840, language: }} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: 5d41402abc4b2a76b9719d911017c592, language: }} |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 202cb962ac59075b964b07152d234b70, language: }} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                                         |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_sha1_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(sha1_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+
        | input                                                                                        | SHA1(?table?.input)                                                                |
        +----------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                 |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                                                 |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                                                 |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                                                 |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                                                                 |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                                                                 |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                                                                 |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                                                                 |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                                                                 |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                                                                 |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: 7e83ca2a65d6f90a809c8570c6c905a941b87732, language: }} |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: 405b5aec5bb7adcf991efe97eaf4c80ef983fe81, language: }} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d, language: }} |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 40bd001563085fc35165329ea1ff5c5ecbdbbeef, language: }} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                                                 |
        +----------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_sha256_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(sha256_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------+
        | input                                                                                        | SHA256(?table?.input)                                                                                      |
        +----------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                                         |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                                                                         |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                                                                         |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                                                                         |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                                                                                         |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                                                                                         |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                                                                                         |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                                                                                         |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                                                                                         |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                                                                                         |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: 7dc96f776c8423e57a2785489a3f9c43fb6e756876d6ad9a9cac4aa4e72ec193, language: }} |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: f9c8bc58fb70985e624a22f3ef9faab71dc1179bcfe015e51e75fb2df59e88f4, language: }} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824, language: }} |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3, language: }} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                                                                         |
        +----------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_sha384_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(sha384_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
        | input                                                                                        | SHA384(?table?.input)                                                                                                                      |
        +----------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                                                                         |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                                                                                                         |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                                                                                                         |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                                                                                                         |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                                                                                                                         |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                                                                                                                         |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                                                                                                                         |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                                                                                                                         |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                                                                                                                         |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                                                                                                                         |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: c038a778b09d0f15057dadb3bd9ad6d76402751fbd96ee0945485d530f385cd2c3c49e6bda494a0a4e51c43284f25af9, language: }} |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: 0f78e0463b4a34f7bb0c8bdb978a49357a192904194f98d9606d68596688201409349c8dcec211813c6ccf24505cd818, language: }} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: 59e1748777448c69de6b800d7a33bbfb9ff1b463e44354c3553bcdb9c666fa90125a3c79f90397bdf5f6a13de828684f, language: }} |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 9a0a82f0c0cf31470d7affede3406cc9aa8410671520b727044eda15b4c25532a9b5cd8aaf9cec4919d76255b6bfb00f, language: }} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                                                                                                         |
        +----------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_sha512_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(sha512_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        | input                                                                                        | SHA512(?table?.input)                                                                                                                                                      |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                                                                                                         |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                                                                                                                                         |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                                                                                                                                         |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                                                                                                                                         |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                                                                                                                                                         |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                                                                                                                                                         |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                                                                                                                                                         |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                                                                                                                                                         |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                                                                                                                                                         |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                                                                                                                                                         |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: 3d7e851b031e23173b21f97b5d149d46b293d6d90182ed5eb615472c03103398cc40e1ae5f07f623e0c61fd0fa591178cff1c90579587a9de7c7876fccabee66, language: }} |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: 46a1eba678ba56928a351f6845f0c69e3989998a61fc3b0a0b5639d90c56a184a73063a87a670a58fb70df07dcdaf61b213751f1f897f6d46d44fe7cc795b55a, language: }} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: 9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043, language: }} |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1eb8b85103e3be7ba613b31bb5c9c36214dc9f14a42fd7a2fdb84856bca5c44c2, language: }} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                                                                                                                                         |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        "
        );
    }
}
