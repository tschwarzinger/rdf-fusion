use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::TypeSignature;
use rdf_fusion_encoding::TermEncoding;
use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

/// Defines the arity of a SPARQL operation and provides helper methods for creating signatures.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum SparqlUDFArity {
    /// No arguments.
    Nullary,
    /// A fixed number of arguments.
    Fixed(NonZeroUsize),
    /// One of the given [SparqlUDFArity].
    OneOf(Vec<SparqlUDFArity>),
    /// Any number of arguments (including zero).
    Variadic,
}

impl SparqlUDFArity {
    /// Returns a [TypeSignature] for the given [SparqlUDFArity].
    pub fn type_signature<TEncoding: TermEncoding>(
        &self,
        encoding: &TEncoding,
    ) -> TypeSignature {
        self.type_signature_for_data_type(encoding.data_type())
    }

    /// Returns a [TypeSignature] for the given [SparqlUDFArity].
    pub fn type_signature_for_data_type(&self, data_type: &DataType) -> TypeSignature {
        match self {
            SparqlUDFArity::Nullary => TypeSignature::Nullary,
            SparqlUDFArity::Fixed(n) => {
                TypeSignature::Uniform(n.get(), vec![data_type.clone()])
            }
            SparqlUDFArity::OneOf(ns) => {
                let inner = ns
                    .iter()
                    .map(|n| n.type_signature_for_data_type(data_type))
                    .collect::<Vec<_>>();
                TypeSignature::OneOf(inner)
            }
            SparqlUDFArity::Variadic => TypeSignature::OneOf(vec![
                TypeSignature::Nullary,
                TypeSignature::Variadic(vec![data_type.clone()]),
            ]),
        }
    }
}

pub struct NoEncodings;
pub struct HasEncodings;

/// A helper for building [`TypeSignature`] tailored for SPARQL operations.
///
/// We use a `State` argument to track the builder state.
///
/// # Example
///
/// ```
/// use datafusion::logical_expr_common::signature::{Signature, Volatility};
/// use rdf_fusion_functions::scalar::SparqlUDFTypeSignatureBuilder;
/// use rdf_fusion_encoding::RdfFusionEncodings;
///
/// #[derive(Clone, PartialEq, Eq, Hash)]
/// # #[allow(dead_code)]
/// struct MySparqlUDF {
///     name: String,
///     signature: Signature,
/// }
///
/// impl MySparqlUDF {
///     # #[allow(dead_code)]
///     fn new(encodings: RdfFusionEncodings) -> Self {
///         // Convenient builder for the type signature
///         let type_signature = SparqlUDFTypeSignatureBuilder::new()
///             .with_supported_encoding(encodings.typed_family().as_ref())
///             .with_unary_arity()
///             .with_binary_arity()
///             .build();
///         Self {
///             name: "<http://www.example.com/sparql/MyOp>".to_owned(),
///             signature: Signature::new(type_signature, Volatility::Immutable),
///         }
///     }
/// }
///
/// // ... Implement the rest of the operation
/// ```
pub struct SparqlUDFTypeSignatureBuilder<State = NoEncodings> {
    supported_data_types: Vec<DataType>,
    supported_arities: BTreeSet<SparqlUDFArity>,
    _marker: PhantomData<State>,
}

impl<State> SparqlUDFTypeSignatureBuilder<State> {
    /// Adds a specific arity.
    pub fn with_arity(mut self, arity: SparqlUDFArity) -> Self {
        self.supported_arities.insert(arity);
        self
    }

    /// Adds support for nullary function calls.
    pub fn with_nullary_arity(self) -> Self {
        self.with_arity(SparqlUDFArity::Nullary)
    }

    /// Adds support for unary function calls.
    pub fn with_unary_arity(self) -> Self {
        self.with_arity(SparqlUDFArity::Fixed(NonZeroUsize::new(1).unwrap()))
    }

    /// Adds support for binary function calls.
    pub fn with_binary_arity(self) -> Self {
        self.with_arity(SparqlUDFArity::Fixed(NonZeroUsize::new(2).unwrap()))
    }

    /// Adds support for ternary function calls.
    pub fn with_ternary_arity(self) -> Self {
        self.with_arity(SparqlUDFArity::Fixed(NonZeroUsize::new(3).unwrap()))
    }

    /// Adds support for variadic function calls.
    pub fn with_variadic_arity(self) -> Self {
        self.with_arity(SparqlUDFArity::Variadic)
    }
}

impl SparqlUDFTypeSignatureBuilder<NoEncodings> {
    /// Creates a completely empty builder.
    pub fn new() -> Self {
        Self {
            supported_data_types: Vec::new(),
            supported_arities: BTreeSet::new(),
            _marker: PhantomData,
        }
    }

    /// Adds the first encoding, transitioning the state to `HasEncodings`.
    pub fn with_supported_encoding<TEncoding: TermEncoding>(
        mut self,
        encoding: &TEncoding,
    ) -> SparqlUDFTypeSignatureBuilder<HasEncodings> {
        self.supported_data_types.push(encoding.data_type().clone());
        SparqlUDFTypeSignatureBuilder {
            supported_data_types: self.supported_data_types,
            supported_arities: self.supported_arities,
            _marker: PhantomData,
        }
    }
}

impl Default for SparqlUDFTypeSignatureBuilder<NoEncodings> {
    fn default() -> Self {
        Self::new()
    }
}

impl SparqlUDFTypeSignatureBuilder<HasEncodings> {
    /// Adds support for additional typed family encodings.
    /// Stays in the `HasEncodings` state.
    pub fn with_supported_encoding<TEncoding: TermEncoding>(
        mut self,
        encoding: &TEncoding,
    ) -> Self {
        self.supported_data_types.push(encoding.data_type().clone());
        self
    }

    /// Adds the first encoding, transitioning the state to `HasEncodings`.
    pub fn with_supported_encoding_opt<TEncoding: TermEncoding>(
        self,
        encoding: Option<&TEncoding>,
    ) -> SparqlUDFTypeSignatureBuilder<HasEncodings> {
        if let Some(encoding) = encoding {
            self.with_supported_encoding(encoding)
        } else {
            self
        }
    }

    /// Creates a [`TypeSignature`] for the given supported encodings.
    /// Notice this no longer returns a `Result`! It's infallible.
    pub fn build(self) -> TypeSignature {
        let mut type_signatures = Vec::new();
        for data_type in self.supported_data_types {
            let type_signatures_for_data_type = self
                .supported_arities
                .iter()
                .map(|a| a.type_signature_for_data_type(&data_type))
                .collect::<Vec<_>>();

            if type_signatures_for_data_type.len() == 1 {
                type_signatures.push(type_signatures_for_data_type[0].clone());
            } else {
                type_signatures.push(TypeSignature::OneOf(type_signatures_for_data_type));
            }
        }

        if type_signatures.len() == 1 {
            type_signatures[0].clone()
        } else {
            TypeSignature::OneOf(type_signatures)
        }
    }
}
