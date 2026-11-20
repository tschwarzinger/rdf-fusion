use crate::aggregates::{
    group_concat_typed_family, max_typed_family, min_typed_family, sparql_avg,
    sparql_count, sparql_sum,
};
use crate::scalar::as_sortable_bytes_udf;
use crate::scalar::comparison::{
    equal_udf, greater_or_equal_udf, greater_than_udf, is_compatible_udf,
    less_or_equal_udf, less_than_udf,
};
use crate::scalar::conversion::encoding::{
    decode_term, with_plain_term_encoding, with_string_encoding,
    with_typed_family_encoding,
};
use crate::scalar::conversion::native::{
    effective_boolean_value_udf, native_boolean_as_term, native_int64_as_term,
};
use crate::scalar::conversion::{
    cast_boolean_udf, cast_datetime_udf, cast_decimal_udf, cast_double_udf,
    cast_float_udf, cast_int_udf, cast_integer_udf, cast_string_udf,
};
use crate::scalar::dates_and_times::{
    day_udf, hours_udf, minutes_udf, month_udf, seconds_udf, timezone_udf, tz_udf,
    year_udf,
};
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::functional_form::{bound_udf, coalesce_udf, sparql_if_udf};
use crate::scalar::numeric::{
    abs_udf, add_udf, ceil_udf, div_udf, floor_udf, mul_udf, rand_udf, round_udf,
    sub_udf, unary_minus_udf, unary_plus_udf,
};
use crate::scalar::strings::{
    concat_udf, contains_udf, encode_for_uri_udf, lang_matches_udf, lcase_udf, md5_udf,
    regex_udf, replace_udf, sha1_udf, sha256_udf, sha384_udf, sha512_udf, str_after_udf,
    str_before_udf, str_ends_udf, str_starts_udf, strlen_udf, sub_str_udf, ucase_udf,
};
use crate::scalar::terms::{
    bnode_udf, datatype_udf, iri_udf, is_blank_udf, is_iri_udf, is_literal_udf,
    is_numeric_udf, lang_udf, str_udf, strdt_udf, strlang_udf, struuid_udf, uuid_udf,
};
use datafusion::common::plan_datafusion_err;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TypeSignature};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingName, RdfFusionEncodings};
use rdf_fusion_extensions::functions::{FunctionName, RdfFusionFunctionRegistry};
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::iter::once;
use std::sync::{Arc, RwLock, RwLockWriteGuard};

/// The default implementation of the `RdfFusionFunctionRegistry` trait.
///
/// This registry provides implementations for all standard SPARQL functions
/// defined in the SPARQL 1.1 specification, mapping them to their corresponding
/// DataFusion UDFs and UDAFs.
///
/// # Additional Resources
/// - [SPARQL 1.1 Query Language - Function Library](https://www.w3.org/TR/sparql11-query/#SparqlUDFs)
pub struct DefaultRdfFusionFunctionRegistry {
    /// The registered encodings.
    encodings: RdfFusionEncodings,
    /// The actual storage for the registered functions.
    ///
    /// Note that this registry is currently *not* connected to the
    /// [`SessionContext`](::datafusion::prelude::SessionContext) of the DataFusion engine.
    inner: Arc<RwLock<RegistryContent>>,
}

/// The actual data storage of the registry.
pub struct RegistryContent {
    /// The supported encodings for each function.
    ///
    /// Currently, this is not needed for aggregate functions as they only support typed values.
    udf_encodings: HashMap<String, Vec<EncodingName>>,
    /// The scalar function registry
    udfs: HashMap<String, Arc<ScalarUDF>>,
    /// The aggregate function registry
    udafs: HashMap<String, Arc<AggregateUDF>>,
}

impl Debug for DefaultRdfFusionFunctionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultRdfFusionFunctionRegistry")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl DefaultRdfFusionFunctionRegistry {
    /// Create a new [DefaultRdfFusionFunctionRegistry].
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        let mut registry = Self {
            encodings,
            inner: Arc::new(RwLock::new(RegistryContent {
                udf_encodings: HashMap::default(),
                udfs: HashMap::default(),
                udafs: HashMap::default(),
            })),
        };
        register_functions(&mut registry)
            .expect("Default functions should be registered without errors");
        registry
    }

    /// Produce an error if the given function's name or aliases have been registered already.
    fn validate_function_names(
        &self,
        lock: &RwLockWriteGuard<'_, RegistryContent>,
        names: &[String],
    ) -> DFResult<()> {
        for name in names {
            if lock.udfs.contains_key(name) {
                Err(SparqlUDFCreationError::new(format!(
                    "Attempted to override an existing user-defined function with name or alias: {name}"
                )))?
            } else if lock.udafs.contains_key(name) {
                Err(SparqlUDFCreationError::new(format!(
                    "Attempted to override an existing user-defined aggregate function with name or alias: {name}"
                )))?
            }
        }
        Ok(())
    }
}

impl RdfFusionFunctionRegistry for DefaultRdfFusionFunctionRegistry {
    fn udf_supported_encodings(
        &self,
        function_name: &FunctionName,
    ) -> DFResult<Vec<EncodingName>> {
        self.inner
            .read()
            .expect("Poisoned lock.")
            .udf_encodings
            .get(&function_name.to_string())
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Function '{function_name}' not found"))
    }

    fn udf(&self, function_name: &FunctionName) -> DFResult<Arc<ScalarUDF>> {
        self.inner
            .read()
            .expect("Poisoned lock.")
            .udfs
            .get(&function_name.to_string())
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Function '{function_name}' not found"))
    }

    fn udaf(&self, function_name: &FunctionName) -> DFResult<Arc<AggregateUDF>> {
        self.inner
            .read()
            .expect("Poisoned lock.")
            .udafs
            .get(&function_name.to_string())
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Function '{function_name}' not found"))
    }

    fn register_udf(&self, udf: Arc<ScalarUDF>) -> DFResult<()> {
        let supported_encodings =
            supported_encodings(&self.encodings, &udf.signature().type_signature);
        let names = once(udf.name().to_owned())
            .chain(udf.aliases().iter().cloned())
            .collect::<Vec<_>>();
        let mut lock = self.inner.write().expect("Poisoned lock.");
        self.validate_function_names(&lock, &names)?;
        for name in names {
            lock.udf_encodings.entry(name.clone()).or_default();
            lock.udf_encodings
                .get_mut(&name)
                .unwrap()
                .extend(supported_encodings.iter().copied());

            lock.udfs.insert(name.clone(), Arc::clone(&udf));
        }
        Ok(())
    }

    fn register_udaf(&self, udaf: Arc<AggregateUDF>) -> DFResult<()> {
        let mut lock = self.inner.write().expect("Poisoned lock.");
        let names = once(udaf.name().to_owned())
            .chain(udaf.aliases().iter().cloned())
            .collect::<Vec<_>>();
        self.validate_function_names(&lock, &names)?;
        for name in names {
            lock.udafs.insert(name.to_owned(), Arc::clone(&udaf));
        }
        Ok(())
    }

    fn unregister_udf(&self, name_or_alias: &str) -> Option<Arc<ScalarUDF>> {
        let mut lock = self.inner.write().expect("Poisoned lock.");
        if let Some(udf) = lock.udfs.remove(name_or_alias) {
            once(udf.name().to_owned())
                .chain(udf.aliases().iter().cloned())
                .for_each(|n| {
                    lock.udfs.remove(&n);
                });
            Some(udf)
        } else {
            None
        }
    }

    fn unregister_udaf(&self, name_or_alias: &str) -> Option<Arc<AggregateUDF>> {
        let mut lock = self.inner.write().expect("Poisoned lock.");
        if let Some(udaf) = lock.udafs.remove(name_or_alias) {
            once(udaf.name().to_owned())
                .chain(udaf.aliases().iter().cloned())
                .for_each(|n| {
                    lock.udafs.remove(&n);
                });
            Some(udaf)
        } else {
            None
        }
    }

    fn udfs(&self) -> Vec<Arc<ScalarUDF>> {
        self.inner
            .read()
            .expect("Poisoned lock.")
            .udfs
            .values()
            .cloned()
            .collect()
    }

    fn udafs(&self) -> Vec<Arc<AggregateUDF>> {
        self.inner
            .read()
            .expect("Poisoned lock.")
            .udafs
            .values()
            .cloned()
            .collect()
    }
}

/// Computes the supported encodings from the given type signature.
fn supported_encodings(
    encodings: &RdfFusionEncodings,
    signature: &TypeSignature,
) -> BTreeSet<EncodingName> {
    match signature {
        TypeSignature::Variadic(data_type) => data_type
            .iter()
            .flat_map(|dt| encodings.encodings_with_data_type(dt))
            .collect(),
        TypeSignature::Uniform(_, data_type) => data_type
            .iter()
            .flat_map(|dt| encodings.encodings_with_data_type(dt))
            .collect(),
        TypeSignature::OneOf(inner) => inner
            .iter()
            .flat_map(|ts| supported_encodings(encodings, ts).into_iter())
            .collect(),
        _ => BTreeSet::new(), // Unsupported type signature.
    }
}

fn register_functions(registry: &mut DefaultRdfFusionFunctionRegistry) -> DFResult<()> {
    let scalar_fns: Vec<ScalarUDF> = vec![
        str_udf(registry.encodings.clone())?,
        lang_udf(registry.encodings.clone())?,
        lang_matches_udf(registry.encodings.clone())?,
        datatype_udf(registry.encodings.clone())?,
        bnode_udf(registry.encodings.clone())?,
        rand_udf(registry.encodings.clone())?,
        abs_udf(registry.encodings.clone())?,
        ceil_udf(registry.encodings.clone())?,
        floor_udf(registry.encodings.clone())?,
        round_udf(registry.encodings.clone())?,
        concat_udf(registry.encodings.clone())?,
        sub_str_udf(registry.encodings.clone())?,
        strlen_udf(registry.encodings.clone())?,
        replace_udf(registry.encodings.clone())?,
        ucase_udf(registry.encodings.clone())?,
        lcase_udf(registry.encodings.clone())?,
        encode_for_uri_udf(registry.encodings.clone())?,
        contains_udf(registry.encodings.clone())?,
        str_starts_udf(registry.encodings.clone())?,
        str_ends_udf(registry.encodings.clone())?,
        str_before_udf(registry.encodings.clone())?,
        str_after_udf(registry.encodings.clone())?,
        year_udf(registry.encodings.clone())?,
        month_udf(registry.encodings.clone())?,
        day_udf(registry.encodings.clone())?,
        hours_udf(registry.encodings.clone())?,
        minutes_udf(registry.encodings.clone())?,
        seconds_udf(registry.encodings.clone())?,
        timezone_udf(registry.encodings.clone())?,
        tz_udf(registry.encodings.clone())?,
        uuid_udf(registry.encodings.clone())?,
        struuid_udf(registry.encodings.clone())?,
        md5_udf(registry.encodings.clone())?,
        sha1_udf(registry.encodings.clone())?,
        sha256_udf(registry.encodings.clone())?,
        sha384_udf(registry.encodings.clone())?,
        sha512_udf(registry.encodings.clone())?,
        strlang_udf(registry.encodings.clone())?,
        strdt_udf(registry.encodings.clone())?,
        is_iri_udf(registry.encodings.clone())?,
        is_blank_udf(registry.encodings.clone())?,
        is_literal_udf(registry.encodings.clone())?,
        is_numeric_udf(registry.encodings.clone())?,
        regex_udf(registry.encodings.clone())?,
        bound_udf(registry.encodings.clone())?,
        as_sortable_bytes_udf(registry.encodings.clone()),
        coalesce_udf(registry.encodings.clone()),
        sparql_if_udf(registry.encodings.clone())?,
        equal_udf(registry.encodings.clone())?,
        greater_than_udf(registry.encodings.clone())?,
        greater_or_equal_udf(registry.encodings.clone())?,
        less_than_udf(registry.encodings.clone())?,
        less_or_equal_udf(registry.encodings.clone())?,
        add_udf(registry.encodings.clone())?,
        div_udf(registry.encodings.clone())?,
        mul_udf(registry.encodings.clone())?,
        sub_udf(registry.encodings.clone())?,
        unary_minus_udf(registry.encodings.clone())?,
        unary_plus_udf(registry.encodings.clone())?,
        cast_string_udf(registry.encodings.clone())?,
        cast_integer_udf(registry.encodings.clone())?,
        cast_int_udf(registry.encodings.clone())?,
        cast_float_udf(registry.encodings.clone())?,
        cast_double_udf(registry.encodings.clone())?,
        cast_decimal_udf(registry.encodings.clone())?,
        cast_datetime_udf(registry.encodings.clone())?,
        cast_boolean_udf(registry.encodings.clone())?,
        iri_udf(registry.encodings.clone())?,
        with_plain_term_encoding(registry.encodings.clone()),
        with_typed_family_encoding(registry.encodings.clone()),
        with_string_encoding(registry.encodings.clone()),
        effective_boolean_value_udf(registry.encodings.clone())?,
        is_compatible_udf(registry.encodings.clone())?,
    ];

    for udf in scalar_fns {
        registry.register_udf(Arc::new(udf))?;
    }

    if let Some(decode) = decode_term(registry.encodings.clone()) {
        registry.register_udf(Arc::new(decode))?;
    }

    // Native conversion functions
    let native_fns = vec![
        native_boolean_as_term(Arc::clone(registry.encodings.typed_family())),
        native_int64_as_term(Arc::clone(registry.encodings.typed_family())),
    ];

    for udf in native_fns {
        registry.register_udf(Arc::new(udf))?;
    }

    // Aggregate functions
    let aggregate_fns: Vec<AggregateUDF> = vec![
        sparql_count(Arc::clone(registry.encodings.typed_family())),
        sparql_sum(Arc::clone(registry.encodings.typed_family())),
        min_typed_family(Arc::clone(registry.encodings.typed_family())),
        max_typed_family(Arc::clone(registry.encodings.typed_family())),
        sparql_avg(Arc::clone(registry.encodings.typed_family())),
        group_concat_typed_family(Arc::clone(registry.encodings.typed_family())),
    ];

    for udaf_information in aggregate_fns {
        registry.register_udaf(Arc::new(udaf_information))?;
    }

    Ok(())
}
