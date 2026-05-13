use crate::benchmarks::windfarm::generate::write_prefixes;
use anyhow::Context;
use rdf_fusion::common::NamedNode;
use std::io::Write;

/// Generates the static part of the data for the windfarm (Chrontext) benchmark.
///
/// This includes:
/// - Wind Farm Sites
/// - Wind Turbines
/// - Generator Systems
/// - Generators
/// - Weather Measuring Systems
pub fn generate_static<W: Write>(
    writer: &mut W,
    num_turbines: usize,
) -> anyhow::Result<()> {
    write_prefixes(writer)?;
    generate_wind_farm_sites(writer)?;
    let turbines = generate_wind_turbines(writer, num_turbines)?;
    generate_generators(writer, &turbines)?;
    generate_weather_measuring_system(writer, &turbines)?;

    Ok(())
}

const WIND_FARM_SITES: [(&str, u32); 4] = [
    ("Wind Mountain", 0),
    ("Gale Valley", 1),
    ("Gusty Plains", 2),
    ("Breezy Field", 3),
];

/// A wind farm site has the following triples:
fn generate_wind_farm_sites<W: Write>(writer: &mut W) -> anyhow::Result<()> {
    for (name, iri_idx) in WIND_FARM_SITES {
        let iri_idx = iri_idx + 1;
        write!(
            writer,
            r#"
wpex:Site{iri_idx} rdf:type rds:Site ;
    rdfs:label "{name}" .
"#
        )?;
    }

    Ok(())
}

/// Generates `n_turbines` wind turbines.
fn generate_wind_turbines<W: Write>(
    writer: &mut W,
    n_turbines: usize,
) -> anyhow::Result<Vec<NamedNode>> {
    const MAX_POWER_VALUES: [u32; 3] = [5_000_000, 10_000_000, 15_000_000];
    let turbines_per_site = n_turbines / WIND_FARM_SITES.len();

    for i in 1..=n_turbines {
        let max_power_value = MAX_POWER_VALUES[i % MAX_POWER_VALUES.len()];
        let site_idx = i / turbines_per_site;
        let idx_within_site = (i % turbines_per_site) + 1;

        write!(
            writer,
            r#"
wpex:WindTurbine{i} rdf:type rds:A ;
    rdfs:label "Wind turbine {i}" ;
    ct:hasTimeSeries wpex:oper{i} ;
    ct:hasStaticProperty wpex:WindTurbineMaximumPower{i} .
wpex:oper{i} ct:hasExternalId "oper{i}" ;
    ct:hasDatatype xsd:boolean ;
    rdfs:label "Operating" .
wpex:WindTurbineMaximumPower{i} rdfs:label "MaximumPower" ;
    ct:hasStaticValue "{max_power_value}"^^xsd:integer .
wpex:Site{site_idx} rds:hasFunctionalAspect wpex:WindTurbineFunctionalAspect{i} .
wpex:WindTurbine{i} rds:hasFunctionalAspectNode wpex:WindTurbineFunctionalAspect{i} .
wpex:WindTurbineFunctionalAspect{i} rdfs:label "A{idx_within_site}" .
"#
        )?;
    }

    let result = (1..=n_turbines)
        .map(|tid| {
            let iri = format!(
                "https://github.com/magbak/chrontext/windpower_example#WindTurbine{tid}",
            );
            NamedNode::new(iri).context("Invalid IRI")
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(result)
}

/// Generates a `GeneratorSystem` and `Generator` for each turbine.
fn generate_generators<W: Write>(
    writer: &mut W,
    turbines: &[NamedNode],
) -> anyhow::Result<()> {
    for (i, turbine) in turbines.iter().enumerate() {
        let i = i + 1;
        write!(
            writer,
            r#"
wpex:GeneratorSystem{i} rdf:type rds:RA ;
    rdfs:label "Weather Measuring System" .
wpex:Generator{i} rdf:type rds:GAA ;
    rdfs:label "Generator" .
{turbine} rds:hasFunctionalAspect wpex:GeneratorSystemFunctionalAspect{i} .
wpex:GeneratorSystem{i} rds:hasFunctionalAspectNode wpex:GeneratorSystemFunctionalAspect{i} .
wpex:GeneratorSystemFunctionalAspect{i} rdfs:label "RA{i}" .
wpex:GeneratorSystem{i} rds:hasFunctionalAspect wpex:GeneratorFunctionalAspect{i} .
wpex:Generator{i} rds:hasFunctionalAspectNode wpex:GeneratorFunctionalAspect{i} .
wpex:GeneratorFunctionalAspect{i} rdfs:label "GAA{i}" .
wpex:Generator{i} ct:hasTimeseries wpex:w{i} .
wpex:w{i} ct:hasExternalId "w{i}" ;
    ct:hasDatatype xsd:double ;
    rdfs:label "Production" .
"#
        )?;
    }

    Ok(())
}

/// Generates a `WeatherMeasuringSystem` and `Generator` for each turbine.
fn generate_weather_measuring_system<W: Write>(
    writer: &mut W,
    turbines: &[NamedNode],
) -> anyhow::Result<()> {
    for (i, turbine) in turbines.iter().enumerate() {
        let i = i + 1;
        write!(
            writer,
            r#"
wpex:WeatherMeasuringSystem{i} rdf:type rds:LE ;
    rdfs:label "Weather Measuring System" .
{turbine} rds:hasFunctionalAspect wpex:WMSFunctionalAspect{i} .
wpex:WeatherMeasuringSystem{i} rds:hasFunctionalAspectNode wpex:WMSFunctionalAspect{i} .
wpex:WMSFunctionalAspect{i} rdfs:label "LE{i}" .
wpex:WeatherMeasuringSystem{i} ct:hasTimeseries wpex:wsp{i} .
wpex:wsp{i} ct:hasExternalId "wsp{i}" ;
    ct:hasDatatype xsd:double ;
    rdfs:label "Windspeed" .
wpex:WeatherMeasuringSystem{i} ct:hasTimeseries wpex:wdir{i} .
wpex:wdir{i} ct:hasExternalId "wdir{i}" ;
    ct:hasDatatype xsd:double ;
    rdfs:label "WindDirection" .
"#
        )?;
    }

    Ok(())
}
