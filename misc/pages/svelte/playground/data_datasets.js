export const DATASETS = [
    {
        id: "bsbm-1000",
        name: "BSBM 1000",
        scale: "1,000 products (~367k triples)",
        queryGroup: "BSBM",
        description: "The Berlin SPARQL Benchmark (BSBM) is an industry-standard SPARQL benchmark based on an e-commerce use case. It simulates a realistic web shop with vendors, products, consumer reviews, and price offers.",
        docUrl: "http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/",
        docLabel: "Official BSBM Specification",
        distributions: [
            {
                id: "bsbm-1000-parquet",
                name: "Parquet (GPOS)",
                format: "Parquet",
                description: "Pre-sorted Parquet file encoded with Graph-Predicate-Object-Subject (GPOS) sorting for high-efficiency SPARQL triple pattern queries.",
                url: "https://rdf-fusion-public.hel1.your-objectstorage.com/datasets/bsbm-1000/bsbm-1000-gpos.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            },
            {
                id: "bsbm-1000-parquet-gspo",
                name: "Parquet (GSPO)",
                format: "Parquet",
                description: "Pre-sorted Parquet file encoded with Graph-Subject-Predicate-Object (GSPO) sorting for high-efficiency SPARQL triple pattern queries.",
                url: "https://rdf-fusion-public.hel1.your-objectstorage.com/datasets/bsbm-1000/bsbm-1000-gspo.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            },
            {
                id: "bsbm-1000-parquet-gosp",
                name: "Parquet (GOSP)",
                format: "Parquet",
                description: "Pre-sorted Parquet file encoded with Graph-Object-Subject-Predicate (GOSP) sorting for high-efficiency SPARQL triple pattern queries.",
                url: "https://rdf-fusion-public.hel1.your-objectstorage.com/datasets/bsbm-1000/bsbm-1000-gosp.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            }
        ]
    },
    {
        id: "bsbm-10000",
        name: "BSBM 10000",
        scale: "10,000 products (~3.5M quads)",
        queryGroup: "BSBM",
        description: "The Berlin SPARQL Benchmark (BSBM) is an industry-standard SPARQL benchmark based on an e-commerce use case. This scaled dataset features 10,000 products and 3,564,773 (~3.5M) quads.",
        docUrl: "http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/",
        docLabel: "Official BSBM Specification",
        distributions: [
            {
                id: "bsbm-10000-parquet",
                name: "Parquet (GPOS)",
                format: "Parquet",
                description: "Pre-sorted Parquet file encoded with Graph-Predicate-Object-Subject (GPOS) sorting for high-efficiency SPARQL triple pattern queries.",
                url: "https://rdf-fusion-public.hel1.your-objectstorage.com/datasets/bsbm-10000/bsbm-10000-gpos.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            },
            {
                id: "bsbm-10000-parquet-gspo",
                name: "Parquet (GSPO)",
                format: "Parquet",
                description: "Pre-sorted Parquet file encoded with Graph-Subject-Predicate-Object (GSPO) sorting for high-efficiency SPARQL triple pattern queries.",
                url: "https://rdf-fusion-public.hel1.your-objectstorage.com/datasets/bsbm-10000/bsbm-10000-gspo.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            },
            {
                id: "bsbm-10000-parquet-gosp",
                name: "Parquet (GOSP)",
                format: "Parquet",
                description: "Pre-sorted Parquet file encoded with Graph-Object-Subject-Predicate (GOSP) sorting for high-efficiency SPARQL triple pattern queries.",
                url: "https://rdf-fusion-public.hel1.your-objectstorage.com/datasets/bsbm-10000/bsbm-10000-gosp.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            }
        ]
    }
];
