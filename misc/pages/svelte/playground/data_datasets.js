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
                url: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/datasets/bsbm-1000/bsbm-1000-gpos.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            },
            {
                id: "bsbm-1000-parquet-gspo",
                name: "Parquet (GSPO)",
                format: "Parquet",
                description: "Pre-sorted Parquet file encoded with Graph-Subject-Predicate-Object (GSPO) sorting for high-efficiency SPARQL triple pattern queries.",
                url: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/datasets/bsbm-1000/bsbm-1000-gspo.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            },
            {
                id: "bsbm-1000-parquet-gosp",
                name: "Parquet (GOSP)",
                format: "Parquet",
                description: "Pre-sorted Parquet file encoded with Graph-Object-Subject-Predicate (GOSP) sorting for high-efficiency SPARQL triple pattern queries.",
                url: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/datasets/bsbm-1000/bsbm-1000-gosp.rdf.parquet",
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
                url: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/datasets/bsbm-10000/bsbm-10000-gpos.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            },
            {
                id: "bsbm-10000-parquet-gspo",
                name: "Parquet (GSPO)",
                format: "Parquet",
                description: "Pre-sorted Parquet file encoded with Graph-Subject-Predicate-Object (GSPO) sorting for high-efficiency SPARQL triple pattern queries.",
                url: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/datasets/bsbm-10000/bsbm-10000-gspo.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            },
            {
                id: "bsbm-10000-parquet-gosp",
                name: "Parquet (GOSP)",
                format: "Parquet",
                description: "Pre-sorted Parquet file encoded with Graph-Object-Subject-Predicate (GOSP) sorting for high-efficiency SPARQL triple pattern queries.",
                url: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/datasets/bsbm-10000/bsbm-10000-gosp.rdf.parquet",
                quadStorage: { type: "parquet", version: "0.1" }
            }
        ]
    },
    {
        id: "dblp",
        name: "dblp",
        scale: "dblp dump of 2026-09-01 (~575M triples)",
        queryGroup: "dblp",
        description: "The dblp computer science bibliography as RDF. It describes publications, authors, venues, and their identifiers (e.g., DOIs and ORCIDs). As the dataset is large, queries should be selective (e.g., by restricting the predicate or subject).",
        docUrl: "https://dblp.org/rdf/docu/",
        docLabel: "dblp RDF Documentation",
        distributions: [
            {
                id: "dblp-2026-09-01-parquet",
                name: "Parquet (GPOS, GSPO, GOSP)",
                format: "Parquet",
                description: "A Parquet dataset that stores the quads sorted in the GPOS, GSPO, and GOSP orders. For each triple pattern, the engine scans the quad table whose sort order matches best. The dataset.ttl file describes the files and their statistics, which allows skipping files without opening them.",
                url: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/datasets/dblp/dblp-2026-09-01-parquet/",
                quadStorage: { type: "parquet", version: "0.2" },
                // The dataset consists of many files (~30 GB) and cannot be stored in IndexedDB.
                downloadable: false
            }
        ]
    }
];