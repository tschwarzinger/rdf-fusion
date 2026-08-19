export const queryLibrary = {
    "Basic SPARQL": [
        {
            name: "Select All Triples",
            query: `SELECT *
WHERE {
  ?s ?p ?o .
}
LIMIT 10`
        },
        {
            name: "Count Triples",
            query: `SELECT (COUNT(*) AS ?count)
WHERE {
  ?s ?p ?o .
}`
        },
        {
            name: "ASK - Does Any Data Exist?",
            query: `ASK { ?s ?p ?o }`
        },
        {
            name: "Distinct Predicates",
            query: `SELECT DISTINCT ?predicate
WHERE { ?s ?predicate ?o }
ORDER BY ?predicate`
        },
        {
            name: "Most Common Predicates",
            query: `SELECT ?predicate (COUNT(*) AS ?triples)
WHERE { ?s ?predicate ?o }
GROUP BY ?predicate
ORDER BY DESC(?triples)
LIMIT 10`
        },
        {
            name: "Two-Hop Connections",
            query: `SELECT DISTINCT ?start ?middle ?end
WHERE {
  ?start ?p1 ?middle .
  ?middle ?p2 ?end .
  FILTER (?start != ?end && ?middle != ?end)
}
LIMIT 10`
        },
        {
            name: "Instance Count per Type",
            query: `SELECT ?type (COUNT(?resource) AS ?instances)
WHERE { ?resource a ?type }
GROUP BY ?type
ORDER BY DESC(?instances)
LIMIT 10`
        },
        {
            name: "Resources Linking a Literal and an IRI",
            query: `SELECT DISTINCT ?resource
WHERE {
  ?resource ?p1 ?object1 .
  ?resource ?p2 ?object2 .
  FILTER (isLiteral(?object1) && isIRI(?object2))
}
LIMIT 20`
        }
    ],
    "BSBM Explore": [
        {
            name: "Query 1: Products by Features & Value",
            query: `PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?product ?label
WHERE {
  ?product rdfs:label ?label .
  ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType66> .
  ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature3> .
  ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1967> .
  ?product bsbm:productPropertyNumeric1 ?value1 .
  FILTER (?value1 > 136)
}
ORDER BY ?label
LIMIT 10`
        },
        {
            name: "Query 2: Product Details & Features",
            query: `PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>

SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2
  ?propertyTextual3 ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5
  ?propertyNumeric4
WHERE {
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> rdfs:label ?label .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> rdfs:comment ?comment .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:producer ?p .
  ?p rdfs:label ?producer .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> dc:publisher ?p .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productFeature ?f .
  ?f rdfs:label ?productFeature .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual1 ?propertyTextual1 .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual2 ?propertyTextual2 .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual3 ?propertyTextual3 .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyNumeric1 ?propertyNumeric1 .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyNumeric2 ?propertyNumeric2 .
  OPTIONAL {
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual4 ?propertyTextual4 .
  }
  OPTIONAL {
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual5 ?propertyTextual5 .
  }
  OPTIONAL {
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyNumeric4 ?propertyNumeric4 .
  }
}`
        },
        {
            name: "Query 3: Numeric Filter with Negation",
            query: `PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?product ?label
WHERE {
  ?product rdfs:label ?label .
  ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType87> .
  ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature541> .
  ?product bsbm:productPropertyNumeric1 ?p1 .
  FILTER (?p1 > 156)
  ?product bsbm:productPropertyNumeric3 ?p3 .
  FILTER (?p3 < 152)
  OPTIONAL {
    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature553> .
    ?product rdfs:label ?testVar .
  }
  FILTER (!bound(?testVar))
}
ORDER BY ?label
LIMIT 10`
        },
        {
            name: "Query 4: Union of Features & Properties",
            query: `PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?product ?label ?propertyTextual
WHERE {
  {
    ?product rdfs:label ?label .
    ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType138> .
    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature4305> .
    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1427> .
    ?product bsbm:productPropertyTextual1 ?propertyTextual .
    ?product bsbm:productPropertyNumeric1 ?p1 .
    FILTER (?p1 > 457)
  }
  UNION
  {
    ?product rdfs:label ?label .
    ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType138> .
    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature4305> .
    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1444> .
    ?product bsbm:productPropertyTextual1 ?propertyTextual .
    ?product bsbm:productPropertyNumeric2 ?p2 .
    FILTER (?p2 > 488)
  }
}
ORDER BY ?label
OFFSET 5
LIMIT 10`
        },
        {
            name: "Query 5: Find Similar Products",
            query: `PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>

SELECT DISTINCT ?product ?productLabel
WHERE {
  ?product rdfs:label ?productLabel .
  FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> != ?product)
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productFeature ?prodFeature .
  ?product bsbm:productFeature ?prodFeature .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productPropertyNumeric1 ?origProperty1 .
  ?product bsbm:productPropertyNumeric1 ?simProperty1 .
  FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productPropertyNumeric2 ?origProperty2 .
  ?product bsbm:productPropertyNumeric2 ?simProperty2 .
  FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170))
}
ORDER BY ?productLabel
LIMIT 5`
        },
        {
            name: "Query 7: Product Offers and Reviews",
            query: `PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rev: <http://purl.org/stuff/rev#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>

SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle ?reviewer
  ?revName ?rating1 ?rating2
WHERE {
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer17/Product801> rdfs:label ?productLabel .
  OPTIONAL {
    ?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer17/Product801> .
    ?offer bsbm:price ?price .
    ?offer bsbm:vendor ?vendor .
    ?vendor rdfs:label ?vendorTitle .
    ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> .
    ?offer dc:publisher ?vendor .
    ?offer bsbm:validTo ?date .
    FILTER (?date > "2008-06-20T00:00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>)
  }
  OPTIONAL {
    ?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer17/Product801> .
    ?review rev:reviewer ?reviewer .
    ?reviewer foaf:name ?revName .
    ?review dc:title ?revTitle .
    OPTIONAL {
      ?review bsbm:rating1 ?rating1 .
    }
    OPTIONAL {
      ?review bsbm:rating2 ?rating2 .
    }
  }
}`
        },
        {
            name: "Query 8: Reviews with Language Filter",
            query: `PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX rev: <http://purl.org/stuff/rev#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 ?rating2 ?rating3
  ?rating4
WHERE {
  ?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer12/Product578> .
  ?review dc:title ?title .
  ?review rev:text ?text .
  FILTER langMatches(lang(?text), "EN")
  ?review bsbm:reviewDate ?reviewDate .
  ?review rev:reviewer ?reviewer .
  ?reviewer foaf:name ?reviewerName .
  OPTIONAL {
    ?review bsbm:rating1 ?rating1 .
  }
  OPTIONAL {
    ?review bsbm:rating2 ?rating2 .
  }
  OPTIONAL {
    ?review bsbm:rating3 ?rating3 .
  }
  OPTIONAL {
    ?review bsbm:rating4 ?rating4 .
  }
}
ORDER BY DESC(?reviewDate)
LIMIT 20`
        },
        {
            name: "Query 9: Describe Reviewer",
            query: `PREFIX rev: <http://purl.org/stuff/rev#>

DESCRIBE ?x
WHERE {
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review4194> rev:reviewer ?x .
}`
        },
        {
            name: "Query 10: Fast Delivery Offers",
            query: `PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>

SELECT DISTINCT ?offer ?price
WHERE {
  ?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer16/Product740> .
  ?offer bsbm:vendor ?vendor .
  ?offer dc:publisher ?vendor .
  ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
  ?offer bsbm:deliveryDays ?deliveryDays .
  FILTER (?deliveryDays <= 3)
  ?offer bsbm:price ?price .
  ?offer bsbm:validTo ?date .
  FILTER (?date > "2008-06-20T00:00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>)
}
ORDER BY xsd:double(str(?price))
LIMIT 10`
        },
        {
            name: "Query 11: Property-Value Lookup (Union)",
            query: `SELECT ?property ?hasValue ?isValueOf
WHERE {
  {
    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1250> ?property ?hasValue .
  }
  UNION
  {
    ?isValueOf ?property <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1250> .
  }
}`
        },
        {
            name: "Query 12: Construct Offer Export",
            query: `PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rev: <http://purl.org/stuff/rev#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>

CONSTRUCT {
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:product ?productURI .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:productlabel ?productlabel .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:vendor ?vendorname .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:vendorhomepage ?vendorhomepage .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:offerURL ?offerURL .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:price ?price .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:deliveryDays ?deliveryDays .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:validuntil ?validTo .
}
WHERE {
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:product ?productURI .
  ?productURI rdfs:label ?productlabel .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:vendor ?vendorURI .
  ?vendorURI rdfs:label ?vendorname .
  ?vendorURI foaf:homepage ?vendorhomepage .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:offerWebpage ?offerURL .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:price ?price .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:deliveryDays ?deliveryDays .
  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:validTo ?validTo .
}`
        }
    ],
    "BSBM Business Intelligence": [
        {
            name: "BI Query 1: Product Types by Review Count",
            query: `PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX rev: <http://purl.org/stuff/rev#>

SELECT ?productType ?reviewCount
WHERE {
  {
    SELECT ?productType (COUNT(?review) AS ?reviewCount)
    WHERE {
      ?productType a bsbm:ProductType .
      ?product a ?productType .
      ?product bsbm:producer ?producer .
      ?producer bsbm:country <http://downlode.org/rdf/iso-3166/countries#AT> .
      ?review bsbm:reviewFor ?product .
      ?review rev:reviewer ?reviewer .
      ?reviewer bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
    }
    GROUP BY ?productType
  }
}
ORDER BY DESC(?reviewCount) ?productType
LIMIT 10`
        },
        {
            name: "BI Query 2: Products with Shared Features",
            query: `PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>

SELECT ?otherProduct ?sameFeatures
WHERE {
  ?otherProduct a bsbm:Product .
  FILTER (?otherProduct != <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product636>)
  {
    SELECT ?otherProduct (COUNT(?otherFeature) AS ?sameFeatures)
    WHERE {
      <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product636> bsbm:productFeature ?feature .
      ?otherProduct bsbm:productFeature ?otherFeature .
      FILTER (?feature = ?otherFeature)
    }
    GROUP BY ?otherProduct
  }
}
ORDER BY DESC(?sameFeatures) ?otherProduct
LIMIT 10`
        },
        {
            name: "BI Query 3: Review Growth Ratio Between Months",
            query: `PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX rev: <http://purl.org/stuff/rev#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?product (xsd:float(?monthCount) / ?monthBeforeCount AS ?ratio)
WHERE {
  {
    SELECT ?product (COUNT(?review) AS ?monthCount)
    WHERE {
      ?review bsbm:reviewFor ?product .
      ?review dc:date ?date .
      FILTER (?date >= "2008-02-29"^^<http://www.w3.org/2001/XMLSchema#date> && ?date < "2008-03-28"^^<http://www.w3.org/2001/XMLSchema#date>)
    }
    GROUP BY ?product
  }
  {
    SELECT ?product (COUNT(?review) AS ?monthBeforeCount)
    WHERE {
      ?review bsbm:reviewFor ?product .
      ?review dc:date ?date .
      FILTER (?date >= "2008-02-01"^^<http://www.w3.org/2001/XMLSchema#date> && ?date < "2008-02-29"^^<http://www.w3.org/2001/XMLSchema#date>)
    }
    GROUP BY ?product
    HAVING (COUNT(?review) > 0)
  }
}
ORDER BY DESC(xsd:float(?monthCount) / ?monthBeforeCount) ?product
LIMIT 10`
        },
        {
            name: "BI Query 4: Feature Price Premium Ratio",
            query: `PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?feature (?withFeaturePrice / ?withoutFeaturePrice AS ?priceRatio)
WHERE {
  {
    SELECT ?feature (AVG(xsd:float(xsd:string(?price))) AS ?withFeaturePrice)
    WHERE {
      ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType139> ;
               bsbm:productFeature ?feature .
      ?offer bsbm:product ?product ;
             bsbm:price ?price .
    }
    GROUP BY ?feature
  }
  {
    SELECT ?feature (AVG(xsd:float(xsd:string(?price))) AS ?withoutFeaturePrice)
    WHERE {
      {
        SELECT DISTINCT ?feature
        WHERE {
          ?p a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType139> ;
             bsbm:productFeature ?feature .
        }
      }
      ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType139> .
      ?offer bsbm:product ?product ;
             bsbm:price ?price .
      FILTER NOT EXISTS {
        ?product bsbm:productFeature ?feature .
      }
    }
    GROUP BY ?feature
  }
}
ORDER BY DESC(?withFeaturePrice / ?withoutFeaturePrice) ?feature
LIMIT 10`
        },
        {
            name: "BI Query 5: Country Reviews & Prices",
            query: `PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX rev: <http://purl.org/stuff/rev#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?country ?product ?nrOfReviews ?avgPrice
WHERE {
  {
    SELECT ?country (MAX(?nrOfReviews) AS ?maxReviews)
    WHERE {
      {
        SELECT ?country ?product (COUNT(?review) AS ?nrOfReviews)
        WHERE {
          ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType12> .
          ?review bsbm:reviewFor ?product ;
                  rev:reviewer ?reviewer .
          ?reviewer bsbm:country ?country .
        }
        GROUP BY ?country ?product
      }
    }
    GROUP BY ?country
  }
  {
    SELECT ?country ?product (AVG(xsd:float(xsd:string(?price))) AS ?avgPrice)
    WHERE {
      ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType12> .
      ?offer bsbm:product ?product .
      ?offer bsbm:price ?price .
    }
    GROUP BY ?country ?product
  }
  {
    SELECT ?country ?product (COUNT(?review) AS ?nrOfReviews)
    WHERE {
      ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType12> .
      ?review bsbm:reviewFor ?product .
      ?review rev:reviewer ?reviewer .
      ?reviewer bsbm:country ?country .
    }
    GROUP BY ?country ?product
  }
  FILTER (?nrOfReviews = ?maxReviews)
}
ORDER BY DESC(?nrOfReviews) ?country ?product`
        },
        {
            name: "BI Query 6: Top Reviewers for Producer",
            query: `PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX rev: <http://purl.org/stuff/rev#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?reviewer (AVG(xsd:float(?score)) AS ?reviewerAvgScore)
WHERE {
  {
    SELECT (AVG(xsd:float(?score)) AS ?avgScore)
    WHERE {
      ?product bsbm:producer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Producer13> .
      ?review bsbm:reviewFor ?product .
      {
        ?review bsbm:rating1 ?score .
      }
      UNION
      {
        ?review bsbm:rating2 ?score .
      }
      UNION
      {
        ?review bsbm:rating3 ?score .
      }
      UNION
      {
        ?review bsbm:rating4 ?score .
      }
    }
  }
  ?product bsbm:producer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Producer13> .
  ?review bsbm:reviewFor ?product .
  ?review rev:reviewer ?reviewer .
  {
    ?review bsbm:rating1 ?score .
  }
  UNION
  {
    ?review bsbm:rating2 ?score .
  }
  UNION
  {
    ?review bsbm:rating3 ?score .
  }
  UNION
  {
    ?review bsbm:rating4 ?score .
  }
}
GROUP BY ?reviewer
HAVING (AVG(xsd:float(?score)) > MIN(?avgScore) * 1.5)`
        },
        {
            name: "BI Query 7: Product Offers Excluding Country",
            query: `PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?product
WHERE {
  {
    SELECT ?product
    WHERE {
      {
        SELECT ?product (COUNT(?offer) AS ?offerCount)
        WHERE {
          ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType6> .
          ?offer bsbm:product ?product .
        }
        GROUP BY ?product
      }
    }
    ORDER BY DESC(?offerCount)
    LIMIT 1000
  }
  FILTER NOT EXISTS {
    ?offer bsbm:product ?product .
    ?offer bsbm:vendor ?vendor .
    ?vendor bsbm:country ?country .
    FILTER (?country = <http://downlode.org/rdf/iso-3166/countries#JP>)
  }
}`
        },
        {
            name: "BI Query 8: Below Average Price Vendors",
            query: `PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?vendor (xsd:float(?belowAvg) / ?offerCount AS ?cheapExpensiveRatio)
WHERE {
  {
    SELECT ?vendor (COUNT(?offer) AS ?belowAvg)
    WHERE {
      {
        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType13> .
        ?offer bsbm:product ?product .
        ?offer bsbm:vendor ?vendor .
        ?offer bsbm:price ?price .
        {
          SELECT ?product (AVG(xsd:float(xsd:string(?price))) AS ?avgPrice)
          WHERE {
            ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType13> .
            ?offer bsbm:product ?product .
            ?offer bsbm:vendor ?vendor .
            ?offer bsbm:price ?price .
          }
          GROUP BY ?product
        }
      }
      FILTER (xsd:float(xsd:string(?price)) < ?avgPrice)
    }
    GROUP BY ?vendor
  }
  {
    SELECT ?vendor (COUNT(?offer) AS ?offerCount)
    WHERE {
      ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType13> .
      ?offer bsbm:product ?product .
      ?offer bsbm:vendor ?vendor .
    }
    GROUP BY ?vendor
  }
}
ORDER BY DESC(xsd:float(?belowAvg) / ?offerCount) ?vendor
LIMIT 10`
        }
    ]
};
