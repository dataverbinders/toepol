from google.cloud import bigquery

schema = dict(
    wpl=[
        bigquery.SchemaField("identificatie", "string", mode="required", description="De unieke aanduiding van een woonplaats, zoals opgenomen in de landelijke woonplaatsentabel."),
        bigquery.SchemaField("naam", "string", mode="nullable", description="De benaming van een door het gemeentebestuuraangewezen woonplaats."),
        bigquery.SchemaField(
            "geometrie",
            "record",
            mode="repeated",
            description="De tweedimensionale geometrische representatie van het vlak dat wordt gevormd door de omtrekken van eenwoonplaats.",
            fields=[
                bigquery.SchemaField("vlak", "record", mode="nullable", fields=[
                    bigquery.SchemaField("Polygon", "record", mode="NULLABLE", fields=[
                        bigquery.SchemaField("interior", "record", mode="NULLABLE", fields=[
                            bigquery.SchemaField("LinearRing", "record", mode="NULLABLE", fields=[
                                bigquery.SchemaField("posList", "string", mode="NULLABLE"),
                            ]),
                        ]),
                        bigquery.SchemaField("exterior", "record", mode="NULLABLE", fields=[
                            bigquery.SchemaField("LinearRing", "record", mode="NULLABLE", fields=[
                                bigquery.SchemaField("posList", "string", mode="NULLABLE"),
                            ]),
                        ]),
                    ]),
                ]),
                bigquery.SchemaField("multivlak", "record", mode="nullable", fields=[
                    bigquery.SchemaField("MultiSurface", "record", mode="nullable", fields=[
                        bigquery.SchemaField("surfaceMember", "record", mode="repeated", fields=[
                            bigquery.SchemaField("Polygon", "record", mode="nullable", fields=[
                                bigquery.SchemaField("interior", "record", mode="repeated", fields=[
                                    bigquery.SchemaField("linearRing", "record", mode="nullable", fields=[
                                        bigquery.SchemaField("posList", "string", mode="nullable"),
                                    ]),
                                ]),
                                bigquery.SchemaField("exterior", "record", mode="repeated", fields=[
                                    bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                                        bigquery.SchemaField("posList", "string", mode="nullable"),
                                    ]),
                                ]),
                            ]),
                        ]),
                    ]),
                ]),
            ],
        ),
        bigquery.SchemaField("status", "string", mode="nullable", description="De fase van de levenscyclus van een woonplaats,waarin de betreffende woonplaats zich bevindt."),
        bigquery.SchemaField("geconstateerd", "string", mode="nullable", description="Een aanduiding waarmee kan worden aangegeven dat een woonplaats in de registratie is opgenomen als gevolg van een feitelijke constatering, zonder dat er op het moment van opname sprake was van een regulier brondocument voor deze opname."),
        bigquery.SchemaField("documentdatum", "date", mode="nullable", description="De datum waarop het brondocument is vastgesteld, opbasis waarvan een opname, mutatie of een verwijderingvan gegevens ten aanzien van een woonplaats heeft plaatsgevonden."),
        bigquery.SchemaField("documentnummer", "string", mode="nullable", description="De unieke aanduiding van het brondocument op basiswaarvan een opname, mutatie of een verwijdering vangegevens ten aanzien van een woonplaats heeft plaatsgevonden, binnen een gemeente."),
        bigquery.SchemaField(
            "voorkomen",
            "record",
            fields=[
                bigquery.SchemaField("Voorkomen", "record", fields=[
                    bigquery.SchemaField("voorkomenidentificatie", "integer", mode="nullable"),
                    bigquery.SchemaField("beginGeldigheid", "date", mode="nullable"),
                    bigquery.SchemaField("eindGeldigheid", "date", mode="nullable"),
                    bigquery.SchemaField("tijdstipRegistratie", "timestamp", mode="nullable"),
                    bigquery.SchemaField("eindRegistratie", "timestamp", mode="nullable"),
                    bigquery.SchemaField("tijdstipInactief", "timestamp", mode="nullable"),
                    bigquery.SchemaField("BeschikbaarLV", "record", fields=[
                        bigquery.SchemaField("tijdstipRegistratieLV", "timestamp", mode="nullable"),
                        bigquery.SchemaField("tijdstipEindRegistratieLV", "timestamp", mode="nullable"),
                        bigquery.SchemaField("tijdstipInactiefLV", "timestamp", mode="nullable"),
                        bigquery.SchemaField("tijdstipNietBagLV", "timestamp", mode="nullable"),
                    ]),
                ]),
            ]),
    ],
    opr=[
        bigquery.SchemaField("identificatie", "integer", mode="required"),
        bigquery.SchemaField("aanduidingRecordInactief", "string", mode="nullable"),
        bigquery.SchemaField("aanduidingRecordCorrectie", "integer", mode="nullable"),
        bigquery.SchemaField("openbareRuimteNaam", "string", mode="nullable"),
        bigquery.SchemaField("officieel", "string", mode="nullable"),
        bigquery.SchemaField(
            "tijdvakgeldigheid",
            "record",
            fields=[
                bigquery.SchemaField("begindatumTijdvakGeldigheid", "integer", mode="NULLABLE"),
                bigquery.SchemaField("einddatumTijdvakGeldigheid", "integer", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("inOnderzoek", "string", mode="nullable"),
        bigquery.SchemaField("openbareRuimteType", "string", mode="nullable"),
        bigquery.SchemaField(
            "bron",
            "record",
            fields=[
                bigquery.SchemaField("documentdatum", "integer", mode="NULLABLE"),
                bigquery.SchemaField("documentnummer", "string", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("openbareRuimteStatus", "string", mode="nullable"),
        bigquery.SchemaField(
            "gerelateerdeWoonplaats",
            "record",
            fields=[
                bigquery.SchemaField("identificatie", "integer", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("VerkorteOpenbareRuimteNaam", "string", mode="nullable"),
    ],
    num=[
        bigquery.SchemaField("identificatie", "string", mode="required", description="De unieke aanduiding van een nummeraanduiding."),
        bigquery.SchemaField("huisnummer", "integer", mode="nullable", description="Een door of namens het gemeentebestuur ten aanzienvan een adresseerbaar object toegekende nummering."),
        bigquery.SchemaField("huisletter", "string", mode="nullable", description="Een door of namens het gemeentebestuur ten aanzienvan een adresseerbaar object toegekende toevoegingaan een huisnummer in de vorm van een alfanumeriekteken."),
        bigquery.SchemaField("huisnummertoevoeging", "string", mode="nullable", description="Een door of namens het gemeentebestuur ten aanzienvan een adresseerbaar object toegekende naderetoevoeging aan een huisnummer of een combinatie vanhuisnummer en huisletter."),
        bigquery.SchemaField("postcode", "string", mode="nullable", description="De door PostNL vastgestelde code behorende bij eenbepaalde combinatie van een straatnaam en eenhuisnummer."),
        bigquery.SchemaField("typeAdresseerbaarObject", "string", mode="nullable", description="De aard van het object waaraan een nummeraanduiding istoegekend."),
        bigquery.SchemaField("status", "string", mode="nullable", description="De fase van de levenscyclus van een nummeraanduiding, waarin de betreffende nummeraanduiding zich bevindt."),
        bigquery.SchemaField("geconstateerd", "string", mode="nullable", description="Een aanduiding waarmee kan worden aangegeven dat een nummeraanduiding in de registratie is opgenomen als gevolg van een feitelijke constatering, zonder dat er op het moment van opname sprake was van een regulier brondocument voor deze opname."),
        bigquery.SchemaField("documentdatum", "date", mode="nullable", description="De datum waarop het brondocument is vastgesteld, opbasis waarvan een opname, mutatie of een verwijderingvan gegevens ten aanzien van een nummeraanduiding heeft plaatsgevonden."),
        bigquery.SchemaField("documentnummer", "string", mode="nullable", description="De unieke aanduiding van het brondocument op basiswaarvan een opname, mutatie of een verwijdering vangegevens ten aanzien van een nummeraanduiding heeft plaatsgevonden, binnen een gemeente."),
        bigquery.SchemaField("ligtIn", "record", fields=[
            bigquery.SchemaField("WoonplaatsRef", "string", mode="nullable", description="Een adresseerbaar object ligt in een woonplaats."),
        ]),
        bigquery.SchemaField(
            "voorkomen",
            "record",
            fields=[
                bigquery.SchemaField("Voorkomen", "record", fields=[
                    bigquery.SchemaField("voorkomenidentificatie", "integer", mode="nullable"),
                    bigquery.SchemaField("beginGeldigheid", "date", mode="nullable"),
                    bigquery.SchemaField("eindGeldigheid", "date", mode="nullable"),
                    bigquery.SchemaField("tijdstipRegistratie", "timestamp", mode="nullable"),
                    bigquery.SchemaField("eindRegistratie", "timestamp", mode="nullable"),
                    bigquery.SchemaField("tijdstipInactief", "timestamp", mode="nullable"),
                    bigquery.SchemaField("BeschikbaarLV", "record", fields=[
                        bigquery.SchemaField("tijdstipRegistratieLV", "timestamp", mode="nullable"),
                        bigquery.SchemaField("tijdstipEindRegistratieLV", "timestamp", mode="nullable"),
                        bigquery.SchemaField("tijdstipInactiefLV", "timestamp", mode="nullable"),
                        bigquery.SchemaField("tijdstipNietBagLV", "timestamp", mode="nullable"),
                    ]),
                ]),
            ]),
        bigquery.SchemaField("ligtAan", "record", fields=[
            bigquery.SchemaField("OpenbareRuimteRef", "string", mode="nullable", description="Een adresseerbaar object ligt aan een openbare ruimte."),
        ]),
    ],
    lig=[
        bigquery.SchemaField(
            "identificatie",
            "integer",
            mode="required"),
        bigquery.SchemaField(
            "aanduidingRecordInactief",
            "string",
            mode="nullable"),
        bigquery.SchemaField(
            "aanduidingRecordCorrectie",
            "integer",
            mode="nullable"),
        bigquery.SchemaField(
            "officieel",
            "string",
            mode="nullable"),
        bigquery.SchemaField(
            "ligplaatsStatus",
            "string",
            mode="nullable"),
        bigquery.SchemaField(
            "ligplaatsGeometrie",
            "record",
            fields=[
                bigquery.SchemaField(
                    "Polygon",
                    "record",
                    mode="NULLABLE",
                    fields=[
                        bigquery.SchemaField(
                            "interior",
                            "record",
                            mode="repeated",
                            fields=[
                                bigquery.SchemaField(
                                    "LinearRing",
                                    "record",
                                    mode="NULLABLE",
                                    fields=[
                                        bigquery.SchemaField(
                                            "posList",
                                            "string",
                                            mode="NULLABLE"),
                                    ]),
                        ]),
                        bigquery.SchemaField(
                            "exterior",
                            "record",
                            mode="NULLABLE",
                            fields=[
                                bigquery.SchemaField(
                                    "LinearRing",
                                    "record",
                                    mode="NULLABLE",
                                    fields=[
                                        bigquery.SchemaField(
                                            "posList",
                                            "string",
                                            mode="NULLABLE"),
                                    ]),
                            ]),
                ]),
            ]),
        bigquery.SchemaField(
            "tijdvakgeldigheid",
            "record",
            fields=[
                bigquery.SchemaField(
                    "begindatumTijdvakGeldigheid",
                    "integer",
                    mode="NULLABLE"),
                bigquery.SchemaField(
                    "einddatumTijdvakGeldigheid",
                    "integer",
                    mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "inOnderzoek",
            "string",
            mode="nullable"),
        bigquery.SchemaField(
            "bron",
            "record",
            fields=[
                bigquery.SchemaField(
                    "documentdatum",
                    "integer",
                    mode="NULLABLE"),
                bigquery.SchemaField(
                    "documentnummer",
                    "string",
                    mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "gerelateerdeAdressen",
            "record",
            fields=[
                bigquery.SchemaField(
                    "hoofdadres",
                    "record",
                    fields=[
                        bigquery.SchemaField(
                            "identificatie",
                            "integer",
                            mode="NULLABLE"),
                    ]
                ),
                bigquery.SchemaField(
                    "nevenadres",
                    "record",
                    mode="repeated",
                    fields=[
                        bigquery.SchemaField(
                            "identificatie",
                            "integer",
                            mode="NULLABLE"),
                        ]
                ),
            ]
        ),
    ],
    sta=[
        bigquery.SchemaField("identificatie", "integer", mode="required"),
        bigquery.SchemaField("aanduidingRecordInactief", "string", mode="nullable"),
        bigquery.SchemaField("aanduidingRecordCorrectie", "integer", mode="nullable"),
        bigquery.SchemaField("officieel", "string", mode="nullable"),
        bigquery.SchemaField("standplaatsStatus", "string", mode="nullable"),
        bigquery.SchemaField(
            "standplaatsGeometrie",
            "record",
            fields=[
                bigquery.SchemaField("Polygon", "record", mode="NULLABLE", fields=[
                    bigquery.SchemaField("interior", "record", mode="repeated", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="NULLABLE", fields=[
                            bigquery.SchemaField("posList", "string", mode="NULLABLE"),
                        ]),
                    ]),
                    bigquery.SchemaField("exterior", "record", mode="NULLABLE", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="NULLABLE", fields=[
                            bigquery.SchemaField("posList", "string", mode="NULLABLE"),
                        ]),
                    ]),
                ]),
            ]),
        bigquery.SchemaField(
            "tijdvakgeldigheid",
            "record",
            fields=[
                bigquery.SchemaField("begindatumTijdvakGeldigheid", "integer", mode="NULLABLE"),
                bigquery.SchemaField("einddatumTijdvakGeldigheid", "integer", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("inOnderzoek", "string", mode="nullable"),
        bigquery.SchemaField(
            "bron",
            "record",
            fields=[
                bigquery.SchemaField("documentdatum", "integer", mode="NULLABLE"),
                bigquery.SchemaField("documentnummer", "string", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "gerelateerdeAdressen",
            "record",
            fields=[
                bigquery.SchemaField(
                    "hoofdadres",
                    "record",
                    fields=[
                        bigquery.SchemaField("identificatie", "integer", mode="NULLABLE"),
                    ]
                ),
                bigquery.SchemaField(
                    "nevenadres",
                    "record",
                    mode="repeated",
                    fields=[
                        bigquery.SchemaField("identificatie", "integer", mode="NULLABLE"),
                    ]
                ),
            ],
        ),
    ],
    pnd=[
        bigquery.SchemaField("identificatie", "integer", mode="required"),
        bigquery.SchemaField("aanduidingRecordInactief", "string", mode="nullable"),
        bigquery.SchemaField("aanduidingRecordCorrectie", "integer", mode="nullable"),
        bigquery.SchemaField("officieel", "string", mode="nullable"),
        bigquery.SchemaField(
            "pandGeometrie",
            "record",
            fields=[
                bigquery.SchemaField("Polygon", "record", mode="NULLABLE", fields=[
                    bigquery.SchemaField("interior", "record", mode="repeated", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="NULLABLE", fields=[
                            bigquery.SchemaField("posList", "string", mode="NULLABLE"),
                        ]),
                    ]),
                    bigquery.SchemaField("exterior", "record", mode="NULLABLE", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="NULLABLE", fields=[
                            bigquery.SchemaField("posList", "string", mode="NULLABLE"),
                        ]),
                    ]),
                ]),
            ]),
        bigquery.SchemaField("bouwjaar", "integer", mode="nullable"),
        bigquery.SchemaField("pandstatus", "string", mode="nullable"),
        bigquery.SchemaField(
            "tijdvakgeldigheid",
            "record",
            fields=[
                bigquery.SchemaField("begindatumTijdvakGeldigheid", "integer", mode="NULLABLE"),
                bigquery.SchemaField("einddatumTijdvakGeldigheid", "integer", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("inOnderzoek", "string", mode="nullable"),
        bigquery.SchemaField(
            "bron",
            "record",
            fields=[
                bigquery.SchemaField("documentdatum", "integer", mode="NULLABLE"),
                bigquery.SchemaField("documentnummer", "string", mode="NULLABLE"),
            ],
        ),
    ],
    vbo=[
        bigquery.SchemaField("identificatie", "integer", mode="required"),
        bigquery.SchemaField("aanduidingRecordInactief", "string", mode="nullable"),
        bigquery.SchemaField("aanduidingRecordCorrectie", "integer", mode="nullable"),
        bigquery.SchemaField("officieel", "string", mode="nullable"),
        bigquery.SchemaField(
            "verblijfsobjectGeometrie",
            "record",
            fields=[
                bigquery.SchemaField("Point", "record", mode="NULLABLE", fields=[
                    bigquery.SchemaField("pos", "string", mode="nullable"),
                ]),
                bigquery.SchemaField("Polygon", "record", mode="NULLABLE", fields=[
                    bigquery.SchemaField("interior", "record", mode="repeated", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="NULLABLE", fields=[
                            bigquery.SchemaField("posList", "string", mode="NULLABLE"),
                        ]),
                    ]),
                    bigquery.SchemaField("exterior", "record", mode="NULLABLE", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="NULLABLE", fields=[
                            bigquery.SchemaField("posList", "string", mode="NULLABLE"),
                        ]),
                    ]),
                ]),
            ]),
        # bigquery.SchemaField("gebruiksdoelVerblijfsobject", "string", mode="repeated"),
        bigquery.SchemaField(
            "gebruiksdoelVerblijfsobject",
            "record",
            mode="repeated",
            fields=[
                bigquery.SchemaField("type", "string", mode="NULLABLE"),
            ]),
        bigquery.SchemaField("oppervlakteVerblijfsobject", "integer", mode="nullable"),
        bigquery.SchemaField("verblijfsobjectStatus", "string", mode="nullable"),
        bigquery.SchemaField(
            "tijdvakgeldigheid",
            "record",
            fields=[
                bigquery.SchemaField("begindatumTijdvakGeldigheid", "integer", mode="NULLABLE"),
                bigquery.SchemaField("einddatumTijdvakGeldigheid", "integer", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("inOnderzoek", "string", mode="nullable"),
        bigquery.SchemaField(
            "bron",
            "record",
            fields=[
                bigquery.SchemaField("documentdatum", "integer", mode="NULLABLE"),
                bigquery.SchemaField("documentnummer", "string", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "gerelateerdPand",
            "record",
            fields=[
                bigquery.SchemaField("identificatie", "integer", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "gerelateerdeAdressen",
            "record",
            fields=[
                bigquery.SchemaField(
                    "hoofdadres",
                    "record",
                    fields=[
                        bigquery.SchemaField("identificatie", "integer", mode="NULLABLE"),
                    ]
                ),
                bigquery.SchemaField(
                    "nevenadres",
                    "record",
                    mode="repeated",
                    fields=[
                        bigquery.SchemaField("identificatie", "integer", mode="NULLABLE"),
                    ]
                ),
            ],
        ),
    ]
)