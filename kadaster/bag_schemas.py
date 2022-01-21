from google.cloud import bigquery

schema = dict(
    wpl=[
        bigquery.SchemaField("identificatie", "integer", mode="required"),
        bigquery.SchemaField("naam", "string", mode="nullable"),
        bigquery.SchemaField(
            "geometrie",
            "record",
            mode="repeated",
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
        bigquery.SchemaField("status", "string", mode="nullable"),
        bigquery.SchemaField("geconstateerd", "string", mode="nullable"),
        bigquery.SchemaField("documentdatum", "date", mode="nullable"),
        bigquery.SchemaField("documentnummer", "string", mode="nullable"),
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
        bigquery.SchemaField("identificatie", "integer", mode="required"),
        bigquery.SchemaField("aanduidingRecordInactief", "string", mode="nullable"),
        bigquery.SchemaField("aanduidingRecordCorrectie", "integer", mode="nullable"),
        bigquery.SchemaField("huisnummer", "integer", mode="nullable"),
        bigquery.SchemaField("officieel", "string", mode="nullable"),
        bigquery.SchemaField("huisletter", "string", mode="nullable"),
        bigquery.SchemaField("huisnummertoevoeging", "string", mode="nullable"),
        bigquery.SchemaField("postcode", "string", mode="nullable"),
        bigquery.SchemaField(
            "tijdvakgeldigheid",
            "record",
            fields=[
                bigquery.SchemaField("begindatumTijdvakGeldigheid", "integer", mode="NULLABLE"),
                bigquery.SchemaField("einddatumTijdvakGeldigheid", "integer", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("inOnderzoek", "string", mode="nullable"),
        bigquery.SchemaField("typeAdresseerbaarObject", "string", mode="nullable"),
        bigquery.SchemaField(
            "bron",
            "record",
            fields=[
                bigquery.SchemaField("documentdatum", "integer", mode="NULLABLE"),
                bigquery.SchemaField("documentnummer", "string", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("nummeraanduidingStatus", "string", mode="nullable"),
        bigquery.SchemaField(
            "gerelateerdeOpenbareRuimte",
            "record",
            fields=[
                bigquery.SchemaField("identificatie", "integer", mode="NULLABLE"),
            ],
        ),
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