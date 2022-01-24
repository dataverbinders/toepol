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
        bigquery.SchemaField("identificatie", "string", mode="required",
                             description=""),
        bigquery.SchemaField("naam", "string", mode="required",
                             description=""),
        bigquery.SchemaField("type", "string", mode="required",
                             description=""),
        bigquery.SchemaField("status", "string", mode="required",
                             description=""),
        bigquery.SchemaField("geconstateerd", "string", mode="required",
                             description=""),
        bigquery.SchemaField("documentdatum", "date", mode="required",
                             description=""),
        bigquery.SchemaField("woonplaats_identificatie", "string",
                             mode="nullable", description=""),
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
        bigquery.SchemaField("identificatie",
                             "string",
                             mode="required",
                             description=""),
        bigquery.SchemaField("status",
                             "string",
                             mode="required",
                             description=""),
        bigquery.SchemaField(
            "geometrie",
            "string",
            mode="required",
            description="",
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
        bigquery.SchemaField("documentdatum",
                             "string",
                             mode="nullable",
                             description=""),
        bigquery.SchemaField("documentnummer",
                             "string",
                             mode="nullable",
                             description=""),
        bigquery.SchemaField("hoofdadres",
                             "string",
                             mode="required",
                             description="")
    ],
    sta=[
        bigquery.SchemaField('identificatie',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField('status',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField(
            'geometrie',
            'string',
            mode='required',
            description='',
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
            ]),
        bigquery.SchemaField('geconstateerd',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField('documentdatum',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField('documentnummer',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField('hoofdadres',
                             'string',
                             mode='nullable',
                             description=''),
        bigquery.SchemaField('nevenadres',
                             'string',
                             mode='nullable',
                             description='')
    ],
    pnd=[
        bigquery.SchemaField('identificatie',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField(
            'geometrie',
            'string',
            mode='required',
            description='',
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
            ]),
        bigquery.SchemaField('oorspronkelijk_bouwjaar',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField('status',
                             'string', mode='required',
                             description=''),
        bigquery.SchemaField('geconstateerd',
                             'string', mode='required',
                             description=''),
        bigquery.SchemaField('documentdatum',
                             'date',
                             mode='required',
                             description=''),
        bigquery.SchemaField('documentnummer',
                             'string',
                             mode='nullable',
                             description='')

    ],
    vbo=[
        bigquery.SchemaField('identificatie',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField(
            'geometrie',
            'string',
            mode='required',
            description='',
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
            ]),
        bigquery.SchemaField('gebruiksdoel',
                             'string',
                             mode='nullable',
                             description=''),
        bigquery.SchemaField('oppervlakte',
                             'int64',
                             mode='required',
                             description=''),
        bigquery.SchemaField('status',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField('geconstateerd',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField('documentdatum',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField('documentnummer',
                             'string',
                             mode='required',
                             description=''),
        bigquery.SchemaField('hoofdadres',
                             'string',
                             mode='nullable',
                             description=''),
        bigquery.SchemaField('nevenadres',
                             'string',
                             mode='nullable',
                             description=''),
        bigquery.SchemaField('pand_identificatie',
                             'string',
                             mode='required',
                             description='')

    ],

)
