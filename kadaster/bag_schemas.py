from google.cloud import bigquery

schema = dict(
    wpl=[
        bigquery.SchemaField("identificatie", "string", mode="required", description="De unieke aanduiding van een woonplaats, zoals opgenomen in de landelijke woonplaatsentabel."),
        bigquery.SchemaField("naam", "string", mode="nullable", description="De benaming van een door het gemeentebestuuraangewezen woonplaats."),
        bigquery.SchemaField("geometrie", "record", mode="repeated", description="De tweedimensionale geometrische representatie van het vlak dat wordt gevormd door de omtrekken van eenwoonplaats.", fields=[
                bigquery.SchemaField("vlak","record", mode="nullable", fields=[
                    bigquery.SchemaField("Polygon", "record", mode="nullable", fields=[
                        bigquery.SchemaField("interior", "record", mode="nullable", fields=[
                            bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                                bigquery.SchemaField("posList", "string", mode="nullable"),
                            ]),
                        ]),
                        bigquery.SchemaField("exterior", "record", mode="nullable", fields=[
                            bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                                bigquery.SchemaField("posList", "string", mode="nullable"),
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
        bigquery.SchemaField("voorkomen", "record", fields=[
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
        bigquery.SchemaField("identificatie", "string", mode="required", description="De unieke aanduiding van een openbare ruimte."),
        bigquery.SchemaField("naam", "string", mode="required", description="Een naam die aan een openbare ruimte is toegekend in een daartoe strekkend formeel gemeentelijk besluit."),
        bigquery.SchemaField("type", "string", mode="required", description="De aard van de als zodanig benoemde openbare ruimte."),
        bigquery.SchemaField("status", "string", mode="required", description="De fase van de levenscyclus van een openbare ruimte, waarin de betreffende openbare ruimte zich bevindt."),
        bigquery.SchemaField("geconstateerd", "string", mode="required", description="Een aanduiding waarmee kan worden aangegeven dat een openbare ruimte in de registratie is opgenomen als gevolg van een feitelijke constatering, zonder dat er op het moment van opname sprake was van een regulier brondocument voor deze opname."),
        bigquery.SchemaField("documentdatum", "date", mode="required", description="De datum waarop het brondocument is vastgesteld, op basis waarvan een opname, mutatie of verwijdering van gegevens ten aanzien van een openbare ruimte heeft plaatsgevonden."),
        bigquery.SchemaField("documentnummer", "string", mode="nullable", description="De unieke aanduiding van het brondocument op basiswaarvan een opname, mutatie of een verwijdering vangegevens ten aanzien van een openbare ruimte heeft plaatsgevonden, binnen een gemeente."),
        bigquery.SchemaField("ligtIn", "record", fields=[
            bigquery.SchemaField("WoonplaatsRef", "string", mode="nullable", description="Een adresseerbaar object ligt in een woonplaats."),
        ]),
        bigquery.SchemaField("voorkomen", "record", fields=[
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
        bigquery.SchemaField("verkorteNaam", "record", mode="nullable", fields=[
            bigquery.SchemaField("verkorteNaamOpenbareRuimte", "record", mode="nullable", fields=[
                bigquery.SchemaField("verkorteNaam", "string", mode="nullable"),
            ]),
        ]),
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
        bigquery.SchemaField("voorkomen", "record", fields=[
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
    pnd=[
        bigquery.SchemaField('identificatie', 'string', mode='required', description='De unieke aanduiding van een pand.'),
        bigquery.SchemaField('geometrie', 'record', mode='required', description='De minimaal tweedimensionale geometrische representatie van het bovenzicht van de omtrekken van een pand.', fields=[
            bigquery.SchemaField("vlak", "record", mode="nullable", fields=[
                bigquery.SchemaField("Polygon", "record", mode="nullable", fields=[
                    bigquery.SchemaField("interior", "record", mode="nullable", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                            bigquery.SchemaField("posList", "string", mode="nullable"),
                        ]),
                    ]),
                    bigquery.SchemaField("exterior", "record", mode="nullable", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                            bigquery.SchemaField("posList", "string", mode="nullable"),
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
        bigquery.SchemaField('oorspronkelijk_bouwjaar', 'FLOAT64', mode='required', description='De aanduiding van het jaar waarin een pand oorspronkelijk als bouwkundig gereed is of zal worden opgeleverd.'),
        bigquery.SchemaField('status', 'string', mode='required', description='De fase van de levenscyclus van een pand, waarin hetbetreffende pand zich bevindt.'),
        bigquery.SchemaField('geconstateerd', 'string', mode='required', description='Een aanduiding waarmee kan worden aangegeven dat een pand in de registratie is opgenomen als gevolg van een feitelijke constatering, zonder dat er op het moment van opname sprake was van een regulier brondocument voor deze opname.'),
        bigquery.SchemaField('documentdatum', 'date', mode='required', description='De datum waarop het brondocument is vastgesteld, opbasis waarvan een opname, mutatie of een verwijderingvan gegevens ten aanzien van een pand heeft plaatsgevonden.'),
        bigquery.SchemaField('documentnummer', 'string', mode='nullable', description='De unieke aanduiding van het brondocument op basiswaarvan een opname, mutatie of een verwijdering vangegevens ten aanzien van een pand heeft plaatsgevonden, binnen een gemeente.'),
        bigquery.SchemaField("voorkomen", "record", fields=[
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
    vbo=[
        bigquery.SchemaField('identificatie', 'string', mode='required', description="De unieke aanduiding van een verblijfsobject."),
        bigquery.SchemaField('geometrie', 'record', mode='required', description="De minimaal tweedimensionale geometrische representatie van een verblijfsobject.", fields=[
            bigquery.SchemaField("vlak", "record", mode="nullable", fields=[
                bigquery.SchemaField("Polygon", "record", mode="nullable", fields=[
                    bigquery.SchemaField("interior", "record", mode="nullable", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                            bigquery.SchemaField("posList", "string", mode="nullable"),
                        ]),
                    ]),
                    bigquery.SchemaField("exterior", "record", mode="nullable", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                            bigquery.SchemaField("posList", "string", mode="nullable"),
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
        bigquery.SchemaField('gebruiksdoel', 'string', mode='nullable', description="Een categorisering van de gebruiksdoelen van het betreffende verblijfsobject, zoals dit formeel door de overheid als zodanig is toegestaan."),
        bigquery.SchemaField('oppervlakte', 'integer', mode='required', description="De gebruiksoppervlakte van een verblijfsobject in gehele vierkante meters."),
        bigquery.SchemaField('status', 'string', mode='required', description="De fase van de levenscyclus van een verblijfsobject, waarin het betreffende verblijfsobject zich bevindt."),
        bigquery.SchemaField('geconstateerd', 'string', mode='required', description='Een aanduiding waarmee kan worden aangegeven dat een verblijfsobject in de registratie is opgenomen als gevolg van een feitelijke constatering, zonder dat er op het moment van opname sprake was van een regulier brondocument voor deze opname.'),
        bigquery.SchemaField('documentdatum', 'date', mode='required', description='De datum waarop het brondocument is vastgesteld, opbasis waarvan een opname, mutatie of een verwijderingvan gegevens ten aanzien van een verblijfsobject heeft plaatsgevonden.'),
        bigquery.SchemaField('documentnummer', 'string', mode='required', description='De unieke aanduiding van het brondocument op basiswaarvan een opname, mutatie of een verwijdering vangegevens ten aanzien van een object heeft plaatsgevonden, binnen een gemeente.'),
        bigquery.SchemaField('maaktDeelUitVan', 'record', mode='nullable', description='Een verblijfsobject maakt onderdeel uit van een pand.', fields=[
            bigquery.SchemaField('PandRef', 'string', mode='nullable', description=''),
        ]),
        bigquery.SchemaField('heeftAlsHoofdadres', 'record', mode='nullable', description='', fields=[
            bigquery.SchemaField("NummeraanduidingRef", "string", mode="nullable"),
        ]),
        bigquery.SchemaField("voorkomen", "record", fields=[
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
        bigquery.SchemaField('heeftAlsNevenadres', 'record', mode='nullable', description='', fields=[
            bigquery.SchemaField("NummeraanduidingRef", "string", mode="nullable"),
        ]),
    ],
    lig=[
        bigquery.SchemaField("identificatie", "string", mode="required", description="De unieke aanduiding van een ligplaats."),
        bigquery.SchemaField("status", "string", mode="required", description="De fase van de levenscyclus van een ligplaats, waarin de betreffende ligplaats zich bevindt."),
        bigquery.SchemaField("geometrie", "record", mode="required", description="De tweedimensionale geometrische representatie van de omtrekken van een ligplaats.", fields=[
            bigquery.SchemaField("Polygon", "record", mode="nullable", fields=[
                    bigquery.SchemaField("interior", "record", mode="nullable", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                            bigquery.SchemaField("posList", "string", mode="nullable"),
                        ]),
                    ]),
                    bigquery.SchemaField("exterior", "record", mode="nullable", fields=[
                        bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                            bigquery.SchemaField("posList", "string", mode="nullable"),
                        ]),
                    ]),
            ]),
        ],
        ),
        bigquery.SchemaField("geconstateerd", "string", mode="nullable", description="Een aanduiding waarmee kan worden aangegeven dat een ligplaats in de registratie is opgenomen als gevolg van een feitelijke constatering, zonder dat er op het moment van opname sprake was van een regulier brondocument voor deze opname."),
        bigquery.SchemaField("documentdatum", "date", mode="nullable", description="De datum waarop het brondocument is vastgesteld, opbasis waarvan een opname, mutatie of een verwijderingvan gegevens ten aanzien van een ligplaats heeft plaatsgevonden."),
        bigquery.SchemaField("documentnummer", "string", mode="nullable", description="De unieke aanduiding van het brondocument op basiswaarvan een opname, mutatie of een verwijdering vangegevens ten aanzien van een ligplaats heeft plaatsgevonden, binnen een gemeente."),
        bigquery.SchemaField('heeftAlsHoofdadres', 'record', mode='nullable', description='', fields=[
            bigquery.SchemaField("NummeraanduidingRef", "string", mode="nullable"),
        ]),
        bigquery.SchemaField("voorkomen", "record", fields=[
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
        bigquery.SchemaField('heeftAlsNevenadres', 'record', mode='nullable', description='', fields=[
            bigquery.SchemaField("NummeraanduidingRef", "string", mode="nullable"),
        ]),
    ],
    sta=[
        bigquery.SchemaField('identificatie', 'string', mode='required', description='De unieke aanduiding van een standplaats.'),
        bigquery.SchemaField('status', 'string', mode='required', description='De fase van de levenscyclus van een standplaats,waarin de betreffende standplaats zich bevindt.'),
        bigquery.SchemaField( 'geometrie', 'record', mode='required', description='De tweedimensionale geometrische representatie van de omtrekken van een standplaats.', fields=[
            bigquery.SchemaField("Polygon", "record", mode="nullable", fields=[
                bigquery.SchemaField("interior", "record", mode="repeated", fields=[
                    bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                        bigquery.SchemaField("posList", "string", mode="nullable"),
                    ]),
                ]),
                bigquery.SchemaField("exterior", "record", mode="nullable", fields=[
                    bigquery.SchemaField("LinearRing", "record", mode="nullable", fields=[
                        bigquery.SchemaField("posList", "string", mode="nullable"),
                    ]),
                ]),
            ]),
        ]),
        bigquery.SchemaField('geconstateerd', 'string', mode='required', description='Een aanduiding waarmee kan worden aangegeven dat een standplaats in de registratie is opgenomen als gevolg van een feitelijke constatering, zonder dat er op het moment van opname sprake was van een regulier brondocument voor deze opname.'),
        bigquery.SchemaField('documentdatum', 'date', mode='required', description='De datum waarop het brondocument is vastgesteld, opbasis waarvan een opname, mutatie of een verwijderingvan gegevens ten aanzien van een standplaats heeft plaatsgevonden.'),
        bigquery.SchemaField('documentnummer', 'string', mode='required', description='De unieke aanduiding van het brondocument op basiswaarvan een opname, mutatie of een verwijdering vangegevens ten aanzien van een standplaats heeft plaatsgevonden, binnen een gemeente.'),
        bigquery.SchemaField('heeftAlsHoofdadres', 'record', mode='nullable', description='', fields=[
            bigquery.SchemaField("NummeraanduidingRef", "string", mode="repeated"),
        ]),
        bigquery.SchemaField("voorkomen", "record", fields=[
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
        bigquery.SchemaField("heeftAlsNevenadres", "record", mode="nullable", description="", fields=[
            bigquery.SchemaField("NummeraanduidingRef", "string", mode="repeated"),
        ]),
    ],
)
