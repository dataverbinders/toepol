CREATE SCHEMA datafundament;

CREATE TABLE datafundament.domeinwaarden_gebruiksdoel ( 
	waarde               string  NOT NULL ,
	omschrijving         string  NOT NULL 
 );

CREATE TABLE datafundament.pand ( 
	identificatie        string  NOT NULL ,
	geomtrie             string  NOT NULL ,
	oorspronkelijk_bouwjaar string  NOT NULL ,
	status               string  NOT NULL ,
	geconstateerd        string  NOT NULL ,
	documentdatum        date  NOT NULL ,
	documentnummer       date   
 );

CREATE TABLE datafundament.woonplaats ( 
	identificatie        string  NOT NULL ,
	naam                 string  NOT NULL ,
	geometrie            string  NOT NULL ,
	status               string  NOT NULL ,
	geconstateerd        string  NOT NULL ,
	documentdatum        date  NOT NULL ,
	documentnummer       string  NOT NULL 
 );

CREATE TABLE datafundament.openbare_ruimte ( 
	identificatie        string  NOT NULL ,
	naam                 string  NOT NULL ,
	type                 string  NOT NULL ,
	status               string  NOT NULL ,
	geconstateerd        string  NOT NULL ,
	documentdatum        date  NOT NULL ,
	documentnummer       string  NOT NULL ,
	woonplaats_identificatie string   
 );

CREATE TABLE datafundament.nummeraanduiding ( 
	identificatie        string  NOT NULL ,
	huisnummer           string  NOT NULL ,
	huisletter           string  NOT NULL ,
	huisnummertoevoeging string  NOT NULL ,
	postcode             string  NOT NULL ,
	type_adresseerbaar_object string  NOT NULL ,
	status               string  NOT NULL ,
	geconstateerd        string  NOT NULL ,
	documentdatum        date  NOT NULL ,
	documentnummer       string  NOT NULL ,
	woonplaats_identificatie string  NOT NULL ,
	openbare_ruimte_identificatie string   
 );

CREATE TABLE datafundament.standplaats ( 
	identificatie        string  NOT NULL ,
	status               string  NOT NULL ,
	geometrie            string  NOT NULL ,
	geconstateerd        string  NOT NULL ,
	documentdatum        string  NOT NULL ,
	documentnummer       string  NOT NULL ,
	hoofdadres           string   ,
	nevenadres           string   
 );

CREATE TABLE datafundament.verblijfsobject ( 
	identificatie        string  NOT NULL ,
	geometrie            string  NOT NULL ,
	gebruiksdoel         string   ,
	oppervlakte          int64  NOT NULL ,
	status               string  NOT NULL ,
	geconstateerd        string  NOT NULL ,
	documentdatum        string  NOT NULL ,
	documentnummer       string  NOT NULL ,
	hoofdadres           string   ,
	nevenadres           string   ,
	pand_identificatie   string  NOT NULL 
 );

CREATE TABLE datafundament.woz_object ( 
	woz_objectnummer     string   ,
	woz_object_geometrie string   ,
	soort_objectcode     string   ,
	datum_begin_geldigheid_woz_objectgegevens date   ,
	datum_einde_geldigheid_woz_objectgegevens date   ,
	to_do_meer_toevoegen date   ,
	nummeraanduiding     string   
 );

CREATE TABLE datafundament.woz_waarde ( 
	woz_objectnummer     string   ,
	gerelateerd_woz_object date   ,
	vastgestelde_waarde  string   ,
	waardepeildatum      date   ,
	toestandpeildatum    date   ,
	heffingsmaatstaf_ozb date   ,
	heffingsmaatstaf_ozb_gebruikers date   ,
	datum_begin_geldigheid_waardegegevens date   ,
	datum_einde_geldigheid_waardegegevens date   ,
	aanduiding_waardegegevens_in_onderzoek int64   ,
	tijdstip_registratie_waardegegevens datetime   ,
	ingangsdatum_waarde  date   
 );

CREATE TABLE datafundament.domeinwaarden_gebruiksdoel_verblijfsobject ( 
	verblijfsobject_ID   int64   ,
	domeinwaarden_gebruiksdoel_ID int64   
 );

CREATE TABLE datafundament.ligplaats ( 
	identificatie        string  NOT NULL ,
	status               string  NOT NULL ,
	geometrie            string  NOT NULL ,
	geconstateerd        string  NOT NULL ,
	documentdatum        string  NOT NULL ,
	documentnummer       string  NOT NULL ,
	hoofdadres           string   ,
	nevenadres           string   
 );
