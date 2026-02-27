# Projectoverzicht

Dit project is een afgeschermde zoekomgeving voor grote accountdatasets. 
De applicatie haalt periodiek nieuwe bronbestanden op, verwerkt deze automatisch en maakt ze doorzoekbaar via een webinterface.

Het systeem is ingericht om:
- automatisch op te starten zonder handmatige import;
- nieuwe data in vaste intervallen te controleren;
- veilig te werken met gescheiden rollen;
- snel te zoeken in grote tabellen;
- eenvoudig te beheren in één Docker Compose stack.

---

## Wat de app doet

Op hoofdlijnen werkt de app zo:

1. **Opstarten**
   - De database start.
   - De fetcher controleert of er al data is.
   - Bij een lege database wordt direct een volledige initiële import uitgevoerd.

2. **Data ophalen en verwerken**
   - Nieuwe dagen worden ontdekt op de bronlocatie.
   - Archieven worden gedownload en uitgepakt.
   - NDJSON-regels worden batch-gewijs in `accounts_*` tabellen geïmporteerd.
   - Importstatus wordt opgeslagen in `_import_tracker`.

3. **Doorzoekbaar maken**
   - De webapp ontdekt automatisch aanwezige `accounts_*` tabellen.
   - Zoeken gebeurt over alle beschikbare accounttabellen.
   - Resultaten zijn pagineerbaar en kunnen verder verfijnd worden.

4. **Periodieke controle**
   - Na bootstrap blijft de fetcher in een interval-loop draaien.
   - Standaard staat dit op **6 uur** (`CHECK_INTERVAL=21600`).

---

## Containerarchitectuur

De stack bestaat uit 4 containers:

### 1) `postgres` (`json_postgres`)
Doel:
- opslag van alle geïmporteerde data;
- tracking van importstatus;
- persistentie via Docker volume.

Belangrijk:
- niet publiek geëxposed naar buiten;
- healthcheck actief;
- read-only gebruikersrechten voor de weblaag.

### 2) `fetcher` (`app_fetcher`)
Doel:
- automatisch nieuwe data detecteren;
- downloaden, uitpakken en importeren;
- import- en statusmeldingen afhandelen.

Belangrijk:
- maakt bij opstarten indien nodig de read-only DB-gebruiker aan;
- zet leesrechten op nieuwe tabellen;
- voert bootstrap uit bij lege database;
- zorgt dat zoekindexen op relevante velden aanwezig zijn.

### 3) `web` (`app_web`)
Doel:
- login en zoekinterface;
- zoeken over alle accounttabellen;
- adminfuncties (sessies en toegangsmode).

Belangrijk:
- gebruikt alleen read-only databaseaccount;
- ondersteunt snelle zoekmodi en verfijnen binnen resultaten;
- bevat extra beveiligingsheaders.

### 4) `nginx` (`app_proxy`)
Doel:
- ingangspunt voor HTTP/HTTPS;
- doorsturen van verkeer naar de webcontainer;
- basisbescherming op request-niveau.

Belangrijk:
- beheert TLS en redirect van HTTP naar HTTPS;
- publieksingang van de stack;
- rate limiting en hardening op webverkeer.

---

## Hoe zoeken werkt

De zoekflow is gericht op snelheid én verfijning:

- Je kiest een veld (bijv. naam, e-mail, stad, postcode).
- Je kiest een matchtype:
  - **Begint met** (standaard, snelste keuze)
  - **Bevat**
  - **Exact**
- Resultaten worden pagina voor pagina geladen.
- Op de resultatenpagina kun je direct extra filteren met een tweede veld + matchtype.

### Praktisch advies voor snelheid

- Gebruik eerst **Begint met** of **Exact**.
- Gebruik daarna een extra verfijning (bijv. op postcode of naam).
- Gebruik **Bevat** alleen als je echt breed wilt zoeken.

---

## Beveiliging (in gewone taal)

Zonder diep technisch te worden, dit is er gedaan om het systeem te beschermen:

- Toegang tot de app is afgeschermd met wachtwoorden.
- Er is een aparte beheer-inlog naast de gewone gebruikersinlog.
- Inlogpogingen worden begrensd om brute force te remmen.
- Sessies worden bijgehouden en verlopen automatisch bij inactiviteit.
- Er is een beheerpagina om actieve sessies te zien en te beëindigen.
- Externe toegang kan centraal open/dicht gezet worden.
- Databasegebruik is opgesplitst zodat de zoeklaag alleen kan lezen.
- Gevoelige waarden staan buiten de code in omgevingsvariabelen.
- Verkeer loopt versleuteld en wordt via één gecontroleerde ingang afgehandeld.
- Er zijn extra veiligheidsregels toegevoegd om onnodige info-lekken te beperken.

---

## Technologieën

Hoofdonderdelen van de stack:

- Python (applicatielogica en importer)
- Flask + Gunicorn (weblaag)
- PostgreSQL (dataopslag)
- Nginx (reverse proxy / TLS)
- Docker & Docker Compose (orchestratie)

---

## Belangrijkste projectbestanden

- `docker-compose.yml` — services, netwerk, volumes en variabelen
- `.env` — lokale configuratie en secrets (niet committen)
- `.env.example` — voorbeeldconfiguratie voor nieuwe omgevingen
- `fetcher/fetcher.py` — ophalen, importeren, indexeren, interval-loop
- `fetcher/Dockerfile` — fetcher image
- `web/app.py` — login, zoeken, admin, beveiligingsregels
- `web/templates/index.html` — startscherm + zoekformulier
- `web/templates/results.html` — resultaten + verfijnen + paginatie
- `nginx/nginx.conf` — routing en basis hardening

---

## Installatie en opstart

### 1) Voorwaarden

- Docker Desktop (of Docker Engine + Compose)
- Toegang tot de bron-URL

### 2) Configuratie

1. Kopieer voorbeeldbestand:

```bash
cp .env.example .env
```

2. Vul alle waarden in `.env` in.

### 3) Starten

```bash
docker compose up -d --build
```

### 4) Controleren

```bash
docker ps
docker logs app_fetcher --tail 50
```

Als de database leeg is, start de fetcher automatisch met bootstrap-import.

---

## Bereikbaarheid

- Lokaal: `https://localhost/login`
- LAN (zelfde netwerk): `https://<jouw-lan-ip>/login`

> Let op: zorg dat poorten 80/443 open staan in je lokale firewall voor LAN-clients.

---

## Dagelijks beheer

Handige commando’s:

```bash
# Alles starten
docker compose up -d

# Alles stoppen
docker compose down

# Logs bekijken
docker logs app_fetcher --tail 100
docker logs app_web --tail 100
docker logs app_proxy --tail 100

# Services opnieuw bouwen
docker compose up -d --build web fetcher nginx
```

---

## Datakwaliteit en consistentie

- Import gebeurt met tracking per dag (`_import_tracker`).
- Import is opgezet om dubbele records te voorkomen.
- Nieuwe tabellen krijgen automatisch rechten voor de read-only gebruiker.
- Zoekindexen op relevante velden worden automatisch toegepast.

---

## Publiceren naar Git

Wel committen:
- broncode;
- `docker-compose.yml`;
- `.env.example`;
- documentatie.

Niet committen:
- `.env`;
- lokale data/volumes;
- tijdelijke bestanden.

---

## Samenvatting

Dit project levert een complete, afgeschermde zoekomgeving voor grote datasets met:
- automatische data-ingest;
- snelle zoekfunctionaliteit met verfijnen;
- duidelijke scheiding van verantwoordelijkheden per container;
- praktische beveiligingsmaatregelen voor dagelijks gebruik.
