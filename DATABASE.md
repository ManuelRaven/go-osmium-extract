# Dokumentation der germany-addresses.db SQLite-Datenbank

## Übersicht

Diese Datenbank enthält Adressdaten aus OpenStreetMap (OSM) für Deutschland. Die Daten werden aus der OpenStreetMap-Datei (germany-latest.osm.pbf) extrahiert, gefiltert und in eine SQLite-Datenbank konvertiert.

## Quelle der Daten

- Ursprungsdatei: `germany-latest.osm.pbf`
- Quelle: [Geofabrik](https://download.geofabrik.de/europe/germany-latest.osm.pbf)
- Gefiltert nach: Adressen (Objekte mit `addr:housenumber`-Tag)
- Zwischenformat: GeoJSON (`filtered.geojson`)

## Datenbankstruktur

### Tabelle: `addresses`

| Spalte        | Typ    | Beschreibung                       |
|---------------|--------|-----------------------------------|
| id            | INTEGER| Primärschlüssel, Auto-Increment    |
| street        | TEXT   | Straßenname                        |
| house_number  | TEXT   | Hausnummer                         |
| city          | TEXT   | Stadt/Ort                          |
| longitude     | REAL   | Geografische Länge (Grad)          |
| latitude      | REAL   | Geografische Breite (Grad)         |

### Virtuelle Tabelle: `address_fts`

Die Datenbank enthält eine FTS5-Virtualtabelle für schnelle Volltextsuche:

| Spalte        | Typ    | Beschreibung                       |
|---------------|--------|-----------------------------------|
| street        | TEXT   | Straßenname (indexiert für Volltextsuche) |
| house_number  | TEXT   | Hausnummer (indexiert für Volltextsuche)  |
| city          | TEXT   | Stadt/Ort (indexiert für Volltextsuche)   |

Die FTS5-Tabelle ist mit der `addresses`-Tabelle über den Primärschlüssel (`id`) verknüpft.

### Indizes

Die Datenbank enthält die folgenden Indizes zur Leistungsoptimierung:

1. `idx_city` - Index auf die Spalte `city`
2. `idx_street` - Index auf die Spalte `street`
3. `idx_street_house` - Kombinierter Index auf die Spalten `street` und `house_number`

Zusätzlich ist ein UNIQUE-Constraint auf der Kombination aus `street`, `house_number` und `city` definiert.


## Datenqualität

- Die Daten enthalten nur Einträge, die vollständige Adressinformationen haben (`addr:street`, `addr:housenumber` und `addr:city`).
- Duplikate werden durch den `INSERT OR IGNORE`-Mechanismus und den UNIQUE-Constraint vermieden.
- Koordinaten (longitude/latitude) werden aus der Geometrie des jeweiligen OSM-Objekts extrahiert.
- Unterstützte Geometrietypen: Point, LineString, Polygon, MultiPolygon.

## FTS5-Volltextsuche

Die Datenbank nutzt SQLite's FTS5-Erweiterung für hochperformante Volltextsuche. Folgende Optimierungen wurden vorgenommen:

- Verwendung des Unicode61-Tokenizers für bessere Unterstützung von Umlauten und Sonderzeichen
- Integration von Bindestrichen (`-`) als Teil der Token für korrekte Suche bei Straßennamen mit Bindestrich
- Deaktivierung der Entfernung von diakritischen Zeichen für präzisere Suchergebnisse


## Geschätzte Datenmenge

Die Datenbank enthält schätzungsweise etwa 33 Millionen Adresseinträge für Deutschland.

## Verwendungszweck

Diese Datenbank kann für verschiedene Zwecke genutzt werden:

- Geocoding (Umwandlung von Adressen in geografische Koordinaten)
- Reverse Geocoding (Umwandlung von Koordinaten in Adressen)
- Analyse der Adressdaten in Deutschland
- Anwendungen, die auf lokale Adressdaten zugreifen müssen

## Beispielabfragen

### Suche nach einer bestimmten Adresse:

```sql
SELECT * FROM addresses 
WHERE city = 'Nürnberg' 
  AND street = 'Hauptmarkt' 
  AND house_number = '1';
```

### Finde alle Adressen in einem bestimmten Umkreis:

```sql
SELECT street, house_number, city,
       (6371 * acos(cos(radians(49.4521)) * cos(radians(latitude)) * 
       cos(radians(longitude) - radians(11.0767)) + 
       sin(radians(49.4521)) * sin(radians(latitude)))) AS distance 
FROM addresses 
HAVING distance < 1 
ORDER BY distance 
LIMIT 100;
```

### Zähle Adressen nach Städten:

```sql
SELECT city, COUNT(*) as address_count 
FROM addresses 
GROUP BY city 
ORDER BY address_count DESC;
```

### Volltextsuche mit FTS5:

```sql
SELECT a.street, a.house_number, a.city, a.longitude, a.latitude
FROM address_fts
JOIN addresses a ON address_fts.rowid = a.id
WHERE address_fts MATCH 'hauptstraße'
ORDER BY rank
LIMIT 10;
```

### Volltextsuche mit Hervorhebung der Treffer:

```sql
SELECT a.street, a.house_number, a.city,
       highlight(address_fts, 0, '<b>', '</b>') as street_match,
       highlight(address_fts, 1, '<b>', '</b>') as house_number_match,
       highlight(address_fts, 2, '<b>', '</b>') as city_match
FROM address_fts
JOIN addresses a ON address_fts.rowid = a.id
WHERE address_fts MATCH 'nürnberg hauptmarkt'
ORDER BY rank
LIMIT 5;
```

## Datenbank-Optimierung

Die Datenbank wurde für optimale Leistung konfiguriert mit:
- Größere Seitengröße (16384 Bytes)
- Memory-mapped I/O
- Optimierte Cache-Einstellungen (bis zu 4GB Cache)
- Mehrere Threads für parallele Verarbeitung
- VACUUM zur Komprimierung nach dem Import
- ANALYZE für optimierte Abfragepläne