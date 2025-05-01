# OSM Adressdaten-Extraktor

Diese Go-Anwendung ermöglicht das Herunterladen, Filtern und Verarbeiten von OpenStreetMap (OSM) Daten mit Fokus auf Adressinformationen. Die Anwendung wandelt die Daten in GeoJSON um und importiert sie in eine SQLite-Datenbank mit Volltextsuchfunktion.

## Voraussetzungen

Bevor Sie die Anwendung verwenden können, müssen Sie folgendes installieren:

- [Go](https://golang.org/dl/) (Version 1.16 oder höher)
- [Osmium Tool](https://osmcode.org/osmium-tool/) für die OSM-Datenverarbeitung
- SQLite3 (wird automatisch als Go-Abhängigkeit installiert)

### Installation von Osmium unter Linux

```bash
sudo apt-get update
sudo apt-get install osmium-tool
```

### Installation von Osmium unter macOS

```bash
brew install osmium-tool
```

## Installation

1. Klonen Sie das Repository:

```bash
git clone <repository-url>
cd go-osmium-etract
```

2. Installieren Sie die Abhängigkeiten:

```bash
go mod download
```

## Verwendung

### Standardausführung

Um die Anwendung mit den Standardeinstellungen zu starten (herunterlädt Daten aus Mittelfranken):

```bash
go run --tags "fts5" main.go
```

### Verwendung mit benutzerdefinierten OSM-Daten

Sie können eine benutzerdefinierte OSM-Datei angeben:

```bash
go run --tags "fts5" main.go -url https://download.geofabrik.de/europe/germany/bayern-latest.osm.pbf
```

### Ausführbare Datei erstellen

Sie können auch eine eigenständige ausführbare Datei erstellen:

```bash
go build --tags "fts5" -o osm-extractor
./osm-extractor -url https://download.geofabrik.de/europe/germany-latest.osm.pbf
```

## Funktionsweise

Die Anwendung führt folgende Schritte aus:

1. **Herunterladen**: Die OSM-PBF-Datei wird von der angegebenen URL heruntergeladen.
2. **Filtern**: Die Daten werden nach Elementen mit Adressinformationen (`addr:street`) gefiltert.
3. **GeoJSON-Umwandlung**: Die gefilterten Daten werden in das GeoJSON-Format umgewandelt.
4. **SQLite-Import**: Die Adressdaten werden in eine SQLite-Datenbank importiert.
5. **Volltextindex-Erstellung**: Ein FTS5-Volltextindex wird erstellt, um schnelle Suchen zu ermöglichen.
6. **Demo-Suche**: Einige Beispielsuchen werden durchgeführt, um die Funktionalität zu demonstrieren.

## Ausgabedateien

- `filtered.osm.pbf`: Die gefilterte OSM-Datei mit nur Adressdaten.
- `filtered.geojson`: Die gefilterten Daten im GeoJSON-Format.
- `*.db`: Die SQLite-Datenbank mit importierten Adressdaten und Volltextindex.

## Datenstruktur

Die Datenbank enthält folgende Tabellen:

### addresses
- `id`: Eindeutige ID (Primärschlüssel)
- `street`: Straßenname
- `house_number`: Hausnummer
- `city`: Stadt/Ort
- `longitude`: Geografische Länge
- `latitude`: Geografische Breite

### address_fts
Eine virtuelle FTS5-Tabelle für Volltextsuche mit Indizes auf:
- `street`
- `house_number`
- `city`

## Beispielsuchen

Die Anwendung führt automatisch folgende Beispielsuchen durch:
- "Berlin"
- "Hauptstraße"
- "München Schillerstraße"
- "Frankfurt am Main"

## Leistungsoptimierung

Die Anwendung verwendet verschiedene Techniken für optimale Leistung:
- Batch-basierte Datenbankaktualisierungen
- Speicheroptimierte SQLite-Konfiguration
- Indizes für schnelle Abfragen
- FTS5 für effiziente Volltextsuche

## Speicheranforderungen

Bei der Verarbeitung großer OSM-Datensätze (wie ganz Deutschland) sollten mindestens 8 GB RAM verfügbar sein.

