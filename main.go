package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultDataURL = "https://download.geofabrik.de/europe/germany/mittelfranken-latest.osm.pbf"
)

var (
	fileName = ""
	dbFile   = ""
	dataURL  = ""
)

// Feature repräsentiert ein GeoJSON-Feature
type Feature struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
	Geometry   Geometry               `json:"geometry"`
}

// Geometry repräsentiert die verschiedenen GeoJSON-Geometrie-Typen
type Geometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

// FeatureCollection repräsentiert eine GeoJSON-Feature-Collection
type FeatureCollection struct {
	Type     string    `json:"type"`
	Features []Feature `json:"features"`
}

// AddressRecord repräsentiert einen Adressdatensatz
type AddressRecord struct {
	Street      string
	HouseNumber string
	City        string
	Lon         float64
	Lat         float64
}

func main() {
	// Kommandozeilenargumente verarbeiten
	var url string
	flag.StringVar(&url, "url", defaultDataURL, "URL der OSM-Datei zum Herunterladen")
	flag.Parse()

	// Setze die dataURL mit dem übergebenen Wert oder dem Standardwert
	dataURL = url

	fmt.Printf("Verwende OSM-Datei von: %s\n", dataURL)

	// Setze den Dateinamen basierend auf der dataURL
	if dataURL == "" {
		log.Fatal("❌ Fehler: dataURL ist leer")
	}
	if fileName == "" {
		fileName = dataURL[strings.LastIndex(dataURL, "/")+1:]
	}
	if dbFile == "" {
		dbFile = fileName[:strings.LastIndex(fileName, ".")] + ".db"
	}

	err := run()
	if err != nil {
		log.Fatalf("❌ Fehler: %v", err)
	}
}

func run() error {
	// OSM-Datei herunterladen
	if err := downloadOSMFile(); err != nil {
		return fmt.Errorf("fehler beim herunterladen der osm-datei: %w", err)
	}

	// OSM-Daten filtern
	if err := filterOSMData(); err != nil {
		return fmt.Errorf("fehler beim filtern der osm-daten: %w", err)
	}

	// Zu GeoJSON exportieren
	if _, err := os.Stat("filtered.osm.pbf"); os.IsNotExist(err) {
		return fmt.Errorf("fehler: gefilterte osm-datei existiert nicht")
	}

	if err := exportToGeoJSON(); err != nil {
		return fmt.Errorf("fehler geojson: %w", err)
	}

	// Nach SQLite konvertieren
	if err := processGeoJSON(); err != nil {
		return fmt.Errorf("fehler geo to sqlite: %w", err)
	}

	// Volltextsuche demonstrieren
	if err := searchAddresses(); err != nil {
		return fmt.Errorf("fehler bei der volltextsuche: %w", err)
	}

	return nil
}

func downloadOSMFile() error {
	// Überprüfen ob die Datei bereits existiert
	if _, err := os.Stat(fileName); err == nil {
		fmt.Println("✔ OSM-Datei bereits heruntergeladen.")
		return nil
	}

	fmt.Println("⬇ Lade OSM-Datei herunter...")

	// HTTP-Anfrage erstellen
	resp, err := http.Get(dataURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download fehler: %s", resp.Status)
	}

	// Datei zum Schreiben öffnen
	out, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer out.Close()

	// Inhalt kopieren
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	fmt.Println("✅ Download abgeschlossen.")
	return nil
}

func filterOSMData() error {
	filteredFile := "filtered.osm.pbf"

	if _, err := os.Stat(filteredFile); err == nil {
		fmt.Println("✔ Gefilterte OSM-Datei existiert bereits.")
		return nil
	}

	fmt.Println("🔍 Filtere OSM-Daten nach Adressen...")

	cmd := exec.Command("osmium", "tags-filter", "-o", filteredFile, fileName, "nwr/addr:street", "-f", "pbf")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return err
	}

	fmt.Println("✅ Filterung abgeschlossen.")
	return nil
}

func exportToGeoJSON() error {
	fmt.Println("📊 Exportiere Daten nach GeoJSON...")

	// Überprüfen ob die Datei bereits existiert
	if _, err := os.Stat("filtered.geojson"); err == nil {
		fmt.Println("✔ GeoJSON-Datei existiert bereits.")
		return nil
	}

	cmd := exec.Command("osmium", "export", "filtered.osm.pbf", "-f", "geojson", "--output=filtered.geojson")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return err
	}

	fmt.Println("✅ Export nach GeoJSON abgeschlossen.")
	return nil
}

func processGeoJSON() error {
	fmt.Println("🛠 Verarbeite GeoJSON zu SQLite...")

	// GeoJSON-Datei öffnen
	file, err := os.Open("filtered.geojson")
	if err != nil {
		return err
	}
	defer file.Close()

	// SQLite-Datenbank erstellen mit noch stärker optimierten Parametern
	os.Remove(dbFile) // Falls die Datei bereits existiert
	db, err := sql.Open("sqlite3", dbFile+"?_journal_mode=OFF&_cache_size=-2000000&_synchronous=0&_temp_store=MEMORY&_locking_mode=EXCLUSIVE&_busy_timeout=5000&_fts=5")
	if err != nil {
		return err
	}
	defer db.Close()

	// FTS5-Modul aktivieren
	_, err = db.Exec("SELECT sqlite_version(); PRAGMA compile_options;")
	if err != nil {
		return fmt.Errorf("fehler beim überprüfen von SQLite-Optionen: %w", err)
	}

	// Zusätzliche PRAGMA-Anweisungen für extreme Schreibgeschwindigkeit
	pragmas := []string{
		"PRAGMA page_size = 16384",          // Größere Seiten für weniger I/O
		"PRAGMA foreign_keys = OFF",         // Deaktivieren für Schreibvorgänge
		"PRAGMA mmap_size = 30000000000",    // Memory-mapped I/O
		"PRAGMA threads = 8",                // Mehr Threads nutzen
		"PRAGMA temp_store = MEMORY",        // Temporäre Tabellen im Speicher
		"PRAGMA cache_size = -4000000",      // Ca. 4GB Cache (negativ = Kilobytes)
		"PRAGMA auto_vacuum = NONE",         // Vacuum deaktivieren während Import
		"PRAGMA checkpoint_fullfsync = OFF", // Vollständiges fsync deaktivieren
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("fehler bei pragma befehl '%s': %w", pragma, err)
		}
	}

	// Tabelle erstellen ohne Indizes
	createTableSQL := `
	CREATE TABLE addresses (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		street TEXT,
		house_number TEXT,
		city TEXT,
		longitude REAL,
		latitude REAL,
		UNIQUE(street, house_number, city)
	);
	`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	// Feature Collection decodieren
	decoder := json.NewDecoder(file)

	// Das erste Token ist '{' - überspringen
	_, err = decoder.Token()
	if err != nil {
		return err
	}

	// Zähler für verarbeitete Datensätze
	totalCount := 0

	batchSize := 500000 // Von 100000 auf 500000 erhöht für bessere Schreibperformance

	// Transaktion vorbereiten
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	startTime := time.Now()
	lastReportTime := startTime
	recordsPerSecond := 0.0
	batchStartTime := time.Now()
	maxBatchDuration := 5 * time.Second // Commit spätestens alle 5 Sekunden

	// Batch für Bulk-Insert vorbereiten
	batch := make([]*AddressRecord, 0, batchSize)

	// JSON-Tokens durchlaufen
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}

		// Suche nach "features"-Key
		if key, ok := token.(string); ok && key == "features" {
			// Array öffnen - '[' - überspringen
			_, err = decoder.Token()
			if err != nil {
				return err
			}

			// Features verarbeiten
			for decoder.More() {
				var feature Feature
				err = decoder.Decode(&feature)
				if err != nil {
					continue
				}

				// Adressdaten extrahieren
				record, ok := extractAddressData(&feature)
				if ok {
					batch = append(batch, record)
					totalCount++

					// Periodisches Commit für große Datensätze
					if len(batch) >= batchSize || time.Since(batchStartTime) >= maxBatchDuration {
						err = bulkInsert(tx, batch)
						if err != nil {
							return err
						}

						err = tx.Commit()
						if err != nil {
							return err
						}

						// Performance-Statistiken berechnen
						now := time.Now()
						elapsedSinceLastReport := now.Sub(lastReportTime).Seconds()
						if elapsedSinceLastReport > 0 {
							recordsPerSecond = float64(len(batch)) / elapsedSinceLastReport
							lastReportTime = now
						}

						estimatedTotal := float64(33000000) // Geschätzte Gesamtanzahl
						remainingRecords := estimatedTotal - float64(totalCount)

						var estimatedRemaining string
						if recordsPerSecond > 0 {
							remainingSeconds := remainingRecords / recordsPerSecond
							estimatedRemaining = fmt.Sprintf("%.1f Minuten verbleibend", remainingSeconds/60)
						} else {
							estimatedRemaining = "berechne..."
						}

						fmt.Printf("Verarbeitet: %d Adressen (%.1f/Sek, %.1f%%, %s)...\n",
							totalCount,
							recordsPerSecond,
							float64(totalCount)/estimatedTotal*100,
							estimatedRemaining)

						// Neue Transaktion starten
						tx, err = db.Begin()
						if err != nil {
							return err
						}

						batch = batch[:0]
						batchStartTime = time.Now()
					}
				}
			}

			// Letzte Transaktion committen
			if len(batch) > 0 {
				err = bulkInsert(tx, batch)
				if err != nil {
					return err
				}

				err = tx.Commit()
				if err != nil {
					return err
				}
			}
		}
	}

	// Nach dem Import die Indizes erstellen
	fmt.Println("📊 Erstelle Indizes...")

	// Indizes parallel erstellen für bessere Performance
	createIndices := []string{
		"CREATE INDEX idx_city ON addresses(city)",
		"CREATE INDEX idx_street ON addresses(street)",
		"CREATE INDEX idx_street_house ON addresses(street, house_number)",
	}

	for _, indexSQL := range createIndices {
		_, err = db.Exec(indexSQL)
		if err != nil {
			return err
		}
	}

	// ANALYZE für den Query Optimizer
	_, err = db.Exec("ANALYZE")
	if err != nil {
		return err
	}

	// Optimieren
	_, err = db.Exec("PRAGMA optimize")
	if err != nil {
		return err
	}

	// Datenbank komprimieren nach dem Import
	fmt.Println("📊 Komprimiere Datenbank (VACUUM)...")
	_, err = db.Exec("VACUUM")
	if err != nil {
		return err
	}

	// FTS5-Virtualtabelle erstellen
	fmt.Println("🔍 Erstelle FTS5-Volltextsuchindex...")
	createFTSTableSQL := `
	CREATE VIRTUAL TABLE address_fts USING fts5(
        street, 
        house_number, 
        city, 
        content='addresses', 
        content_rowid='id',
        tokenize="unicode61 remove_diacritics 0 tokenchars '\x2d'"
	);
	`
	_, err = db.Exec(createFTSTableSQL)
	if err != nil {
		return fmt.Errorf("fehler beim erstellen der FTS5-Tabelle: %w", err)
	}

	// FTS5-Index mit Daten befüllen
	fmt.Println("🔄 Befülle FTS5-Index mit Daten...")
	_, err = db.Exec("INSERT INTO address_fts(rowid, street, house_number, city) SELECT id, street, house_number, city FROM addresses")
	if err != nil {
		return fmt.Errorf("fehler beim befüllen des FTS5-Index: %w", err)
	}

	fmt.Println("✅ FTS5-Volltextsuchindex wurde erfolgreich erstellt.")

	// Statistiken ausgeben
	totalTime := time.Since(startTime).Seconds()
	fmt.Printf("✅ Fertig. %d Adressen wurden in %.1f Sekunden importiert (%.1f Einträge/Sek).\n",
		totalCount,
		totalTime,
		float64(totalCount)/totalTime,
	)

	return nil
}

func bulkInsert(tx *sql.Tx, records []*AddressRecord) error {
	if len(records) == 0 {
		return nil
	}

	// SQLite hat eine Begrenzung für die Anzahl der Variablen
	// Ein Adressdatensatz verwendet 5 Variablen, begrenzen wir auf maximal 500 Datensätze pro Insert
	// Dies entspricht 2500 Variablen, was deutlich unter dem SQLite-Limit liegt
	const maxRecordsPerBatch = 500

	for i := 0; i < len(records); i += maxRecordsPerBatch {
		end := i + maxRecordsPerBatch
		if end > len(records) {
			end = len(records)
		}

		currentBatch := records[i:end]

		// Baue SQL für Multi-Value Insert
		query := "INSERT OR IGNORE INTO addresses (street, house_number, city, longitude, latitude) VALUES "
		args := make([]interface{}, 0, len(currentBatch)*5)

		for j, rec := range currentBatch {
			if j > 0 {
				query += ","
			}
			query += "(?, ?, ?, ?, ?)"
			args = append(args, rec.Street, rec.HouseNumber, rec.City, rec.Lon, rec.Lat)
		}

		_, err := tx.Exec(query, args...)
		if err != nil {
			return err
		}
	}

	return nil
}

func extractAddressData(feature *Feature) (*AddressRecord, bool) {
	properties := feature.Properties

	// Prüfen, ob alle erforderlichen Adressfelder vorhanden sind
	street, hasStreet := properties["addr:street"]
	houseNumber := properties["addr:housenumber"]
	city, hasCity := properties["addr:city"]

	// IF addr:city ist nicht vorhanden, dann probieren wir addr:town wenn das auch nicht vorhanden ist dann probieren wir addr:village wenn dann setzen wir es auf "".
	if !hasCity {
		if town, hasTown := properties["addr:town"]; hasTown {
			city = town
		} else if village, hasVillage := properties["addr:village"]; hasVillage {
			city = village
		} else {
			city = ""
		}
	}

	if !hasStreet {
		return nil, false
	}

	// Koordinaten extrahieren
	var lon, lat float64

	switch feature.Geometry.Type {
	case "Point":
		var coords []float64
		if err := json.Unmarshal(feature.Geometry.Coordinates, &coords); err == nil && len(coords) >= 2 {
			lon, lat = coords[0], coords[1]
		}
	case "LineString":
		var coords [][]float64
		if err := json.Unmarshal(feature.Geometry.Coordinates, &coords); err == nil && len(coords) > 0 && len(coords[0]) >= 2 {
			lon, lat = coords[0][0], coords[0][1]
		}
	case "Polygon":
		var coords [][][]float64
		if err := json.Unmarshal(feature.Geometry.Coordinates, &coords); err == nil && len(coords) > 0 && len(coords[0]) > 0 && len(coords[0][0]) >= 2 {
			lon, lat = coords[0][0][0], coords[0][0][1]
		}
	case "MultiPolygon":
		var coords [][][][]float64
		if err := json.Unmarshal(feature.Geometry.Coordinates, &coords); err == nil && len(coords) > 0 && len(coords[0]) > 0 && len(coords[0][0]) > 0 && len(coords[0][0][0]) >= 2 {
			lon, lat = coords[0][0][0][0], coords[0][0][0][1]
		}
	default:
		return nil, false
	}

	if lon == 0 && lat == 0 {
		return nil, false
	}

	return &AddressRecord{
		Street:      fmt.Sprintf("%v", street),
		HouseNumber: fmt.Sprintf("%v", houseNumber),
		City:        fmt.Sprintf("%v", city),
		Lon:         lon,
		Lat:         lat,
	}, true
}

func searchAddresses() error {
	fmt.Println("🔍 FTS5-Volltextsuche Demo")

	// Datenbank öffnen
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return fmt.Errorf("fehler beim öffnen der datenbank: %w", err)
	}
	defer db.Close()

	// Beispielsuchen
	queries := []string{
		"Berlin",
		"Hauptstraße",
		"München Schillerstraße",
		"Frankfurt am Main",
	}

	for _, query := range queries {
		fmt.Printf("\nSuche nach: %s\n", query)
		fmt.Println("-----------------------------------")

		// FTS5-Volltextsuche durchführen
		// Die match Syntax ist optimiert für FTS5-Suche
		rows, err := db.Query(`
			SELECT a.street, a.house_number, a.city, a.longitude, a.latitude,
				highlight(address_fts, 0, '<b>', '</b>') as street_match,
				highlight(address_fts, 1, '<b>', '</b>') as house_number_match,
				highlight(address_fts, 2, '<b>', '</b>') as city_match,
				rank
			FROM address_fts
			JOIN addresses a ON address_fts.rowid = a.id
			WHERE address_fts MATCH ?
			ORDER BY rank
			LIMIT 5
		`, query)

		if err != nil {
			return fmt.Errorf("fehler bei der suche: %w", err)
		}
		defer rows.Close()

		found := false
		for rows.Next() {
			found = true
			var street, houseNumber, city string
			var lon, lat float64
			var streetMatch, houseMatch, cityMatch string
			var rank float64

			err = rows.Scan(&street, &houseNumber, &city, &lon, &lat,
				&streetMatch, &houseMatch, &cityMatch, &rank)
			if err != nil {
				return fmt.Errorf("fehler beim scannen der ergebnisse: %w", err)
			}

			fmt.Printf("Adresse: %s %s, %s (%.6f, %.6f)\n",
				street, houseNumber, city, lon, lat)
			fmt.Printf("Match: %s %s, %s (Rang: %.2f)\n",
				streetMatch, houseMatch, cityMatch, rank)
		}

		if !found {
			fmt.Println("Keine Ergebnisse gefunden.")
		}
	}

	return nil
}
