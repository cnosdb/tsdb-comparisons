package constants

// Formats supported for generation
const (
	FormatInflux      = "influx"
	FormatTimescaleDB = "timescaledb"
	FormatCnosDB      = "cnosdb"
	FormatTDEngine    = "tdengine"
	FormatIOTDB       = "iotdb"
)

func SupportedFormats() []string {
	return []string{
		FormatInflux,
		FormatTimescaleDB,
		FormatCnosDB,
		FormatTDEngine,
		FormatIOTDB,
	}
}
