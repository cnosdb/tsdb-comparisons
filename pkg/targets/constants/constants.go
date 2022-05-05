package constants

// Formats supported for generation
const (
	FormatInflux      = "influx"
	FormatTimescaleDB = "timescaledb"
	FormatCnosDB      = "cnosdb"
	FormatTDengine    = "tdengine"
)

func SupportedFormats() []string {
	return []string{
		FormatInflux,
		FormatTimescaleDB,
		FormatCnosDB,
		FormatInflux,
	}
}
