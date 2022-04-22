package constants

// Formats supported for generation
const (
	FormatInflux      = "influx"
	FormatTimescaleDB = "timescaledb"
)

func SupportedFormats() []string {
	return []string{
		FormatInflux,
		FormatTimescaleDB,
	}
}
