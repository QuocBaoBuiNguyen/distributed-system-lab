package config

import (
	"github.com/marcelloh/fastdb"
)

func InitializeDB() (*fastdb.DB, error) {
	return fastdb.Open(":memory:", 100)
}
