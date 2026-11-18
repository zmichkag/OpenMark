package core

import (
	"rovnoMark/internal/drivers/bizerba"
	"rovnoMark/internal/drivers/extserver"
	"rovnoMark/internal/drivers/markem"
	"rovnoMark/internal/drivers/markem_async"
	"rovnoMark/internal/drivers/savema"
	"rovnoMark/internal/drivers/valentine"
	"rovnoMark/internal/drivers/videojet"
	"rovnoMark/internal/models"
	"rovnoMark/internal/storage"
)

// FieldMapper интерфейс для драйверов, поддерживающих маппинг полей макета.
type FieldMapper interface {
	SetFieldMap(fieldMap map[string]string)
}

// CreateDriver — единственная точка создания любого драйвера в системе.
func CreateDriver(cfg models.PrinterConfig, store *storage.Store) Printer {
	var drv Printer

	switch cfg.DriverType {
	case "savema":
		drv = savema.New(cfg.IP, cfg.Port)
	case "videojet":
		drv = videojet.New(cfg.IP, cfg.Port)
	case "valentine_nice":
		drv = valentine.NewNiceLabelDriver(cfg.ID, cfg.IP, cfg.Port)
	case "valentine_native":
		drv = valentine.NewNativeDriver(cfg.IP, cfg.Port, cfg.Settings)
	case "markem":
		drv = markem.New(cfg.IP, cfg.Port, "Actor1")
	case "markem_async":
		drv = markem_async.New(cfg.IP, cfg.Port, "Actor1")
	case "ext_server", "nicelabel_http":
		drv = extserver.New(cfg.IP, cfg.Port)
	case "bizerba":
		drv = bizerba.CreateDriver(cfg, store)
	default:
		return nil
	}

	if drv == nil {
		return nil
	}

	// Автоматически применяем маппинг полей, если драйвер его поддерживает
	if mapper, ok := drv.(FieldMapper); ok {
		mapper.SetFieldMap(cfg.GetFieldMap())
	}

	return drv
}
