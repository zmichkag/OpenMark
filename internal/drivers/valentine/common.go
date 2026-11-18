package valentine

import (
	"rovnoMark/internal/models"
	"strconv"
	"strings"
)

// Константы протокола Carl Valentin (CVPL)
const (
	SOH = 0x01 // Start of Header (Начало блока передачи данных)
	ETB = 0x17 // End of Text Block (Конец блока передачи данных)
	CR  = 0x0D // Carriage Return
	LF  = 0x0A // Line Feed
	ESC = 0x1B // Escape-символ для служебных запросов реального времени
)

// Валентиновские статусы ошибок (из мануала)
const (
	BitRibbonError = 1 << 0 // Конец красящей ленты
	BitPaperError  = 1 << 1 // Конец этикеток
	BitHeadError   = 1 << 2 // Ошибка термоголовки
	BitPauseMode   = 1 << 5 // Режим паузы
	BitReadyMode   = 1 << 4 // Ожидание внешнего сигнала / Печать
)

// resolveBMIndex преобразует ключ 1С в номер регистра BM принтера Carl Valentin.
func resolveBMIndex(fMap map[string]string, key string, defaultIdx int) (int, bool) {
	mapped := models.ResolveKeyFromMap(fMap, key)
	if matches := bmIndexRegex.FindStringSubmatch(strings.TrimSpace(mapped)); len(matches) == 2 {
		if idx, err := strconv.Atoi(matches[1]); err == nil {
			return idx, true
		}
	}
	// Фоллбэк на стандартные регистры при отсутствии маппинга
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "date01":
		return 18, true
	case "date02":
		return 19, true
	case "datamatrix":
		return 20, true
	case "text01":
		return 21, true
	}
	if defaultIdx > 0 {
		return defaultIdx, true
	}
	return 0, false
}
