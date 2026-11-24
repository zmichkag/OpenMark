package valentine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type NativeDriver struct {
	Address     string
	Port        int
	Timeout     time.Duration
	conn        net.Conn
	mu          sync.Mutex
	curTemplate string

	// Читается из settings_json: {"trace_wire": true}
	TraceWire bool `json:"trace_wire"`

	// Монотонные счетчики
	totalSent    int
	totalPrinted int

	// Подсчет количества
	isPrinting bool // Отмечаем процеес печати для защиты от дребезга
	d1Counted  bool // Чтобы D 00001 + последующий Wait от одной этикетки не посчитались дважды

	// Ограничитель очереди "в тракте"
	maxInFlight     int
	inFlight        int
	refillThreshold int

	// Управление асинхронным чтением
	stopChan chan struct{}
	readDone chan struct{}

	// Таблица маппинга полей (1С -> Макет принтера)
	fieldMap map[string]string
}

func NewNativeDriver(ip string, port int, settings json.RawMessage) *NativeDriver {
	if port <= 0 {
		port = 9100
	}
	d := &NativeDriver{
		Address:         ip,
		Port:            port,
		Timeout:         3 * time.Second,
		maxInFlight:     15,
		refillThreshold: 5,
		fieldMap:        make(map[string]string),
	}

	if len(settings) > 0 {
		_ = json.Unmarshal(settings, d)
		var raw struct {
			FieldMap map[string]string `json:"field_map"`
		}
		if err := json.Unmarshal(settings, &raw); err == nil && raw.FieldMap != nil {
			d.SetFieldMap(raw.FieldMap)
		}
	}
	return d
}

func (d *NativeDriver) SetFieldMap(m map[string]string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fieldMap = make(map[string]string, len(m))
	for k, v := range m {
		d.fieldMap[k] = v
	}
}

func (d *NativeDriver) getFieldMapCopy() map[string]string {
	cp := make(map[string]string, len(d.fieldMap))
	for k, v := range d.fieldMap {
		cp[k] = v
	}
	return cp
}

// InitSession инициализирует счетчики сессии без разрыва уже открытого сокета
func (d *NativeDriver) InitSession(fieldName string, maxQueue int, staticFields map[string]string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.conn == nil {
		if err := d.reconnectNoLock(); err != nil {
			return fmt.Errorf("native init: ошибка сокета %s: %w", d.Address, err)
		}
	}

	d.totalSent = 0
	d.totalPrinted = 0
	d.inFlight = 0
	d.isPrinting = false
	d.d1Counted = false

	slog.Info("VALENTIN-NATIVE: Сессия инициализирована, TD-ридер активен", "addr", d.Address, "trace_wire", d.TraceWire)
	return nil
}

// SelectTemplate загружает макет, заполняет статику и переводит диспенсер в Режим 1
func (d *NativeDriver) SelectTemplate(template string, staticFields map[string]string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if template != "" {
		d.curTemplate = template
	} else if d.curTemplate == "" {
		return fmt.Errorf("native select: передан пустой PLU")
	}

	if err := d.reconnectNoLock(); err != nil {
		return fmt.Errorf("native select: ошибка подключения: %w", err)
	}

	fMap := d.getFieldMapCopy()

	slog.Info("VALENTIN-NATIVE: Покомандная загрузка макета и статики",
		"template", d.curTemplate,
		"fields_count", len(staticFields),
	)

	if err := d.writeRawCmdNoLock("FHM---rSEP1", "FHM"); err != nil {
		return fmt.Errorf("сбой FHM: %w", err)
	}
	time.Sleep(10 * time.Millisecond)

	if err := d.writeRawCmdNoLock("FHA---r2", "FHA"); err != nil {
		return fmt.Errorf("сбой FHA: %w", err)
	}
	time.Sleep(10 * time.Millisecond)

	cmdFMB := fmt.Sprintf("FMB---r%s", d.curTemplate)
	if err := d.writeRawCmdNoLock(cmdFMB, "FMB"); err != nil {
		return fmt.Errorf("сбой FMB: %w", err)
	}
	time.Sleep(15 * time.Millisecond)

	if err := d.writeRawCmdNoLock("FCDC--r1-------", "FCDC 1"); err != nil {
		return fmt.Errorf("сбой установки FCDC 1: %w", err)
	}
	time.Sleep(10 * time.Millisecond)

	for k, val := range staticFields {
		if strings.TrimSpace(val) == "" {
			continue
		}
		idx, ok := resolveBMIndex(fMap, k, 0)
		if !ok {
			continue
		}
		cmd := fmt.Sprintf("BM[%d]%s", idx, val)
		if err := d.writeRawCmdNoLock(cmd, fmt.Sprintf("BM%d", idx)); err != nil {
			return fmt.Errorf("сбой BM[%d]: %w", idx, err)
		}
		slog.Info("VALENTIN-NATIVE: Статика загружена", "field", k, "bm_index", idx, "value", val)
		time.Sleep(10 * time.Millisecond)
	}

	d.totalSent = 0
	d.totalPrinted = 0
	d.inFlight = 0
	d.isPrinting = false
	return nil
}

// stopReaderNoLock безопасно завершает фоновый ридер БЕЗ взаимной блокировки с readLoop
func (d *NativeDriver) stopReaderNoLock() {
	if d.stopChan == nil {
		return
	}
	stopCh := d.stopChan
	doneCh := d.readDone
	conn := d.conn

	d.stopChan = nil
	d.readDone = nil

	close(stopCh)
	if conn != nil {
		_ = conn.SetReadDeadline(time.Now())
	}

	// Временно отпускаем мьютекс, пока горутина readLoop завершается
	d.mu.Unlock()
	<-doneCh
	d.mu.Lock()
}

func (d *NativeDriver) startReaderNoLock() {
	d.stopReaderNoLock()

	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	d.stopChan = stopCh
	d.readDone = doneCh

	go d.readLoop(d.conn, stopCh, doneCh)
}

// PrintBatchIndexed отправляет кадры под нож
func (d *NativeDriver) PrintBatchIndexed(fieldName string, startIndex int, codes []string) (int, error) {
	if len(codes) == 0 {
		return 0, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.conn == nil {
		if err := d.reconnectNoLock(); err != nil {
			return 0, fmt.Errorf("native print: ошибка сокета: %w", err)
		}
	}

	fMap := d.getFieldMapCopy()
	lookupKey := fieldName
	if strings.TrimSpace(lookupKey) == "" {
		lookupKey = "datamatrix"
	}

	idx, ok := resolveBMIndex(fMap, lookupKey, 0)
	if !ok && !strings.EqualFold(lookupKey, "datamatrix") {
		idx, _ = resolveBMIndex(fMap, "datamatrix", 20)
	}
	if idx == 0 {
		idx = 20
	}

	sent := 0
	for _, rawCode := range codes {
		cleanCode := rawCode
		if idxPipe := strings.Index(cleanCode, "|"); idxPipe != -1 {
			cleanCode = cleanCode[:idxPipe]
		}
		cleanCode = strings.ReplaceAll(cleanCode, "<GS>", "\x1d")
		if !strings.HasPrefix(cleanCode, "~1") {
			cleanCode = "~1" + cleanCode
		}
		cleanCode = strings.TrimSpace(cleanCode)

		if err := d.writeRawCmdNoLock(fmt.Sprintf("BM[%d]%s", idx, cleanCode), fmt.Sprintf("BM%d DataMatrix", idx)); err != nil {
			return sent, fmt.Errorf("сбой отправки BM[%d]: %w", idx, err)
		}
		time.Sleep(5 * time.Millisecond)

		if err := d.writeRawCmdNoLock("FBBA--r00001---", "FBBA 1"); err != nil {
			return sent, fmt.Errorf("сбой отправки FBBA: %w", err)
		}
		time.Sleep(5 * time.Millisecond)

		if err := d.writeRawCmdNoLock("FBC---r--------", "FBC Arm"); err != nil {
			return sent, fmt.Errorf("сбой отправки FBC: %w", err)
		}

		// Сбрасываем защелку при взводе нового задания, чтобы следующий D=1 гарантированно сработал
		d.totalSent++
		d.inFlight++
		sent++
	}

	return sent, nil
}

func (d *NativeDriver) GetBufferFreeSpace() (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	inFlight := d.totalSent - d.totalPrinted
	if inFlight < 0 {
		inFlight = 0
	}

	if inFlight > d.refillThreshold {
		return 0, nil
	}

	freeSlots := d.maxInFlight - inFlight
	if freeSlots <= 0 {
		return 0, nil
	}

	return freeSlots, nil
}

func (d *NativeDriver) GetCurrentPrintCount() (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return strconv.Itoa(d.totalPrinted), nil
}

func (d *NativeDriver) GetLastPrintedIndex() (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.totalPrinted, nil
}

func (d *NativeDriver) GetTotalPrints() (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(d.totalPrinted), nil
}

func (d *NativeDriver) writeRawCmdNoLock(cmd string, desc ...string) error {
	if d.conn == nil {
		return fmt.Errorf("сокет не инициализирован")
	}
	frame := []byte(fmt.Sprintf("%c%s%c", SOH, cmd, ETB))
	_ = d.conn.SetWriteDeadline(time.Now().Add(d.Timeout))

	tag := "CMD"
	if len(desc) > 0 && desc[0] != "" {
		tag = desc[0]
	}
	slog.Debug("VALENTIN-RAW-OUT", "desc", tag, "payload", cmd)

	if _, err := d.conn.Write(frame); err != nil {
		d.closeConnNoLock()
		return fmt.Errorf("сбой отправки [%s]: %w", tag, err)
	}

	time.Sleep(15 * time.Millisecond)
	return nil
}

// readLoop собирает полные кадры по ETB (0x17) без потери разорванных пакетов
func (d *NativeDriver) readLoop(conn net.Conn, stopChan chan struct{}, done chan struct{}) {
	defer close(done)
	buf := make([]byte, 1024)
	var rxBuf []byte

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := conn.Read(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			select {
			case <-stopChan:
				// Штатное закрытие при остановке/переподключении — не шумим ошибкой
			default:
				slog.Warn("VALENTIN-WIRE: Сокет закрыт принтером", "err", err)
			}
			return
		}

		if n > 0 {
			chunk := buf[:n]
			if d.TraceWire {
				slog.Debug("VALENTIN-WIRE-IN",
					"bytes_len", n,
					"hex", fmt.Sprintf("%x", chunk),
					"ascii", strings.ReplaceAll(string(chunk), "\x1d", "<GS>"),
				)
			}

			rxBuf = append(rxBuf, chunk...)
			for {
				etbIdx := bytes.IndexByte(rxBuf, byte(ETB))
				if etbIdx == -1 {
					break
				}
				frame := string(rxBuf[:etbIdx])
				rxBuf = rxBuf[etbIdx+1:]
				d.handleSingleFrame(frame)
			}
		}
	}
}

// handleSingleFrame обрабатывает строго один завершённый кадр (TD или HS)
func (d *NativeDriver) handleSingleFrame(frame string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Убираем ведущие байты keep-alive (0x00) и SOH (0x01) перед проверкой префиксов
	frame = strings.TrimLeft(frame, "\x00\x01")

	// 1. Проверка аппаратных ошибок на всех языках (EN / DE / RU)
	if strings.Contains(frame, "Error") ||
		strings.Contains(frame, "Fehler") ||
		strings.Contains(frame, "Ошибка") {
		slog.Error("VALENTIN-NATIVE: Аппаратная ошибка на контроллере", "display", frame)
		return
	}

	// 2. Обработка машинных кадров синхронизации (HSStart / HSProgress / HSDone)
	// Если принтер шлёт HSDone-<PLU>-<N>, это самый надёжный сигнал завершения цикла, не зависящий от языка меню.
	if strings.HasPrefix(frame, "HSDone-") {
		if !d.d1Counted {
			d.d1Counted = true
			d.isPrinting = false
			d.incrementPrinted("HSDone")
		}
		return
	}
	if strings.HasPrefix(frame, "HSStart-") || strings.HasPrefix(frame, "HSProgress-") {
		d.isPrinting = true
		return
	}

	// 3. Обработка дисплейных кадров TD (EN: R/D, RU: З/С)
	if !strings.HasPrefix(frame, "TD") {
		return
	}

	hasRemainingZero := strings.Contains(frame, "R 0") || strings.Contains(frame, "З 0")
	hasDoneZero := strings.Contains(frame, "D 0") || strings.Contains(frame, "С 0")

	// Игнорируем промежуточные кадры перерисовки без счётчиков ("TD\"3328 Печать \nЗ С \";1;16;0")
	if !hasRemainingZero && !hasDoneZero {
		return
	}

	isDone0 := strings.Contains(frame, "D 00000") || strings.Contains(frame, "С 00000")
	isDone1 := strings.Contains(frame, "D 00001") || strings.Contains(frame, "С 00001")

	// Сброс защёлки при старте следующего задания (С 00000 / D 00000)
	if isDone0 {
		d.d1Counted = false
	}

	isPrintState := strings.Contains(frame, "Print") ||
		strings.Contains(frame, "Печать") ||
		strings.Contains(frame, "Druck")

	isWaitState := strings.Contains(frame, "Wait") ||
		strings.Contains(frame, "Подожди") ||
		strings.Contains(frame, "Warten")

	if isPrintState {
		d.isPrinting = true
		// Если на ходу зафиксирован сход С 00001 / D 00001 — считаем сразу
		if isDone1 && !d.d1Counted {
			d.d1Counted = true
			d.isPrinting = false
			d.incrementPrinted("Print(D=1)")
		}
		return
	}

	if isWaitState {
		if (d.isPrinting || isDone1) && !d.d1Counted {
			d.incrementPrinted("Print->Wait")
		}
		d.isPrinting = false
		d.d1Counted = false
	}
}

func (d *NativeDriver) incrementPrinted(reason string) {
	d.totalPrinted++
	if d.inFlight > 0 {
		d.inFlight--
	}
	slog.Info("VALENTIN-NATIVE: Цикл печати завершён",
		"reason", reason,
		"total_printed", d.totalPrinted,
		"in_flight", d.inFlight,
	)
}

func (d *NativeDriver) optimizeSocket(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(5 * time.Second)
	}
}

func (d *NativeDriver) reconnectNoLock() error {
	d.stopReaderNoLock()
	d.closeConnNoLock()

	addr := net.JoinHostPort(d.Address, strconv.Itoa(d.Port))
	conn, err := net.DialTimeout("tcp", addr, d.Timeout)
	if err != nil {
		return err
	}
	d.optimizeSocket(conn)
	d.conn = conn

	d.startReaderNoLock()
	return nil
}

func (d *NativeDriver) closeConnNoLock() {
	if d.conn != nil {
		_ = d.conn.Close()
		d.conn = nil
	}
}

func (d *NativeDriver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopReaderNoLock()
	d.closeConnNoLock()
	return nil
}

// ClearQueue очищает стек заданий в RAM для прошивки V1.53
func (d *NativeDriver) ClearQueue() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.conn == nil {
		if err := d.reconnectNoLock(); err != nil {
			return err
		}
	}

	_ = d.writeRawCmdNoLock("FD----r0", "Stop")
	time.Sleep(20 * time.Millisecond)

	_ = d.writeRawCmdNoLock("FGA---r", "Clear Layout & Spooler")
	time.Sleep(30 * time.Millisecond)

	_ = d.writeRawCmdNoLock("FCDC--r3", "Reset Dispenser Mode")
	time.Sleep(10 * time.Millisecond)

	d.totalSent = 0
	d.totalPrinted = 0
	d.inFlight = 0
	d.isPrinting = false
	d.curTemplate = ""
	d.d1Counted = false

	slog.Info("VALENTIN-NATIVE: Очередь и макет V1.53 успешно сброшены в ОЗУ")
	return nil
}

func (d *NativeDriver) UpdateStaticFields(staticFields map[string]string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.conn == nil {
		if err := d.reconnectNoLock(); err != nil {
			return fmt.Errorf("native update static: ошибка сокета: %w", err)
		}
	}

	fMap := d.getFieldMapCopy()

	for k, val := range staticFields {
		if strings.TrimSpace(val) == "" {
			continue
		}
		idx, ok := resolveBMIndex(fMap, k, 0)
		if !ok {
			continue
		}
		cmd := fmt.Sprintf("BM[%d]%s", idx, val)
		if err := d.writeRawCmdNoLock(cmd, fmt.Sprintf("BM%d Update", idx)); err != nil {
			return fmt.Errorf("сбой обновления BM[%d]: %w", idx, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

// Заглушки core.Printer
func (d *NativeDriver) GetStatus() (string, error)                        { return "ГОТОВ", nil }
func (d *NativeDriver) PrintTemplate(t string, f map[string]string) error { return nil }
func (d *NativeDriver) GetTemplates() ([]string, error)                   { return []string{d.curTemplate}, nil }
func (d *NativeDriver) GetTemplateFields(t string) ([]string, error) {
	return []string{"18", "19", "20", "21"}, nil
}
func (d *NativeDriver) GetRemainingRibbon() (string, error)       { return "N/A", nil }
func (d *NativeDriver) GetQueueCapacity(q string) (string, error) { return "N/A", nil }
func (d *NativeDriver) GetPrintSpeed() (string, error)            { return "N/A", nil }
func (d *NativeDriver) GetCurrentTemplate() (string, error)       { return d.curTemplate, nil }
func (d *NativeDriver) SetDispenserMode(mode int) error           { return nil }
func (d *NativeDriver) SetPause(pause bool) error                 { return nil }
