package valentine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var tdPrintedRegex = regexp.MustCompile(`(?i)(?:напечатано|printed:?|gedruckt:?|\b[СD])\s*(\d{1,6})`)

type NativeDriver struct {
	ID          int
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

	// Подсчет количества (комбинированный по TD + фоллбэк Print -> Wait)
	inPrintCycle  bool
	cycleCounted  int
	lastTDPrinted int

	// Учет событий сканера-верификатора в рамках сессии
	verifiedCount int
	noReadCount   int

	// Ограничитель очереди "в тракте"
	maxInFlight     int
	inFlight        int
	refillThreshold int

	// Управление фоновым чтением (без разблокировки d.mu)
	stopChan chan struct{}
	readWG   sync.WaitGroup

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

func (d *NativeDriver) resetSessionCountersNoLock() {
	d.totalSent = 0
	d.totalPrinted = 0
	d.inFlight = 0
	d.inPrintCycle = false
	d.cycleCounted = 0
	d.lastTDPrinted = 0
	d.verifiedCount = 0
	d.noReadCount = 0
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

	d.resetSessionCountersNoLock()

	slog.Info("VALENTIN-NATIVE: Сессия инициализирована, TD-ридер активен",
		"printer_id", d.ID,
		"addr", d.Address,
		"trace_wire", d.TraceWire,
	)
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
		"printer_id", d.ID,
		"addr", d.Address,
		"template", d.curTemplate,
		"fields_count", len(staticFields),
	)

	// Включаем HS ответы и автоотправку TD/событий в ОЗУ
	for _, step := range []struct {
		cmd   string
		desc  string
		pause time.Duration
	}{
		{"FHM---rSEP1", "FHM", 10 * time.Millisecond},
		{"FHA---r2", "FHA", 10 * time.Millisecond},
		{"FCRA--r2-------", "FCRA 2 (TD Auto-Send)", 10 * time.Millisecond},
		{"FCRB--r0001----", "FCRB 0001 (Event Channel)", 10 * time.Millisecond},
		{fmt.Sprintf("FMB---r%s", d.curTemplate), "FMB", 15 * time.Millisecond},
		{"FCDC--r1-------", "FCDC 1", 10 * time.Millisecond},
	} {
		if err := d.writeRawCmdNoLock(step.cmd, step.desc); err != nil {
			return fmt.Errorf("сбой %s: %w", step.desc, err)
		}
		time.Sleep(step.pause)
	}

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
		slog.Info("VALENTIN-NATIVE: Статика загружена",
			"printer_id", d.ID,
			"field", k,
			"bm_index", idx,
			"value", val,
		)
		time.Sleep(10 * time.Millisecond)
	}

	d.resetSessionCountersNoLock()
	return nil
}

// stopReaderNoLock сигнализирует фоновому ридеру об остановке и закрывает сокет
// БЕЗ отпускания d.mu, чтобы избежать гонок состояния драйвера.
func (d *NativeDriver) stopReaderNoLock() {
	if d.stopChan != nil {
		close(d.stopChan)
		d.stopChan = nil
	}
	if d.conn != nil {
		_ = d.conn.Close()
		d.conn = nil
	}
}

func (d *NativeDriver) startReaderNoLock(conn net.Conn) {
	d.stopReaderNoLock()

	stopCh := make(chan struct{})
	d.stopChan = stopCh

	d.readWG.Add(1)
	go d.readLoop(conn, stopCh)
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

// OnVerifierScan корректирует счетчик и очередь при подтверждении прохода верификатором
func (d *NativeDriver) OnVerifierScan(matched bool, isNoRead bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if isNoRead {
		d.noReadCount++
		physicalPassed := d.verifiedCount + d.noReadCount
		if physicalPassed > d.totalPrinted && physicalPassed <= d.totalSent {
			d.addPrinted(physicalPassed-d.totalPrinted, "Verifier(NoRead)")
		}
		return
	}

	if matched {
		d.verifiedCount++
		physicalPassed := d.verifiedCount + d.noReadCount
		if physicalPassed > d.totalPrinted && physicalPassed <= d.totalSent {
			d.addPrinted(physicalPassed-d.totalPrinted, "Verifier(Matched)")
		}
	}
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
	slog.Debug("VALENTIN-RAW-OUT",
		"printer_id", d.ID,
		"addr", d.Address,
		"desc", tag,
		"payload", cmd,
	)

	if _, err := d.conn.Write(frame); err != nil {
		d.stopReaderNoLock()
		return fmt.Errorf("сбой отправки [%s]: %w", tag, err)
	}

	time.Sleep(15 * time.Millisecond)
	return nil
}

// readLoop читает кадры из конкретного переданного conn и завершается по stopChan,
// закрытию сокета или сетевой ошибке.
func (d *NativeDriver) readLoop(conn net.Conn, stopChan chan struct{}) {
	defer d.readWG.Done()
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
			default:
				slog.Warn("VALENTIN-WIRE: Сокет закрыт принтером",
					"printer_id", d.ID,
					"addr", d.Address,
					"err", err,
				)
			}
			return
		}

		if n > 0 {
			chunk := buf[:n]
			if d.TraceWire {
				slog.Debug("VALENTIN-WIRE-IN",
					"printer_id", d.ID,
					"addr", d.Address,
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

func (d *NativeDriver) handleSingleFrame(frame string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	frame = strings.TrimLeft(frame, "\x00\x01")

	if strings.Contains(frame, "Error") ||
		strings.Contains(frame, "Fehler") ||
		strings.Contains(frame, "Ошибка") {
		slog.Error("VALENTIN-NATIVE: Аппаратная ошибка на контроллере",
			"printer_id", d.ID,
			"addr", d.Address,
			"display", frame,
		)
		return
	}

	if strings.HasPrefix(frame, "HSStart-") || strings.HasPrefix(frame, "HSProgress-") {
		d.inPrintCycle = true
		return
	}
	if strings.HasPrefix(frame, "HSDone-") {
		if d.cycleCounted == 0 {
			d.cycleCounted = 1
			d.addPrinted(1, "HSDone")
		}
		d.inPrintCycle = false
		return
	}

	if !strings.HasPrefix(frame, "TD") {
		return
	}

	isPrintState := strings.Contains(frame, "Print") ||
		strings.Contains(frame, "Печать") ||
		strings.Contains(frame, "Druck")

	isWaitState := strings.Contains(frame, "Wait") ||
		strings.Contains(frame, "Подожди") ||
		strings.Contains(frame, "Warten")

	hasCounter := false
	curPrinted := 0
	if matches := tdPrintedRegex.FindStringSubmatch(frame); len(matches) >= 2 {
		if val, err := strconv.Atoi(matches[1]); err == nil {
			hasCounter = true
			curPrinted = val
		}
	}

	if hasCounter {
		if curPrinted == 0 {
			d.lastTDPrinted = 0
		} else if curPrinted > d.lastTDPrinted {
			delta := curPrinted - d.lastTDPrinted
			d.lastTDPrinted = curPrinted
			if d.inPrintCycle {
				d.cycleCounted += delta
			}
			d.addPrinted(delta, fmt.Sprintf("TD-Count(%d->%d)", curPrinted-delta, curPrinted))
		} else if curPrinted < d.lastTDPrinted {
			d.lastTDPrinted = curPrinted
		}
	}

	if isPrintState {
		d.inPrintCycle = true
		return
	}

	if isWaitState {
		if d.inPrintCycle && d.cycleCounted == 0 {
			d.addPrinted(1, "Print->Wait(blind)")
		}
		d.inPrintCycle = false
		d.cycleCounted = 0
	}
}

func (d *NativeDriver) addPrinted(delta int, reason string) {
	if delta <= 0 {
		return
	}
	d.totalPrinted += delta
	d.inFlight -= delta
	if d.inFlight < 0 {
		d.inFlight = 0
	}
	slog.Info("VALENTIN-NATIVE: Цикл печати завершён",
		"printer_id", d.ID,
		"addr", d.Address,
		"reason", reason,
		"delta", delta,
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

	addr := net.JoinHostPort(d.Address, strconv.Itoa(d.Port))
	conn, err := net.DialTimeout("tcp", addr, d.Timeout)
	if err != nil {
		return err
	}
	d.optimizeSocket(conn)
	d.conn = conn
	d.startReaderNoLock(conn)
	return nil
}

func (d *NativeDriver) Close() error {
	d.mu.Lock()
	d.stopReaderNoLock()
	d.mu.Unlock()

	d.readWG.Wait()
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

	d.resetSessionCountersNoLock()
	d.curTemplate = ""

	slog.Info("VALENTIN-NATIVE: Очередь и макет V1.53 успешно сброшены в ОЗУ",
		"printer_id", d.ID,
		"addr", d.Address,
	)
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
