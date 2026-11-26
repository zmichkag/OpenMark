package bizerba

import (
	"fmt"
	"log/slog"
	"rovnoMark/internal/core/marking"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultRequestTimeout = 3 * time.Second
	receivePollTimeout    = 200 * time.Millisecond
	spontaneousQueue      = "DUSTBIN"
)

type queuedMark struct {
	index  int
	code   string
	serial string
	crypto string
}

type sessionRequest struct {
	prepared *preparedSession
	kind     string
	marks    []queuedMark
	call     func(bcsConnection) error
	resp     chan sessionResponse
}

type sessionResponse struct {
	value int
	err   error
}

type markSession struct {
	requests chan sessionRequest
	done     chan struct{}
	start    chan error
	errMu    sync.RWMutex
	err      error
}

type preparedSession struct {
	capacity     int
	staticFields map[string]string
}

// Driver управляет Bizerba через COM Automation API службы BCS ConnectService.
// Параметры host и port сохранены в New только для совместимости с общей конфигурацией принтеров.
type Driver struct {
	device    string
	timeout   time.Duration
	newConn   connectionFactory
	equipment bizerbaEquipment

	transientMu   sync.Mutex
	startMu       sync.Mutex
	mu            sync.RWMutex
	session       *markSession
	prepared      *preparedSession
	sessionErr    error
	lastPrinted   int
	currentPLU    string
	fields        map[string]string
	lastPLURead   time.Time
	recordGXNET   bool
	capturePaused bool
	closed        bool
	captureStop   chan struct{}
	captureDone   chan struct{}
}

type deduplicationEntry struct {
	msgType   string
	timestamp time.Time
}

type pw02Deduplicator struct {
	seen map[string]deduplicationEntry
	ttl  time.Duration
}

func newPW02Deduplicator() *pw02Deduplicator {
	return &pw02Deduplicator{
		seen: make(map[string]deduplicationEntry),
		// Кольцевой счётчик PW02 на Bizerba оборачивается каждые 15 (GLP) или 31 (GLM-E) упаковок,
		// поэтому окно дедупликации удерживаем коротким (1с), чтобы не отбрасывать живые упаковки.
		ttl: 1 * time.Second,
	}
}

func (d *pw02Deduplicator) shouldProcess(handle, msgType string, now time.Time) bool {
	for k, v := range d.seen {
		if now.Sub(v.timestamp) > d.ttl {
			delete(d.seen, k)
		}
	}
	if entry, ok := d.seen[handle]; ok && now.Sub(entry.timestamp) <= d.ttl {
		return false
	}
	d.seen[handle] = deduplicationEntry{msgType: msgType, timestamp: now}
	return true
}

func (d *pw02Deduplicator) reset() {
	d.seen = make(map[string]deduplicationEntry)
}

// New создаёт драйвер для устройства с указанным именем подключения в BCS.
// Значения host и port для COM-драйвера не используются.
func New(_ string, _ int, device string) *Driver {
	return newDriver(device, newBCSConnection)
}

// NewWithProfile создаёт драйвер с Bizerba-специфичным профилем оборудования.
func NewWithProfile(_ string, _ int, device string, profile Profile) *Driver {
	return newDriverWithProfile(device, newBCSConnection, profile)
}

// newDriver создаёт драйвер с заданной фабрикой COM-соединений.
// Отдельная фабрика позволяет проверять логику драйвера без настоящего оборудования.
func newDriver(device string, factory connectionFactory) *Driver {
	return newDriverWithProfile(device, factory, Profile{})
}

// newDriverWithProfile создаёт тестируемый драйвер с заданным профилем оборудования.
func newDriverWithProfile(device string, factory connectionFactory, profile Profile) *Driver {
	d := &Driver{device: strings.TrimSpace(device), timeout: defaultRequestTimeout,
		recordGXNET:   profile.RecordGXNET && profile.RecordResponse != nil,
		capturePaused: true,
		equipment:     configuredBizerbaEquipment(profile), lastPrinted: -1, fields: make(map[string]string)}
	if profile.RecordGXNET && profile.RecordResponse != nil {
		baseFactory := factory
		factory = func() (bcsConnection, error) {
			conn, err := baseFactory()
			if err != nil {
				return nil, err
			}
			return &recordingConnection{bcsConnection: conn, record: func(r Response) error {
				d.mu.RLock()
				paused := d.capturePaused || d.closed
				d.mu.RUnlock()
				if paused {
					return nil
				}
				return profile.RecordResponse(r)
			}, requests: make(map[string]gxnetRequest)}, nil
		}
	}
	d.newConn = factory
	return d
}

// withConnection выполняет одну операцию в коротком COM-сеансе без спонтанных сообщений.
func (d *Driver) withConnection(fn func(bcsConnection) error) error {
	if d.device == "" {
		return fmt.Errorf("не задано имя устройства Bizerba в BCS")
	}
	d.transientMu.Lock()
	defer d.transientMu.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	conn, err := d.newConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.Open(d.device, d.device, false); err != nil {
		return err
	}
	return fn(conn)
}

// sendWrite отправляет команду записи и проверяет статус, возвращённый BCS.
func sendWrite(conn bcsConnection, header, data string, timeout time.Duration) error {
	_, status, err := conn.Send(header, data, timeout)
	if err != nil {
		return err
	}
	if status != bcsStatusOK {
		return fmt.Errorf("BCS Send(%q): получен статус %d", header, status)
	}
	return nil
}

// executeQuery отправляет команду чтения и получает ответ по выданному BCS дескриптору.
func executeQuery(conn bcsConnection, command, parameter string, timeout time.Duration) (string, error) {
	handle, status, err := conn.Send(command, parameter, timeout)
	if err != nil {
		return "", err
	}
	if (status != bcsStatusOK && status != bcsStatusMore) || strings.TrimSpace(handle) == "" {
		return "", fmt.Errorf("BCS Send(%q): получен статус %d и дескриптор %q", command, status, handle)
	}
	payload, status, err := conn.ReceiveOne(handle, timeout)
	if err != nil {
		return "", err
	}
	if status != bcsStatusOK && status != bcsStatusMore {
		return "", fmt.Errorf("BCS ReceiveOne(%q): получен статус %d", handle, status)
	}
	return payload, nil
}

// runCommand выполняет команду в активной сессии марок либо открывает короткий COM-сеанс.
func (d *Driver) runCommand(fn func(bcsConnection) error) error {
	if d.recordGXNET {
		d.startMu.Lock()
		err := d.ensureCaptureLocked()
		d.startMu.Unlock()
		if err != nil {
			return err
		}
	}
	d.mu.RLock()
	s := d.session
	closed := d.closed
	d.mu.RUnlock()
	if closed {
		return fmt.Errorf("драйвер Bizerba закрыт")
	}
	if s != nil {
		return s.call(fn)
	}
	return d.withConnection(fn)
}

// call передаёт произвольную операцию в поток активной COM-сессии.
func (s *markSession) call(fn func(bcsConnection) error) error {
	_, err := s.request(sessionRequest{kind: "call", call: fn})
	return err
}

// request передаёт управляющий запрос в сессию и ожидает результат или её завершение.
func (s *markSession) request(req sessionRequest) (int, error) {
	req.resp = make(chan sessionResponse, 1)
	select {
	case s.requests <- req:
	case <-s.done:
		return 0, s.closedError()
	}
	select {
	case result := <-req.resp:
		return result.value, result.err
	case <-s.done:
		select {
		case result := <-req.resp:
			return result.value, result.err
		default:
			return 0, s.closedError()
		}
	}
}

// setError сохраняет последнюю ошибку сессии.
func (s *markSession) setError(err error) {
	if err == nil {
		return
	}
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
}

// failure возвращает сохранённую ошибку сессии.
func (s *markSession) failure() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.err
}

// closedError возвращает причину аварийного завершения либо сообщение о закрытой сессии.
func (s *markSession) closedError() error {
	if err := s.failure(); err != nil {
		return err
	}
	return fmt.Errorf("сессия передачи марок Bizerba закрыта")
}

// SelectTemplate устанавливает на устройстве выбранный PLU и статические поля.
func (d *Driver) SelectTemplate(plu string, fields map[string]string) error {
	plu = strings.TrimSpace(plu)
	if plu == "" {
		return fmt.Errorf("не задан код PLU Bizerba")
	}
	err := d.runCommand(func(conn bcsConnection) error {
		if loadErr := sendWrite(conn, "A!XV00|GL19|LX02", plu, d.timeout); loadErr != nil {
			// Some GLPMax versions apply GL19 but report "Datensatz nicht vorhanden"
			// for the LX02 screen refresh. Accept the operation only after reading
			// the selected PLU back from the device.
			response, verifyErr := executeQuery(conn, "A?GL19", "0", d.timeout)
			loadedPLU, parseErr := ParseFieldResponse(response, "GL19")
			if verifyErr != nil || parseErr != nil || loadedPLU != plu {
				return fmt.Errorf("ошибка установки PLU Bizerba %q: %w", plu, loadErr)
			}
			slog.Warn("Bizerba загрузила PLU, но не выполнила обновление экрана", "device", d.device, "plu", plu, "err", loadErr)
		}
		return d.updateStaticFieldsOn(conn, fields)
	})
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.currentPLU, d.fields, d.lastPLURead = plu, cloneFields(fields), time.Now()
	d.mu.Unlock()
	return nil
}

// PrintTemplate выбирает PLU и заполняет его статические поля.
func (d *Driver) PrintTemplate(plu string, fields map[string]string) error {
	return d.SelectTemplate(plu, fields)
}

// UpdateStaticFields обновляет поддерживаемые статические поля текущего PLU.
func (d *Driver) UpdateStaticFields(fields map[string]string) error {
	if err := d.runCommand(func(conn bcsConnection) error { return d.updateStaticFieldsOn(conn, fields) }); err != nil {
		return err
	}
	d.mu.Lock()
	for key, value := range fields {
		d.fields[key] = value
	}
	d.mu.Unlock()
	return nil
}

func (d *Driver) updateStaticFieldsOn(conn bcsConnection, fields map[string]string) error {
	if len(fields) == 0 {
		return nil
	}

	for key, value := range fields {
		val := strings.TrimSpace(value)
		if val == "" {
			continue
		}

		targetRegister := ""
		// 1. Ищем в StaticMap (без учёта регистра)
		for mapKey, mapReg := range d.equipment.mapping.StaticMap {
			if strings.EqualFold(strings.TrimSpace(mapKey), strings.TrimSpace(key)) && strings.TrimSpace(mapReg) != "" {
				targetRegister = strings.ToUpper(strings.TrimSpace(mapReg))
				break
			}
		}

		// 2. Fallback: если в StaticMap пусто, проверяем стандартные имена и прямые регистры
		if targetRegister == "" {
			lowerKey := strings.ToLower(strings.TrimSpace(key))
			switch lowerKey {
			case "date", "date01", "production_date", "дата":
				targetRegister = "GL06"
			case "date02", "exp_date", "expiration_date", "годен_до":
				targetRegister = "GL07"
			case "brigade", "team", "бригада":
				targetRegister = "GL15"
			default:
				upper := strings.ToUpper(strings.TrimSpace(key))
				if isBizerbaRegister(upper) {
					targetRegister = upper
				}
			}
		}

		if targetRegister == "" {
			continue
		}

		formattedValue := val
		// Регистры дат Bizerba: GL06 (Дата 1), GL07 (Дата 2), GL08 (Дата 3)
		if targetRegister == "GL06" || targetRegister == "GL07" || targetRegister == "GL08" {
			normDate, err := normalizeBizerbaDate(val)
			if err != nil {
				return fmt.Errorf("некорректная дата для поля %s (%s=%q): %w", targetRegister, key, val, err)
			}
			formattedValue = normDate
		}

		if err := sendWrite(conn, "A!"+targetRegister, formattedValue, d.timeout); err != nil {
			return fmt.Errorf("ошибка установки поля Bizerba %s (%s=%q): %w", targetRegister, key, formattedValue, err)
		}
	}
	return nil
}

func isBizerbaRegister(reg string) bool {
	if len(reg) != 4 {
		return false
	}
	prefix := reg[:2]
	return prefix == "GT" || prefix == "GL" || prefix == "GW" || prefix == "GD"
}

// GetCurrentTemplate возвращает текущий PLU, используя кратковременный кэш чтения.
func (d *Driver) GetCurrentTemplate() (string, error) {
	d.mu.RLock()
	cached, readAt := d.currentPLU, d.lastPLURead
	d.mu.RUnlock()
	if cached != "" && time.Since(readAt) < 3*time.Second {
		return cached, nil
	}
	var response string
	err := d.runCommand(func(conn bcsConnection) error {
		var err error
		response, err = executeQuery(conn, "A?GL19", "0", d.timeout)
		return err
	})
	if err != nil {
		return "", fmt.Errorf("ошибка чтения PLU Bizerba: %w", err)
	}
	plu, err := ParseFieldResponse(response, "GL19")
	if err != nil {
		return "", err
	}
	d.mu.Lock()
	d.currentPLU, d.lastPLURead = plu, time.Now()
	d.mu.Unlock()
	return plu, nil
}

// ParseFieldResponse извлекает значение ожидаемого поля из ответа протокола Bizerba.
func ParseFieldResponse(response, expectedField string) (string, error) {
	parts := strings.SplitN(strings.TrimSpace(response), "|", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) != "A!"+expectedField {
		return "", fmt.Errorf("неожиданный ответ Bizerba %q для поля %s", response, expectedField)
	}
	value := strings.TrimSpace(parts[1])
	if value == "" {
		return "", fmt.Errorf("поле Bizerba %s пустое", expectedField)
	}
	return value, nil
}

// GetStatus проверяет доступность устройства чтением общего состояния PL03.
// Проверка не зависит от того, выбран ли на машине PLU и существует ли его текст.
func (d *Driver) GetStatus() (string, error) {
	var response string
	err := d.runCommand(func(conn bcsConnection) error {
		var err error
		response, err = executeQuery(conn, "A?PL03", "0", d.timeout)
		return err
	})
	if err != nil {
		return "OFFLINE", err
	}
	if _, err := ParseFieldResponse(response, "PL03"); err != nil {
		return "ОШИБКА", err
	}
	return "ГОТОВ", nil
}

// InitSession сохраняет параметры будущей сессии марок, не открывая COM и канал E.
func (d *Driver) InitSession(_ string, maxQueue int, staticFields map[string]string) error {
	if maxQueue <= 0 {
		return fmt.Errorf("размер очереди Bizerba должен быть больше нуля")
	}
	if d.device == "" {
		return fmt.Errorf("не задано имя устройства Bizerba в BCS")
	}
	d.startMu.Lock()
	defer d.startMu.Unlock()
	d.mu.RLock()
	closed := d.closed
	d.mu.RUnlock()
	if closed {
		return fmt.Errorf("драйвер Bizerba закрыт")
	}
	if err := d.stopSession(); err != nil {
		return err
	}
	d.mu.Lock()
	d.lastPrinted = -1
	d.sessionErr = nil
	d.capturePaused = false
	d.prepared = &preparedSession{capacity: maxQueue, staticFields: cloneFields(staticFields)}
	d.mu.Unlock()
	if d.recordGXNET {
		return d.ensureCaptureLocked()
	}
	return nil
}

// startPreparedSession запускается очередью Bizerba после поступления списка марок.
// Благодаря отложенному запуску /task/create не захватывает спонтанный COM-клиент
// и не открывает канал E до первого успешного /task/append.
func (d *Driver) startPreparedSession() error {
	d.startMu.Lock()
	defer d.startMu.Unlock()
	d.mu.RLock()
	closed := d.closed
	d.mu.RUnlock()
	if closed {
		return fmt.Errorf("драйвер Bizerba закрыт")
	}
	if d.recordGXNET {
		if err := d.ensureCaptureLocked(); err != nil {
			return err
		}
		d.mu.RLock()
		prepared, s, previousErr := d.prepared, d.session, d.sessionErr
		d.mu.RUnlock()
		if prepared == nil {
			return previousErr
		}
		_, err := s.request(sessionRequest{kind: "prepare", prepared: prepared})
		if err == nil {
			d.mu.Lock()
			d.prepared = nil
			d.mu.Unlock()
		}
		return err
	}

	d.mu.RLock()
	active, prepared, previousErr := d.session, d.prepared, d.sessionErr
	d.mu.RUnlock()
	if active != nil {
		return nil
	}
	if prepared == nil {
		return previousErr
	}

	s := &markSession{requests: make(chan sessionRequest), done: make(chan struct{}), start: make(chan error, 1)}
	d.mu.Lock()
	d.session = s
	d.prepared = nil
	d.mu.Unlock()
	go d.runSession(s, prepared.capacity, prepared.staticFields)
	if err := <-s.start; err != nil {
		d.mu.Lock()
		if d.session == s {
			d.session = nil
		}
		d.mu.Unlock()
		return err
	}
	return nil
}

// runSession обслуживает очередь марок в одном закреплённом COM-потоке.
// Двухфазный цикл конвейера Bizerba:
//
//	Фаза 1 (PW05 / PSV_PCK): упаковка прошла взвешивание и захватила текущий код из GT03/GT04
//	  в буфер печати контроллера — переносим марку в inFlight[handle] и немедленно грузим следующую.
//	Фаза 2 (PV01/PV04/PV05 / PSV_DATA): аппликатор нанёс этикетку — подтверждаем марку из
//	  inFlight[PW02], сохраняем вес PD00 и обновляем lastPrinted.
func (d *Driver) runSession(s *markSession, capacity int, staticFields map[string]string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer func() {
		d.mu.Lock()
		if d.session == s {
			d.session = nil
		}
		d.sessionErr = s.failure()
		d.mu.Unlock()
		close(s.done)
	}()

	conn, err := d.newConn()
	if err == nil {
		err = conn.Open(d.device, d.device, true)
	}
	if err == nil {
		err = sendWrite(conn, "A!GWC3", "1", d.timeout)
	}
	if err == nil {
		err = d.updateStaticFieldsOn(conn, staticFields)
	}
	s.setError(err)
	s.start <- err
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return
	}
	defer conn.Close()

	var pending []queuedMark
	var loaded *queuedMark
	inFlight := make(map[string]queuedMark)
	var inFlightOrder []string
	dedup := newPW02Deduplicator()

	serialReg := d.equipment.mapping.SerialField
	if serialReg == "" {
		serialReg = "GT03"
	}
	cryptoReg := d.equipment.mapping.CryptoField
	if cryptoReg == "" {
		cryptoReg = "GT04"
	}

	clearMarkFields := func() error {
		if err := sendWrite(conn, "A!"+serialReg, "", d.timeout); err != nil {
			return fmt.Errorf("ошибка очистки поля Bizerba %s: %w", serialReg, err)
		}
		if err := sendWrite(conn, "A!"+cryptoReg, "", d.timeout); err != nil {
			return fmt.Errorf("ошибка очистки поля Bizerba %s: %w", cryptoReg, err)
		}
		return nil
	}

	installNext := func() error {
		if loaded != nil || len(pending) == 0 {
			return nil
		}
		next := pending[0]
		pending = pending[1:]
		if err := sendWrite(conn, "A!"+serialReg, next.serial, d.timeout); err != nil {
			return fmt.Errorf("ошибка установки поля Bizerba %s: %w", serialReg, err)
		}
		if err := sendWrite(conn, "A!"+cryptoReg, next.crypto, d.timeout); err != nil {
			return fmt.Errorf("ошибка установки поля Bizerba %s: %w", cryptoReg, err)
		}
		loaded = &next
		return nil
	}

	removeInFlight := func(handle string) {
		delete(inFlight, handle)
		for idx, h := range inFlightOrder {
			if h == handle {
				inFlightOrder = append(inFlightOrder[:idx], inFlightOrder[idx+1:]...)
				break
			}
		}
	}

	usedSlots := func() int {
		used := len(pending) + len(inFlight)
		if loaded != nil {
			used++
		}
		return used
	}

	resetQueueState := func() {
		dedup.reset()
		loaded = nil
		pending = nil
		inFlight = make(map[string]queuedMark)
		inFlightOrder = nil
	}

	for {
		drainRequests := true
		for drainRequests {
			select {
			case req := <-s.requests:
				switch req.kind {
				case "enqueue":
					if capacity <= 0 {
						req.resp <- sessionResponse{err: fmt.Errorf("сессия передачи марок Bizerba не инициализирована")}
						continue
					}
					free := capacity - usedSlots()
					if free < len(req.marks) {
						req.resp <- sessionResponse{err: fmt.Errorf("в очереди Bizerba свободно место для %d из %d марок", free, len(req.marks))}
						continue
					}
					pending = append(pending, req.marks...)
					err := installNext()
					req.resp <- sessionResponse{value: len(req.marks), err: err}
					if err != nil {
						s.setError(err)
						_ = sendWrite(conn, "A!GWC3", "0", d.timeout)
						return
					}
				case "free":
					req.resp <- sessionResponse{value: capacity - usedSlots()}
				case "call":
					req.resp <- sessionResponse{err: req.call(conn)}
				case "prepare":
					resetQueueState()
					err := d.updateStaticFieldsOn(conn, req.prepared.staticFields)
					if err == nil {
						capacity = req.prepared.capacity
					}
					req.resp <- sessionResponse{err: err}
				case "stop", "close":
					resetQueueState()
					_ = clearMarkFields()
					err := sendWrite(conn, "A!GWC3", "0", d.timeout)
					req.resp <- sessionResponse{err: err}
					return
				}
			default:
				drainRequests = false
			}
		}

		payload, status, receiveErr := conn.ReceiveOne(spontaneousQueue, receivePollTimeout)
		if receiveErr != nil {
			s.setError(receiveErr)
			_ = sendWrite(conn, "A!GWC3", "0", d.timeout)
			return
		}
		if status != bcsStatusOK && status != bcsStatusMore && status != bcsStatusTimeout {
			s.setError(fmt.Errorf("BCS ReceiveOne(%q): получен статус %d", spontaneousQueue, status))
			_ = sendWrite(conn, "A!GWC3", "0", d.timeout)
			return
		}

		if status == bcsStatusTimeout {
			if loaded == nil && len(inFlight) == 0 {
				time.Sleep(10 * time.Millisecond)
			}
			continue
		}

		// Фаза 1: PW05 (PSV_PCK) — упаковка прошла взвешивание и забрала текущий код из GT03/GT04.
		// Немедленно фиксируем привязку handle -> марка в inFlight и грузим следующую марку на устройство,
		// не дожидаясь доезда упаковки до аппликатора (PV04).
		if hasField(payload, "PW05") {
			handle, hasHandle := bizerbaFieldValue(payload, "PW05")
			handle = strings.TrimSpace(handle)
			if !hasHandle || handle == "" {
				slog.Warn("Bizerba: PW05 без корректного номера упаковки пропущен", "packet", payload)
				continue
			}
			if loaded == nil {
				slog.Debug("Bizerba: PW05 получен при пустых регистрах GT03/GT04", "handle", handle, "packet", payload)
				continue
			}

			if _, exists := inFlight[handle]; exists {
				slog.Warn("Bizerba: перезапись неподтверждённой записи inFlight при повторном PW05", "handle", handle)
				removeInFlight(handle)
			}
			inFlight[handle] = *loaded
			inFlightOrder = append(inFlightOrder, handle)
			loaded = nil

			if err := installNext(); err != nil {
				s.setError(err)
				_ = sendWrite(conn, "A!GWC3", "0", d.timeout)
				return
			}
			// Если очередь pending исчерпана, сразу очищаем GT03/GT04: текущая марка уже захвачена
			// контроллером Bizerba по PW05, очистка защищает от печати дубля при холостом проходе.
			if loaded == nil && len(pending) == 0 {
				if err := clearMarkFields(); err != nil {
					slog.Warn("Не удалось очистить поля марки после захвата последней марки по PW05", "err", err)
				}
			}
			continue
		}

		// PW07 — отмена/сброс цикла упаковки на Bizerba; оставляем запись в inFlight до возможного PV05,
		// чтобы сохранить синхронизацию при последующем завершении цикла.
		if hasField(payload, "PW07") {
			handle, _ := bizerbaFieldValue(payload, "PW07")
			slog.Debug("Bizerba: получен сигнал отмены/сброса PW07", "handle", strings.TrimSpace(handle), "packet", payload)
			continue
		}

		// Фаза 2: PV01 / PV04 / PV05 (PSV_DATA) — подтверждение аппликации/печати и фиксация веса.
		msgType := ""
		switch {
		case hasField(payload, "PV01"):
			msgType = "PV01"
		case hasField(payload, "PV04"):
			msgType = "PV04"
		case hasField(payload, "PV05"):
			msgType = "PV05"
		default:
			continue
		}

		if len(inFlight) == 0 && loaded == nil {
			continue
		}

		handle, hasHandle := bizerbaFieldValue(payload, "PW02")
		handle = strings.TrimSpace(handle)
		if !hasHandle || handle == "" {
			slog.Warn("Bizerba: сообщение без корректного PW02 пропущено", "type", msgType, "packet", payload)
			continue
		}

		if !dedup.shouldProcess(handle, msgType, time.Now()) {
			slog.Debug("Bizerba: повторное сообщение для PW02 в окне дедупликации пропущено", "handle", handle, "type", msgType)
			continue
		}

		mark, matchedInFlight := inFlight[handle]
		fallbackFromLoaded := false
		if !matchedInFlight {
			// Fallback для режимов без предварительного PW05 (например, одиночная ручная печать на GLP):
			// если inFlight пуст, но марка загружена в loaded, подтверждаем её напрямую.
			if len(inFlight) == 0 && loaded != nil {
				mark = *loaded
				fallbackFromLoaded = true
			} else {
				slog.Warn("Bizerba: получен PV-отчёт для неизвестного PW02 (пропуск)", "handle", handle, "type", msgType, "packet", payload)
				continue
			}
		}

		weight := ""
		switch msgType {
		case "PV01", "PV04":
			if d.equipment.captureWeight {
				w, err := bizerbaWeight(payload)
				if err != nil {
					slog.Warn("Bizerba: сообщение отклонено из-за некорректного/отсутствующего PD00", "type", msgType, "err", err, "packet", payload)
					delete(dedup.seen, handle)
					continue
				}
				weight = w
			} else {
				weight = "0"
			}
		case "PV05":
			weight = "0"
		}

		if matchedInFlight {
			removeInFlight(handle)
		} else if fallbackFromLoaded {
			loaded = nil
		}

		if weight != "" && d.equipment.recordWeight != nil {
			if err := d.equipment.recordWeight(mark.index, mark.code, weight); err != nil {
				slog.Error("Не удалось сохранить вес напечатанной марки", "mark", mark.code, "weight", weight, "err", err)
			}
		}

		d.mu.Lock()
		if mark.index > d.lastPrinted {
			d.lastPrinted = mark.index
		}
		d.mu.Unlock()

		if fallbackFromLoaded {
			if err := installNext(); err != nil {
				s.setError(err)
				_ = sendWrite(conn, "A!GWC3", "0", d.timeout)
				return
			}
			if loaded == nil && len(pending) == 0 {
				if err := clearMarkFields(); err != nil {
					slog.Warn("Не удалось очистить поля марки в конце очереди Bizerba", "err", err)
				}
			}
		}
	}
}

// parseGxNetPacket разбирает пакет Bizerba в обоих форматах:
//  1. Табличный (Old: Header - Data): "!PV04|PW02|GW09|GL19|PD00|PD04|LX02 5|2|3840|KG|-3|1000|KG|-3|1000"
//  2. Парный (New: HeaderData):     "A!PV01|PW02|6|PW00|0|GW09|2|GL16|0|PD00|KG;-3;200|LX02"
func parseGxNetPacket(packet string) (fields map[string]string, hasHeader map[string]bool) {
	fields = make(map[string]string)
	hasHeader = make(map[string]bool)

	trimmed := strings.TrimSpace(packet)
	if trimmed == "" {
		return fields, hasHeader
	}

	if spaceIdx := strings.IndexByte(trimmed, ' '); spaceIdx > 0 {
		headerPart := trimmed[:spaceIdx]
		dataPart := strings.TrimSpace(trimmed[spaceIdx+1:])
		if strings.Contains(headerPart, "|") && strings.Contains(dataPart, "|") {
			headers := strings.Split(headerPart, "|")
			values := strings.Split(dataPart, "|")
			valIdx := 0
			for _, rawH := range headers {
				h := normalizeFieldName(rawH)
				if h == "" {
					continue
				}
				hasHeader[h] = true
				if strings.HasPrefix(h, "PV") || strings.HasPrefix(h, "LX") || strings.HasPrefix(h, "XV") {
					continue
				}
				if valIdx >= len(values) {
					break
				}
				if isTripletField(h) && valIdx+2 < len(values) {
					fields[h] = strings.TrimSpace(values[valIdx]) + ";" +
						strings.TrimSpace(values[valIdx+1]) + ";" +
						strings.TrimSpace(values[valIdx+2])
					valIdx += 3
				} else {
					fields[h] = strings.TrimSpace(values[valIdx])
					valIdx++
				}
			}
			return fields, hasHeader
		}
	}

	parts := strings.Split(trimmed, "|")
	for i, part := range parts {
		name := normalizeFieldName(part)
		if name == "" {
			continue
		}
		hasHeader[name] = true
		if i+1 < len(parts) {
			fields[name] = strings.TrimSpace(parts[i+1])
		}
	}
	return fields, hasHeader
}

func normalizeFieldName(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.TrimPrefix(s, "A!")
	s = strings.TrimPrefix(s, "A?")
	s = strings.TrimPrefix(s, "!")
	s = strings.TrimPrefix(s, "?")
	return s
}

func isTripletField(field string) bool {
	switch field {
	case "PD00", "PD01", "PD02", "PD03", "PD04", "PD05", "PD06", "PD07", "PD08", "PD09":
		return true
	default:
		return false
	}
}

// hasField проверяет наличие поля в пакете протокола Bizerba.
func hasField(packet, field string) bool {
	_, hasHeader := parseGxNetPacket(packet)
	return hasHeader[normalizeFieldName(field)]
}

// bizerbaWeight извлекает PD00 и преобразует представление «KG;степень;целое» в килограммы.
func bizerbaWeight(packet string) (string, error) {
	raw, ok := bizerbaFieldValue(packet, "PD00")
	if !ok || raw == "" {
		return "", fmt.Errorf("в пакете отсутствует поле PD00")
	}
	parts := strings.Split(raw, ";")
	if len(parts) == 1 {
		if _, err := strconv.ParseFloat(raw, 64); err != nil {
			return "", fmt.Errorf("некорректное значение PD00 %q", raw)
		}
		return raw, nil
	}
	if len(parts) != 3 || !strings.EqualFold(strings.TrimSpace(parts[0]), "KG") {
		return "", fmt.Errorf("неподдерживаемое значение PD00 %q", raw)
	}
	exponent, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return "", fmt.Errorf("некорректная степень PD00 %q: %w", parts[1], err)
	}
	weight, err := formatScaledInteger(strings.TrimSpace(parts[2]), exponent)
	if err != nil {
		return "", fmt.Errorf("некорректный вес PD00 %q: %w", parts[2], err)
	}
	return weight, nil
}

// bizerbaFieldValue возвращает значение, следующее за именем поля в пакете Bizerba.
func bizerbaFieldValue(packet, field string) (string, bool) {
	fields, _ := parseGxNetPacket(packet)
	val, ok := fields[normalizeFieldName(field)]
	return val, ok && val != ""
}

// formatScaledInteger форматирует целое с десятичной степенью без потери точности.
func formatScaledInteger(value string, exponent int) (string, error) {
	negative := strings.HasPrefix(value, "-")
	digits := strings.TrimPrefix(strings.TrimPrefix(value, "-"), "+")
	if digits == "" || strings.Trim(digits, "0123456789") != "" {
		return "", fmt.Errorf("ожидалось целое число")
	}
	sign := ""
	if negative {
		sign = "-"
	}
	if exponent >= 0 {
		return sign + digits + strings.Repeat("0", exponent), nil
	}
	places := -exponent
	if len(digits) <= places {
		digits = strings.Repeat("0", places-len(digits)+1) + digits
	}
	point := len(digits) - places
	return sign + digits[:point] + "." + digits[point:], nil
}

// PrintBatchIndexed разбирает GS1-коды и добавляет их в очередь активной сессии.
func (d *Driver) PrintBatchIndexed(_ string, startIndex int, codes []string) (int, error) {
	if len(codes) == 0 {
		return 0, nil
	}

	marks := make([]queuedMark, 0, len(codes))
	var activeGTIN string

	for i, code := range codes {
		parsed, err := marking.ParseAndValidateShortGS1(strings.TrimSpace(code))
		if err != nil {
			return 0, fmt.Errorf("некорректная марка с индексом %d: %w", i, err)
		}

		if i == 0 {
			activeGTIN = parsed.GTIN // 14 цифр GTIN из первой марки
		} else if parsed.GTIN != activeGTIN {
			return 0, fmt.Errorf("в партии обнаружен чужой GTIN на марке #%d: %s (ожидался %s)", i, parsed.GTIN, activeGTIN)
		}

		marks = append(marks, queuedMark{
			index:  startIndex + i,
			code:   parsed.ToDBFormat(),
			serial: parsed.CountryCode + parsed.Serial,
			crypto: "@1D93" + parsed.CryptoTail,
		})
	}

	// Если задан gtin_field (ШК2) — перед стартом отправляем туда чистый GTIN марки
	gtinReg := strings.ToUpper(strings.TrimSpace(d.equipment.mapping.GTINField))
	if gtinReg != "" && activeGTIN != "" {
		d.mu.Lock()
		if d.prepared != nil {
			if d.prepared.staticFields == nil {
				d.prepared.staticFields = make(map[string]string)
			}
			d.prepared.staticFields[gtinReg] = activeGTIN
		}
		d.mu.Unlock()
	}

	if err := d.startPreparedSession(); err != nil {
		return 0, err
	}

	d.mu.RLock()
	s := d.session
	d.mu.RUnlock()
	if s == nil {
		d.mu.RLock()
		err := d.sessionErr
		d.mu.RUnlock()
		if err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("сессия передачи марок Bizerba не инициализирована")
	}

	return s.request(sessionRequest{kind: "enqueue", marks: marks})
}

// stopSession закрывает канал E и завершает постоянный COM-сеанс.
func (d *Driver) stopSession() error {
	d.mu.RLock()
	s := d.session
	d.mu.RUnlock()
	if s == nil {
		d.mu.Lock()
		err := d.sessionErr
		d.sessionErr = nil
		d.mu.Unlock()
		return err
	}
	_, err := s.request(sessionRequest{kind: "stop"})
	<-s.done
	d.mu.Lock()
	if d.session == s {
		d.session = nil
	}
	d.mu.Unlock()
	return err
}

// Close освобождает ресурсы активной или подготовленной сессии.
func (d *Driver) Close() error {
	d.startMu.Lock()
	defer d.startMu.Unlock()
	d.mu.Lock()
	d.closed = true
	d.prepared = nil
	s := d.session
	d.mu.Unlock()
	if d.captureStop != nil {
		close(d.captureStop)
		d.captureStop = nil
	}
	if s == nil {
		return nil
	}
	_, err := s.request(sessionRequest{kind: "close"})
	<-s.done
	return err
}

// ClearQueue отменяет подготовленную очередь либо закрывает активную сессию марок.
func (d *Driver) ClearQueue() error { return d.clearSession() }

// clearSession удаляет отложенную конфигурацию и останавливает активную сессию.
func (d *Driver) clearSession() error {
	d.startMu.Lock()
	defer d.startMu.Unlock()
	d.mu.Lock()
	d.prepared = nil
	d.capturePaused = true
	d.mu.Unlock()
	return d.stopSession()
}

// GetBufferFreeSpace запускает подготовленную сессию и возвращает свободное место очереди.
func (d *Driver) GetBufferFreeSpace() (int, error) {
	if err := d.startPreparedSession(); err != nil {
		return 0, err
	}
	d.mu.RLock()
	s := d.session
	d.mu.RUnlock()
	if s == nil {
		d.mu.RLock()
		err := d.sessionErr
		d.mu.RUnlock()
		return 0, err
	}
	return s.request(sessionRequest{kind: "free"})
}

// GetLastPrintedIndex возвращает индекс последней марки, подтверждённой пакетом PV01, PV04 или PV05.
func (d *Driver) GetLastPrintedIndex() (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastPrinted, nil
}

// GetTemplates возвращает текущий PLU как доступный шаблон устройства.
func (d *Driver) GetTemplates() ([]string, error) {
	plu, err := d.GetCurrentTemplate()
	if err != nil {
		return nil, err
	}
	return []string{plu}, nil
}

// GetTemplateFields возвращает поля, поддерживаемые драйвером Bizerba.
func (d *Driver) GetTemplateFields(string) ([]string, error) {
	return []string{"date", "brigade", "DATAMATRIX"}, nil
}

// GetRemainingRibbon возвращает заглушку: BCS-драйвер пока не читает остаток ленты.
func (d *Driver) GetRemainingRibbon() (string, error) { return "N/A", nil }

// GetQueueCapacity возвращает заглушку: физическая ёмкость очереди устройством не сообщается.
func (d *Driver) GetQueueCapacity(string) (string, error) { return "0", nil }

// GetPrintSpeed возвращает заглушку: скорость печати через BCS пока не читается.
func (d *Driver) GetPrintSpeed() (string, error) { return "N/A", nil }

// GetCurrentPrintCount возвращает заглушку: количество определяется по пакетам PV01/PV04/PV05.
func (d *Driver) GetCurrentPrintCount() (string, error) { return "N/A", nil }

// firstField ищет первое непустое значение по одному из допустимых имён поля.
func firstField(fields map[string]string, names ...string) string {
	for key, value := range fields {
		for _, name := range names {
			if strings.EqualFold(strings.TrimSpace(key), name) {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

// normalizeBizerbaDate преобразует поддерживаемые представления даты в формат ddMMyy.
func normalizeBizerbaDate(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("пустая дата")
	}

	// Если уже ddMMyy (6 цифр)
	if len(value) == 6 {
		if _, err := time.Parse("020106", value); err == nil {
			return value, nil
		}
	}

	// Если yyyyMMdd (8 цифр, частый формат 1С)
	if len(value) == 8 {
		if parsed, err := time.Parse("20060102", value); err == nil {
			return parsed.Format("020106"), nil
		}
	}

	// Отрезаем время, если 1С прислала "2026-11-12T08:00:00" или "12.11.2026 00:00:00"
	datePart := value
	if idx := strings.IndexAny(value, " T"); idx != -1 {
		datePart = value[:idx]
	}

	for _, layout := range []string{"02.01.2006", "2006-01-02", "02/01/2006"} {
		if parsed, err := time.Parse(layout, datePart); err == nil {
			return parsed.Format("020106"), nil
		}
	}

	return "", fmt.Errorf("неподдерживаемый формат даты Bizerba %q", value)
}

// cloneFields создаёт независимую копию набора статических полей.
func cloneFields(fields map[string]string) map[string]string {
	result := make(map[string]string, len(fields))
	for key, value := range fields {
		result[key] = value
	}
	return result
}

// GetTotalPrints удовлетворяет контракту core.Printer для снятия показаний одометра.
// Для Bizerba возвращает индекс последней подтверждённой марки либо 0.
func (d *Driver) GetTotalPrints() (int64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.lastPrinted < 0 {
		return 0, nil
	}
	return int64(d.lastPrinted), nil
}
