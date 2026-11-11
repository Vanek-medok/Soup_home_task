// Chaos-харнесс для сервиса очереди training-job'ов.
//
// Идея: недостаточно показать, что «happy path» зелёный. Харнесс намеренно ломает систему
// (убивает воркеры, замораживает их, шлёт дубли и отмены) и после этого проверяет
// инварианты, которые должны выполняться ВСЕГДА, как бы ни сложились тайминги.
//
// Что происходит за один прогон:
//   - отправляется N задач с уникальными idempotency-ключами; часть ключей шлётся
//     повторно и параллельно (имитация клиентских ретраев);
//   - параллельно отменяются случайные задачи (в очереди, в работе и уже завершённые);
//   - примерно раз в KillEvery случайному воркеру-контейнеру прилетает SIGKILL;
//     иногда вместо этого SIGSTOP на время дольше lease и затем SIGCONT (zombie-воркер);
//   - watcher опрашивает БД и ловит запрещённые переходы (терминальный статус -> другой);
//   - когда очередь опустела, инварианты проверяются SQL-запросами и через API;
//   - сырой вывод пишется в runs/<run-id>/run.log и result.json; код выхода 1 при нарушениях.
//
// КОНТРАКТ, который харнесс предполагает (если у вас иначе, поправьте константы и запросы):
//
//	POST /jobs                {"idempotency_key","payload"} -> 200/201 {"id","status"}
//	POST /jobs/{id}/cancel    -> любой код, тело {"id","status"} (статус на момент ответа)
//	GET  /jobs?limit=&cursor= -> {"items":[{"id",...}],"next_cursor":""}
//	jobs(id uuid, idempotency_key text, status text, attempt int, ...)
//	effects(job_id uuid, worker_id text, attempt int, ...)  <- ТОЛЬКО ДОБАВЛЕНИЕ, БЕЗ UNIQUE(job_id)!
//
// Почему у effects не должно быть UNIQUE(job_id): иначе двойное исполнение будет молча
// проглочено через ON CONFLICT DO NOTHING, и харнесс не сможет его посчитать.
// Гарантия «ровно один эффект» должна обеспечиваться вашим fencing'ом, его мы и проверяем.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// config: все параметры прогона. Сохраняется в result.json (кроме DSN, там может быть пароль),
// чтобы по сырому выводу можно было понять, с какими настройками шёл каждый прогон.
type config struct {
	API          string        `json:"api"`             // базовый URL API
	DSN          string        `json:"-"`               // строка подключения к Postgres (в отчёт не пишем)
	Jobs         int           `json:"jobs"`            // сколько уникальных задач создать
	Conc         int           `json:"conc"`            // число параллельных HTTP-горутин генератора нагрузки
	RPS          int           `json:"rps"`             // целевая скорость создания задач (0 = без ограничения)
	DupRate      float64       `json:"dup_rate"`        // доля задач, которые POST'ятся несколько раз параллельно
	CancelRPS    int           `json:"cancel_rps"`      // сколько запросов cancel в секунду
	KillEvery    time.Duration `json:"kill_every"`      // средний интервал между действиями хаоса
	FreezeProb   float64       `json:"freeze_prob"`     // вероятность, что действие хаоса = SIGSTOP, а не SIGKILL
	FreezeFor    time.Duration `json:"freeze_for"`      // сколько воркер остаётся замороженным (должно быть > lease)
	WorkerSvc    string        `json:"worker_service"`  // имя сервиса воркеров в docker compose
	Project      string        `json:"compose_project"` // имя compose-проекта (необязательный фильтр)
	DrainTimeout time.Duration `json:"drain_timeout"`   // сколько максимум ждать, пока очередь опустеет
	Settle       time.Duration `json:"settle"`          // сколько ещё наблюдать после дренажа (поздние записи zombie)
	Seed         int64         `json:"seed"`            // seed генератора случайных чисел
	OutDir       string        `json:"out_dir"`         // куда писать сырой вывод
	Reset        bool          `json:"reset"`           // очистить таблицы перед прогоном
}

// jobResp: минимальный ответ API, который нам нужен (id и статус задачи).
type jobResp struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// listResp: страница списка задач. Курсорная пагинация: пока next_cursor не пуст, есть ещё страницы.
type listResp struct {
	Items []struct {
		ID string `json:"id"`
	} `json:"items"`
	NextCursor string `json:"next_cursor"`
}

// cancelRec: что именно API ответил на запрос отмены. Позже сверяем это с итоговым статусом в БД.
type cancelRec struct {
	ID     string
	Code   int    // HTTP-код ответа
	Status string // статус задачи в теле ответа (как его увидел клиент)
}

// Violation: одно нарушение инварианта (check = краткий код проверки, detail = подробности).
type Violation struct {
	Check  string `json:"check"`
	Detail string `json:"detail"`
}

// Report: итоговый отчёт прогона, сериализуется в result.json.
type Report struct {
	RunID      string           `json:"run_id"`
	Config     config           `json:"config"`
	Started    time.Time        `json:"started"`
	Finished   time.Time        `json:"finished"`
	Stats      map[string]int64 `json:"stats"`              // счётчики: kills, freezes, post_retry, final_* и т.д.
	Counts     map[string]int   `json:"violation_counts"`   // сколько нарушений каждого вида
	Violations []Violation      `json:"violation_examples"` // до 20 примеров на каждый вид
	Warnings   []string         `json:"warnings"`           // «этот прогон мало что доказывает»
	Pass       bool             `json:"pass"`
}

// harness: общее состояние прогона. Все поля, помеченные «под mu», читаются и пишутся
// только под мьютексом, потому что к ним ходят десятки горутин одновременно.
type harness struct {
	c      config
	prefix string // префикс ключей этого прогона; изолирует его данные от прошлых прогонов
	hc     *http.Client
	pool   *pgxpool.Pool

	mu      sync.Mutex
	ids     map[string]map[string]bool // ключ -> все id, которые API когда-либо вернул для него (должен быть ровно один)
	created []string                   // id созданных задач; из них canceller выбирает цели
	cancels []cancelRec                // все ответы на cancel
	viol    []Violation                // примеры нарушений
	vcount  map[string]int             // счётчик нарушений по видам
	stats   map[string]int64           // общие счётчики
	frozen  map[string]bool            // контейнеры, которые сейчас заморожены SIGSTOP
	warns   []string
	bg      sync.WaitGroup // фоновые горутины cancel'ов, чтобы дождаться их до проверок
}

// env возвращает переменную окружения или значение по умолчанию.
func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	// --- разбор параметров командной строки ---
	var c config
	flag.StringVar(&c.API, "api", env("API_URL", "http://localhost:8080"), "базовый URL API")
	flag.StringVar(&c.DSN, "dsn", env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/jobs"), "строка подключения к Postgres")
	flag.IntVar(&c.Jobs, "jobs", 10000, "число уникальных задач")
	flag.IntVar(&c.Conc, "conc", 64, "число параллельных HTTP-горутин генератора нагрузки")
	flag.IntVar(&c.RPS, "rps", 500, "целевая скорость создания задач (0 = без ограничения)")
	flag.Float64Var(&c.DupRate, "dup-rate", 0.3, "доля задач, которые отправляются несколько раз параллельно")
	flag.IntVar(&c.CancelRPS, "cancel-rps", 20, "запросов cancel в секунду")
	flag.DurationVar(&c.KillEvery, "kill-every", 2*time.Second, "средний интервал между действиями хаоса")
	flag.Float64Var(&c.FreezeProb, "freeze-prob", 0.05, "вероятность SIGSTOP вместо SIGKILL")
	flag.DurationVar(&c.FreezeFor, "freeze-for", 45*time.Second, "сколько воркер остаётся замороженным (должно превышать lease)")
	flag.StringVar(&c.WorkerSvc, "worker-service", "worker", "имя сервиса воркеров в docker compose")
	flag.StringVar(&c.Project, "compose-project", "", "имя docker compose проекта (необязательный фильтр)")
	flag.DurationVar(&c.DrainTimeout, "drain-timeout", 10*time.Minute, "максимум ожидания, пока не останется queued/running")
	flag.DurationVar(&c.Settle, "settle", 15*time.Second, "сколько наблюдать после дренажа, чтобы поймать поздние записи zombie-воркеров")
	flag.Int64Var(&c.Seed, "seed", 0, "seed (0 = по времени). Фиксирует нагрузку, но не тайминги")
	flag.StringVar(&c.OutDir, "out", "runs", "каталог для сырого вывода")
	flag.BoolVar(&c.Reset, "reset", false, "выполнить TRUNCATE effects, jobs перед прогоном")
	flag.Parse()
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano()
	}

	// --- каталог прогона и лог ---
	// Каждый прогон получает свой каталог по времени запуска. Лог пишется и в stdout,
	// и в файл: сырой вывод всех прогонов (включая упавшие) нужно сдать по условию задания.
	runID := time.Now().UTC().Format("20060102T150405Z")
	outDir := filepath.Join(c.OutDir, runID)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	lf, err := os.Create(filepath.Join(outDir, "run.log"))
	if err != nil {
		log.Fatal(err)
	}
	defer lf.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, lf))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// --- подключение к БД ---
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, c.DSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	h := &harness{
		c:      c,
		prefix: "chaos-" + runID + "-", // все ключи этого прогона начинаются с этого префикса
		hc: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: 256}, // иначе под нагрузкой упрёмся в пул соединений
		},
		pool:   pool,
		ids:    map[string]map[string]bool{},
		vcount: map[string]int{},
		stats:  map[string]int64{},
		frozen: map[string]bool{},
	}
	started := time.Now()
	log.Printf("run %s seed=%d config=%+v", runID, c.Seed, c)

	if c.Reset {
		if _, err := pool.Exec(ctx, `TRUNCATE effects, jobs`); err != nil {
			log.Fatalf("reset: %v", err)
		}
	}

	// --- фоновые участники: хаос, отмены, наблюдатель ---
	// У хаоса/отмен и у наблюдателя разные контексты намеренно: хаос останавливаем сразу
	// после дренажа, а наблюдатель живёт ещё Settle секунд, чтобы увидеть поздние
	// записи размороженных zombie-воркеров.
	chaosCtx, stopChaos := context.WithCancel(ctx)
	watchCtx, stopWatch := context.WithCancel(ctx)
	var cwg sync.WaitGroup
	cwg.Add(3)
	go func() { defer cwg.Done(); h.chaos(chaosCtx) }()
	go func() { defer cwg.Done(); h.canceller(chaosCtx) }()
	go func() { defer cwg.Done(); h.watch(watchCtx) }()

	// --- основная фаза ---
	h.load() // блокируется, пока все задачи не отправлены
	log.Printf("load finished: acked keys=%d", len(h.ids))
	q, r := h.drain(ctx) // ждём, пока воркеры (под хаосом!) обработают всё
	log.Printf("drain finished: queued=%d running=%d", q, r)

	stopChaos() // chaos() перед выходом сам размораживает замороженные воркеры
	h.bg.Wait()
	log.Printf("chaos stopped, settling %s to catch late zombie writes", c.Settle)
	time.Sleep(c.Settle)
	stopWatch()
	cwg.Wait()

	// --- проверка инвариантов и отчёт ---
	h.check(ctx)

	rep := Report{
		RunID: runID, Config: c, Started: started, Finished: time.Now(),
		Stats: h.stats, Counts: h.vcount, Violations: h.viol, Warnings: h.warns,
		Pass: len(h.vcount) == 0,
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	_ = os.WriteFile(filepath.Join(outDir, "result.json"), b, 0o644)
	log.Printf("stats: %v", h.stats)
	for _, w := range h.warns {
		log.Printf("WARNING: %s", w)
	}
	if rep.Pass {
		log.Printf("RESULT: PASS (run %s)", runID)
		return
	}
	log.Printf("RESULT: FAIL (run %s): %v", runID, h.vcount)
	os.Exit(1) // ненулевой код, чтобы run_many.sh посчитал прогон упавшим
}

// ---------- учёт ----------

// inc увеличивает счётчик статистики (потокобезопасно).
func (h *harness) inc(name string, n int64) {
	h.mu.Lock()
	h.stats[name] += n
	h.mu.Unlock()
}

// violate фиксирует нарушение инварианта. Считаем ВСЕ нарушения, но сохраняем только
// первые 20 примеров каждого вида, чтобы отчёт не раздувался на 10 000 одинаковых строк.
func (h *harness) violate(check, f string, a ...any) {
	msg := fmt.Sprintf(f, a...)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.vcount[check]++
	if h.vcount[check] <= 20 {
		h.viol = append(h.viol, Violation{check, msg})
	}
	log.Printf("VIOLATION %s: %s", check, msg)
}

// key строит idempotency-ключ для задачи i. Префикс прогона делает ключи уникальными между прогонами.
func (h *harness) key(i int) string { return fmt.Sprintf("%s%06d", h.prefix, i) }

// ---------- HTTP ----------

// call выполняет HTTP-запрос и возвращает код, тело и ошибку транспорта.
// Ошибка != nil значит, что ответа мы не получили (таймаут, обрыв): исход на сервере неизвестен.
func (h *harness) call(method, path string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, h.c.API+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

// createJob создаёт задачу, ретраясь при сетевых ошибках и 5xx, как делал бы настоящий клиент.
// Именно ретрай с тем же ключом и должен переживать механизм идемпотентности.
// Для КАЖДОГО успешного ответа запоминаем возвращённый id: позже проверим, что для одного ключа
// API всегда возвращал один и тот же id.
func (h *harness) createJob(key string, n int) (string, bool) {
	for attempt := 1; attempt <= 6; attempt++ {
		code, b, err := h.call("POST", "/jobs", map[string]any{
			"idempotency_key": key,
			"payload":         map[string]int{"n": n}, // payload одинаков для всех ретраев этого ключа
		})
		if err == nil && (code == 200 || code == 201) {
			var r jobResp
			if json.Unmarshal(b, &r) == nil && r.ID != "" {
				h.mu.Lock()
				if h.ids[key] == nil {
					h.ids[key] = map[string]bool{}
				}
				h.ids[key][r.ID] = true
				h.mu.Unlock()
				h.inc("post_ok", 1)
				return r.ID, true
			}
		}
		// 409 на тот же payload означает, что сервис считает ретрай конфликтом: это баг идемпотентности.
		if err == nil && code == 409 {
			h.violate("idempotency_conflict", "key %s got 409 for an identical payload", key)
			return "", false
		}
		h.inc("post_retry", 1)
		time.Sleep(time.Duration(attempt) * 100 * time.Millisecond) // линейный backoff
	}
	h.inc("post_failed", 1)
	return "", false
}

// cancelJob отправляет cancel и запоминает, что именно ответил API.
// Если ответа нет (ошибка сети), ничего не проверяем: результат неизвестен.
func (h *harness) cancelJob(id string) {
	code, b, err := h.call("POST", "/jobs/"+id+"/cancel", nil)
	if err != nil {
		h.inc("cancel_err", 1)
		return
	}
	var r jobResp
	_ = json.Unmarshal(b, &r)
	h.mu.Lock()
	h.cancels = append(h.cancels, cancelRec{id, code, r.Status})
	h.mu.Unlock()
	h.inc(fmt.Sprintf("cancel_http_%d", code), 1)
}

// ---------- генератор нагрузки ----------

// load создаёт все задачи. Индексы 0..Jobs-1 раздаются Conc горутинам через канал,
// а тикер ограничивает общую скорость до RPS.
func (h *harness) load() {
	idx := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < h.c.Conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				h.one(i)
			}
		}()
	}
	var tick <-chan time.Time
	if h.c.RPS > 0 {
		t := time.NewTicker(time.Second / time.Duration(h.c.RPS))
		defer t.Stop()
		tick = t.C
	}
	for i := 0; i < h.c.Jobs; i++ {
		if tick != nil {
			<-tick // пейсинг: не чаще RPS задач в секунду
		}
		idx <- i
	}
	close(idx)
	wg.Wait()
}

// one создаёт задачу i. Дубли запускаются ОДНОВРЕМЕННО с оригиналом, чтобы по-настоящему
// проверить гонку INSERT ... ON CONFLICT; часть дублей откладывается на случайное время,
// имитируя запоздалый клиентский ретрай.
func (h *harness) one(i int) {
	// Отдельный генератор на задачу: решения (сколько дублей, какие задержки) детерминированы
	// по seed и не зависят от порядка выполнения горутин.
	rng := rand.New(rand.NewSource(h.c.Seed + int64(i)))
	key := h.key(i)

	copies := 1
	if rng.Float64() < h.c.DupRate {
		copies += 1 + rng.Intn(4) // от 1 до 4 дополнительных копий запроса
	}
	delays := make([]time.Duration, copies)
	for d := 1; d < copies; d++ {
		if rng.Intn(2) == 0 {
			delays[d] = time.Duration(rng.Intn(500)) * time.Millisecond
		}
	}
	got := make([]string, copies)
	var wg sync.WaitGroup
	for d := 0; d < copies; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			time.Sleep(delays[d])
			got[d], _ = h.createJob(key, i)
		}(d)
	}
	wg.Wait()
	h.inc("dup_posts", int64(copies-1))

	// Регистрируем id (первый непустой) как цель для будущих cancel'ов.
	for _, id := range got {
		if id != "" {
			h.mu.Lock()
			h.created = append(h.created, id)
			h.mu.Unlock()
			break
		}
	}
}

// pickID выбирает задачу для отмены. 70% выбора идёт из последних 500 созданных
// (они, скорее всего, ещё queued или running), остальное из всех (включая уже завершённые,
// чтобы проверить, что отмена завершённой задачи ничего не портит).
func (h *harness) pickID(rng *rand.Rand) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := len(h.created)
	if n == 0 {
		return ""
	}
	if n > 500 && rng.Float64() < 0.7 {
		return h.created[n-1-rng.Intn(500)]
	}
	return h.created[rng.Intn(n)]
}

// canceller непрерывно шлёт cancel'ы со скоростью CancelRPS, пока не отменят контекст.
// Работает параллельно с созданием задач и с хаосом: так cancel гоняется с claim и complete.
func (h *harness) canceller(ctx context.Context) {
	if h.c.CancelRPS <= 0 {
		return
	}
	rng := rand.New(rand.NewSource(h.c.Seed ^ 0xca11))
	t := time.NewTicker(time.Second / time.Duration(h.c.CancelRPS))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		id := h.pickID(rng)
		if id == "" {
			continue
		}
		copies := 1
		if rng.Float64() < 0.3 {
			copies = 2 // параллельные дубли cancel'а на одну и ту же задачу
		}
		for k := 0; k < copies; k++ {
			h.bg.Add(1)
			go func() { defer h.bg.Done(); h.cancelJob(id) }()
		}
	}
}

// ---------- хаос ----------

// workers возвращает ID контейнеров воркеров через docker CLI (фильтр по меткам docker compose).
// Для этого контейнеру харнесса нужен docker CLI и смонтированный /var/run/docker.sock.
func (h *harness) workers(ctx context.Context) ([]string, error) {
	args := []string{"ps", "--filter", "label=com.docker.compose.service=" + h.c.WorkerSvc, "--format", "{{.ID}}"}
	if h.c.Project != "" {
		args = append(args, "--filter", "label=com.docker.compose.project="+h.c.Project)
	}
	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	return strings.Fields(string(out)), err
}

// signal отправляет сигнал главному процессу контейнера. SIGKILL и SIGSTOP работают даже
// для PID 1 внутри контейнера, потому что приходят из родительского namespace.
// Ошибки (например, контейнер как раз перезапускается) логируем, но не считаем фатальными.

// signal отправляет сигнал процессу Go внутри контейнера,
// а не самому контейнеру через Docker API.
//
// Это позволяет Docker обнаружить падение основного процесса
// и применить restart policy. PID находим динамически через pidof.
func (h *harness) signal(id, sig string) error {
	pid, err := exec.Command(
		"docker", "exec", id, "pidof", "service",
	).Output()
	if err != nil {
		log.Printf("chaos: cannot find service process in %s: %v", id, err)
		return err
	}

	processID := strings.TrimSpace(string(pid))
	if processID == "" || strings.Contains(processID, " ") {
		return fmt.Errorf("unexpected service PID in container %s: %q", id, processID)
	}

	out, err := exec.Command(
		"docker", "exec", id, "kill", "-"+sig, processID,
	).CombinedOutput()

	if err != nil {
		log.Printf(
			"chaos: kill -%s %s in %s failed: %v %s",
			sig, processID, id, err, strings.TrimSpace(string(out)),
		)
	}

	return err
}

// chaos: цикл «подождать (с джиттером) -> выбрать воркер -> убить или заморозить».
// Убитый воркер поднимется заново благодаря restart: always в compose.
func (h *harness) chaos(ctx context.Context) {
	rng := rand.New(rand.NewSource(h.c.Seed ^ 0x5eed))
	var fw sync.WaitGroup
	defer fw.Wait() // перед выходом дожидаемся разморозки всех замороженных воркеров
	for {
		d := h.c.KillEvery/2 + time.Duration(rng.Int63n(int64(h.c.KillEvery))) // джиттер: 0.5x..1.5x от KillEvery
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
		ids, err := h.workers(ctx)
		if err != nil || len(ids) == 0 {
			h.inc("chaos_no_workers", 1)
			continue
		}
		target := ids[rng.Intn(len(ids))]
		// Не трогаем уже замороженный воркер и не замораживаем больше двух сразу,
		// иначе пропускная способность упадёт и прогон затянется.
		h.mu.Lock()
		skip := h.frozen[target] || len(h.frozen) >= 2
		h.mu.Unlock()
		if skip {
			continue
		}
		if rng.Float64() < h.c.FreezeProb {
			h.mu.Lock()
			h.frozen[target] = true
			h.mu.Unlock()
			fw.Add(1)
			go func() { defer fw.Done(); h.freeze(ctx, target) }()
			continue
		}
		if h.signal(target, "KILL") == nil {
			h.inc("kills", 1)
			log.Printf("chaos: SIGKILL %s", target[:12])
		}
	}
}

// freeze: сценарий zombie-воркера. Воркер замораживается на время, превышающее lease,
// пропускает heartbeat'ы, его задачу забирает другой воркер, а потом он «просыпается»
// и пытается записать устаревший результат. Fencing должен этот результат отвергнуть.
// Размораживаем и при завершении прогона (ctx.Done), чтобы не оставить контейнер замороженным.
func (h *harness) freeze(ctx context.Context, id string) {
	defer func() {
		h.signal(id, "CONT")
		h.mu.Lock()
		delete(h.frozen, id)
		h.mu.Unlock()
		h.inc("thaws", 1)
		log.Printf("chaos: SIGCONT %s", id[:12])
	}()
	if h.signal(id, "STOP") != nil {
		return
	}
	h.inc("freezes", 1)
	log.Printf("chaos: SIGSTOP %s for %s", id[:12], h.c.FreezeFor)
	select {
	case <-time.After(h.c.FreezeFor):
	case <-ctx.Done():
	}
}

// ---------- наблюдатели ----------

// terminal: статус, из которого задача выходить не должна.
func terminal(s string) bool { return s == "succeeded" || s == "failed" || s == "cancelled" }

// watch раз в 0,5 с снимает срез таблицы jobs и ищет переходы, которых быть не должно:
//   - терминальный статус сменился на другой (например, cancelled -> succeeded);
//   - счётчик attempt уменьшился.
//
// ОГРАНИЧЕНИЕ: это выборка. Нарушение, которое жило меньше интервала опроса, невидимо.
func (h *harness) watch(ctx context.Context) {
	prev := map[string]string{} // ключ -> предыдущий статус
	prevAtt := map[string]int{} // ключ -> предыдущий attempt
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rows, err := h.pool.Query(ctx, `SELECT idempotency_key, status, attempt FROM jobs WHERE idempotency_key LIKE $1`, h.prefix+"%")
		if err != nil {
			continue
		}
		for rows.Next() {
			var k, s string
			var a int
			if rows.Scan(&k, &s, &a) != nil {
				continue
			}
			if p, ok := prev[k]; ok {
				if terminal(p) && p != s {
					h.violate("terminal_transition", "%s: %s -> %s", k, p, s)
				}
				if a < prevAtt[k] {
					h.violate("attempt_decreased", "%s: %d -> %d", k, prevAtt[k], a)
				}
			}
			prev[k], prevAtt[k] = s, a
		}
		rows.Close()
		h.inc("watch_polls", 1)
	}
}

// drain ждёт, пока среди задач этого прогона не останется queued и running.
// Хаос в это время продолжается, поэтому дренаж проверяет и то, что система
// самовосстанавливается после убийств (lease истекает, задачи возвращаются в очередь).
// Если за DrainTimeout очередь не опустела, это нарушение: задачи потеряны или застряли.
func (h *harness) drain(ctx context.Context) (int, int) {
	deadline := time.Now().Add(h.c.DrainTimeout)
	last := time.Now()
	for {
		var q, r int
		err := h.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='queued'), count(*) FILTER (WHERE status='running')
			FROM jobs WHERE idempotency_key LIKE $1`, h.prefix+"%").Scan(&q, &r)
		if err != nil {
			log.Printf("drain query: %v", err)
		} else {
			if time.Since(last) > 5*time.Second {
				log.Printf("draining: queued=%d running=%d", q, r)
				last = time.Now()
			}
			if q+r == 0 {
				return q, r
			}
			if time.Now().After(deadline) {
				h.violate("drain_timeout", "after %s still queued=%d running=%d", h.c.DrainTimeout, q, r)
				return q, r
			}
		}
		time.Sleep(time.Second)
	}
}

// ---------- инварианты ----------

// rows выполняет запрос и вызывает fn для каждой строки. Ошибка запроса или скана
// сама считается нарушением: молчаливо проглоченная ошибка проверки дала бы ложный «PASS».
func (h *harness) rows(ctx context.Context, q string, args []any, fn func(pgx.Rows) error) {
	rows, err := h.pool.Query(ctx, q, args...)
	if err != nil {
		h.violate("check_query_error", "%v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(rows); err != nil {
			h.violate("check_scan_error", "%v", err)
			return
		}
	}
}

// check запускает все итоговые проверки. Вызывается, когда очередь опустела и хаос остановлен.
func (h *harness) check(ctx context.Context) {
	p := []any{h.prefix + "%"} // параметр для LIKE: только задачи этого прогона

	// Итоговое состояние всех задач прогона в БД: ключ -> (id, статус) и id -> статус.
	type fin struct{ id, status string }
	byKey := map[string]fin{}
	byID := map[string]string{}
	h.rows(ctx, `SELECT id::text, idempotency_key, status FROM jobs WHERE idempotency_key LIKE $1`, p, func(r pgx.Rows) error {
		var id, k, s string
		if err := r.Scan(&id, &k, &s); err != nil {
			return err
		}
		byKey[k] = fin{id, s}
		byID[id] = s
		h.inc("final_"+s, 1) // распределение итоговых статусов попадает в отчёт
		return nil
	})
	h.inc("jobs_in_db", int64(len(byKey)))
	h.inc("keys_acked", int64(len(h.ids)))

	// 1. Ни одна задача не потеряна + идемпотентность:
	//    если API подтвердил ключ, он обязан быть в БД; на один ключ приходился ровно один id,
	//    и именно тот, что лежит в БД.
	for key, set := range h.ids {
		f, ok := byKey[key]
		if !ok {
			h.violate("lost_job", "key %s was acknowledged by the API but is not in the DB", key)
			continue
		}
		if len(set) > 1 {
			h.violate("idempotency_multiple_ids", "key %s returned %d different ids", key, len(set))
		}
		for id := range set {
			if id != f.id {
				h.violate("idempotency_id_mismatch", "key %s: API returned %s, DB has %s", key, id, f.id)
			}
		}
	}
	// Ключи, которые создались, но клиент так и не получил ответ (обрыв связи): это не нарушение, просто статистика.
	h.inc("keys_unacked_in_db", int64(len(byKey)-len(h.ids)))

	// 2. Ничего не застряло в queued/running после дренажа.
	h.rows(ctx, `SELECT idempotency_key, status FROM jobs WHERE idempotency_key LIKE $1 AND status IN ('queued','running')`, p, func(r pgx.Rows) error {
		var k, s string
		if err := r.Scan(&k, &s); err != nil {
			return err
		}
		h.violate("stuck", "%s is still %s", k, s)
		return nil
	})

	// 3. Не более одного побочного эффекта на задачу (главная гарантия «exactly-once effect»).
	h.rows(ctx, `SELECT e.job_id::text, count(*) FROM effects e JOIN jobs j ON j.id=e.job_id
		WHERE j.idempotency_key LIKE $1 GROUP BY e.job_id HAVING count(*) > 1`, p, func(r pgx.Rows) error {
		var id string
		var n int
		if err := r.Scan(&id, &n); err != nil {
			return err
		}
		h.violate("duplicate_effect", "job %s has %d effects", id, n)
		return nil
	})

	// 4. Согласованность статуса и эффектов: succeeded <=> ровно один эффект.
	//    Обратное направление: у задач в другом статусе эффектов быть не должно.
	h.rows(ctx, `SELECT j.id::text, count(e.job_id) FROM jobs j LEFT JOIN effects e ON e.job_id=j.id
		WHERE j.idempotency_key LIKE $1 AND j.status='succeeded' GROUP BY j.id HAVING count(e.job_id) <> 1`, p, func(r pgx.Rows) error {
		var id string
		var n int
		if err := r.Scan(&id, &n); err != nil {
			return err
		}
		h.violate("succeeded_effect_count", "job %s is succeeded with %d effects", id, n)
		return nil
	})
	h.rows(ctx, `SELECT j.id::text, j.status, count(*) FROM jobs j JOIN effects e ON e.job_id=j.id
		WHERE j.idempotency_key LIKE $1 AND j.status <> 'succeeded' GROUP BY j.id, j.status`, p, func(r pgx.Rows) error {
		var id, s string
		var n int
		if err := r.Scan(&id, &s, &n); err != nil {
			return err
		}
		h.violate("effect_without_success", "job %s is %s but has %d effects", id, s, n)
		return nil
	})

	// 5. Fencing: эффект должен принадлежать той попытке, которая записана как финальная.
	//    Расхождение значит, что результат устаревшего (zombie) воркера прошёл мимо проверки.
	h.rows(ctx, `SELECT j.id::text, j.attempt, e.attempt FROM jobs j JOIN effects e ON e.job_id=j.id
		WHERE j.idempotency_key LIKE $1 AND j.status='succeeded' AND e.attempt <> j.attempt`, p, func(r pgx.Rows) error {
		var id string
		var ja, ea int
		if err := r.Scan(&id, &ja, &ea); err != nil {
			return err
		}
		h.violate("stale_attempt_effect", "job %s: jobs.attempt=%d but effect.attempt=%d", id, ja, ea)
		return nil
	})

	// 6. Контракт cancel: то, что API сообщил клиенту, должно совпадать с итоговым состоянием.
	//    - ответил «cancelled» -> итог обязан быть cancelled (иначе клиенту соврали);
	//    - ответил «succeeded/failed» -> статус не должен был измениться потом.
	//    Ответ «running» (отмена запрошена) не оценивается: исход там допустим любой.
	h.mu.Lock()
	cancels := append([]cancelRec(nil), h.cancels...)
	h.mu.Unlock()
	for _, cr := range cancels {
		fs, ok := byID[cr.ID]
		if !ok {
			continue
		}
		switch cr.Status {
		case "cancelled":
			if fs != "cancelled" {
				h.violate("cancel_ack_lost", "job %s: cancel answered 'cancelled' (http %d) but final=%s", cr.ID, cr.Code, fs)
			}
		case "succeeded", "failed":
			if fs != cr.Status {
				h.violate("terminal_changed_after_cancel", "job %s: cancel saw %s, final=%s", cr.ID, cr.Status, fs)
			}
		}
	}

	// 7. Доказательство, что хаос реально сработал. Без него зелёный прогон ничего не значит:
	//    если ни одну задачу не пришлось перезахватывать (attempt > 1), kill'ы ни разу
	//    не попали в running-задачу и код восстановления вообще не выполнялся.
	var reclaimed, maxAttempt int64
	_ = h.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE attempt > 1), coalesce(max(attempt),0)
		FROM jobs WHERE idempotency_key LIKE $1`, p...).Scan(&reclaimed, &maxAttempt)
	h.inc("jobs_reclaimed", reclaimed)
	h.inc("max_attempt", maxAttempt)
	var effTotal int64
	_ = h.pool.QueryRow(ctx, `SELECT count(*) FROM effects e JOIN jobs j ON j.id=e.job_id WHERE j.idempotency_key LIKE $1`, p...).Scan(&effTotal)
	h.inc("effects_total", effTotal)

	h.mu.Lock()
	kills, freezes := h.stats["kills"], h.stats["freezes"]
	h.mu.Unlock()
	if kills == 0 && freezes == 0 {
		h.warns = append(h.warns, "no chaos action was executed: this run proves nothing (docker socket mounted? worker service name?)")
	}
	if reclaimed == 0 {
		h.warns = append(h.warns, "no job was ever re-claimed (attempt>1): kills never hit a running job, the run is not informative; slow the worker down")
	}
	if freezes > 0 && h.stats["watch_polls"] == 0 {
		h.warns = append(h.warns, "watcher never polled")
	}

	// 8. Пагинация (см. ниже).
	h.checkPagination(ctx, byID)
}

// checkPagination обходит GET /jobs до конца: каждая задача из БД должна встретиться ровно один раз.
// ОГРАНИЧЕНИЕ: проверка выполняется на неизменяющихся данных (после дренажа). Пропуски и дубли
// при вставках ВО ВРЕМЯ обхода этим кодом не ловятся.
func (h *harness) checkPagination(ctx context.Context, byID map[string]string) {
	seen := map[string]int{}
	cursor := ""
	pages := 0
	for {
		path := "/jobs?limit=200"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		code, b, err := h.call("GET", path, nil)
		if err != nil || code != 200 {
			h.violate("pagination_http", "GET %s -> code=%d err=%v", path, code, err)
			return
		}
		var lr listResp
		if err := json.Unmarshal(b, &lr); err != nil {
			h.violate("pagination_decode", "%v", err)
			return
		}
		for _, it := range lr.Items {
			seen[it.ID]++
		}
		pages++
		if lr.NextCursor == "" {
			break
		}
		cursor = lr.NextCursor
		if pages > 100000 { // защита от бесконечного цикла, если курсор никогда не заканчивается
			h.violate("pagination_loop", "cursor never ends")
			return
		}
	}
	for id, n := range seen {
		if n > 1 {
			h.violate("pagination_duplicate", "job %s listed %d times", id, n)
		}
	}
	for id := range byID {
		if seen[id] == 0 {
			h.violate("pagination_missing", "job %s is in the DB but never listed", id)
		}
	}
	h.inc("list_pages", int64(pages))
}
