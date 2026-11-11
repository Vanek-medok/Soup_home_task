package worker

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"job-queue/internal/config"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// claimed: что воркер получил при захвате. Attempt и есть fencing token:
// он растёт при каждом захвате, и только держатель ТЕКУЩЕГО значения может завершить задачу.
type claimed struct {
	ID      string
	Attempt int
}

type worker struct {
	cfg  config.Config
	pool *pgxpool.Pool
	id   string
}

// newWorkerID: hostname + случайный суффикс. Один hostname у контейнера сохраняется
// после рестарта, поэтому одного его мало: новый процесс должен иметь новый id.
func newWorkerID() string {
	h, _ := os.Hostname()
	b := make([]byte, 4)
	_, _ = crand.Read(b)
	return h + "-" + hex.EncodeToString(b)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d)))
}

func Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool) {
	w := &worker{cfg: cfg, pool: pool, id: newWorkerID()}
	log.Printf("worker %s started: lease=%s heartbeat=%s reap=%s job=%s..%s",
		w.id, cfg.Lease, cfg.Heartbeat, cfg.Reap, cfg.JobMin, cfg.JobMax)

	go w.reaper(ctx)

	// SIGTERM: новых задач не берём, текущую доделываем (execute не зависит от ctx).
	// SIGKILL: ничего не успеваем, задачу подберёт reaper после истечения lease.
	for ctx.Err() == nil {
		j, err := w.claim(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("claim: %v", err)
			}
			sleepCtx(ctx, time.Second)
			continue
		}
		if j == nil {
			sleepCtx(ctx, jitter(w.cfg.Poll))
			continue
		}
		w.execute(j)
	}
	log.Printf("worker %s stopped", w.id)
}

// ---------- захват ----------

// Атомарный захват одним оператором. FOR UPDATE SKIP LOCKED: параллельные воркеры
// пропускают строки, которые уже взял кто-то другой, и не ждут друг друга.
// Условие status='queued' в внешнем UPDATE дублирует подзапрос (страховка).
const claimSQL = `
UPDATE jobs
SET status = 'running',
    attempt = attempt + 1,
    lease_owner = $1,
    lease_expires_at = now() + make_interval(secs => $2),
    updated_at = now()
WHERE status = 'queued'
  AND id = (SELECT id FROM jobs WHERE status = 'queued'
            ORDER BY seq
            FOR UPDATE SKIP LOCKED
            LIMIT 1)
RETURNING id::text, attempt`

func (w *worker) claim(ctx context.Context) (*claimed, error) {
	if w.cfg.UnsafeClaim {
		return w.claimUnsafe(ctx)
	}
	var c claimed
	err := w.pool.QueryRow(ctx, claimSQL, w.id, w.cfg.Lease.Seconds()).Scan(&c.ID, &c.Attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// МУТАЦИЯ UNSAFE_CLAIM: классическая гонка «проверил, потом сделал».
// SELECT без блокировки, затем UPDATE без проверки статуса. Два воркера могут взять
// одну задачу, а задачу, отменённую между двумя шагами, можно «воскресить» в running.
func (w *worker) claimUnsafe(ctx context.Context) (*claimed, error) {
	var id string
	err := w.pool.QueryRow(ctx, `SELECT id::text FROM jobs WHERE status = 'queued' ORDER BY seq LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var c claimed
	err = w.pool.QueryRow(ctx,
		`UPDATE jobs SET status='running', attempt=attempt+1, lease_owner=$2,
		        lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
		 WHERE id = $1::text::uuid
		 RETURNING id::text, attempt`, id, w.id, w.cfg.Lease.Seconds()).Scan(&c.ID, &c.Attempt)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// ---------- исполнение ----------

func (w *worker) execute(j *claimed) {
	// Контекст исполнения НЕ привязан к SIGTERM, но его отменяет heartbeat,
	// если lease потерян или задачу отменили.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w.event(j, "started")
	go w.heartbeat(ctx, j, cancel)

	// Эмуляция работы. Побочный эффект НЕ пишется здесь: он пишется атомарно
	// вместе со сменой статуса в finish(), поэтому устаревший воркер ничего не оставит.
	dur := w.cfg.JobMin
	if span := int64(w.cfg.JobMax - w.cfg.JobMin); span > 0 {
		dur += time.Duration(rand.Int63n(span + 1))
	}
	select {
	case <-time.After(dur):
	case <-ctx.Done():
		w.event(j, "lease_lost")
		return
	}
	w.finish(j)
}

// heartbeat продлевает lease. Условие attempt + status='running' гарантирует, что
// продлевает только текущий владелец. 0 строк значит: задачу отменили или её уже забрал
// reaper (lease истёк). Тогда воркер должен бросить работу.
// Если БД недоступна, просто пробуем снова: решение принимает не воркер, а CAS в finish().
func (w *worker) heartbeat(ctx context.Context, j *claimed, abort context.CancelFunc) {
	t := time.NewTicker(w.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		tag, err := w.pool.Exec(ctx,
			`UPDATE jobs
			SET lease_expires_at = clock_timestamp()
				+ make_interval(secs => $3)
			WHERE id = $1::text::uuid
				AND attempt = $2
				AND status = 'running'
				AND lease_expires_at > clock_timestamp()`,
			j.ID, j.Attempt, w.cfg.Lease.Seconds())
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("heartbeat %s: %v", j.ID, err)
			}
			continue
		}
		if tag.RowsAffected() == 0 {
			abort()
			return
		}
	}
}

const completeSet = `UPDATE jobs SET status='succeeded', finished_at=now(), updated_at=now(),
	lease_owner=NULL, lease_expires_at=NULL`

// finish: завершение с fencing. В ОДНОЙ транзакции:
//  1. CAS: переводим в succeeded только если задача всё ещё running и attempt совпадает;
//  2. если 0 строк, результат устарел (lease истёк, задачу забрал другой или отменили):
//     откат, эффекта нет;
//  3. иначе пишем эффект и коммитим.
//
// Статус и эффект меняются атомарно, поэтому «succeeded» всегда означает ровно один эффект
// от именно этой попытки. Это даёт exactly-once effect поверх at-least-once исполнения.
func (w *worker) finish(j *claimed) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := w.pool.Begin(ctx) // READ COMMITTED
	if err != nil {
		log.Printf("finish begin %s: %v", j.ID, err)
		return // lease истечёт, задачу подберёт reaper
	}
	defer tx.Rollback(ctx)

	q := completeSet + `
    WHERE id = $1::text::uuid
      AND attempt = $2
      AND status = 'running'
      AND lease_expires_at > clock_timestamp()
	  `
	args := []any{j.ID, j.Attempt}
	if w.cfg.DisableFencing { // МУТАЦИЯ: безусловная перезапись
		q = completeSet + ` WHERE id = $1::text::uuid`
		args = []any{j.ID}
	}
	tag, err := tx.Exec(ctx, q, args...)
	if err != nil {
		log.Printf("finish update %s: %v", j.ID, err)
		return
	}
	if tag.RowsAffected() == 0 {
		_ = tx.Rollback(ctx)
		w.event(j, "complete_rejected")
		log.Printf("job %s attempt %d: stale completion rejected", j.ID, j.Attempt)
		return
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO effects (job_id, worker_id, attempt) VALUES ($1::text::uuid, $2, $3)`,
		j.ID, w.id, j.Attempt); err != nil {
		log.Printf("finish effect %s: %v", j.ID, err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		// Исход неизвестен (например, обрыв связи на коммите). Безопасно в обе стороны:
		// закоммитилось -> задача succeeded; нет -> lease истечёт и reaper вернёт её в очередь.
		log.Printf("finish commit %s: %v", j.ID, err)
	}
}

// ---------- reaper ----------

// Возвращает в очередь задачи умерших/замороженных воркеров: running с истёкшим lease.
// Время берётся из БД (now()), поэтому рассинхрон часов воркеров ни на что не влияет.
// После max_attempts захватов задача уходит в failed, а не крутится вечно.
// SKIP LOCKED: несколько reaper'ов (по одному в каждом воркере) не мешают друг другу,
// а строку, которую в этот момент продлевает heartbeat, пропускают до следующего цикла.
const reapSQL = `
WITH r AS (
    UPDATE jobs
    SET status = CASE WHEN attempt >= max_attempts THEN 'failed' ELSE 'queued' END,
        finished_at = CASE WHEN attempt >= max_attempts THEN now() END,
        lease_owner = NULL,
        lease_expires_at = NULL,
        updated_at = now()
    WHERE id IN (SELECT id FROM jobs
                 WHERE status = 'running' AND lease_expires_at < now()
                 ORDER BY lease_expires_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT 100)
    RETURNING id, attempt, status
)
INSERT INTO events (job_id, worker_id, attempt, kind)
SELECT id, $1, attempt, 'reaped_' || status FROM r`

func (w *worker) reaper(ctx context.Context) {
	for sleepCtx(ctx, jitter(w.cfg.Reap)) {
		tag, err := w.pool.Exec(ctx, reapSQL, w.id)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("reaper: %v", err)
			}
			continue
		}
		if n := tag.RowsAffected(); n > 0 {
			log.Printf("reaper %s: recovered %d job(s) with expired lease", w.id, n)
		}
	}
}

// event пишет в журнал доказательств (best effort: ошибки не влияют на корректность).
func (w *worker) event(j *claimed, kind string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = w.pool.Exec(ctx,
		`INSERT INTO events (job_id, worker_id, attempt, kind) VALUES ($1::text::uuid, $2, $3, $4)`,
		j.ID, w.id, j.Attempt, kind)
}
