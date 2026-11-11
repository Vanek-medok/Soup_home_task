-- Схема очереди. Применяется один раз при первой инициализации тома Postgres
-- (docker-entrypoint-initdb.d). Чтобы применить заново: docker compose down -v.

CREATE TABLE IF NOT EXISTS jobs (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  text        NOT NULL UNIQUE,            -- защита от дублей на уровне БД
    payload          jsonb       NOT NULL,
    status           text        NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
    attempt          int         NOT NULL DEFAULT 0,         -- номер захвата = fencing token
    max_attempts     int         NOT NULL DEFAULT 10,        -- защита от «ядовитых» задач
    lease_owner      text,
    lease_expires_at timestamptz,                            -- считается по now() БД, не по часам воркера
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    seq              bigserial   NOT NULL UNIQUE             -- ключ курсорной пагинации
);

-- Очередь: воркеры берут самую раннюю queued-задачу.
CREATE INDEX IF NOT EXISTS jobs_queued_idx ON jobs (seq) WHERE status = 'queued';
-- Reaper: быстрый поиск running-задач с просроченным lease.
CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs (lease_expires_at) WHERE status = 'running';
-- Харнесс выбирает задачи прогона через LIKE 'prefix%'.
CREATE INDEX IF NOT EXISTS jobs_key_prefix_idx ON jobs (idempotency_key text_pattern_ops);

-- «Исполнение» = запись сюда. Намеренно БЕЗ UNIQUE(job_id): харнесс должен уметь
-- посчитать двойное исполнение, а не получать его молча проглоченным.
CREATE TABLE IF NOT EXISTS effects (
    id         bigserial PRIMARY KEY,
    job_id     uuid        NOT NULL,
    worker_id  text        NOT NULL,
    attempt    int         NOT NULL,
    written_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS effects_job_idx ON effects (job_id);

-- Журнал событий для доказательств (харнесс его не читает; см. scripts/evidence.sql):
--   started          воркер начал исполнение попытки
--   complete_rejected  fencing отверг устаревшее завершение (zombie/отмена)
--   lease_lost       heartbeat обнаружил потерю lease или отмену
--   reaped_queued / reaped_failed   reaper забрал задачу у умершего воркера
CREATE TABLE IF NOT EXISTS events (
    id        bigserial PRIMARY KEY,
    job_id    uuid        NOT NULL,
    worker_id text        NOT NULL,
    attempt   int         NOT NULL,
    kind      text        NOT NULL,
    at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_job_idx ON events (job_id);