package database

import (
"context"
"fmt"
"time"

"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(
ctx context.Context,
url string,
size int32,
) (*pgxpool.Pool, error) {

pc, err := pgxpool.ParseConfig(url)
if err != nil {
return nil, fmt.Errorf("parse database config: %w", err)
}

if size > 0 {
pc.MaxConns = size
}

for i := 0; i < 60; i++ {
pool, err := pgxpool.NewWithConfig(ctx, pc)

if err == nil {
if err = pool.Ping(ctx); err == nil {
_, err = pool.Exec(ctx, `SELECT 1 FROM jobs LIMIT 0`)
if err == nil {
return pool, nil
}
}

pool.Close()
}

select {
case <-ctx.Done():
return nil, ctx.Err()
case <-time.After(time.Second):
}
}

return nil, fmt.Errorf("database or jobs schema is unavailable")
}
