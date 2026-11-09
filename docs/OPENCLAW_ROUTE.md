# OpenClaw agentTurn через durable queue (opt-in)


Broker route `POST /openclaw/api/chat` реализует **нативный Ollama chat** для
выбранных OpenClaw turns. Это отдельный путь: старые `/api/chat` и
`/api/generate` остаются асинхронными admission-only endpoints с ответом `202`.
Провайдер OpenClaw использует `api: "ollama"` и `baseUrl` вида
`http://127.0.0.1:8088/openclaw`; OpenClaw добавляет `/api/chat`. Внутренний
broker source остаётся `openclaw` при любом OpenClaw-facing provider key.
Wire model обязан совпадать с одним из server-owned route-профилей ниже.

| Wire model | Broker profile | Context/output | Image |
| --- | --- | --- | --- |
| `gemma4:12b` | `openclaw-gemma4` | 262144/16384 | yes |
| `qwen3-vl:30b` | `openclaw-qwen3-vl` | 212992/16384 | yes |
| `nemotron3:33b` | `openclaw-nemotron3` | 131072/8192 | yes |
| `qwen3.8:ad-iq2-xs` | `openclaw` (legacy queue compatibility) | 131072/16384 | no |
| `qwen3.8:unc-rvn-iq2xxs` | `openclaw-unc-rvn-iq2xxs` | 131072/16384 | no |
| `frob/ministral-3:14b-thinking-q4_K_M` | `openclaw-ministral3` | 262144/16384 | yes |
| `nemotron-3-nano:30b-a3b-q4_K_M` | `openclaw-nemotron-nano` | 262144/16384 | no |
| `nemotron-3.5-bolt:e4b` | `openclaw-bolt-e4b` | 65536/8192 | yes |
| `qwen3-vl:2b-instruct-q4_K_M` | `openclaw-qwen3-vl-2b` | 8192/2048 | yes |
| `deepseek-ocr:latest` | `openclaw-deepseek-ocr` | 8192/2048 | yes |
| `glm-ocr:latest` | `openclaw-glm-ocr` | 131072/8192 | yes |

Limits and modalities match the `ollama-main-pc` model entries verified against
the live MAIN-PC `/api/tags` and `/api/show` on 2026-11-09. Context ceilings are
the native metadata of those exact tags (`deepseekocr.context_length=8192`,
`glm4.context_length=131072`), not library figures; the small VLM smoke lanes
deliberately keep their narrower bounds instead of widening. Each profile pins
model, keepalive 300 s, and an executor timeout (600 s for 2B/DeepSeek-OCR,
900 s otherwise). Admission preserves up to 512 messages and 128 tools. Text JSON
requests are limited to 2 MiB; vision requests to 32 MiB, at most 16 images
and 24 MiB decoded images. Tools are limited to 512 KiB. Byte limits protect
SQLite/HTTP and can narrow oversized image turns; exact token budget
остаётся ответственностью OpenClaw/Ollama, поэтому `truncate` и `shift`
запрещены. Queue admission не ограничивает количество накопленных jobs: backlog
любого размера накапливается и планируется scheduler-ом. Защита остаётся
байтовой (2 MiB text / 32 MiB vision, disk reserve, WAL) и per-profile
max_concurrency на исполнении. Запросы проходят обычный source scheduler,
model switch и durable result storage; route не вызывает Ollama напрямую.

### Удалённые и сохранённые артефакты

`gpt-oss:20b` и оба `hf.co/ggml-org/SmolVLM2-*` удалены с MAIN-PC, поэтому их
wire model **больше не принимаются** route-ом (`openclaw-gpt-oss`,
`openclaw-smolvlm2-2b`, `openclaw-smolvlm2-500m`). Их профили сохранены в
`PROFILES` под `OPENCLAW_LEGACY_PROFILE_NAMES`: уже поставленный в очередь job
всё ещё может назвать такой профиль через `/v1/jobs`, и executor-контракт для
него остаётся валидным (admission/dispatch-гварды и проверка результата идут по
`OPENCLAW_EXECUTOR_PROFILE_NAMES`, а route — только по
`OPENCLAW_PROFILES_BY_MODEL`). Не добавляйте модель в route, пока артефакт не
установлен на executor. Живые queued jobs (`nextcloud-eval-qwen-64k-v1`,
source `nextcloud`) этим изменением не затрагиваются: профиль сохранён без
изменений.

`stream=false` возвращает настоящий Ollama JSON после terminal completion.
`stream=true` возвращает Ollama NDJSON: пустые heartbeat-кадры примерно каждые
2 s во время queue/model wait и один итоговый `done=true` кадр с text,
thinking, tool calls и usage. Это корректный поток для OpenClaw agentTurn,
но **не** progressive token streaming: executor получает целый non-stream
ответ Ollama до записи durable result. `Idempotency-Key` (опциональный HTTP
header, не генерируется OpenClaw автоматически) привязывает повтор к тому же
job и отвергает другое содержимое с HTTP 400. При отключении клиента без ключа
queued job отменяется, running получает `cancel_requested` и освобождает GPU
после возврата Ollama; с ключом durable job остаётся доступным для повторного
ожидания. Время ожидания HTTP — 1800 s, затем HTTP 504 / NDJSON error; для
keyed request job сохраняется. Для OpenClaw provider timeout нужен >1800 s,
а timeout выбранного cron agentTurn также должен покрывать очередь и inference.

Перед rollout main/ops отдельно добавляет `openclaw` в **live** source policy с
нужными `enabled`, `admission_allowed` и weight, обновляет broker release и
переключает OpenClaw provider/models. Ни код, ни этот документ не меняют live
policy или automations. Rollback: вернуть прежний provider/model routing,
закрыть admission для `openclaw`, дождаться/отменить его
jobs и вернуть прежний broker release; другие sources и SQLite не удалять.
