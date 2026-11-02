# HTTP API брокера

Контракт проверен по исходникам main `ca72746`: [HTTP](../broker/http.py),
[submit и policy](../broker/service.py), [профили](../broker/profiles.py),
[адаптеры](../broker/compat.py), [ASR](../broker/asr.py).
Это API приёма в durable очередь, а не прямой доступ к Ollama.

## POST /v1/jobs

Неизменённая legacy-форма:

```json
{"profile":"interactive","kind":"generate","payload":{"prompt":"Привет"},"source":"example","source_item_id":"1","external_id":"example-1"}
```

Обязательны известный `profile` и `kind` (`chat|generate|transcribe`).
`payload` — объект (при отсутствии HTTP подставляет `{}`).
`source` по умолчанию равен профилю; `source_item_id`, `external_id`,
`producer_storage` необязательны в общем контракте, но обязательны в отдельных lanes.
`producer_storage` включает отдельный [контракт хранения](PRODUCER_STORAGE.md)
и требует storage Bearer token уже при приёме.

Расширенная форма добавляет только необязательные поля:

```json
{"profile":"interactive","kind":"generate","payload":{"prompt":"Привет"},"source":"example","model":"gemma4:12b","options":{"num_ctx":8192,"num_predict":256},"weight":5}
```

Top-level `model` перекрывает `payload.model`; top-level `options` сливается
поверх `payload.options` по ключам. Значения задания перекрывают defaults профиля.
Отсутствие полей (или top-level `null`) сохраняет legacy-путь.
`options` должен быть объектом; разрешённость остальных ключей зависит от адаптера.
`weight` — вес регистрации source, не отдельный приоритет задания и не способ
изменить вес уже существующего source.

Успех: HTTP 202, объект задания (в том числе `id`, `state`, `profile`, `source`).
Результат читается через GET задания. Состояния:
`queued → running → completed|failed`; отмена использует также
`cancel_requested` и `cancelled`. Повтор использует тот же job id и новую попытку.
`external_id` участвует в source-specific idempotency; несовпадение входа/корреляции
может дать 400, а не новый job. Не следует считать его универсальным глобальным ключом.
Для nextcloud разрешена `?projection=nextcloud-final-v1` (при POST нужен
`source=nextcloud`); неподдерживаемая projection даёт 400.

### Реальный scope overrides: важная особенность реализации

В `Broker.submit()` есть ДВА этапа, поэтому список из промежуточного условия
нельзя описывать как окончательный запрет overrides:

| Профили | До lane-валидации |
| --- | --- |
| `OPENCLAW_PROFILE_NAMES` (все 13 openclaw-профилей из profiles.py), `EVALUATION_PROFILE_NAMES` (четыре eval-профиля/pilot), `NIGHTLY_PROFILE_NAMES` (четыре telegram-nightly-профиля), `interactive`, `cron` | Сохраняют объединённые model/options; валидаторы видят overrides |
| Остальные, включая nextcloud, syncopia-memory-*, olya-*, shutterstock-* | Возвращаются к `legacy_payload`; из top-level options перед валидатором сохраняются только ключи кроме num_ctx/num_predict |

Но после ВСЕХ адаптеров `submit()` снова записывает `requested_model`
(если отличается от модели профиля) и все top-level `options`
([service.py:818–821](../broker/service.py#L818)).
Следовательно, в этой ревизии nextcloud, syncopia-memory-*, olya-*,
shutterstock-* **не остаются окончательно на legacy runtime**:
адаптер проверяет legacy-вход, затем явные top-level overrides вновь попадают
в сохранённый payload. Исполнение берёт модель через `_target_profile`
и defaults через `options.get` в `_execute`
([service.py](../broker/service.py#L2243), [options](../broker/service.py#L2304)).
Это описание кода, не обещание одинаковой семантики всех адаптеров.
Legacy payload-only значения могут быть отклонены или нормализованы адаптером.
В частности, не переносите это поведение на синхронные compatibility routes:
они сначала применяют собственный контракт.

Для transcribe явные top-level model/options запрещены; работают выделенные
voice/ASR executors. Обязательные source, correlation, media, schema и thinking
проверки overrides не снимают.

### Обязательные lane-поля

Точные schemas и bounds задаются указанными валидаторами, а не названием source.

| Lane | Обязательные условия |
| --- | --- |
| interactive / cron | Известный профиль, допустимый kind и object payload; для полезного исполнения prompt либо messages по выбранному kind |
| OpenClaw | source=openclaw **или** openclaw-профиль включает chat-нормализацию; kind=chat, inline payload, без producer_storage; messages и остальные поля проверяет [normalize_request](../broker/openclaw.py). Текст ошибки говорит «source openclaw», но submit не требует именно этот source при openclaw-профиле |
| Evaluation/pilot | source=pilot-mainpc, один из eval-профилей, chat, без producer_storage; system/user messages (до 16, нужен user), bounded images, think=false, stream=false; [evaluation.py](../broker/evaluation.py). Durable IDs нужны клиентскому pilot-контракту; submit здесь не проверяет оба ID на обязательность |
| Nightly | source=telegram-nightly-eval-20261028-v1, один из NIGHTLY_PROFILE_NAMES, chat, оба correlation ID, без producer_storage; текст, явный native think и native_thinking metadata, stream/truncate/shift=false; [nightly_eval.py](../broker/nightly_eval.py) |
| Nextcloud | source=nextcloud, профиль из NEXTCLOUD_PROFILES, generate, external_id и source_item_id; prompt, 1..4 base64 images и точные adapter options; verify-gemma26 дополнительно требует think=false и точные identities/settings; [compat.py](../broker/compat.py#L420) |
| Local voice | source=local-voice, профиль из VOICE_PROFILE_NAMES, transcribe, [voice payload](../broker/voice.py); CPU voice требует оба correlation ID |
| stt-whisper | Только отдельный authenticated ASR API; executor должен быть настроен; legacy HTTP mutations запрещены |
| syncopia-memory-qwen38 | source=syncopia-telegram-memory; messages/schema/thinking проверяет validate_syncopia_memory_payload; сервер нормализует tools=[], stream=false |
| syncopia-memory-qwen38-128k-v1 | source=syncopia-telegram-memory, chat, оба ID и producer_storage; normalize_request в [syncopia_daily.py](../broker/syncopia_daily.py) |
| olya-vision-gemma / olya-vision-qwen | source=olya-vision; prompt, images, format (JSON Schema) и model contract в validate_olya_vision_payload |
| olya-decision-qwen38 | source=olya-decision; prompt и format (JSON Schema), text-only; validate_olya_decision_payload |
| shutterstock-canary / shutterstock-video | source совпадает с профилем; prompt/media/format проверяют одноимённые validate_* в compat.py |
| uncensored-eval-* | source=uncensored-eval; text-only, без draft/MTP параметров |

Для olya/shutterstock/syncopia-memory-qwen38 submit не добавляет самостоятельной
проверки kind=chat/generate: не следует выдавать намерение адаптера за HTTP guard.

### Регистрация source

Если policy настроена, неизвестный source автоматически записывается в её файл
через lock, временный файл, fsync, atomic replace и fsync каталога.
Новый entry: `enabled=true`, `admission_allowed=true`, `weight` из задания,
иначе 5. Для регистрации вес должен быть конечным положительным числом, не bool;
source — непустая строка до 160 символов.
Существующие entries, maintenance и admission controls сохраняются.
Регистрация не обходит maintenance и не разрешает приём для заблокированного source.
Без policy записи в файл нет. Вес существующего source меняется отдельным endpoint
(там допустим только integer 1..10). См. SourcePolicy.register/_mutate_source.

### Контекст и output

Для любого non-transcribe submit сначала выполняется `_validate_model_options`.
Модель — непустой string id до 256 символов.
Явные `num_ctx` и `num_predict` — положительные integers, bool не принимается.
Предел для обоих полей: максимальный `max_context` среди настроенных профилей
с точно таким model id; иначе native `/api/show` → `model_info` →
положительные integer поля с суффиксом `.context_length` (при нескольких — минимум).
Если metadata недоступна/невалидна — 400, fail closed.

Проверяются также effective defaults: default_context/max_context и max_output.
Превышение не обрезается: 400,
`{"error":"requested num_ctx=… exceeds physical limit … for model …"}`
(аналогично num_predict). Неизвестный предел:
`{"error":"physical context limit unavailable for model …"}`.
Это физический потолок, не гарантия доступной памяти или успешного исполнения.

## Каталог endpoints

Авторизация: «нет» означает отсутствие HTTP auth guard, не публичный API.
«Storage» — `Authorization: Bearer <storage_token>`; при ненастроенном token 503,
при неверном/отсутствующем — 401. «Условно» — Storage для producer-storage jobs.
ASR использует отдельный Bearer token.

| Метод | Путь | Назначение / тело | Auth |
| --- | --- | --- | --- |
| POST | /v1/jobs | Приём, 202; формы выше | Условно |
| GET | /v1/jobs/{id} | Полное состояние/результат, 200 | Условно |
| GET | /v1/jobs/{id}/status | Compact status | Storage всегда |
| POST | /v1/jobs/{id}/cancel | Отмена | Условно |
| POST | /v1/jobs/{id}/retry | Явный retry | Условно |
| GET | /v1/jobs/{id}/attempts | Объект attempts | Условно |
| GET | /v1/jobs/{id}/receipt | Durable receipt | Storage |
| POST | /v1/jobs/{id}/input-received | ACK входа по storage contract | Storage |
| POST | /v1/jobs/{id}/ack | ACK результата по storage contract | Storage |
| POST | /v1/queue/pause | Только {"paused": boolean} | Нет |
| GET | /v1/sources | Snapshot policy либо policy:null | Нет |
| POST | /v1/sources/{name}/weight | Только {"weight": integer 1..10} | Нет |
| POST | /v1/sources/{name}/enabled | Только {"enabled": boolean}, dispatch | Нет |
| POST | /v1/sources/{name}/dispatch | Только {"paused": boolean} | Нет |
| POST | /v1/sources/{name}/admission | Только {"allowed": boolean} | Нет |
| POST | /v1/sources/{name}/queued/cancel | Только {"confirm":true} | Storage если затронут producer storage |
| POST | /v1/sources/{name}/failed/retry | Только {"confirm":true} | Storage если затронут producer storage |
| POST | /v1/maintenance/admission | enabled, expected_sha256; admission barrier | Storage |
| POST | /v1/maintenance/compact | source; operation=preview по умолчанию, limit, confirm, max_bytes; см. storage contract | Storage |
| POST | /openclaw/api/chat | Ожидание результата; [wire contract](OPENCLAW_ROUTE.md), Idempotency-Key | Нет |
| POST | /api/chat, /api/generate | Compatibility admission, 202; stream=true возвращает NDJSON admission, не inference | Нет |
| POST | /v1/shutterstock-canary/generate, /v1/shutterstock-video/generate | Синхронное ожидание dedicated generate | Нет |
| POST | /v1/olya-vision/generate, /v1/olya-decision/generate | Синхронное ожидание dedicated adapter | Нет |
| POST | /v1/syncopia-memory/extract | Синхронное ожидание memory extraction | Нет |
| GET | /dashboard | HTML панель | Нет |
| GET | /dashboard/assets/{name} | JS/CSS assets | Нет |
| GET | /v1/dashboard | JSON наблюдения | Нет |
| GET | /v1/forecast | limit=10 (максимум 20) | Нет |
| GET | /v1/analytics | window=seconds, допускает CSV/повторение; defaults 300,1800,10800,86400; 1..31536000 | Нет |
| GET | /v1/history | limit=30, cursor=time:id | Нет |
| GET | /v1/audit-events | limit=100, job_id, source, since (число) | Нет |
| GET | /v1/correlations | source, source_item_id, external_id, limit=100 | Нет |
| GET | /v1/metrics | Метрики брокера | Нет |
| GET | /v1/storage/health | Здоровье storage | Нет |
| GET | /healthz | Локальная health без обращения к executor | Нет |
| GET | /v1/nightly-eval/contract | Nightly contract | Нет |
| GET | /v1/voice/capabilities | CPU voice capabilities | Нет |
| POST | /v1/transcriptions | Async binary audio upload; 202 id/state | ASR |
| GET | /v1/transcriptions/{id} | State/result/gpu_release_pending | ASR |
| POST | /v1/transcriptions/{id}/{cancel,retry,ack} | Cancel=200, retry=202, terminal ACK=200 | ASR |

Read views dashboard/forecast/history/audit/correlations без Storage token
скрывают producer-storage данные, а не возвращают 401.
Source names URL-decoded; stt-whisper mutations через legacy API дают 403.

ASR: Content-Length обязателен, Transfer-Encoding запрещён;
Idempotency-Key соответствует `[A-Za-z0-9_-]{16,100}`;
размер audio 1..64 MiB. ACK удаляет артефакты после durable receipt.
Подробности: [Whisper executor](WHISPER_EXECUTOR.md).

## Ошибки и HTTP-коды

Клиент обязан поддерживать оба типа `error`:

```json
{"error":"текст"}
{"error":{"code":"source_admission_blocked","message":"текст"},"source":"…","admission_allowed":false}
```

Для storage auth structured error содержит только code, без message:
`storage_auth_required`, `storage_api_unavailable`.
Receipt conflict использует `{"error":{"code":"ack_conflict","message":"…"}}`.
Ниже «plain» означает строковый error; конкретный текст validator error не является
стабильным machine code.

| Код | Точный класс триггера / тело | Действие caller |
| --- | --- | --- |
| 200 | Успех чтения/контроля, synchronous result | Обработать endpoint-specific JSON/HTML |
| 202 | Приём задания/compatibility, ASR upload/retry | Сохранить id, опрашивать состояние |
| 304 | Совпал If-None-Match для dashboard/asset; тела нет | Использовать cache |
| 400 | JSON decode; неизвестный profile/kind; schema/type/media/model/options/physical ceiling; correlation/idempotency/retention conflict в submit; неверная projection; поля controls/policy/checksum; некорректные numeric query/cursor. Plain {"error":"…"} | Исправить запрос, не повторять неизменённый. При unavailable metadata/policy сначала восстановить сервис |
| 400 | ASR: upload/key/size/Transfer-Encoding/deadline/spool/idempotency/ACK nonterminal и прочие exceptions кроме admission/full; plain {"error":"ASR request rejected"} | Проверить вход и состояние; upload timeout здесь НЕ 408 |
| 401 | Storage token неверен/нет: structured storage_auth_required; ASR token неверен: plain "ASR authentication required" | Исправить token; не повторять без изменений |
| 403 | Source admission/maintenance запрет: structured source_admission_blocked + source/admission_allowed=false. ASR admission block — plain. Legacy попытка доступа к stt-whisper: plain "use authenticated ASR API" | Дождаться снятия запрета либо использовать ASR API; не делать tight retry |
| 404 | Неизвестный route/job/asset; ASR неподходящий job kind или операция; plain "not found" | Проверить путь/id; не повторять без причины |
| 408 | Только OpenClaw: чтение request body дольше 10 s; plain "request body read timed out" | Повторить передачу с тем же idempotency key |
| 409 | Retry в недопустимом состоянии (ValueError), guards maintenance/compact (StorageContractError/KeyError) — plain; receipt ACK conflict — structured ack_conflict; ASR retry при fence — plain "GPU release pending"; OpenClaw cancelled — plain error + job_id | Проверить состояние/receipt/guards; ждать release для ASR; не повторять конфликтующий ACK |
| 410 | OpenClaw completed без result: plain "broker result has expired" + job_id. ASR acknowledged/expired/unavailable artifact: plain error БЕЗ job_id | Восстановить producer artifact/использовать новый корректный lifecycle; тот же запрос не вернёт исчезнувший результат |
| 411 | OpenClaw: отсутствует либо не integer Content-Length; plain "Content-Length is required" | Добавить корректный header |
| 413 | OpenClaw: size <=0 или >2 MiB; plain "request exceeds 2 MiB or is empty" | Исправить/уменьшить тело |
| 429 | SourceQueueFull: pilot 512 MiB активного normalized input либо nextcloud evaluation 256 MiB; plain error. ASR также при занятых двух admission slots: plain "ASR admission busy" | Backoff с jitter, повтор с прежней корреляцией; count caps отсутствуют |
| 500 | Synchronous adapter wait вернул None: plain "submitted job disappeared" | Проверить job/state, сообщить оператору; не плодить новые jobs |
| 502 | Synchronous adapter/OpenClaw job failed: plain "broker job failed" + job_id | Прочитать ошибку/attempts; только явный обоснованный retry |
| 503 | Storage token не настроен: structured storage_api_unavailable. ASR не настроен, CPU voice capability/asset недоступны: plain error. Dashboard observation unavailable: JSON dashboard либо HTML, НЕ error envelope | Backoff до восстановления/configuration |
| 504 | Synchronous adapters: {"error":"broker job timed out","job_id":"…"}. OpenClaw: {"error":"broker queue wait timed out","job_id":"…"} | Сначала проверить job: timeout не доказывает отсутствие/неуспех исполнения |

При OpenClaw stream=true заголовок HTTP 200 отправлен до ожидания:
последующие 409/410/502/504 передаются NDJSON-кадром
`{"error":"…","status":504}`, без job_id в самом кадре.
ID — в X-Broker-Job-Id. Между ними приходят heartbeat.
Без Idempotency-Key timeout/disconnect отменяет job; с ключом job сохраняется.
Нельзя считать HTTP 200 доказательством успешного terminal result.

Каталог описывает явные ответы `http.py`/ASRAPI.
Неподдерживаемые HTTP-методы наследуют BaseHTTPRequestHandler: 501 с HTML error page,
не JSON; исправить метод. Невалидный HTTP framing также может быть отвергнут stdlib
до router (например 400/414/431 с HTML); ошибки необработанных исключений
могут оборвать соединение, а не гарантированно вернуть JSON 500.

## Что изменилось и совместимость

Существующему caller менять форму запроса не требуется.
model/options/weight теперь необязательны; README не заменяет этот справочник.
Без overrides действуют прежние defaults и lane-контракты.
Новые sources регистрируются автоматически, но существующие controls сохраняются.
Теперь caller может получить 400 за неверный runtime override, превышение
физического потолка/недоступные metadata либо ошибку регистрации policy/weight.
403 при admission block и 429 при byte-budget/ASR admission остаются отдельными
состояниями; не интерпретировать их как удалённые count caps.
Описание «все специальные lanes окончательно откатывают overrides» противоречит
финальному reapply в submit этой ревизии.

## Минимальный цикл curl

Legacy; затем чтение результата по id из ответа:

```bash
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"profile":"interactive","kind":"generate","payload":{"prompt":"Привет"},"source":"example"}' \
  http://127.0.0.1:8088/v1/jobs
curl --fail-with-body http://127.0.0.1:8088/v1/jobs/JOB_ID
```

Расширенный запрос (модель должна быть установлена у executor):

```bash
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"profile":"interactive","kind":"generate","payload":{"prompt":"Привет"},"source":"example-runtime","model":"gemma4:12b","options":{"num_ctx":8192,"num_predict":256},"weight":5}' \
  http://127.0.0.1:8088/v1/jobs
curl --fail-with-body http://127.0.0.1:8088/v1/jobs/JOB_ID
```
