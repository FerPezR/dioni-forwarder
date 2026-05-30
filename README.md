# Dioni Forwarder

Monitor + forward de señales `@dioniescalante` desde topic 92 del grupo `RM apuetas inversas` (`-1002087337771`) al topic 101 del canal `@analisis_forense_cripto` (`-1003894658753`).

Además programa alertas Telegram 30 min pre-partido y exporta lista de Calendar pending events.

## Flujo

1. Cron cada 30 min (GH Actions `17,47 * * * *`).
2. Lee últimas señales Dioni del topic 92 en ventana ~30h.
3. Detecta `target_date` por heurística (si GT >= 18:00 → día siguiente).
4. Para cada señal nueva:
   - **Forward** a topic 101 (con caption "Señales originales para el día X" la primera del día).
   - **Schedule alert Telegram** 30 min pre-partido como reply al topic 101.
   - **Append** evento a `calendar-pending.json` para Calendar.
5. Idempotente: `state.json` track `{date: {msg_ids, caption_posted, scheduled_alerts}}`.

## Reglas operativas

- Hora Dioni = Venezuela TZ (UTC-4). GT = VE - 2h.
- 30 min wait después de 1ra señal del lote antes del primer forward.
- 3-6 señales/día variable.

## Secrets requeridos

```bash
gh secret set TELEGRAM_API_ID -b "<api_id>"
gh secret set TELEGRAM_API_HASH -b "<api_hash>"
gh secret set TELEGRAM_SESSION_STRING -b "<session_string>"
```

## Manual run

```bash
python forwarder.py            # auto target_date
python forwarder.py 2026-05-30 # override
```

## Memoria relacionada

- `reference_dioni_signals_forward_workflow.md` — workflow canónico
- `reference_rm_contabilidad_yuanes.md` — contabilidad RM ¥/7
- `feedback_auto_rm_close_day_workflow.md` — flujos privado/público RM
