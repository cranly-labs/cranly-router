import type { ProtocolConversion } from '@/api/usage.types'

// protocolConversion.ts formats bounded protocol-conversion diagnostic badges,
// wire-protocol route labels, deterministic LossKind summaries, and structural
// equality checks shared by administrator usage tables and request detail cards.
// Unknown future protocol or LossKind identifiers fall back to their raw token
// strings so newly introduced backend diagnostics remain readable without
// breaking rendering.

// TranslationLookup resolves an i18n key with an optional fallback label.
export type TranslationLookup = (key: string, fallback?: string) => string

// ProtocolConversionLossEntry pairs a canonical LossKind identifier with its
// localized display label and positive occurrence count.
export interface ProtocolConversionLossEntry {
  kind: string // Canonical snake_case LossKind identifier.
  label: string // Localized human-readable loss category description.
  count: number // Positive aggregated occurrence count across request and response.
}

// MODE_BADGE_CLASSES maps each conversion mode to its compact pill styling in
// light and dark themes while leaving unknown future modes neutral.
const MODE_BADGE_CLASSES: Record<string, string> = {
  ir_converted:
    'bg-sky-100 text-sky-700 ring-1 ring-inset ring-sky-200 dark:bg-sky-500/20 dark:text-sky-300 dark:ring-sky-500/30',
  tool_adapted:
    'bg-violet-100 text-violet-700 ring-1 ring-inset ring-violet-200 dark:bg-violet-500/20 dark:text-violet-300 dark:ring-violet-500/30',
  native_replay:
    'bg-teal-100 text-teal-700 ring-1 ring-inset ring-teal-200 dark:bg-teal-500/20 dark:text-teal-300 dark:ring-teal-500/30',
}

// getProtocolConversionModeLabel translates mode through requests.protocolConversion.modes
// and returns the raw mode string when no locale entry matches.
export function getProtocolConversionModeLabel(mode: string, t: TranslationLookup): string {
  const key = `requests.protocolConversion.modes.${mode}`
  const resolved = t(key, mode)
  return resolved === key ? mode : resolved
}

// getProtocolConversionBadgeClass returns the Tailwind color and ring classes
// for a conversion mode badge, falling back to a neutral gray pill for unknown modes.
export function getProtocolConversionBadgeClass(mode: string): string {
  return (
    MODE_BADGE_CLASSES[mode] ??
    'bg-gray-100 text-gray-700 ring-1 ring-inset ring-gray-200 dark:bg-dark-700 dark:text-dark-200 dark:ring-dark-600'
  )
}

// getProtocolDisplayName translates a wire protocol identifier into its concise
// administrator display name and retains unknown identifiers verbatim.
export function getProtocolDisplayName(protocolId: string, t: TranslationLookup): string {
  const trimmed = (protocolId || '').trim()
  if (!trimmed) return '-'
  const key = `requests.protocolConversion.protocols.${trimmed}`
  const resolved = t(key, trimmed)
  return resolved === key ? trimmed : resolved
}

// formatProtocolConversionRoute formats client_protocol and upstream_protocol
// as a readable directional pair for administrator inspection.
export function formatProtocolConversionRoute(
  conversion: Pick<ProtocolConversion, 'client_protocol' | 'upstream_protocol'>,
  t: TranslationLookup,
): string {
  const client = getProtocolDisplayName(conversion.client_protocol, t)
  const upstream = getProtocolDisplayName(conversion.upstream_protocol, t)
  return `${client} -> ${upstream}`
}

// getProtocolConversionTotalLosses sums positive occurrence counts across
// loss_counts and returns zero when no losses were recorded.
export function getProtocolConversionTotalLosses(
  conversion: ProtocolConversion | null | undefined,
): number {
  if (!conversion?.loss_counts) return 0
  let total = 0
  for (const count of Object.values(conversion.loss_counts)) {
    if (typeof count === 'number' && Number.isFinite(count) && count > 0) {
      total += count
    }
  }
  return total
}

// getProtocolConversionLossItems converts positive loss_counts entries into
// lexicographically sorted display items so repeated renders keep stable order.
export function getProtocolConversionLossItems(
  conversion: ProtocolConversion | null | undefined,
  t: TranslationLookup,
): ProtocolConversionLossEntry[] {
  if (!conversion?.loss_counts) return []
  return Object.entries(conversion.loss_counts)
    .filter(([, count]) => typeof count === 'number' && Number.isFinite(count) && count > 0)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([kind, count]) => {
      const key = `requests.protocolConversion.lossKinds.${kind}`
      const resolved = t(key, kind)
      return {
        kind,
        label: resolved === key ? kind : resolved,
        count,
      }
    })
}

// protocolConversionsEqual reports whether two ProtocolConversion snapshots
// carry identical mode, protocol pair, dialect, replay_scheme, and positive
// loss_counts so RequestDetailCard avoids duplicating attempt diagnostics in Usage.
export function protocolConversionsEqual(
  a: ProtocolConversion | null | undefined,
  b: ProtocolConversion | null | undefined,
): boolean {
  if (!a || !b) return false
  if (
    a.mode !== b.mode ||
    a.client_protocol !== b.client_protocol ||
    a.upstream_protocol !== b.upstream_protocol ||
    (a.dialect || '') !== (b.dialect || '') ||
    (a.replay_scheme || '') !== (b.replay_scheme || '')
  ) {
    return false
  }
  const entriesA = Object.entries(a.loss_counts ?? {})
    .filter(([, count]) => count > 0)
    .sort(([k1], [k2]) => k1.localeCompare(k2))
  const entriesB = Object.entries(b.loss_counts ?? {})
    .filter(([, count]) => count > 0)
    .sort(([k1], [k2]) => k1.localeCompare(k2))
  if (entriesA.length !== entriesB.length) return false
  return entriesA.every(([k, v], index) => {
    const [otherKey, otherVal] = entriesB[index]
    return k === otherKey && v === otherVal
  })
}
