import { describe, expect, it } from 'vitest'
import enRequests from '@/i18n/locales/en/requests'
import zhRequests from '@/i18n/locales/zh/requests'
import {
  formatProtocolConversionRoute,
  getProtocolConversionBadgeClass,
  getProtocolConversionLossItems,
  getProtocolConversionModeLabel,
  getProtocolConversionTotalLosses,
  getProtocolDisplayName,
  protocolConversionsEqual,
} from '@/utils/protocolConversion'
import type { ProtocolConversion } from '@/api/usage.types'

// protocolConversion.spec.ts verifies administrator protocol-conversion badge
// labels, wire-protocol route formatting, deterministic LossKind sorting,
// structural equality checks, and bilingual locale key symmetry across all
// twenty-four canonical IR LossKind values.

// CANONICAL_LOSS_KINDS lists every LossKind emitted by backend/internal/protocol/ir/loss.go.
const CANONICAL_LOSS_KINDS = [
  'annotation_dropped',
  'cache_mark_dropped',
  'candidate_count_ignored',
  'client_marker_dropped',
  'extra_dropped',
  'hosted_tool_dropped',
  'id_rewritten',
  'media_relocated',
  'media_unsupported',
  'opaque_item_dropped',
  'output_format_approximated',
  'parts_reordered',
  'reasoning_text_dropped',
  'reasoning_text_inlined',
  'refusal_inlined',
  'replay_token_dropped',
  'sampling_dropped',
  'schema_approximated',
  'stop_dropped',
  'system_relocated',
  'tool_choice_approximated',
  'tool_history_inlined',
  'tool_lowered',
  'tool_pairing_repaired',
] as const

// resolveEn looks up dotted keys in the English requests locale dictionary for
// deterministic unit assertions without mounting Vue components.
function resolveEn(key: string, fallback?: string): string {
  const value = key
    .split('.')
    .reduce<unknown>(
      (node, part) =>
        node && typeof node === 'object' ? (node as Record<string, unknown>)[part] : undefined,
      { requests: enRequests },
    )
  return typeof value === 'string' ? value : (fallback ?? key)
}

describe('protocolConversion utilities', () => {
  // Known conversion modes and wire protocols map to localized display strings
  // and distinct badge classes, while unrecognized values fall back cleanly.
  // This protects forward compatibility when future backend versions add new protocol IDs.
  it('formats conversion modes, badge classes, and directional protocol routes with fallbacks', () => {
    expect(getProtocolConversionModeLabel('ir_converted', resolveEn)).toBe('IR converted')
    expect(getProtocolConversionModeLabel('tool_adapted', resolveEn)).toBe('Tool adapted')
    expect(getProtocolConversionModeLabel('native_replay', resolveEn)).toBe('Envelope replay')
    expect(getProtocolConversionModeLabel('future_mode', resolveEn)).toBe('future_mode')

    expect(getProtocolConversionBadgeClass('ir_converted')).toContain('bg-sky-100')
    expect(getProtocolConversionBadgeClass('tool_adapted')).toContain('bg-violet-100')
    expect(getProtocolConversionBadgeClass('native_replay')).toContain('bg-teal-100')
    expect(getProtocolConversionBadgeClass('future_mode')).toContain('bg-gray-100')

    expect(getProtocolDisplayName('anthropic_messages', resolveEn)).toBe('Anthropic Messages')
    expect(getProtocolDisplayName('future_proto', resolveEn)).toBe('future_proto')
    expect(getProtocolDisplayName('', resolveEn)).toBe('-')

    expect(
      formatProtocolConversionRoute(
        {
          client_protocol: 'openai_responses_websocket',
          upstream_protocol: 'openai_responses',
        },
        resolveEn,
      ),
    ).toBe('Responses WebSocket -> OpenAI Responses')
  })

  // Unsorted loss_counts maps can also contain zero or nonpositive entries from
  // synthetic fixtures. Sorting positive entries lexicographically and summing
  // only positive counts keeps table tooltips and detail cards deterministic.
  it('sums positive loss counts and sorts loss entries lexicographically by LossKind', () => {
    const sample: ProtocolConversion = {
      mode: 'ir_converted',
      client_protocol: 'anthropic_messages',
      upstream_protocol: 'gemini_generate_content',
      dialect: 'gemini',
      replay_scheme: 'gemini',
      loss_counts: {
        schema_approximated: 2,
        cache_mark_dropped: 3,
        zero_ignored: 0,
        replay_token_dropped: 1,
      },
    }

    expect(getProtocolConversionTotalLosses(null)).toBe(0)
    expect(getProtocolConversionTotalLosses(sample)).toBe(6)

    const items = getProtocolConversionLossItems(sample, resolveEn)
    expect(items).toEqual([
      { kind: 'cache_mark_dropped', label: 'Cache breakpoint dropped', count: 3 },
      { kind: 'replay_token_dropped', label: 'Replay token dropped', count: 1 },
      { kind: 'schema_approximated', label: 'Tool schema approximated', count: 2 },
    ])
  })

  // RequestDetailCard compares attempt and usage ProtocolConversion snapshots to
  // suppress duplicate rendering on matched attempts while still showing legacy
  // or diverged usage records. The comparison must ignore key insertion order.
  it('compares ProtocolConversion snapshots structurally regardless of loss key order', () => {
    const first: ProtocolConversion = {
      mode: 'tool_adapted',
      client_protocol: 'openai_responses',
      upstream_protocol: 'openai_responses',
      dialect: 'openai',
      replay_scheme: 'openai',
      loss_counts: { tool_lowered: 2, replay_token_dropped: 1 },
    }
    const second: ProtocolConversion = {
      mode: 'tool_adapted',
      client_protocol: 'openai_responses',
      upstream_protocol: 'openai_responses',
      dialect: 'openai',
      replay_scheme: 'openai',
      loss_counts: { replay_token_dropped: 1, tool_lowered: 2 },
    }

    expect(protocolConversionsEqual(first, second)).toBe(true)
    expect(
      protocolConversionsEqual(first, {
        ...second,
        loss_counts: { replay_token_dropped: 2, tool_lowered: 2 },
      }),
    ).toBe(false)
    expect(protocolConversionsEqual(first, null)).toBe(false)
  })

  // English and Simplified Chinese request dictionaries must define identical
  // protocolConversion keys and cover all twenty-three backend LossKind values.
  // Missing translations would expose raw snake_case keys in administrator views.
  it('keeps English and Chinese protocolConversion locale trees symmetric across all LossKinds', () => {
    expect(Object.keys(enRequests.protocolConversion).sort()).toEqual(
      Object.keys(zhRequests.protocolConversion).sort(),
    )
    expect(Object.keys(enRequests.protocolConversion.modes).sort()).toEqual(
      Object.keys(zhRequests.protocolConversion.modes).sort(),
    )
    expect(Object.keys(enRequests.protocolConversion.protocols).sort()).toEqual(
      Object.keys(zhRequests.protocolConversion.protocols).sort(),
    )
    expect(Object.keys(enRequests.protocolConversion.lossKinds).sort()).toEqual(
      Object.keys(zhRequests.protocolConversion.lossKinds).sort(),
    )
    expect(Object.keys(enRequests.protocolConversion.lossKinds).sort()).toEqual(
      [...CANONICAL_LOSS_KINDS].sort(),
    )
  })
})
