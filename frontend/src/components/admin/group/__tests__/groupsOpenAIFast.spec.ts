import { describe, expect, it } from 'vitest'
import { normalizeGroupOpenAIFastPolicy } from '../groupsOpenAIFast'
import en from '@/i18n/locales/en/admin/overview'
import zh from '@/i18n/locales/zh/admin/overview'

// groupsOpenAIFast.spec.ts checks normalized group Fast policies and bilingual labels for their
// group form controls. Stored policy values remain available to provider execution, while missing
// or unsupported input resolves to the form default used during submission.

describe('groupsOpenAIFast', () => {
  // Supported group Fast policies and an unknown value pass through normalization. Recognized
  // policies remain unchanged and the unsupported value selects the follow-request default for
  // storage.
  it('分组保存策略，执行时按提供商能力应用', () => {
    expect(normalizeGroupOpenAIFastPolicy('force_ultrafast')).toBe('force_ultrafast')
    expect(normalizeGroupOpenAIFastPolicy('unknown')).toBe('follow_request')
  })

  // Both locales supply labels for each policy the group form can save. The accompanying
  // hint explains the effect of a forced tier on requests using this group.
  it('提供四种分组加速策略的中英文文案', () => {
    for (const locale of [en, zh]) {
      expect(locale.groups.openaiFast).toMatchObject({
        title: expect.any(String),
        followRequest: expect.any(String),
        force: expect.any(String),
        forceUltrafast: expect.any(String),
        forceOff: expect.any(String),
        hint: expect.stringContaining('Ultra Fast'),
      })
    }
  })
})
