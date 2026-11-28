import { flushPromises, shallowMount } from '@vue/test-utils'
import { describe, expect, it, vi } from 'vitest'
import { createI18n } from 'vue-i18n'
import UserErrorDetailModal from '../UserErrorDetailModal.vue'
import zh from '@/i18n/locales/zh'

// UserErrorDetailModal.spec.ts opens the owner's dialog with a historical
// stream record and then a record with observed HTTP headers. The mocked API
// supplies the same display code in both cases so only wire provenance can
// determine whether the explanation appears.

// The dialog fetches the selected row through the owner's usage endpoint each time its identity changes.
const { getMyErrorDetail } = vi.hoisted(() => ({ getMyErrorDetail: vi.fn() }))
vi.mock('@/api/usage', () => ({ getMyErrorDetail }))

describe('UserErrorDetailModal', () => {
  // Both rows have display status 400. A missing wire observation must explain
  // the legacy fallback, and a confirmed HTTP 400 must remove that explanation.
  it('explains the source of a historical stream status to its owner', async () => {
    const row = {
      status_code: 400,
      stream: true,
      category: 'upstream',
      created_at: '2026-11-20T00:00:00Z',
    }
    getMyErrorDetail
      .mockResolvedValueOnce(row)
      .mockResolvedValueOnce({ ...row, client_status_code: 400 })
    const wrapper = shallowMount(UserErrorDetailModal, {
      props: { show: false, errorId: 1 },
      global: {
        plugins: [createI18n({ legacy: false, locale: 'zh', messages: { zh } })],
        stubs: { BaseDialog: { template: '<div><slot /></div>' } },
      },
    })
    await wrapper.setProps({ show: true })
    await flushPromises()
    const explanation = zh.usage.errors.detail.historicalClientStatusFallback
    expect(wrapper.text()).toContain(explanation)
    await wrapper.setProps({ errorId: 2 })
    await flushPromises()
    expect(wrapper.text()).toContain('400')
    expect(wrapper.text()).not.toContain(explanation)
    wrapper.unmount()
  })
})
