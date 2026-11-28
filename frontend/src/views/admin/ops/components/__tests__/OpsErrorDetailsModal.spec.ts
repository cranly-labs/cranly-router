import { createPinia, setActivePinia } from 'pinia'
import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createI18n } from 'vue-i18n'
import OpsErrorDetailsModal from '../OpsErrorDetailsModal.vue'
import zhLocale from '@/i18n/locales/zh'

// OpsErrorDetailsModal.spec.ts verifies accessible filter names, selected
// trigger text, and request-error status query parameters on the Ops error list
// dialog. Mounting real Select controls with Chinese locale messages protects
// distinct client and upstream status labels when identical status codes are
// chosen.

// Mock API calls capture query parameters sent by OpsErrorDetailsModal without
// performing network requests.
const mocks = vi.hoisted(() => ({
  listRequestErrors: vi.fn(),
  listUpstreamErrors: vi.fn(),
}))

vi.mock('@/api/admin/ops', () => ({ default: mocks, opsAPI: mocks }))

// i18n compiles Chinese operational strings so trigger aria-labels and
// parameterized selected-status labels render their production text.
const i18n = createI18n({
  legacy: false,
  locale: 'zh',
  messages: { zh: zhLocale as any },
})

describe('OpsErrorDetailsModal status filters', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.resetAllMocks()
    mocks.listRequestErrors.mockResolvedValue({ items: [], total: 0 })
    mocks.listUpstreamErrors.mockResolvedValue({ items: [], total: 0 })
  })

  // Both status dropdowns can select the same numeric code such as 429 or list
  // HTTP 200 on request errors. Each control must expose a distinct localized
  // aria-label, retain its status-type prefix on the button after selection,
  // and forward independent query parameters.
  it('exposes distinct localized accessible names and retains status type labels after selection', async () => {
    const wrapper = mount(OpsErrorDetailsModal, {
      attachTo: document.body,
      props: {
        show: true,
        timeRange: '1h',
        errorType: 'request',
      },
      global: {
        plugins: [i18n],
        stubs: {
          BaseDialog: { props: ['show'], template: '<div v-if="show"><slot /></div>' },
          MotionTransition: { template: '<div><slot /></div>' },
          OpsErrorLogTable: true,
          IpGeoBatchToolbar: true,
        },
      },
    })
    await flushPromises()

    const clientTrigger = wrapper.find('button[aria-label="客户端状态码"]')
    const upstreamTrigger = wrapper.find('button[aria-label="上游状态码"]')
    expect(clientTrigger.exists()).toBe(true)
    expect(upstreamTrigger.exists()).toBe(true)
    expect(wrapper.findAll('button[aria-label="Select option"]')).toHaveLength(0)

    await clientTrigger.trigger('click')
    await flushPromises()
    const clientOptions = Array.from(document.body.querySelectorAll('[role="option"]'))
    expect(clientOptions.some((el) => el.textContent?.trim() === '200')).toBe(true)
    const client429 = clientOptions.find((el) => el.textContent?.trim() === '429') as HTMLElement
    expect(client429).toBeTruthy()
    client429.click()
    await flushPromises()

    await upstreamTrigger.trigger('click')
    await flushPromises()
    const upstreamOptions = Array.from(document.body.querySelectorAll('[role="option"]'))
    const upstream429 = upstreamOptions.find(
      (el) => el.textContent?.trim() === '429',
    ) as HTMLElement
    expect(upstream429).toBeTruthy()
    upstream429.click()
    await flushPromises()

    expect(clientTrigger.text()).toContain('客户端状态码：429')
    expect(upstreamTrigger.text()).toContain('上游状态码：429')
    expect(mocks.listRequestErrors).toHaveBeenLastCalledWith(
      expect.objectContaining({
        status_codes: '429',
        upstream_status_codes: '429',
      }),
    )

    wrapper.unmount()
  })
})
