import { flushPromises, mount } from '@vue/test-utils'
import { computed } from 'vue'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import ProfileBalanceNotifyCard from '@/components/user/profile/ProfileBalanceNotifyCard.vue'
import type { NotifyEmailEntry } from '@/api/notifications.types'

// ProfileBalanceNotifyCard.spec.ts covers the card states users rely on to tell whether low
// balance alerts will be delivered. Profile API, stores, translations, and balance
// formatting are replaced with controlled fakes so assertions observe only the card's own
// recipient warning, collapse behavior, and threshold save rules.

// Hoisted API and notification spies let each test inspect profile writes and recovery
// messages without a backend or Pinia store.
const { updateProfileMock, showSuccessMock, showErrorMock } = vi.hoisted(() => ({
  updateProfileMock: vi.fn(),
  showSuccessMock: vi.fn(),
  showErrorMock: vi.fn(),
}))

vi.mock('@/api', () => ({
  userAPI: {
    updateProfile: updateProfileMock,
    toggleNotifyEmail: vi.fn(),
    sendNotifyEmailCode: vi.fn(),
    verifyNotifyEmail: vi.fn(),
    removeNotifyEmail: vi.fn(),
    getProfile: vi.fn(),
  },
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => ({ user: null }),
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({
    showSuccess: showSuccessMock,
    showError: showErrorMock,
  }),
}))

vi.mock('@/composables/useBalanceDisplay', () => ({
  useBalanceDisplay: () => ({
    balanceUnitSymbol: computed(() => '$'),
    formatBalanceAmount: (value: number) => `$${value.toFixed(2)}`,
  }),
}))

vi.mock('vue-i18n', async (importOriginal) => {
  const actual = await importOriginal<typeof import('vue-i18n')>()
  return {
    ...actual,
    useI18n: () => ({
      t: (key: string, params?: Record<string, unknown>) =>
        params ? `${key} ${JSON.stringify(params)}` : key,
    }),
  }
})

// CardProps mirrors the component props so each test can override only the fields it needs.
interface CardProps {
  enabled: boolean // Persisted enablement flag.
  threshold: number | null // Persisted custom threshold.
  extraEmails: NotifyEmailEntry[] // Saved notification addresses.
  systemDefaultThreshold: number // Administrator default threshold.
  userEmail: string // Account email used for prefill.
}

// mountCard renders the card with enabled notifications, no saved addresses, and a ten-unit
// system default unless the test supplies overrides.
function mountCard(overrides: Partial<CardProps> = {}) {
  const props: CardProps = {
    enabled: true,
    threshold: null,
    extraEmails: [],
    systemDefaultThreshold: 10,
    userEmail: 'user@example.com',
    ...overrides,
  }
  return mount(ProfileBalanceNotifyCard, { props })
}

describe('ProfileBalanceNotifyCard', () => {
  beforeEach(() => {
    updateProfileMock.mockReset()
    showSuccessMock.mockReset()
    showErrorMock.mockReset()
  })

  // Notifications are on but the only saved address is unverified or disabled. The test
  // protects the warning that tells users no alert email will actually be sent.
  it('warns when no saved address can receive alerts', () => {
    const wrapper = mountCard({
      extraEmails: [
        { email: 'pending@example.com', verified: false, disabled: false },
        { email: 'paused@example.com', verified: true, disabled: true },
      ],
    })

    expect(wrapper.find('[data-testid="balance-notify-no-recipient"]').exists()).toBe(true)
  })

  // A verified address with delivery left on exists. The test protects removal of the
  // warning once at least one recipient qualifies under the backend filter.
  it('hides the warning when a verified enabled address exists', () => {
    const wrapper = mountCard({
      extraEmails: [{ email: 'ops@example.com', verified: true, disabled: false }],
    })

    expect(wrapper.find('[data-testid="balance-notify-no-recipient"]').exists()).toBe(false)
  })

  // Notifications start disabled. The test protects the collapsed body so dependent
  // threshold and address fields are hidden behind the header switch.
  it('collapses the settings body while notifications are disabled', () => {
    const wrapper = mountCard({ enabled: false })

    expect(wrapper.get('[data-testid="balance-notify-body"]').isVisible()).toBe(false)
  })

  // The persisted threshold is empty and the user types a new amount. The test protects the
  // inactive save button before editing and the numeric value written after saving.
  it('enables threshold saving only after the value changes', async () => {
    updateProfileMock.mockResolvedValue({ balance_notify_threshold: 25 })
    const wrapper = mountCard()
    const saveButton = wrapper.get('[data-testid="balance-notify-threshold-save"]')

    expect(saveButton.attributes('disabled')).toBeDefined()

    await wrapper.get('#balance-notify-threshold').setValue('25')
    expect(saveButton.attributes('disabled')).toBeUndefined()

    await saveButton.trigger('click')
    await flushPromises()

    expect(updateProfileMock).toHaveBeenCalledWith({ balance_notify_threshold: 25 })
  })

  // The stored threshold is zero, which selects the system default. The test protects the
  // empty input and the hint that names the default amount instead of showing a literal 0.
  it('shows the system default instead of a stored zero threshold', () => {
    const wrapper = mountCard({ threshold: 0 })

    const input = wrapper.get('#balance-notify-threshold').element as HTMLInputElement
    expect(input.value).toBe('')
    expect(wrapper.get('#balance-notify-threshold-hint').text()).toContain('$10.00')
  })

  // Saving the header switch fails at the server. The test protects the error toast and the
  // restored switch state so the card keeps matching the persisted setting.
  it('restores the switch when saving enablement fails', async () => {
    updateProfileMock.mockRejectedValue(new Error('save failed'))
    const wrapper = mountCard({ enabled: true })
    const toggle = wrapper.get('[data-testid="balance-notify-enabled"]')

    await toggle.trigger('click')
    await flushPromises()

    expect(updateProfileMock).toHaveBeenCalledWith({ balance_notify_enabled: false })
    expect(showErrorMock).toHaveBeenCalled()
    expect(toggle.attributes('aria-checked')).toBe('true')
  })
})
