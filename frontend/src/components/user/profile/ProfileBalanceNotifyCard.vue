<template>
  <section class="card" data-testid="balance-notify-card">
    <!-- The header switch is the master control; the body collapses when notifications are off. -->
    <header class="flex items-center justify-between gap-4 px-6 py-4">
      <div class="min-w-0">
        <h2 class="text-lg font-medium text-gray-900 dark:text-white">
          {{ t('profile.balanceNotify.title') }}
        </h2>
        <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">
          {{ t('profile.balanceNotify.description') }}
        </p>
      </div>
      <Toggle
        :model-value="notifyEnabled"
        :disabled="savingEnabled"
        :aria-label="t('profile.balanceNotify.enabled')"
        data-testid="balance-notify-enabled"
        @update:model-value="handleToggle"
      />
    </header>

    <Collapse :open="notifyEnabled">
      <div
        class="space-y-4 border-t border-gray-100 px-6 py-6 dark:border-dark-700"
        data-testid="balance-notify-body"
      >
        <!-- Delivery only reaches verified, enabled addresses, so an empty recipient set must be visible. -->
        <SettingsNotice
          v-if="!hasActiveRecipient"
          tone="warning"
          data-testid="balance-notify-no-recipient"
        >
          {{ t('profile.balanceNotify.noActiveRecipient') }}
        </SettingsNotice>

        <div>
          <label for="balance-notify-threshold" class="input-label">
            {{ t('profile.balanceNotify.threshold') }}
          </label>
          <div class="flex gap-2">
            <div class="input-icon-wrap flex-1">
              <span
                class="input-icon font-medium text-gray-500 dark:text-dark-400"
                aria-hidden="true"
              >
                {{ balanceUnitSymbol }}
              </span>
              <input
                id="balance-notify-threshold"
                v-model.number="customThreshold"
                type="number"
                min="0"
                step="0.01"
                inputmode="decimal"
                class="input input-has-icon"
                :class="thresholdPrefixClass"
                :placeholder="t('profile.balanceNotify.thresholdPlaceholder')"
                aria-describedby="balance-notify-threshold-hint"
                @keyup.enter="handleThresholdUpdate"
              />
            </div>
            <button
              type="button"
              class="btn btn-secondary whitespace-nowrap"
              :disabled="!thresholdChanged || savingThreshold"
              data-testid="balance-notify-threshold-save"
              @click="handleThresholdUpdate"
            >
              {{ savingThreshold ? t('common.saving') : t('common.save') }}
            </button>
          </div>
          <p id="balance-notify-threshold-hint" class="input-hint">
            {{ thresholdHint }}
          </p>
        </div>

        <div>
          <label for="balance-notify-new-email" class="input-label">
            {{ t('profile.balanceNotify.extraEmails') }}
          </label>

          <div class="space-y-2">
            <!-- Saved addresses: verified rows carry a delivery switch, unverified rows offer verification. -->
            <div
              v-for="entry in emailEntries"
              :key="entry.email"
              class="rounded-control border border-gray-200 px-3 py-2 dark:border-dark-600"
              data-testid="balance-notify-email-row"
            >
              <div class="flex items-center gap-3">
                <span
                  class="min-w-0 flex-1 truncate text-sm"
                  :class="
                    entry.verified && entry.disabled
                      ? 'text-gray-400 dark:text-dark-400'
                      : 'text-gray-900 dark:text-dark-100'
                  "
                >
                  {{ entry.email }}
                </span>
                <span
                  class="badge shrink-0"
                  :class="entry.verified ? 'badge-success' : 'badge-warning'"
                >
                  {{
                    entry.verified
                      ? t('profile.balanceNotify.verified')
                      : t('profile.balanceNotify.unverified')
                  }}
                </span>
                <Toggle
                  v-if="entry.verified"
                  :model-value="!entry.disabled"
                  size="sm"
                  :aria-label="t('profile.balanceNotify.emailEnabled', { email: entry.email })"
                  @update:model-value="handleEmailToggle(entry)"
                />
                <button
                  v-else-if="verifyingEmail !== entry.email"
                  type="button"
                  class="btn btn-secondary btn-sm shrink-0"
                  :disabled="sendingSavedCode"
                  @click="sendCodeForSaved(entry.email)"
                >
                  {{ t('profile.balanceNotify.verify') }}
                </button>
                <button
                  type="button"
                  class="btn btn-ghost btn-icon-sm shrink-0 hover:text-red-600 dark:hover:text-red-400"
                  :aria-label="t('profile.balanceNotify.removeEmail')"
                  :title="t('profile.balanceNotify.removeEmail')"
                  @click="handleRemoveEmail(entry.email)"
                >
                  <Icon name="trash" size="sm" />
                </button>
              </div>

              <div
                v-if="!entry.verified && verifyingEmail === entry.email"
                class="mt-2 flex flex-wrap items-center gap-2"
              >
                <input
                  v-model="verifyCode"
                  type="text"
                  inputmode="numeric"
                  autocomplete="one-time-code"
                  maxlength="6"
                  class="input w-32"
                  :aria-label="t('profile.balanceNotify.codePlaceholder')"
                  :placeholder="t('profile.balanceNotify.codePlaceholder')"
                  @keyup.enter="verifySavedEmail(entry.email)"
                />
                <button
                  type="button"
                  class="btn btn-primary"
                  :disabled="verifyCode.length !== 6 || verifyingSaved"
                  @click="verifySavedEmail(entry.email)"
                >
                  {{ t('profile.balanceNotify.verify') }}
                </button>
                <button
                  type="button"
                  class="btn btn-secondary"
                  :disabled="verifyCountdown > 0 || sendingSavedCode"
                  @click="sendCodeForSaved(entry.email)"
                >
                  {{ resendLabel(verifyCountdown) }}
                </button>
                <button type="button" class="btn btn-ghost" @click="verifyingEmail = ''">
                  {{ t('common.cancel') }}
                </button>
              </div>
            </div>

            <!-- Pending addresses share the unverified row layout until their code is accepted. -->
            <div
              v-for="(pe, idx) in pendingEmails"
              :key="pe.email"
              class="rounded-control border border-gray-200 px-3 py-2 dark:border-dark-600"
              data-testid="balance-notify-pending-row"
            >
              <div class="flex items-center gap-3">
                <span class="min-w-0 flex-1 truncate text-sm text-gray-900 dark:text-dark-100">
                  {{ pe.email }}
                </span>
                <span class="badge badge-warning shrink-0">
                  {{ t('profile.balanceNotify.unverified') }}
                </span>
                <button
                  v-if="!pe.codeSent"
                  type="button"
                  class="btn btn-secondary btn-sm shrink-0"
                  :disabled="pe.sending"
                  @click="sendCodeFor(idx)"
                >
                  {{ t('profile.balanceNotify.sendCode') }}
                </button>
                <button
                  type="button"
                  class="btn btn-ghost btn-icon-sm shrink-0 hover:text-red-600 dark:hover:text-red-400"
                  :aria-label="t('profile.balanceNotify.removeEmail')"
                  :title="t('profile.balanceNotify.removeEmail')"
                  @click="removePendingEmail(idx)"
                >
                  <Icon name="trash" size="sm" />
                </button>
              </div>

              <div v-if="pe.codeSent" class="mt-2 flex flex-wrap items-center gap-2">
                <input
                  v-model="pe.code"
                  type="text"
                  inputmode="numeric"
                  autocomplete="one-time-code"
                  maxlength="6"
                  class="input w-32"
                  :aria-label="t('profile.balanceNotify.codePlaceholder')"
                  :placeholder="t('profile.balanceNotify.codePlaceholder')"
                  @keyup.enter="verifyPending(idx)"
                />
                <button
                  type="button"
                  class="btn btn-primary"
                  :disabled="pe.code.length !== 6 || pe.verifying"
                  @click="verifyPending(idx)"
                >
                  {{ t('profile.balanceNotify.verify') }}
                </button>
                <button
                  type="button"
                  class="btn btn-secondary"
                  :disabled="pe.countdown > 0 || pe.sending"
                  @click="sendCodeFor(idx)"
                >
                  {{ resendLabel(pe.countdown) }}
                </button>
              </div>
            </div>

            <div v-if="canAddMore" class="flex gap-2">
              <input
                id="balance-notify-new-email"
                v-model="newEmail"
                type="email"
                class="input flex-1"
                :placeholder="t('profile.balanceNotify.emailPlaceholder')"
                aria-describedby="balance-notify-email-hint"
                @keyup.enter="addPendingEmail"
              />
              <button
                type="button"
                class="btn btn-secondary whitespace-nowrap"
                :disabled="!newEmail.trim()"
                @click="addPendingEmail"
              >
                {{ t('common.add') }}
              </button>
            </div>
          </div>
          <p id="balance-notify-email-hint" class="input-hint">
            {{ t('profile.balanceNotify.extraEmailsHint', { max: maxTotalEmails }) }}
          </p>
        </div>
      </div>
    </Collapse>
  </section>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onUnmounted } from 'vue'
import { useI18n } from 'vue-i18n'
import { useAuthStore } from '@/stores/auth'
import { useAppStore } from '@/stores/app'
import { userAPI } from '@/api'
import { useBalanceDisplay } from '@/composables/useBalanceDisplay'
import { extractApiErrorMessage } from '@/utils/apiError'
import type { NotifyEmailEntry } from '@/api/notifications.types'
import Collapse from '@/components/common/Collapse.vue'
import Toggle from '@/components/common/Toggle.vue'
import SettingsNotice from '@/components/common/settings/SettingsNotice.vue'
import Icon from '@/components/icons/Icon.vue'

// ProfileBalanceNotifyCard.vue edits balance-notification settings and verifies additional
// delivery addresses. Persisted profile fields are mirrored into local drafts, while pending
// addresses retain their own code, request flags, and countdown interval. Saved unverified
// addresses share a separate verification form and timer.
//
// The header switch owns enablement and collapses the threshold and address fields when it
// is off. The backend delivers alerts only to verified addresses that are not disabled, and
// the account email is never an implicit recipient, so the card warns whenever no address
// qualifies instead of letting an enabled switch imply working delivery.
//
// Verification and removal mutate server state before requesting a fresh profile, so a
// failed refresh can follow a successful mutation. Pending verification also removes its
// draft before that refresh. Existing countdown handles are cleared on unmount, but in-
// flight send requests are not cancelled and can finish later; handlers and template
// controls rely on stable pending row indices while requests run.

// maxTotalEmails limits the combined saved and pending notification-address rows offered by
// the add-email control in this card.
const maxTotalEmails = 3

// PendingEmail retains an unsaved notification address and its verification request state.
interface PendingEmail {
  email: string // Trimmed notification destination.
  codeSent: boolean // Verification form availability flag.
  code: string // Entered verification code.
  sending: boolean // Code request in progress.
  verifying: boolean // Verification request in progress.
  countdown: number // Remaining resend cooldown seconds.
  // timer owns this row's countdown interval until completion, successful verification,
  // or component disposal clears the handle that is currently stored here.
  timer: ReturnType<typeof setInterval> | null
}

// The parent supplies persisted notification settings and the account email; local drafts
// follow later profile updates through the watchers below.
const props = defineProps<{
  enabled: boolean // Persisted notification enablement flag.
  threshold: number | null // Optional custom balance threshold.
  extraEmails: NotifyEmailEntry[] // Persisted notification email records.
  systemDefaultThreshold: number // System balance notification threshold.
  userEmail: string // Account email for prefilling.
}>()

// Auth state receives refreshed profiles while app notifications and configured balance
// formatting describe notification settings and request outcomes.
const { t } = useI18n()
const authStore = useAuthStore()
const appStore = useAppStore()
const { balanceUnitSymbol, formatBalanceAmount } = useBalanceDisplay()

// notifyEnabled, customThreshold, and emailEntries mirror persisted settings; pendingEmails
// and newEmail hold unsaved address verification drafts within this card. savingEnabled
// and savingThreshold disable their controls while the matching profile update runs.
const notifyEnabled = ref(props.enabled)
// customThreshold holds the input draft. An empty input from v-model.number arrives as an
// empty string, and a stored zero is shown as empty because zero selects the system default.
const customThreshold = ref<number | string | null>(thresholdInputValue(props.threshold))
const emailEntries = ref<NotifyEmailEntry[]>([...props.extraEmails])
const pendingEmails = ref<PendingEmail[]>([])
const newEmail = ref('')
const savingEnabled = ref(false)
const savingThreshold = ref(false)

// verifyingEmail and verifyCode identify the saved address being verified; request flags and
// a single countdown timer govern that shared verification form.
const verifyingEmail = ref('')
const verifyCode = ref('')
const verifyingSaved = ref(false)
const sendingSavedCode = ref(false)
const verifyCountdown = ref(0)
let verifyTimer: ReturnType<typeof setInterval> | null = null

// canAddMore reports whether saved and pending addresses leave room below the three-row
// limit, controlling whether the template displays the add-email input.
const canAddMore = computed(() => {
  return emailEntries.value.length + pendingEmails.value.length < maxTotalEmails
})

// hasActiveRecipient reports whether any saved address will receive alerts, matching the
// backend recipient filter that skips unverified, disabled, and empty-address entries.
const hasActiveRecipient = computed(() => {
  return emailEntries.value.some(
    (entry) => entry.verified && !entry.disabled && entry.email.trim() !== '',
  )
})

// thresholdChanged reports whether the normalized draft differs from the persisted
// threshold, so the save button stays inactive until there is something to save.
const thresholdChanged = computed(() => {
  return normalizeThreshold(customThreshold.value) !== normalizeThreshold(props.threshold)
})

// thresholdHint names the effective default amount when the administrator configured one;
// otherwise it only explains that an empty field falls back to the default.
const thresholdHint = computed(() => {
  if (props.systemDefaultThreshold <= 0) {
    return t('profile.balanceNotify.thresholdHint')
  }
  const amount = formatBalanceAmount(props.systemDefaultThreshold, { fractionDigits: 2 })
  return t('profile.balanceNotify.thresholdHintWithDefault', { amount })
})

// thresholdPrefixClass narrows the prefix slot for single-character symbols, matching
// AmountInput; longer or multi-unit symbols such as emoji keep the default slot width.
const thresholdPrefixClass = computed(() => {
  return balanceUnitSymbol.value.length === 1 ? 'input-icon-text' : ''
})

watch(
  () => props.enabled,
  (val) => {
    notifyEnabled.value = val
  },
)
watch(
  () => props.threshold,
  (val) => {
    customThreshold.value = thresholdInputValue(val)
  },
)
watch(
  () => props.extraEmails,
  (val) => {
    emailEntries.value = [...val]
  },
)

// normalizeThreshold converts a draft or persisted threshold into the saved number. Empty,
// non-numeric, zero, and negative values become zero, which the backend treats as the
// system default.
function normalizeThreshold(value: number | string | null): number {
  return typeof value === 'number' && value > 0 ? value : 0
}

// thresholdInputValue maps a persisted threshold to the input draft, leaving the field
// empty for the zero or null values that select the system default.
function thresholdInputValue(value: number | null): number | null {
  return normalizeThreshold(value) > 0 ? value : null
}

// resendLabel returns the resend button text, appending the remaining cooldown seconds
// while a countdown is active.
function resendLabel(countdown: number): string {
  const label = t('profile.balanceNotify.resend')
  return countdown > 0 ? `${label} (${countdown}s)` : label
}

// When list is empty on mount, pre-fill the add input with user's email
onMounted(() => {
  if (emailEntries.value.length === 0 && props.userEmail) {
    newEmail.value = props.userEmail
  }
})

// Clear the currently registered pending-row and saved-address countdowns when the card
// unmounts, releasing intervals that would otherwise keep changing detached form state.
onUnmounted(() => {
  for (const pe of pendingEmails.value) {
    if (pe.timer) clearInterval(pe.timer)
  }
  if (verifyTimer) clearInterval(verifyTimer)
})

// handleToggle shows the requested enablement immediately, so the body expands or collapses
// without waiting, then saves it and publishes the returned profile. The switch stays
// disabled during the request; failure reports the API error and restores the previous
// value.
const handleToggle = async (value: boolean) => {
  notifyEnabled.value = value
  savingEnabled.value = true
  try {
    const updated = await userAPI.updateProfile({ balance_notify_enabled: value })
    authStore.user = updated
  } catch (err: unknown) {
    appStore.showError(extractApiErrorMessage(err, t('common.error')))
    // The switch was updated optimistically above; revert it so the card matches the
    // setting the server still holds.
    notifyEnabled.value = !value
  } finally {
    savingEnabled.value = false
  }
}

// handleThresholdUpdate saves a positive custom threshold or zero for the default setting.
// The Enter key reaches it directly, so it ignores calls while unchanged or already saving.
// It publishes the returned profile after success and resets its save indicator after
// either success or failure.
const handleThresholdUpdate = async () => {
  if (!thresholdChanged.value || savingThreshold.value) return
  savingThreshold.value = true
  try {
    const threshold = normalizeThreshold(customThreshold.value)
    const updated = await userAPI.updateProfile({ balance_notify_threshold: threshold })
    authStore.user = updated
    appStore.showSuccess(t('common.saved'))
  } catch (err: unknown) {
    appStore.showError(extractApiErrorMessage(err, t('common.error')))
  } finally {
    savingThreshold.value = false
  }
}

// handleEmailToggle persists the opposite disabled flag for one saved address. The returned
// profile replaces both auth state and the displayed address list after the API accepts the
// change.
async function handleEmailToggle(entry: NotifyEmailEntry) {
  const newDisabled = !entry.disabled
  try {
    const updated = await userAPI.toggleNotifyEmail(entry.email, newDisabled)
    authStore.user = updated
    emailEntries.value = [...updated.balance_notify_extra_emails]
  } catch (err: unknown) {
    appStore.showError(extractApiErrorMessage(err, t('common.error')))
  }
}

// addPendingEmail trims the new address and opens a verification draft when it is nonempty
// and unique. Duplicate matching is case-insensitive across saved and pending addresses, and
// the template owns the row-count limit.
function addPendingEmail() {
  const email = newEmail.value.trim()
  if (!email) return
  // Saved and pending destinations share a case-insensitive uniqueness check so
  // separate verification drafts cannot introduce another row for the same address.
  const isDuplicate =
    emailEntries.value.some((e) => e.email.toLowerCase() === email.toLowerCase()) ||
    pendingEmails.value.some((p) => p.email.toLowerCase() === email.toLowerCase())
  if (isDuplicate) {
    appStore.showError(t('profile.balanceNotify.emailDuplicate'))
    return
  }
  pendingEmails.value.push({
    email,
    codeSent: false,
    code: '',
    sending: false,
    verifying: false,
    countdown: 0,
    timer: null,
  })
  newEmail.value = ''
}

// removePendingEmail discards an unsaved address draft at the supplied index. Its resend
// countdown is cleared first because the unmount cleanup only reaches rows still listed.
function removePendingEmail(idx: number) {
  const pe = pendingEmails.value[idx]
  if (!pe) return
  if (pe.timer) clearInterval(pe.timer)
  pendingEmails.value.splice(idx, 1)
}

// sendCodeFor requests verification for a pending row identified by its current index.
// Successful sending starts that row's sixty-second countdown; the template disables
// resending during cooldown and component disposal clears existing row timers.
async function sendCodeFor(idx: number) {
  const pe = pendingEmails.value[idx]
  if (!pe) return
  pe.sending = true
  try {
    await userAPI.sendNotifyEmailCode(pe.email)
    pe.codeSent = true
    pe.countdown = 60
    pe.timer = setInterval(() => {
      pe.countdown--
      if (pe.countdown <= 0 && pe.timer) {
        clearInterval(pe.timer)
        pe.timer = null
      }
    }, 1000)
    appStore.showSuccess(t('profile.balanceNotify.codeSent'))
  } catch (err: unknown) {
    appStore.showError(extractApiErrorMessage(err, t('common.error')))
  } finally {
    pe.sending = false
  }
}

// verifyPending submits a six-character code for the pending row at the supplied index.
// Successful verification clears its timer and removes the draft before fetching the
// authoritative profile. The server mutation has already completed when that refresh begins,
// so a refresh failure reports an error after the local draft was removed. Callers keep row
// identity stable while the request is pending because removal uses its captured index.
async function verifyPending(idx: number) {
  const pe = pendingEmails.value[idx]
  if (!pe || !pe.code || pe.code.length !== 6) return
  pe.verifying = true
  try {
    await userAPI.verifyNotifyEmail(pe.email, pe.code)
    // Stop the verified row's countdown before removing its draft so the retained row object
    // is no longer updated by its interval.
    if (pe.timer) clearInterval(pe.timer)
    pendingEmails.value.splice(idx, 1)
    appStore.showSuccess(t('profile.balanceNotify.verifySuccess'))
    const updated = await userAPI.getProfile()
    authStore.user = updated
    emailEntries.value = [...updated.balance_notify_extra_emails]
  } catch (err: unknown) {
    appStore.showError(extractApiErrorMessage(err, t('common.error')))
  } finally {
    pe.verifying = false
  }
}

// handleRemoveEmail removes a persisted address and then reloads the profile. The server
// removal completes before the refresh, so refresh failure may leave the old list visible
// until another profile update arrives.
const handleRemoveEmail = async (email: string) => {
  try {
    await userAPI.removeNotifyEmail(email)
    appStore.showSuccess(t('profile.balanceNotify.removeSuccess'))
    const updated = await userAPI.getProfile()
    authStore.user = updated
    emailEntries.value = [...updated.balance_notify_extra_emails]
  } catch (err: unknown) {
    appStore.showError(extractApiErrorMessage(err, t('common.error')))
  }
}

// sendCodeForSaved sends a verification code for an existing unverified address and opens
// its shared verification form. It clears prior entered digits and replaces the shared
// countdown interval after a successful send.
async function sendCodeForSaved(email: string) {
  sendingSavedCode.value = true
  try {
    await userAPI.sendNotifyEmailCode(email)
    verifyingEmail.value = email
    verifyCode.value = ''
    verifyCountdown.value = 60
    if (verifyTimer) clearInterval(verifyTimer)
    verifyTimer = setInterval(() => {
      verifyCountdown.value--
      if (verifyCountdown.value <= 0 && verifyTimer) {
        clearInterval(verifyTimer)
        verifyTimer = null
      }
    }, 1000)
    appStore.showSuccess(t('profile.balanceNotify.codeSent'))
  } catch (err: unknown) {
    appStore.showError(extractApiErrorMessage(err, t('common.error')))
  } finally {
    sendingSavedCode.value = false
  }
}

// verifySavedEmail submits the shared six-character code for the supplied saved address.
// Success clears the form and timer before reloading the profile; a later refresh failure
// reports an error after verification has completed.
async function verifySavedEmail(email: string) {
  if (!verifyCode.value || verifyCode.value.length !== 6) return
  verifyingSaved.value = true
  try {
    await userAPI.verifyNotifyEmail(email, verifyCode.value)
    verifyingEmail.value = ''
    verifyCode.value = ''
    if (verifyTimer) {
      clearInterval(verifyTimer)
      verifyTimer = null
    }
    appStore.showSuccess(t('profile.balanceNotify.verifySuccess'))
    const updated = await userAPI.getProfile()
    authStore.user = updated
    emailEntries.value = [...updated.balance_notify_extra_emails]
  } catch (err: unknown) {
    appStore.showError(extractApiErrorMessage(err, t('common.error')))
  } finally {
    verifyingSaved.value = false
  }
}
</script>
