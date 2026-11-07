// audit.ts supplies English labels for audit log filtering, inspection, and
// clearing. Action names, redacted payload descriptions, and confirmation
// messages share the audit namespace so administrators can read management
// activity in the selected interface language.

export default {
  audit: {
    title: 'Журнал аудита',
    description:
      'Фиксирует операции уровня управления, выполняемые администраторами и пользователями. В учётных данных заголовков сохраняются только первые и последние символы, тела запросов редактируются. Записи нельзя удалять по отдельности; для полной очистки требуется двухфакторная проверка.',
    clearAll: 'Очистить всё',
    empty: 'Записей аудита пока нет',
    loadFailed: 'Не удалось загрузить журнал аудита',
    filters: {
      all: 'Все',
      q: 'Ключевое слово',
      qPlaceholder: 'Путь / действие / email исполнителя',
      actorEmail: 'Email исполнителя',
      action: 'Действие',
      clientIp: 'IP клиента',
      method: 'Метод',
      authMethod: 'Способ аутентификации',
      result: 'Результат',
      resultSuccess: 'Успех',
      resultFailure: 'Ошибка',
      startTime: 'Начало периода',
      endTime: 'Конец периода',
    },
    columns: {
      time: 'Время',
      actor: 'Исполнитель',
      action: 'Действие',
      method: 'Метод',
      result: 'Результат',
      clientIp: 'IP клиента',
      detail: 'Детали',
    },
    detail: {
      title: 'Запись журнала аудита',
      actorRole: 'Роль',
      methodPath: 'Метод / путь',
      latency: 'Задержка',
      credential: 'Учётные данные (маскированы)',
      userAgent: 'User-Agent',
      requestBody: 'Тело запроса (отредактировано)',
      extra: 'Дополнительно',
    },
    clearConfirm: {
      title: 'Очистка всего журнала аудита',
      message:
        'Все записи аудита будут удалены безвозвратно. Сама операция очистки фиксируется в журнале. Продолжить?',
      totpTitle: 'Введите код двухфакторной аутентификации',
      totpHint: 'Для очистки журнала аудита требуется свежая проверка TOTP.',
      success: 'Очищено записей аудита: {count}',
      failed: 'Не удалось очистить журнал аудита',
    },
  },
}
