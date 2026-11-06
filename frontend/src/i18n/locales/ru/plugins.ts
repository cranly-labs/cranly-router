// plugins.ts supplies localized plugin navigation and recovery messages for
// authenticated contribution pages. The host owns these labels while installed
// plugins provide their own page titles and translations through the frontend SDK
// and accepted manifest descriptions.

export default {
  fields: {
    secretSaved: 'Сохранено; оставьте без изменений, чтобы сохранить',
    secretHint: 'Введите новое значение, чтобы заменить сохранённый секрет.',
    secretWillClear: 'Сохранённый секрет будет удалён при сохранении.',
    keepSecret: 'Оставить сохранённое значение',
    clearSecret: 'Очистить сохранённое значение',
    errors: {
      required: 'Введите значение.',
      number: 'Введите конечное число.',
      integer: 'Введите целое число в безопасном диапазоне.',
      minimum: 'Введите не меньше {limit}.',
      maximum: 'Введите не больше {limit}.',
      length: 'Используйте не больше {limit} символов.',
      choice: 'Выберите доступное значение.',
      pattern: 'Значение не соответствует требуемому формату.',
      rejected: 'Плагин отклонил это значение.',
    },
  },

  pageTitle: 'Плагин',
  loadFailed: 'Не удалось загрузить плагин',
  componentMissing: 'Этот плагин не зарегистрировал запрошенную страницу.',
  notFound: 'Страница плагина недоступна',
  notFoundDescription: 'Возможно, эта страница удалена, отключена или доступна только другой роли.',
}
