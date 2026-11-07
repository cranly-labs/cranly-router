import landing from './landing'
import common from './common'
import dashboard from './dashboard'
import creative from './creative'
import admin from './admin'
import misc from './misc'
import team from './team'
import requests from './requests'
import plugins from './plugins'

// index.ts assembles the English interface catalog from domain dictionaries and
// shared legal and localization messages. Named request and administrator
// namespaces coexist with root-level spreads, whose order determines the value
// retained if domain modules define the same key.

export default {
  plugins,
  requests,
  legal: {
    login: 'Войти',
    loadFailed: 'Не удалось загрузить документ',
    retry: 'Обновите страницу и попробуйте снова.',
    notFound: 'Документ не найден',
    notFoundDescription: 'Такого документа нет или он был удалён.',
    title: 'Условия входа',
    empty: 'Содержимое не предоставлено.',
    updatedAt: 'Обновлено: {date}',
    acceptedPrefix: 'Я прочитал и согласен с',
    consentRequired: 'Примите актуальные условия, чтобы продолжить вход.',
    disabledUntilAccepted: 'Пароль и быстрый вход станут доступны после принятия условий.',
    viewTerms: 'Посмотреть условия',
    updateNotice: 'Условия обновлены',
    changedNotice:
      'Наши условия были обновлены {date}. Прочитайте документы, прежде чем продолжить.',
    recently: 'недавняя дата',
    documents: 'Документы',
    reject: 'Отклонить',
    accept: 'Принять и продолжить',
  },
  notFoundPage: {
    description: 'Страница, которую вы ищете, не существует или была перемещена.',
    back: 'Назад',
    dashboard: 'Перейти в панель управления',
    help: 'Нужна помощь?',
    support: 'Связаться с поддержкой',
  },

  localization: {
    useAsOriginal: 'Использовать как оригинал',
    languageConflict: 'Для языка {language} уже есть перевод. Какой из них должен быть оригиналом?',
    nameRequired: 'Введите название.',
    displayName: 'Отображаемое название',
    productName: 'Название платёжного продукта',
    fields: {
      site_name: 'Название сайта',
      site_title: 'Заголовок главной страницы',
      site_subtitle: 'Подзаголовок',
    },
    original: 'Оригинал',
    originalLanguage: 'Язык оригинала',
    chooseOriginalLanguage: 'Выберите язык оригинала',
    unknownOriginal:
      'У этого оригинала пока нет языка. Выберите его язык выше, прежде чем редактировать.',
    addTranslation: 'Добавить перевод',
    languageCount: 'Языков: {n}',
    needsUpdate: 'Требуется обновление',
    dialogTitle: 'Перевод · {field}',
    dialogHint:
      'Выберите язык слева. Пользователи видят оригинал, если перевод отсутствует или оригинал изменился.',
    status: {
      translated: 'Переведено',
      stale: 'Требуется обновление',
      missing: 'Не переведено',
    },
    staleNotice: 'Оригинал изменился, поэтому этот перевод скрыт.',
    stillValid: 'Всё ещё подходит',
    removeTranslation: 'Удалить перевод',
    languages: 'Языки',
    translations: 'Переводы',
    originalLanguageUnset: 'Язык не задан',
    originalHint: 'Пользователи видят этот оригинал, если для их языка нет подходящего перевода.',
    reference: 'Оригинал · {language}',
    missingHint: 'Для этого языка пока нет перевода. Пользователи видят оригинал.',
    startTranslation: 'Начать перевод',
    emptyValue: 'Пусто',
    keepOriginal: 'Оставить текущий оригинал и удалить этот перевод',
    useTranslationAsOriginal: 'Использовать перевод на {language} как оригинал',
    done: 'Готово',
    defaultLanguage: 'Язык по умолчанию',
  },
  ...landing,
  ...common,
  ...dashboard,
  ...creative,
  admin,
  ...misc,
  ...team,
}
