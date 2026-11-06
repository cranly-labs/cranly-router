import landing from './landing'
import common from './common'
import dashboard from './dashboard'
import creative from './creative'
import admin from './admin'
import misc from './misc'
import team from './team'
import requests from './requests'
import plugins from './plugins'

// index.ts assembles the Russian interface catalog from domain dictionaries and
// shared legal and localization messages. Named request and administrator
// namespaces coexist with root-level spreads, whose order determines the value
// retained if domain modules define the same key.

export default {
  plugins,
  requests,
  legal: {
    login: 'Войти',
    loadFailed: 'Не удалось загрузить документ',
    retry: 'Обновите страницу, чтобы попробовать снова.',
    notFound: 'Документ не найден',
    notFoundDescription: 'Этот документ не существует или был удалён.',
    title: 'Условия входа',
    empty: 'Содержимое не предоставлено.',
    updatedAt: 'Обновлено: {date}',
    acceptedPrefix: 'Я прочитал и принимаю',
    consentRequired: 'Примите актуальные условия, чтобы продолжить вход.',
    disabledUntilAccepted: 'Пароль и быстрый вход станут доступны после принятия условий.',
    viewTerms: 'Посмотреть условия',
    updateNotice: 'Условия обновлены',
    changedNotice:
      'Наши условия были обновлены {date}. Прочитайте документы, прежде чем продолжить.',
    recently: 'недавно',
    documents: 'Документы',
    reject: 'Отказаться',
    accept: 'Принять и продолжить',
  },
  notFoundPage: {
    description: 'Страница, которую вы ищете, не существует или была перемещена.',
    back: 'Назад',
    dashboard: 'Перейти в панель',
    help: 'Нужна помощь?',
    support: 'Связаться с поддержкой',
  },

  localization: {
    useAsOriginal: 'Сделать оригиналом',
    languageConflict: 'У языка {language} уже есть перевод. Что считать оригиналом?',
    nameRequired: 'Введите название.',
    displayName: 'Отображаемое имя',
    productName: 'Название товара для оплаты',
    fields: {
      site_name: 'Название сайта',
      site_title: 'Заголовок главной страницы',
      site_subtitle: 'Подзаголовок',
    },
    original: 'Оригинал',
    originalLanguage: 'Язык оригинала',
    chooseOriginalLanguage: 'Выберите язык оригинала',
    unknownOriginal:
      'У этого оригинала ещё не указан язык. Выберите его язык выше, прежде чем редактировать.',
    addTranslation: 'Добавить перевод',
    languageCount: 'Языков: {n}',
    needsUpdate: 'Требует обновления',
    dialogTitle: 'Перевод · {field}',
    dialogHint:
      'Выберите язык слева. Пользователи видят оригинал, если перевода нет или оригинал изменился.',
    status: {
      translated: 'Переведено',
      stale: 'Требует обновления',
      missing: 'Без перевода',
    },
    staleNotice: 'Оригинал изменился, поэтому этот перевод скрыт.',
    stillValid: 'Всё ещё подходит',
    removeTranslation: 'Удалить перевод',
    languages: 'Языки',
    translations: 'Переводы',
    originalLanguageUnset: 'Язык не указан',
    originalHint: 'Пользователи видят этот оригинал, когда для их языка нет подходящего перевода.',
    reference: 'Оригинал · {language}',
    missingHint: 'Для этого языка перевода ещё нет. Пользователи пока видят оригинал.',
    startTranslation: 'Начать перевод',
    emptyValue: 'Пусто',
    keepOriginal: 'Оставить текущий оригинал и удалить тот перевод',
    useTranslationAsOriginal: 'Использовать перевод {language} как оригинал',
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
