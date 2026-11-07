// landing.ts supplies English text for the public homepage, key usage lookup,
// and setup wizard. Public navigation and introductory sections share this
// dictionary with setup progress and usage details so these entry pages resolve
// messages through the root catalog.

export default {
  // Home Page
  home: {
    viewOnGithub: 'Смотреть на GitHub',
    viewDocs: 'Документация',
    docs: 'Документация',
    switchToLight: 'Включить светлую тему',
    switchToDark: 'Включить тёмную тему',
    dashboard: 'Панель',
    login: 'Войти',
    getStarted: 'Начать работу',
    goToDashboard: 'Перейти в панель',
    exploreMarketplace: 'Каталог моделей',
    viewAll: 'Смотреть все',
    nav: {
      searchModels: 'Поиск по моделям, провайдерам и возможностям',
      models: 'Модели',
    },
    // User-focused value proposition
    heroBadge: 'Единый доступ, умная маршрутизация, оплата по факту',
    heroTitle: 'Единый интерфейс для AI-моделей',
    heroSubtitle: 'Один ключ — все AI-модели',
    heroDescription:
      'Больше не нужно держать несколько подписок. Claude, GPT, Gemini и другие модели доступны по одному API-ключу',
    tags: {
      subscriptionToApi: 'Подписка в формате API',
      stickySession: 'Сохранение сессии',
      realtimeBilling: 'Оплата по факту',
    },
    // Pain points section
    painPoints: {
      title: 'Знакомо?',
      items: {
        expensive: {
          title: 'Дорогие подписки',
          desc: 'Платите за несколько AI-подписок, и каждый месяц сумма растёт',
        },
        complex: {
          title: 'Хаос провайдеров',
          desc: 'Провайдеры и API-ключи разбросаны по разным платформам',
        },
        unstable: {
          title: 'Перебои в работе',
          desc: 'Один провайдер упирается в лимиты — и работа останавливается',
        },
        noControl: {
          title: 'Нет контроля расходов',
          desc: 'Непонятно, куда уходят деньги, и нельзя ограничить траты участников команды',
        },
      },
    },
    // Solutions section
    solutions: {
      title: 'Мы решаем эти задачи',
      subtitle: 'Три простых шага до доступа к AI без лишних хлопот',
    },
    features: {
      unifiedGateway: 'Доступ в один клик',
      unifiedGatewayDesc:
        'Один API-ключ для всех подключённых AI-моделей. Отдельные приложения не нужны.',
      multiProvider: 'Маршрутизация между провайдерами',
      multiProviderDesc:
        'Запросы распределяются между провайдерами с балансировкой нагрузки и автоматическим переключением при сбое.',
      balanceQuota: 'Платите за использование',
      balanceQuotaDesc:
        'Оплата по фактическому расходу с лимитами квот. Полная картина потребления команды.',
      dataPolicies: 'Свои правила работы с данными',
      dataPoliciesDesc:
        'Модели, квоты и права доступа настраиваются в одном месте, поэтому запросы команды идут только в доверенные сервисы.',
      browseAll: 'Смотреть все',
      learnMore: 'Подробнее',
      viewUsage: 'Смотреть расход',
      usageChart: 'Динамика расхода',
    },
    // Comparison section
    comparison: {
      title: 'Почему мы?',
      headers: {
        feature: 'Сравнение',
        official: 'Официальные подписки',
        us: 'Наша платформа',
      },
      items: {
        pricing: {
          feature: 'Оплата',
          official: 'Фиксированная сумма каждый месяц, в том числе за простой',
          us: 'Платите только за то, что используете',
        },
        models: {
          feature: 'Выбор модели',
          official: 'Только один провайдер',
          us: 'Свободное переключение между моделями',
        },
        management: {
          feature: 'Управление провайдерами',
          official: 'Каждый сервис настраивается отдельно',
          us: 'Единый ключ, одна панель',
        },
        stability: {
          feature: 'Стабильность',
          official: 'Лимиты одного провайдера',
          us: 'Пул провайдеров, автоматическое переключение',
        },
        control: {
          feature: 'Контроль расходов',
          official: 'Недоступно',
          us: 'Квоты и подробная аналитика',
        },
      },
    },
    providers: {
      title: 'Поддерживаемые AI-модели',
      description: 'Один API, множество вариантов',
      supported: 'Поддерживается',
      soon: 'Скоро',
      claude: 'Claude',
      gpt: 'GPT',
      gemini: 'Gemini',
      antigravity: 'Antigravity',
      more: 'Ещё',
      empty: 'Пока нет моделей для отображения',
      unavailable: 'Данные о моделях временно недоступны',
      groups: 'групп сервисов',
      modelCount: 'Модели',
    },
    featured: {
      byProvider: 'от {provider}',
      discountOff: 'Скидка до {percent}%',
    },
    stats: {
      todayTokens: 'Токенов за сегодня',
      totalTokens: 'Токенов за всё время',
      totalUsers: 'Зарегистрировано пользователей',
      supportedModels: 'Поддерживаемых моделей',
      providerTypes: 'типов моделей',
      unavailable: 'Статистика временно недоступна',
    },
    steps: {
      signup: {
        title: 'Зарегистрируйтесь',
        description:
          'Создайте аккаунт и добавьте API-ключи. Командное пространство и квоты можно настроить позже.',
      },
      browse: {
        title: 'Выберите модели',
        description:
          'В каталоге моделей можно сравнить поддерживаемые модели, типы провайдеров и относительные цены.',
      },
      apiKey: {
        title: 'Получите API-ключ',
        description:
          'Отправляйте запросы в формате, совместимом с OpenAI, OpenAI Responses или Anthropic, а оплата идёт по фактическому расходу токенов.',
      },
    },
    // CTA section
    cta: {
      title: 'Подключите своё приложение',
      description: 'Создайте аккаунт, чтобы управлять API-ключами и отслеживать расход моделей.',
      button: 'Создать аккаунт',
    },
    footer: {
      allRightsReserved: 'Все права защищены.',
      quickLinks: 'Быстрые ссылки',
    },
  },
  // Key Usage Query Page
  keyUsage: {
    title: 'Расход по API-ключу',
    subtitle: 'Введите API-ключ, чтобы увидеть расходы и использование в реальном времени',
    placeholder: 'sk-ant-mirror-xxxxxxxxxxxx',
    query: 'Проверить',
    querying: 'Проверяем...',
    privacyNote: 'Ключ обрабатывается локально в браузере и не сохраняется',
    dateRange: 'Период:',
    dateRange15m: 'Последние 15 минут',
    dateRange30m: 'Последние 30 минут',
    dateRangeToday: 'Сегодня',
    dateRange7d: '7 дней',
    dateRange30d: '30 дней',
    dateRange90d: '90 дней',
    dateRangeCustom: 'Произвольный',
    apply: 'Применить',
    used: 'Использовано',
    detailInfo: 'Подробная информация',
    tokenStats: 'Статистика по токенам',
    dailyDetail: 'Детализация по дням',
    modelStats: 'Статистика использования моделей',
    // Table headers
    date: 'Дата',
    model: 'Модель',
    requests: 'Запросы',
    inputTokens: 'Входные токены',
    outputTokens: 'Выходные токены',
    cacheCreationTokens: 'Создание кэша',
    cacheReadTokens: 'Чтение кэша',
    cacheWriteTokens: 'Запись кэша',
    totalTokens: 'Всего токенов',
    cost: 'Стоимость',
    // Status
    quotaMode: 'Режим квоты ключа',
    walletBalance: 'Баланс кошелька',
    billingMode: 'Режим оплаты',
    billingUnavailable: 'Сейчас недоступно',
    billingModes: {
      auto: 'Автоматически',
      subscription: 'Выбранная подписка',
      balance: 'Только баланс',
    },
    billingSources: {
      subscription: 'Подписка',
      balance: 'Баланс',
    },
    // Ring card titles
    totalQuota: 'Общая квота',
    limit5h: 'Лимит на 5 часов',
    limitDaily: 'Суточный лимит',
    limit7d: 'Лимит на 7 дней',
    limitWeekly: 'Недельный лимит',
    limitMonthly: 'Месячный лимит',
    // Detail rows
    remainingQuota: 'Остаток квоты',
    expiresAt: 'Действует до',
    todayExpires: '(истекает сегодня)',
    daysLeft: '(осталось {days} дн.)',
    usedQuota: 'Израсходовано',
    resetNow: 'Скоро сброс',
    subscriptionType: 'Тип подписки',
    subscriptionExpires: 'Подписка действует до',
    // Usage stat cells
    todayRequests: 'Запросов сегодня',
    todayInputTokens: 'Входящих сегодня',
    todayOutputTokens: 'Исходящих сегодня',
    todayTokens: 'Токенов сегодня',
    todayCacheCreation: 'Создание кэша сегодня',
    todayCacheRead: 'Чтение кэша сегодня',
    todayCost: 'Расход за сегодня',
    rpmTpm: 'RPM / TPM',
    totalRequests: 'Всего запросов',
    totalInputTokens: 'Входящих всего',
    totalOutputTokens: 'Исходящих всего',
    totalTokensLabel: 'Всего токенов',
    totalCacheCreation: 'Создание кэша всего',
    totalCacheRead: 'Чтение кэша всего',
    totalCost: 'Всего расходов',
    avgDuration: 'Средняя длительность',
    // Messages
    enterApiKey: 'Введите API-ключ',
    querySuccess: 'Данные получены',
    queryFailed: 'Не удалось получить данные',
    queryFailedRetry: 'Не удалось получить данные, попробуйте позже',
    noDailyUsage: 'Нет данных о расходе по дням',
  },
  // Setup Wizard
  setup: {
    title: 'Настройка TokenRouter',
    description: 'Настройте ваш экземпляр TokenRouter',
    database: {
      title: 'Настройка базы данных',
      description: 'Подключение к базе данных PostgreSQL',
      host: 'Хост',
      port: 'Порт',
      username: 'Имя пользователя',
      password: 'Пароль',
      databaseName: 'Имя базы данных',
      sslMode: 'Режим SSL',
      passwordPlaceholder: 'Пароль',
      ssl: {
        disable: 'Отключено',
        require: 'Требуется',
        verifyCa: 'Проверка CA',
        verifyFull: 'Полная проверка',
      },
    },
    redis: {
      title: 'Настройка Redis',
      description: 'Подключение к серверу Redis',
      host: 'Хост',
      port: 'Порт',
      username: 'Имя пользователя (необязательно)',
      password: 'Пароль (необязательно)',
      database: 'База данных',
      usernamePlaceholder: 'Оставьте пустым для пользователя по умолчанию',
      passwordPlaceholder: 'Пароль',
      enableTls: 'Включить TLS',
      enableTlsHint: 'Использовать TLS при подключении к Redis (публичные сертификаты CA)',
    },
    admin: {
      title: 'Аккаунт администратора',
      description: 'Создайте аккаунт администратора',
      email: 'Email',
      password: 'Пароль',
      confirmPassword: 'Подтвердите пароль',
      passwordPlaceholder: 'Минимум 8 символов',
      confirmPasswordPlaceholder: 'Повторите пароль',
      passwordMismatch: 'Пароли не совпадают',
    },
    ready: {
      title: 'Готово к установке',
      description: 'Проверьте конфигурацию и завершите установку',
      database: 'База данных',
      redis: 'Redis',
      adminEmail: 'Email администратора',
    },
    status: {
      testing: 'Проверка...',
      success: 'Подключение установлено',
      testConnection: 'Проверить подключение',
      installing: 'Установка...',
      completeInstallation: 'Завершить установку',
      completed: 'Установка завершена!',
      redirecting: 'Переход на страницу входа...',
      restarting: 'Сервис перезапускается, подождите...',
      timeout:
        'Перезапуск сервиса занимает больше времени, чем ожидалось. Обновите страницу вручную.',
    },
  },
}
