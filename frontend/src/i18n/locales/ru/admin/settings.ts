// settings.ts supplies English messages for site and gateway configuration,
// error handling, and TLS fingerprint settings. Form labels, effective-value
// hints, validation failures, and save results describe the settings exposed by
// administrator panels through stable translation paths.

export default {
  // Settings
  settings: {
    responsesWS: {
      title: 'Длинные соединения и пул соединений',
      description:
        'Настройка пула, таймаутов и параметров сессий WebSocket для Responses. Изменения применяются на лету после сохранения. Разрешения протоколов на уровне группы определяют, могут ли клиенты использовать длинные соединения.',
      advanced: 'Дополнительные параметры',
      advancedHint: 'Динамические лимиты, прогрев и продолжение сессий',
      restore: 'Сбросить все к значениям по умолчанию',
      resetField: 'Использовать значение по умолчанию',
      applies:
        'Пустые поля используют значения развёртывания; серое число в поле — фактически действующее значение. Лимиты пула меняются постепенно, а активные соединения закрываются после завершения их сессий. Таймауты вступают в силу со следующего хода или ожидания.',
      loadError: 'Не удалось загрузить настройки соединений. Повторите попытку.',
      saveError: 'Не удалось сохранить настройки соединений.',
      applyError:
        'Настройки сохранены, но применить их не удалось. Проверьте логи сервиса и действующие значения.',
      retry: 'Обновить',
      restoring: 'После сохранения все параметры вернутся к значениям развёртывания.',
      units: {
        seconds: 'с',
        ms: 'мс',
        mib: 'МиБ',
      },
      groups: {
        pool: {
          title: 'Пул вышестоящих соединений',
          hint: 'У каждого провайдера есть собственный пул соединений, которые используют сессии.',
        },
        client: {
          title: 'Клиентские соединения',
          hint: 'WebSocket-соединения от клиентов к шлюзу.',
        },
        messageSize: {
          title: 'Размер сообщений',
        },
        timeout: {
          title: 'Таймауты вышестоящей стороны',
          hint: 'Сколько ждать при подключении к вышестоящей стороне и обмене сообщениями.',
        },
        dynamic: {
          title: 'Динамический лимит соединений',
        },
        prewarm: {
          title: 'Прогрев',
        },
        session: {
          title: 'Продолжение сессий',
        },
      },
      fields: {
        max_conns_per_provider: 'Максимум соединений на провайдера',
        queue_limit_per_conn: 'Лимит очереди на соединение',
        min_idle_per_provider: 'Минимум простаивающих соединений',
        max_idle_per_provider: 'Максимум простаивающих соединений',
        max_ingress_connections_per_api_key: 'Максимум соединений на API-ключ',
        client_first_message_timeout_seconds: 'Таймаут первого сообщения',
        ingress_inter_turn_idle_timeout_seconds: 'Максимальное время простоя между ходами',
        client_read_limit_bytes: 'Максимальный размер сообщения',
        http_bridge_threshold_bytes: 'Порог перехода на HTTP',
        dial_timeout_seconds: 'Подключение',
        read_timeout_seconds: 'Ожидание сообщения от вышестоящей стороны',
        write_timeout_seconds: 'Отправка сообщения',
        dynamic_max_conns_by_provider_concurrency_enabled:
          'Масштабировать по параллелизму провайдера',
        oauth_max_conns_factor: 'Коэффициент для OAuth-провайдеров',
        apikey_max_conns_factor: 'Коэффициент для провайдеров с API-ключом',
        pool_target_utilization: 'Целевая утилизация',
        prewarm_cooldown_ms: 'Минимальный интервал прогрева',
        ingress_previous_response_recovery_enabled:
          'Восстанавливаться, если вышестоящая сторона потеряла предыдущий ход',
        sticky_session_ttl_seconds: 'Хранение сессии провайдера',
        sticky_response_id_ttl_seconds: 'Хранение продолжения ответа',
      },
      hints: {
        max_conns_per_provider:
          'Лимиты, рассчитанные по параллелизму, ограничиваются этим значением.',
        queue_limit_per_conn:
          'Сколько запросов могут ждать на каждом соединении, когда все соединения заняты.',
        min_idle_per_provider: 'Сколько соединений поддерживать готовыми во время простоя.',
        max_idle_per_provider:
          'Значение 0 означает, что простаивающие соединения не поддерживаются.',
        max_ingress_connections_per_api_key: 'Значение 0 означает отсутствие ограничений.',
        ingress_inter_turn_idle_timeout_seconds: 'Значение 0 означает отсутствие ограничений.',
        http_bridge_threshold_bytes:
          'Более крупные запросы отправляются вышестоящей стороне по HTTP. Группа должна разрешать такое преобразование.',
        dynamic_max_conns_by_provider_concurrency_enabled:
          'Лимит каждого провайдера равен его параллелизму, умноженному на соответствующий коэффициент, но не выше максимума на провайдера. Провайдеры с неограниченным параллелизмом используют этот максимум.',
        pool_target_utilization:
          'От 0 до 1. Пул держит число соединений, равное частному запросов и целевой утилизации, поэтому меньшие значения оставляют больше свободных соединений.',
        ingress_previous_response_recovery_enabled:
          'Когда вышестоящая сторона не находит предыдущий ответ, запрос повторяется один раз без его ID. Запросы с выводом вызова инструментов возвращают ошибку.',
      },
    },

    title: 'Системные настройки',
    description:
      'Управление регистрацией, подтверждением email, значениями по умолчанию и настройками SMTP',
    tabs: {
      general: 'Общие',
      agreement: 'Соглашение',
      features: 'Возможности',
      security: 'Безопасность',
      users: 'Пользователи',
      gateway: 'Шлюз',
      email: 'Email',
      backup: 'Резервное копирование',
      payment: 'Оплата',
    },
    gatewaySections: {
      label: 'Категории настроек шлюза',
      general: 'Общие',
      anthropic: 'Anthropic',
      openai: 'OpenAI',
      grok: 'Grok',
      antigravity: 'Antigravity',
      ollamaCloud: 'Ollama Cloud',
    },
    preAggregation: {
      title: 'Предварительная агрегация',
      description:
        'Управление задачами агрегации использования и операций, а также состоянием покрытия данных.',
      usage: 'Агрегация использования',
      ops: 'Агрегация операций',
      enabled: 'Включить задачу',
      interval: 'Интервал обновления (секунды)',
      coverage: 'Покрытие',
      phase: 'Этап',
      lastSuccess: 'Последний успешный запуск',
      lastDuration: 'Длительность последнего запуска',
      lag: 'Отставание данных',
      lastError: 'Последняя ошибка',
      unavailable: 'Отключено конфигурацией развёртывания',
      noData: 'Нет данных',
      loadFailed: 'Не удалось загрузить настройки предварительной агрегации',
      saveFailed: 'Не удалось сохранить настройки предварительной агрегации',
      backfill: 'Ручное заполнение',
      backfillDays: 'Последние дни',
      startBackfill: 'Запустить заполнение',
      backfillAccepted: 'Задача заполнения принята',
      backfillFailed: 'Не удалось запустить заполнение',
      phases: {
        idle: 'Простой',
        live: 'Агрегация в реальном времени',
        backfill: 'Историческое заполнение',
        pending: 'Ожидание первого запуска',
        disabled: 'Отключено',
        error: 'Ошибка',
        unavailable: 'Недоступно',
      },
    },
    emailTabDisabledTitle: 'Подтверждение email не включено',
    emailTabDisabledHint:
      'Включите подтверждение email на вкладке «Безопасность», чтобы настроить SMTP.',
    usageRanking: {
      title: 'Рейтинг использования',
      description:
        'Управление разделом рейтинга использования: его доступностью, сортировкой и отображаемыми полями.',
      enabled: 'Включить рейтинг использования',
      enabledHint:
        'Когда рейтинг отключён, пользователи не могут открыть его ни из навигации, ни через API.',
      sortBy: 'Сортировать по',
      sortByHint:
        'В рейтинг попадают только пользователи с положительным значением выбранной метрики, и эта метрика всегда отображается.',
      limit: 'Число участников рейтинга',
      limitHint: 'Показывать от 1 до 100 пользователей в каждом временном диапазоне.',
      fields: 'Отображаемые поля',
      fieldsHint: 'Скрытые поля также исключаются из ответа API для пользователей.',
      totalTokens: 'Всего токенов',
      requests: 'Запросы',
      actualCost: 'Фактическая стоимость',
      sortOptions: {
        totalTokens: 'Всего токенов',
        requests: 'Запросы',
        actualCost: 'Фактическая стоимость',
      },
    },
    marketplaceAvailability: {
      title: 'Доступность на маркетплейсе',
      description:
        'Настройка окна и размера интервала для полосы доступности на публичном маркетплейсе. Сохранённые изменения применяются при следующем обновлении маркетплейса.',
      windowDays: 'Окно (дни)',
      windowDaysHint: 'Диапазон от {min} до {max} дней. По умолчанию 7 дней.',
      bucketMinutes: 'Размер интервала (минуты)',
      bucketMinutesHint: 'Диапазон от {min} до {max} минут. По умолчанию 120 минут.',
      bucketLimitHint:
        'Если из-за окна и размера интервала получается больше 720 полос, серверная часть автоматически увеличивает размер интервала.',
    },
    features: {
      team: {
        title: 'Командные возможности',
        description:
          'Управление доступностью командных страниц для пользователей и администраторов.',
        enabled: 'Включить командные возможности',
        enabledHint:
          'Когда функция отключена, соответствующие пункты меню скрыты, а прямой переход по URL заблокирован.',
      },
      creative: {
        title: 'Creative Studio',
        description: 'Управление доступностью пользовательской страницы Creative Studio.',
        enabled: 'Включить Creative Studio',
        enabledHint:
          'Когда функция отключена, пункт студии скрыт, список моделей пуст, а создание запусков заблокировано.',
        workerCount: 'Количество обработчиков',
        workerCountHint:
          'Определяет, сколько задач Creative Studio может выполняться одновременно. По умолчанию: 128; изменения применяются сразу.',
        workerCountInvalid: 'Количество обработчиков должно быть положительным целым числом.',
        workerUsage: 'Текущая загрузка',
        modelSettings: {
          title: 'Модели изображений и их возможности',
          description:
            'Creative Studio может использовать только выбранные модели из выбранных групп. Пустой список отключает все модели изображений.',
          add: 'Добавить модель',
          remove: 'Удалить модель',
          modelColumn: 'Модель',
          operationsColumn: 'Возможности',
          selectModel: 'Выберите группу и модель',
          loading: 'Загрузка доступных моделей...',
          loadError: 'Не удалось загрузить список моделей; текущая конфигурация сохранена.',
          empty: 'Модели не настроены. Creative Studio не может использовать модели изображений.',
          unavailable: 'Сейчас недоступна',
          unavailableHint:
            'Этой модели сейчас нет в списке кандидатов. Конфигурация сохранена и применится, когда модель вернётся.',
          validationError:
            'Оставьте хотя бы одну возможность для каждой модели и не выбирайте одну и ту же группу с моделью дважды.',
          operations: {
            generate: 'Текст в изображение',
            edit: 'Изображение в изображение',
            inpaint: 'Дорисовка',
          },
        },
      },
      riskControl: {
        title: 'Контроль рисков',
        description:
          'Включает раздел модерации контента и точку входа аудита шлюза. По умолчанию отключено.',
        configureLink: 'Настроить модерацию контента в разделе контроля рисков',
        enabled: 'Включить контроль рисков',
        enabledHint:
          'Когда функция отключена, пункт в боковом меню администратора скрыт, а модерация на шлюзе не выполняется.',
        cyberSessionBlockEnabled: 'Блокировка сессии при нарушении политики',
        cyberSessionBlockEnabledHint:
          'Применяется только к группам, входящим в область контроля рисков. После ответа вышестоящей стороны с cyber_policy последующие запросы в той же явной session_id / conversation_id / prompt_cache_key временно отклоняются.',
        cyberSessionBlockTTLSeconds: 'Длительность блокировки (секунды)',
        cyberSessionBlockTTLSecondsHint:
          'По умолчанию 3600 секунд. Когда функция отключена, записываются только аудит и записи об использовании.',
      },
      affiliate: {
        title: 'Партнёрские вознаграждения',
        description:
          'Пригласившие получают вознаграждение в зависимости от суммы {unitName}, купленной приглашёнными.',
        enabled: 'Включить партнёрские вознаграждения',
        enabledHint:
          'Когда функция отключена, партнёрские пункты меню скрыты, коды приглашения при регистрации игнорируются, а новые пополнения не приносят вознаграждения.',
        adminRechargeRebate: 'Начислять вознаграждение за пополнения администратором',
        adminRechargeRebateHint:
          'Когда включено, баланс, добавленный через «Управление пользователями» > «Пополнение», приносит партнёрское вознаграждение. Установка баланса и вывод средств — нет.',
        rebateRate: 'Общая ставка вознаграждения',
        rebateRateHint:
          'Доля от купленной суммы в {unitName}, от 0 до 100. При покупке на 1000 {unitName} и ставке 20% вознаграждение составит 200 {unitName}.',
        freezeHours: 'Период заморозки вознаграждения (часы)',
        freezeHoursDesc:
          'В течение этого периода новые вознаграждения нельзя вывести. 0 = без заморозки.',
        durationDays: 'Срок начисления вознаграждения (дни)',
        durationDaysDesc:
          'Пополнения учитываются для вознаграждения в течение этого числа дней после регистрации приглашённого. 0 = бессрочно.',
        perInviteeCap: 'Лимит вознаграждения на одного приглашённого ({unitName})',
        perInviteeCapDesc:
          'Максимальное вознаграждение в {unitName} от одного приглашённого. 0 = без ограничений.',
      },
    },
    registration: {
      title: 'Настройки регистрации',
      description: 'Управление регистрацией и подтверждением пользователей',
      enableRegistration: 'Включить регистрацию',
      enableRegistrationHint: 'Разрешить регистрацию новых пользователей',
      emailVerification: 'Подтверждение email',
      emailVerificationHint: 'Требовать подтверждение email при регистрации',
      emailSuffixWhitelist: 'Белый список доменов email',
      emailSuffixWhitelistHint:
        "Регистрация разрешена только с адресов указанных доменов; оставьте пустым для отказа от ограничений (например, {'@'}qq.com, {'@'}gmail.com, *.edu.cn)",
      emailSuffixWhitelistPlaceholder: "{'@'}example.com, *.edu.cn",
      emailSuffixWhitelistInputHint:
        'Оставьте пустым для отказа от ограничений. Используйте *.edu.cn, чтобы охватить edu.cn и его поддомены.',
      emailDomainQuota: 'Квота для доменов вне белого списка',
      emailDomainQuotaHint:
        'Когда функция включена и белый список не пуст, с каждого остального регистрируемого домена можно создать одну учётную запись. Когда она отключена, домены вне белого списка отклоняются. Не действует, пока белый список пуст.',
      emailNormalization: 'Нормализация email',
      emailNormalizationHint:
        'Когда функция включена, при регистрации и смене email у всех доменов отбрасываются +теги, варианты Gmail/Googlemail с точками и завершающая точка домена считаются одним и тем же адресом, а проверка идёт по всем зарегистрированным учётным записям. Уже сохранённые адреса не меняются.',
      userEmailChange: 'Разрешить пользователям менять email',
      userEmailChangeHint:
        'Разрешить пользователям с привязанным email заменять основной адрес с помощью кода подтверждения и текущего пароля. Первичная привязка email остаётся доступной и при отключённой функции.',
      promoCode: 'Промокод',
      promoCodeHint: 'Разрешить пользователям применять промокоды при регистрации',
      invitationCode: 'Регистрация по коду приглашения',
      invitationCodeHint:
        'Когда функция включена, для регистрации нужно ввести действительный код приглашения',
      passwordReset: 'Сброс пароля',
      passwordResetHint: 'Разрешить пользователям сбрасывать пароль по email',
      frontendUrl: 'URL фронтенда',
      frontendUrlPlaceholder: 'https://example.com',
      frontendUrlHint:
        'Используется в ссылках в письмах и в OAuth для MCP, например https://example.com. Смена источника сайта требует повторного подключения MCP-приложений.',
      totp: 'Двухфакторная аутентификация (2FA)',
      totpHint:
        'Разрешить пользователям использовать приложения-аутентификаторы, например Google Authenticator',
      totpKeyNotConfigured:
        'Сначала задайте TOTP_ENCRYPTION_KEY в переменных окружения. Создать ключ: openssl rand -hex 32',
    },
    security: {
      stepUp: 'Подтверждение 2FA для чувствительных операций',
      stepUpHint:
        'Когда функция включена, для чувствительных операций (экспорт провайдеров и прокси, создание и скачивание резервных копий, изменение настроек S3, назначение администраторов) требуется недавнее подтверждение по TOTP (действует 15 минут). Перед включением у вашей учётной записи должна быть включена 2FA; для отключения также требуется подтверждение.',
      stepUpEnableRequiresTotp:
        'Включите 2FA (TOTP) для своей учётной записи в профиле, прежде чем включать подтверждение чувствительных операций.',
      sessionBinding: 'Привязка сессии к IP и User-Agent',
      sessionBindingHint:
        'Привязывает сессии входа к IP-адресу и User-Agent клиента. Любое изменение немедленно делает сессию недействительной и требует повторного входа, что усложняет повторное использование украденных учётных данных.',
      auditRetention: 'Хранение журнала аудита (дни)',
      auditRetentionHint:
        'Записи аудита старше этого срока удаляются автоматически. Укажите 0, чтобы хранить их бессрочно (только ручная очистка).',
    },
    panelRateLimit: {
      title: 'Ограничение частоты запросов к API панели',
      description:
        'Ограничивает частоту запросов к API панели, чтобы частое опрашивание (статистика использования, запросы дашборда) не перегружало базу данных',
      proxySafeNote:
        'Запросы к защищённым эндпоинтам считаются по учётной записи пользователя, независимо от исходного IP, поэтому обратные прокси и общий NAT никогда не блокируются ошибочно. Запросы к публичным эндпоинтам считаются по реальному IP клиента, а петлевые и частные адреса (внутренние переходы прокси) автоматически пропускаются.',
      enabled: 'Включить ограничение частоты для панели',
      enabledHint:
        'Ограничивает защищённые эндпоинты панели на пользователя. Запросы сверх порога получают HTTP 429 и восстанавливаются автоматически при сбросе окна.',
      userRpm: 'Запросов на пользователя',
      userRpmHint:
        'Общее число запросов к API панели на пользователя в минуту. Обычная работа в интерфейсе далека от этого значения. 0 = без ограничений.',
      heavyRpm: 'Тяжёлых запросов на пользователя',
      heavyRpmHint:
        'Число агрегирующих запросов об использовании и дашборде на пользователя в минуту (они наиболее затратны для базы данных). 0 = без ограничений.',
      publicIpRpm: 'Публичных эндпоинтов на IP',
      publicIpRpmHint:
        'Число запросов в минуту на реальный IP клиента для эндпоинтов без аутентификации (например, публичных настроек сайта). 0 = без ограничений.',
      perMinute: 'запросов/мин',
      exemptAdmin: 'Исключить администраторов',
      exemptAdminHint:
        'Когда функция включена, учётные записи администраторов не подпадают под ограничения панели, поэтому массовые операции не тормозятся.',
      saveFailed: 'Не удалось сохранить настройки ограничения частоты для панели',
    },
    turnstile: {
      title: 'Cloudflare Turnstile',
      description: 'Защита от ботов при входе и регистрации',
      enableTurnstile: 'Включить Turnstile',
      enableTurnstileHint: 'Требовать проверку Cloudflare Turnstile',
      siteKey: 'Site Key',
      secretKey: 'Secret Key',
      siteKeyHint: 'Получите это значение в панели Cloudflare',
      cloudflareDashboard: 'Панель Cloudflare',
      secretKeyHint: 'Ключ для проверки на сервере (храните его в секрете)',
      secretKeyConfiguredHint:
        'Секретный ключ задан. Оставьте поле пустым, чтобы сохранить текущее значение.',
    },
    captcha: {
      title: 'CAPTCHA',
      description: 'Защита от ботов при входе и регистрации',
      enable: 'Включить CAPTCHA',
      enableHint: 'Требовать проверку человека при входе, регистрации и в связанных сценариях',
      provider: 'Провайдер',
      providerTurnstile: 'Cloudflare Turnstile',
      providerTencent: 'Tencent Captcha',
      providerAliyun: 'Aliyun Captcha 2.0',
    },
    tencentCaptcha: {
      title: 'Tencent Captcha',
      description:
        'Защита слайдер-капчей при входе, регистрации и создании учётных записей через сторонние сервисы',
      enable: 'Включить Tencent Captcha',
      enableHint: 'Использовать слайдер-капчу Tencent во всех существующих сценариях Turnstile',
      keepExisting: 'Оставьте пустым, чтобы сохранить текущее значение',
      configured: 'Настроено. Оставьте пустым, чтобы сохранить текущее значение.',
      required: 'Обязательно перед включением.',
      mutualExclusion:
        'Tencent Captcha, Cloudflare Turnstile и Aliyun Captcha взаимоисключающие. Включение одной отключает остальные.',
      region: 'Площадка сервиса',
      regionCn: 'Материковый Китай',
      regionIntl: 'Международная',
      regionHint:
        'Определяет скрипт SDK и эндпоинт проверки на сервере. Он должен соответствовать площадке, где выдан ваш CaptchaAppId; международные приложения создаются в консоли tencentcloud.com.',
      appCredentialsTitle: 'Учётные данные приложения Captcha',
      appCredentialsHint:
        'Получите CaptchaAppId и AppSecretKey в разделе управления проверками в консоли Captcha.',
      cloudCredentialsTitle: 'Учётные данные Cloud API',
      cloudCredentialsHint:
        'SecretId и SecretKey авторизуют серверные запросы DescribeCaptchaResult.',
      appId: 'CaptchaAppId',
      appSecretKey: 'AppSecretKey',
      cloudSecretId: 'Tencent Cloud SecretId',
      cloudSecretKey: 'Tencent Cloud SecretKey',
      camPermissionHint:
        'Создайте CAM-субпользователя с правами QcloudCaptchaFullAccess вместо постоянных учётных данных root-аккаунта.',
      aidEncryptedHint:
        'aidEncrypted пока не поддерживается. Оставьте обязательную проверку CaptchaAppId отключённой в консоли Captcha.',
      openCaptchaConsole: 'Открыть консоль Captcha',
      createCloudKeys: 'Создать SecretId / SecretKey',
      openWebDocs: 'Открыть руководство по веб-интеграции',
    },
    aliyunCaptcha: {
      accessKeyId: 'AccessKey ID',
      accessKeyIdHint:
        'AccessKey ID Alibaba Cloud для проверки на сервере; рекомендуется RAM-пользователь только с правами captcha',
      accessKeySecret: 'AccessKey Secret',
      accessKeySecretHint: 'Секрет для проверки на сервере (храните его в секрете)',
      accessKeySecretConfiguredHint:
        'Секрет задан. Оставьте поле пустым, чтобы сохранить текущее значение.',
      sceneId: 'Scene ID',
      sceneIdHint:
        'Создайте сцену проверки в консоли Alibaba Cloud Captcha; тип капчи (невидимая, слайдер, пазл) настраивается там для каждой сцены',
      prefix: 'Префикс',
      prefixHint: 'Указан в информации об экземпляре на странице обзора в консоли',
      region: 'Регион',
      regionCn: 'Материковый Китай',
      regionSgp: 'Сингапур',
      regionHint:
        'Определяет регион фронтенд-скрипта и серверный эндпоинт; должен совпадать с регионом вашего экземпляра капчи',
    },
    apiKeyAcl: {
      title: 'Контроль доступа к API-ключам по IP',
      description:
        'Определяет, какой IP клиента используется для списков разрешённых и запрещённых адресов API-ключей, журналов аудита администратора и привязки сессии к IP и User-Agent',
      trustForwardedIp: 'Доверять переданному IP клиента',
      trustForwardedIpHint:
        'Включено по умолчанию для совместимости при обновлении. Когда функция включена, необработанные значения CF-Connecting-IP, X-Real-IP или X-Forwarded-For переопределяют server.trusted_proxies при определении IP клиента. Отключите её, чтобы использовать цепочку доверенных прокси Gin, заданную в server.trusted_proxies. Включайте режим переопределения только если источник недоступен напрямую. Переключение меняет отпечатки IP существующих сессий.',
      forwardedClientIpHeaders: 'Свои заголовки с IP клиента',
      forwardedClientIpHeadersHint:
        'Добавьте имена заголовков CDN или прокси, которые нужно проверять раньше встроенных.',
      forwardedClientIpHeadersPlaceholder: 'X-Client-IP',
      forwardedClientIpHeadersRiskHint:
        'Эти необработанные заголовки можно подделать, если источник доступен напрямую. Ограничьте доступ к источнику, прежде чем доверять им.',
      forwardedClientIpHeaderInvalid: 'Введите корректное имя HTTP-заголовка.',
      forwardedClientIpHeadersLimit: 'Допускается не более {max} своих заголовков с IP клиента.',
      removeForwardedClientIpHeader: 'Удалить {header}',
    },
    linuxdo: {
      title: 'Вход через LinuxDo Connect',
      description: 'Настройка OAuth LinuxDo Connect для входа пользователей в TokenRouter',
      enable: 'Включить вход через LinuxDo',
      enableHint: 'Показывать вход через LinuxDo на страницах входа и регистрации',
      clientId: 'Client ID',
      clientIdPlaceholder: 'например, hprJ5pC3...',
      clientIdHint: 'Получите это значение в Connect.Linux.Do',
      clientSecret: 'Client Secret',
      clientSecretPlaceholder: '********',
      clientSecretHint: 'Используется серверной частью для обмена токенов (храните его в секрете)',
      clientSecretConfiguredPlaceholder: '********',
      clientSecretConfiguredHint:
        'Секрет задан. Оставьте поле пустым, чтобы сохранить текущее значение.',
      redirectUrl: 'URL перенаправления',
      redirectUrlPlaceholder: 'https://your-domain.com/api/v1/auth/oauth/linuxdo/callback',
      redirectUrlHint:
        'Должен совпадать с URL перенаправления, настроенным в Connect.Linux.Do (должен быть абсолютным URL http(s))',
      quickSetCopy: 'Создать и скопировать (текущий сайт)',
      redirectUrlSetAndCopied: 'URL перенаправления создан и скопирован в буфер обмена',
    },
    dingtalk: {
      title: 'Вход через DingTalk',
      description: 'Настройка OAuth DingTalk для входа пользователей в TokenRouter',
      enable: 'Включить вход через DingTalk (внутреннее корпоративное приложение)',
      enableHint: 'Показывать вход через DingTalk на страницах входа и регистрации',
      clientId: 'Client ID (AppKey)',
      clientIdPlaceholder: 'например, dingxxxxxxxxxxxxxxxx',
      clientIdHint: 'Получите это значение на странице приложения в открытой платформе DingTalk',
      clientSecret: 'Client Secret (AppSecret)',
      clientSecretPlaceholder: '********',
      clientSecretHint: 'Используется серверной частью для обмена токенов (храните его в секрете)',
      clientSecretConfiguredPlaceholder: '********',
      clientSecretConfiguredHint:
        'Секрет задан. Оставьте поле пустым, чтобы сохранить текущее значение.',
      redirectUrl: 'URL перенаправления',
      redirectUrlPlaceholder: 'https://your-domain.com/api/v1/auth/oauth/dingtalk/callback',
      redirectUrlHint:
        'Должен совпадать с URL перенаправления, настроенным в открытой платформе DingTalk (должен быть абсолютным URL http(s))',
      corpPolicy: {
        label: 'Политика ограничения по организациям',
        hint: 'Определяет, каким учётным записям DingTalk (организациям) разрешён вход',
        none: 'Без ограничений (разрешены все учётные записи DingTalk)',
        internalOnly: 'Только внутри организации (одна организация)',
      },
      bypassRegistration: 'Разрешить регистрацию через DingTalk',
      bypassRegistrationHint:
        'Разрешить новым пользователям регистрироваться через DingTalk, даже когда публичная регистрация отключена.',
      syncDisplayName: 'Синхронизировать отображаемое имя DingTalk',
      syncDisplayNameHint:
        'Перезаписывать имя пользователя именем сотрудника из DingTalk при каждом входе (также сохраняется в атрибут dingtalk_name).',
      syncCorpEmail: 'Синхронизировать корпоративный email',
      syncCorpEmailHint:
        'Записывать корпоративный email из DingTalk в атрибут dingtalk_email при каждом входе (email для входа не меняется).',
      syncCorpEmailPermissionHint:
        'Требуется, чтобы приложению на открытой платформе DingTalk было выдано разрешение OAPI «Личная информация, включая email (fieldEmail)», иначе OAPI не вернёт поле email.',
      syncDept: 'Синхронизировать подразделение',
      syncDeptHint:
        'Записывать полный путь подразделения из DingTalk в атрибут dingtalk_department при каждом входе (запрашивается заново каждый раз).',
      syncDeptPermissionHint:
        'Требуется, чтобы приложению на открытой платформе DingTalk было выдано разрешение OAPI «Чтение информации о подразделениях (qyapi_get_department_list)», иначе путь подразделения определить не удастся.',
      syncDisplayNameTarget: 'Ключ атрибута',
      syncDisplayNameTargetHint:
        'По умолчанию dingtalk_name / DingTalk Name. При сохранении настроек пользовательский атрибут создаётся автоматически по указанным выше ключу и отображаемому имени (у существующего определения синхронизируется только отображаемое имя).',
      syncCorpEmailTarget: 'Ключ атрибута',
      syncCorpEmailTargetHint:
        'По умолчанию dingtalk_email / DingTalk Corporate Email. При сохранении настроек пользовательский атрибут создаётся автоматически по указанным выше ключу и отображаемому имени (у существующего определения синхронизируется только отображаемое имя).',
      syncDeptTarget: 'Ключ атрибута',
      syncDeptTargetHint:
        'По умолчанию dingtalk_department / DingTalk Department. При сохранении настроек пользовательский атрибут создаётся автоматически по указанным выше ключу и отображаемому имени (у существующего определения синхронизируется только отображаемое имя).',
      syncAttrDisplayName: 'Отображаемое имя',
    },
    oidc: {
      title: 'Вход через OIDC',
      description: 'Настройка стандартного провайдера OIDC (например, Keycloak)',
      enable: 'Включить вход через OIDC',
      enableHint: 'Показывать вход через OIDC на страницах входа и регистрации',
      providerName: 'Название провайдера',
      providerNamePlaceholder: 'например, Keycloak',
      clientId: 'Client ID',
      clientIdPlaceholder: 'идентификатор клиента OIDC',
      clientSecret: 'Client Secret',
      clientSecretPlaceholder: '********',
      clientSecretHint: 'Используется серверной частью для обмена токенов (храните его в секрете)',
      clientSecretConfiguredPlaceholder: '********',
      clientSecretConfiguredHint:
        'Секрет задан. Оставьте поле пустым, чтобы сохранить текущее значение.',
      issuerUrl: 'URL издателя',
      issuerUrlPlaceholder: 'https://id.example.com/realms/main',
      discoveryUrl: 'URL обнаружения',
      discoveryUrlPlaceholder:
        'Необязательно, оставьте пустым для автоматического определения по издателю',
      authorizeUrl: 'URL авторизации',
      authorizeUrlPlaceholder: 'Необязательно, может быть определён автоматически',
      tokenUrl: 'URL токена',
      tokenUrlPlaceholder: 'Необязательно, может быть определён автоматически',
      userinfoUrl: 'URL UserInfo',
      userinfoUrlPlaceholder: 'Необязательно, может быть определён автоматически',
      jwksUrl: 'URL JWKS',
      jwksUrlPlaceholder: 'Необязательно, требуется при включённой строгой проверке ID-токена',
      scopes: 'Области доступа',
      scopesPlaceholder: 'openid email profile',
      scopesHint: 'Обязательно должен быть указан openid',
      redirectUrl: 'URL перенаправления серверной части',
      redirectUrlPlaceholder: 'https://your-domain.com/api/v1/auth/oauth/oidc/callback',
      redirectUrlHint: 'Должен совпадать с URL обратного вызова, настроенным у провайдера OIDC',
      quickSetCopy: 'Создать и скопировать (текущий сайт)',
      redirectUrlSetAndCopied: 'URL перенаправления создан и скопирован в буфер обмена',
      frontendRedirectUrl: 'Путь обратного вызова во фронтенде',
      frontendRedirectUrlPlaceholder: '/auth/oidc/callback',
      frontendRedirectUrlHint:
        'Маршрут фронтенда, используемый после обратного вызова серверной части',
      tokenAuthMethod: 'Метод аутентификации токена',
      clockSkewSeconds: 'Расхождение часов (секунды)',
      allowedSigningAlgs: 'Разрешённые алгоритмы подписи',
      allowedSigningAlgsPlaceholder: 'RS256,ES256,PS256',
      usePkce: 'Использовать PKCE',
      validateIdToken: 'Проверять ID-токен',
      requireEmailVerified: 'Требовать подтверждённый email',
      userinfoEmailPath: 'Путь к email в UserInfo',
      userinfoEmailPathPlaceholder: 'например, data.email',
      userinfoIdPath: 'Путь к ID в UserInfo',
      userinfoIdPathPlaceholder: 'например, data.id',
      userinfoUsernamePath: 'Путь к имени пользователя в UserInfo',
      userinfoUsernamePathPlaceholder: 'например, data.username',
    },
    defaults: {
      title: 'Настройки пользователей по умолчанию',
      description: 'Значения по умолчанию для новых пользователей',
      defaultBalance: 'Баланс по умолчанию',
      defaultBalanceHint: 'Начальный баланс новых пользователей',
      defaultConcurrency: 'Параллелизм по умолчанию',
      defaultConcurrencyHint: 'Максимум одновременных запросов для новых пользователей',
      defaultUserRpmLimit: 'Лимит запросов в минуту по умолчанию',
      defaultUserRpmLimitHint:
        'Максимум запросов в минуту для новых пользователей по умолчанию; 0 = без ограничений. Применяется только при создании пользователя.',
      defaultUserApiKeyLimit: 'Лимит API-ключей по умолчанию',
      defaultUserApiKeyLimitHint:
        'Сколько API-ключей новые пользователи могут создать по умолчанию; 0 = без ограничений. Применяется только при создании пользователя.',
      defaultUserApiKeyLimitInvalid:
        'Лимит API-ключей по умолчанию должен быть целым числом от 0 до 2 147 483 647',
      defaultSubscriptions: 'Подписки по умолчанию',
      defaultSubscriptionsHint:
        'Автоматически назначать эти подписки при создании или регистрации нового пользователя',
      addDefaultSubscription: 'Добавить подписку по умолчанию',
      defaultSubscriptionsEmpty: 'Подписки по умолчанию не настроены.',
      defaultSubscriptionsDuplicate:
        'Дублирующийся тарифный план: {planId}. Каждый план может быть указан только один раз.',
      subscriptionGroup: 'Тарифный план',
      subscriptionValidityDays: 'Срок действия (дни)',
    },
    balanceDisplay: {
      title: 'Отображение баланса',
      description:
        'Настройка названия, символа и значка внутренней единицы баланса без изменения внешних валют оплаты.',
      unitName: 'Название единицы',
      unitNamePlaceholder: 'USD',
      unitNameHint: 'Отображается как название единицы баланса, например USD, Credits или Points.',
      unitSymbol: 'Символ единицы',
      unitSymbolPlaceholder: '$',
      unitSymbolHint: 'Отображается как префикс суммы, например $, ¥ или C.',
      reasoningPointRmbUnitPrice: 'Цена 1 {unitName} в юанях',
      reasoningPointRmbUnitPricePlaceholder: 'например, 1',
      reasoningPointRmbUnitPriceHint:
        'Используется для отображения скидок на маркетплейсе. Стоимость 1 {unitName} в юанях.',
      usdExchangeRate: 'Курс доллара',
      usdExchangeRatePlaceholder: 'например, 7.2',
      usdExchangeRateHint:
        'Используется для отображения скидок на маркетплейсе. Юаней за 1 доллар США.',
      iconSvg: 'SVG-значок баланса',
      iconHint:
        'Загрузите свой SVG-значок. Оставьте пустым, чтобы использовать значок по умолчанию.',
      uploadSvg: 'Загрузить SVG',
      removeSvg: 'Удалить SVG',
      previewLabel: 'Предпросмотр',
      previewUnit: 'Единица баланса: {unitName}',
      previewHint:
        'Этот предпросмотр применяется к отображению внутреннего баланса, квоты и стоимости использования по всему сайту.',
    },
    claudeCode: {
      title: 'Настройки Claude Code',
      description: 'Управление требованиями к версии клиента Claude Code',
      minVersion: 'Минимальная версия',
      minVersionPlaceholder: 'например, 2.1.63',
      minVersionHint:
        'Отклонять клиенты Claude Code ниже этой версии (формат semver). Оставьте пустым, чтобы отключить проверку версии.',
      maxVersion: 'Максимальная версия',
      maxVersionPlaceholder: 'например, 2.5.0',
      maxVersionHint:
        'Отклонять клиенты Claude Code выше этой версии (формат semver). Оставьте пустым, чтобы разрешить любую версию.',
    },
    scheduling: {
      title: 'Настройки планирования на шлюзе',
      description: 'Управление поведением планировщика API-ключей',
      providerSchedulingThresholdsTitle: 'Пороги автопаузы для провайдеров платформ',
      providerSchedulingThresholdsDescription:
        'Когда текущее окно использования провайдера (сессия OpenAI Codex/Anthropic или утилизация запросов и токенов Grok) достигает этого процента, он временно исключается из планирования до сброса окна. Укажите 100, чтобы отключить.',
      providerSchedulingThresholdsGlobalHint:
        'Значение по умолчанию для всей системы для каждого провайдера этой платформы. Отдельные провайдеры могут переопределить его в редакторе провайдера.',
      providerSchedulingThresholdsDisabledHint:
        'Значение 100 отключает автопаузу для платформы. Значения от 1 до 99 приостанавливают планирование, когда утилизация достигает этого процента.',
      providerSchedulingThresholdsRangeHint:
        'Целое число от 1 до 100 (проценты). Только для OpenAI, Anthropic и Grok.',
      advancedTitle: 'Общий расширенный планировщик',
      advancedDescription:
        'Расширенное планирование включается для каждой группы отдельно. Оно использует общую оценку кандидатов, взвешенный выбор Top-K и обратную связь по частоте ошибок и времени до первого токена во время работы. Базовые группы продолжают использовать прежний планировщик.',
      stickyWeightedTitle: 'Взвешивание привязки',
      stickyWeightedDescription:
        'Когда функция включена, привязка к предыдущему ответу и к сессии учитывается как сигнал оценки; когда отключена, сохраняется прежнее поведение с приоритетом привязки.',
      subscriptionPriorityTitle: 'Приоритет подписки',
      subscriptionPriorityDescription:
        'Провайдеры с возможностями подписки получают оценку приоритета подписки; провайдеры без этого сигнала остаются нейтральными.',
      stickyEscapeTitle: 'Аварийная смена привязки',
      stickyEscapeDescription:
        'Когда привязанный провайдер превышает порог частоты ошибок или времени до первого токена, этот запрос может переключиться на другого провайдера с сохранением исходной привязки.',
      stickyEscapeEnabled: 'Включить принудительную смену привязки',
      stickyEscapeTTFT: 'Порог переключения по времени до первого токена (мс)',
      stickyEscapeErrorRate: 'Порог переключения по частоте ошибок (0–1)',
      ewmaTitle: 'Коэффициенты обратной связи EWMA',
      ewmaDescription:
        'Определяют, насколько сильно обратная связь по частоте ошибок и времени до первого токена реагирует на последний замер; большие значения реагируют быстрее.',
      ewmaErrorRateAlpha: 'Коэффициент EWMA для частоты ошибок',
      ewmaTTFTAlpha: 'Коэффициент EWMA для времени до первого токена',
      weightsTitle: 'Веса расширенного планировщика',
      weightsDescription:
        'Пустые значения используют конфигурацию или встроенные значения по умолчанию. Заданные значения имеют приоритет и применяются ко всем расширенным группам.',
      defaultPlaceholder: 'конфигурация/по умолчанию: {value}',
      topKLabel: 'Top-K',
      priorityWeight: 'Приоритет',
      loadWeight: 'Нагрузка',
      queueWeight: 'Очередь',
      errorRateWeight: 'Частота ошибок',
      ttftWeight: 'Время до первого токена',
      resetWeight: 'Сброс окна',
      quotaHeadroomWeight: 'Запас квоты',
      previousResponseWeight: 'Привязка к предыдущему ответу',
      sessionStickyWeight: 'Привязка к сессии',
      advancedHelp: {
        trigger: 'Как работает оценка и выбор в расширенном планировщике',
        title: 'Как работает расширенное планирование',
        summary:
          'Сначала запрос проходит жёсткие фильтры, затем оставшиеся кандидаты-провайдеры получают оценки; кандидаты с наивысшими оценками попадают в пул Top-K.',
        formula: 'Оценка = нормализованные сигналы × веса + дополнительные бонусы за привязку',
        hardFilterTitle: 'Жёсткие фильтры:',
        hardFilter:
          'Исключаются провайдеры, не прошедшие по ограничениям группы, модели и возможности, статуса провайдера, лимита частоты, прокси или слота параллелизма.',
        scoreTitle: 'Оценка кандидатов:',
        score:
          'Приоритет, меньшая нагрузка, более короткая очередь, меньшая частота ошибок, меньшее время до первого токена, сброс окна и запас квоты учитываются совместно; недоступные необязательные сигналы остаются нейтральными.',
        feedbackTitle: 'Обратная связь по работе:',
        feedback:
          'Результаты запросов и время до первого токена обновляют сглаженные показатели провайдеров, поэтому последующие запросы могут обходить провайдеров, которые недавно работали плохо.',
        selectionTitle: 'Выбор из Top-K:',
        selection:
          'Сохраняются кандидаты Top-K с наивысшими оценками, затем выбор идёт по весу оценки, чтобы избежать долгосрочной концентрации на одном провайдере. Взвешивание привязки даёт дополнительное предпочтение провайдерам предыдущего ответа и сессии.',
        boundary:
          'Каждая попытка заново проверяет жёсткие ограничения. Резервная группа планирует запрос заново для своей целевой группы, а провайдер никогда не меняется после начала потока ответа.',
      },
    },
    openaiScheduling: {
      title: 'Настройки планирования OpenAI',
      description: 'Управление автоматической паузой по квоте для провайдеров OpenAI',
    },
    ollamaCloudUsage: {
      title: 'Обновление использования Ollama Cloud',
      description:
        'Обновление использования с официальной страницы настроек Ollama по запросам к моделям для провайдеров, которые включили это индивидуально. По умолчанию отключено. Простаивающие провайдеры не опрашиваются.',
      enabled: 'Включить глобальное автоматическое обновление',
      enabledHint:
        'Обновляются только провайдеры с сохранённой сессией браузера и собственным включённым автоматическим обновлением, и только после последующих запросов к моделям. Ручное обновление остаётся доступным.',
      intervalMinutes: 'Максимальное ожидание при непрерывных запросах (минуты)',
      intervalHint:
        'Диапазон: от 15 до 1440 минут. Если непрерывные запросы постоянно сдвигают задержку, принудительно обновить после этого ожидания.',
      debounceMinutes: 'Период тишины после последнего запроса (минуты)',
      debounceHint:
        'Диапазон: от 1 до 60 минут. Обновление выполняется, когда после последнего запроса к модели не было активности указанное время.',
      saveFailed: 'Не удалось сохранить настройки обновления использования Ollama Cloud',
    },
    gatewayForwarding: {
      anthropicTitle: 'Передача запросов Anthropic',
      anthropicDescription:
        'Управление идентичностью, промптами и работой с кешем для запросов Anthropic OAuth и Setup Token',
      openaiTitle: 'Передача запросов OpenAI / Codex',
      openaiDescription:
        'Управление идентичностью на стороне OpenAI Codex и совместимостью доступа клиентов',
      grokTitle: 'Передача запросов Grok',
      grokDescription:
        'Управление моделью Grok по умолчанию, сопоставлением между клиентами и регионом вышестоящей стороны',
      antigravityTitle: 'Передача запросов Antigravity',
      antigravityDescription: 'Управление идентичностью клиента для запросов к Antigravity',
      grokDefaultTextModel: 'Текстовая модель Grok по умолчанию',
      grokDefaultTextModelHint:
        'Используется только когда эндпоинт Grok допускает пропуск модели. Явно указанные идентификаторы моделей передаются без изменений.',
      grokDefaultBaseURLMode: 'Адрес Grok по умолчанию',
      grokDefaultBaseURLModeHint:
        'Используется, когда у провайдера Grok не задан явный базовый URL. Эндпоинты медиа и голоса по-прежнему используют свои официальные хосты API.',
      grokBaseURLModeCLI: 'Прокси чата CLI',
      grokBaseURLModeAPI: 'Публичный API',
      grokBaseURLModeUSEast1: 'Региональный API (us-east-1)',
      grokBaseURLModeUSWest2: 'Региональный API (us-west-2)',
      grokBaseURLModeEUWest1: 'Региональный API (eu-west-1)',
      ttftMode: 'Метрика первого вывода или первого события',
      ttftModeFirstEvent: 'Первое событие от вышестоящей стороны',
      ttftModeFirstOutput: 'Первый полезный вывод',
      ttftModeHint:
        'Применяется ко всем текстовым стриминговым платформам и протоколам и отсчитывается от начала попытки обращения к вышестоящей стороне. Изменения влияют на новые запросы; TPS всегда считается по интервалу полезного содержимого.',
      fingerprintUnification: 'Унификация отпечатков',
      fingerprintUnificationHint:
        'Приводит заголовки X-Stainless-* к единому виду у пользователей одного и того же OAuth-провайдера. При отключении передаются исходные заголовки каждого клиента.',
      metadataPassthrough: 'Передача метаданных',
      metadataPassthroughHint:
        'Передавать исходное значение metadata.user_id клиента без перезаписи. Может повысить долю попаданий в кеш на вышестоящей стороне.',
      claudeOAuthSystemPromptInjection: 'Инъекция системного промпта для Claude OAuth',
      claudeOAuthSystemPromptInjectionHint:
        'Подставлять системные блоки Claude Code, когда клиенты, отличные от Claude Code, используют Claude OAuth. При отключении системный промпт клиента сохраняется.',
      claudeOAuthSystemPrompt: 'Системный промпт расширения',
      claudeOAuthSystemPromptPlaceholder:
        'Оставьте пустым, чтобы использовать встроенный промпт расширения Claude Code',
      claudeOAuthSystemPromptHint:
        "Ссылайтесь на него из JSON блоков через {'{'}claude_code_expansion_prompt{'}'}.",
      claudeOAuthSystemPromptBlocks: 'JSON системных блоков',
      claudeOAuthSystemPromptBlocksPlaceholder: `{'{"blocks":[{"type":"text","text":"{billing_header}"},{"type":"text","text":"{claude_code_system_prompt}"},{"type":"text","text":"{claude_code_expansion_prompt}","cache_control":true}]}'}`,
      claudeOAuthSystemPromptBlocksHint:
        "Поддерживаются {'{'}billing_header{'}'}, {'{'}cc_version{'}'}, {'{'}fp{'}'}, {'{'}claude_code_system_prompt{'}'} и {'{'}claude_code_expansion_prompt{'}'}; оставьте пустым для встроенных трёх блоков.",
      anthropicCacheTTL1hInjection: 'Подстановка TTL кеша Anthropic',
      anthropicCacheTTL1hInjectionHint:
        'Когда функция включена, существующие блоки ephemeral cache_control в телах запросов Anthropic OAuth и Setup Token принудительно переводятся на 1 ч; использование в ответе по умолчанию тарифицируется как 5 мин, но переопределение тарификации TTL на уровне провайдера имеет приоритет.',
      rewriteMessageCacheControl: 'Перезаписывать точки кеширования сообщений',
      rewriteMessageCacheControlHint:
        'По умолчанию выключено: cache_control клиента в блоках содержимого сообщений сохраняется. Когда функция включена, точки клиента удаляются, а вместо них подставляются точки прокси — для клиентов, которые сами не управляют кешированием.',
      clientDatelineNormalization: 'Нормализация даты в клиентских запросах',
      clientDatelineNormalizationHint:
        "По умолчанию включено. Приводит фразу «Today's date is …» в запросах Anthropic OAuth и Setup Token обратно к каноническому формату даты с прямым апострофом и дефисами, убирая стеганографические биты отпечатка, которые некоторые клиенты добавляют, обнаружив неофициальный базовый URL. Применяется только к системным промптам и блокам <system-reminder>; провайдеры с API-ключом не затрагиваются.",
      antigravityUserAgentVersion: 'Версия UA Antigravity',
      antigravityUserAgentVersionPlaceholder: '1.23.2',
      antigravityUserAgentVersionHint:
        'Оставьте пустым, чтобы использовать ANTIGRAVITY_USER_AGENT_VERSION или встроенное значение по умолчанию 1.23.2; если значение задано, приоритет у настройки администратора.',
      openaiCodexUserAgent: 'User-Agent OpenAI Codex',
      openaiCodexUserAgentPlaceholder:
        'codex-tui/0.144.1 (Ubuntu 22.4.0; x86_64) xterm-256color (codex-tui; 0.144.1)',
      openaiCodexUserAgentHint:
        'Используется для обхода проверок User-Agent браузера в Cloudflare на стороне OpenAI. Применяется только когда User-Agent клиента определён как браузерный (Mozilla/...). Оставьте пустым, чтобы использовать встроенное значение codex-tui.',
      openaiAllowClaudeCodeCodexPlugin: 'Разрешить плагин Codex в Claude Code',
      openaiAllowClaudeCodeCodexPluginDesc:
        'Глобальный переключатель; влияет только на провайдеров OpenAI OAuth с включённой опцией «Только официальные клиенты Codex». Когда он включён, все такие провайдеры дополнительно принимают запросы от плагина Codex в Claude Code (точное совпадение originator=Claude Code) без отдельной настройки для каждого провайдера; запросы к вышестоящей стороне передаются без изменений.',
    },
    userPromptReplacement: {
      title: 'Замена пользовательских промптов',
      description:
        'Глобально сопоставлять текст сообщений пользователя в поддерживаемых запросах шлюза с помощью регулярных выражений и заменять его статическим значением или динамическим значением на основе часового пояса. Сюда входят запросы OpenAI Responses WebSocket.',
      addRule: 'Добавить правило',
      resetDefault: 'Восстановить значения по умолчанию',
      empty: 'Правила замены не настроены',
      namePlaceholder: 'Название правила',
      pattern: 'Регулярное выражение для поиска',
      patternPlaceholder:
        '(?s)(<environment_context\\b[^>]*>.*?<timezone>)([^<]*)(</timezone>.*?</environment_context>)',
      targetGroup: 'Целевая группа',
      replacementType: 'Тип замены',
      typeStatic: 'Статический текст',
      typeTimezoneName: 'Название часового пояса',
      typeCurrentTime: 'Текущее время',
      staticText: 'Статический текст',
      staticTextPlaceholder: 'Текст замены',
      timezone: 'Целевой часовой пояс',
      useTimezone: 'Использовать часовой пояс',
      timeFormat: 'Формат времени',
    },
    webSearchEmulation: {
      title: 'Эмуляция веб-поиска',
      description:
        'Добавляет возможность веб-поиска провайдерам Anthropic с API-ключом, которые не поддерживают её изначально',
      enabled: 'Включить эмуляцию веб-поиска',
      enabledHint:
        'Глобальный переключатель. Когда он отключён, эмуляция веб-поиска не работает ни для одного канала и провайдера.',
      providers: 'Провайдеры поиска',
      addProvider: 'Добавить провайдера',
      providerType: 'Тип провайдера',
      apiKey: 'API-ключ',
      apiKeyPlaceholder: 'Введите API-ключ',
      apiKeyConfigured: 'Настроен',
      showApiKey: 'Показать',
      hideApiKey: 'Скрыть',
      copyApiKey: 'Скопировать',
      copied: 'Скопировано',
      quotaLimit: 'Лимит квоты',
      quotaLimitHint: 'Оставьте пустым для отсутствия ограничений; если задано, должно быть > 0',
      quotaLimitMustBePositive: 'Лимит квоты должен быть больше 0',
      subscribedAt: 'Дата подписки',
      subscribedAtHint:
        'Квота сбрасывается раз в месяц начиная с этой даты; оставьте пустым, чтобы отключить автоматический сброс',
      quotaUsage: 'Использование',
      resetUsage: 'Сбросить',
      resetUsageConfirm: 'Сбросить счётчик использования для этого провайдера?',
      resetUsageSuccess: 'Счётчик использования сброшен',
      proxy: 'Прокси',
      removeProvider: 'Удалить',
      noProviders: 'Провайдеры поиска не настроены',
      test: 'Проверить',
      testDefaultQuery: 'Главные мировые события этого года',
      testing: 'Поиск...',
      testResultTitle: 'Результаты поиска',
      testResultProvider: 'Провайдер',
      testNoResults: 'Ничего не найдено',
    },
    site: {
      title: 'Настройки сайта',
      description: 'Настройка оформления сайта',
      backendMode: 'Режим серверной части',
      backendModeDescription:
        'Отключает регистрацию пользователей, публичную часть сайта и функции самообслуживания. Войти и управлять платформой может только администратор.',
      siteName: 'Название сайта',
      siteNamePlaceholder: 'TokenRouter',
      siteNameHint: 'Отображается в письмах и заголовках страниц',
      siteSubtitle: 'Подзаголовок сайта',
      siteSubtitlePlaceholder: 'Subscription to API Conversion Platform',
      siteSubtitleHint: 'Отображается на страницах входа и регистрации',
      siteCopyTitle: 'Тексты сайта',
      siteCopyDescription: 'Настройка локализованных названия, заголовка и подзаголовка сайта.',
      siteNameZh: 'Название сайта (китайский)',
      siteNameZhPlaceholder: 'TokenRouter',
      siteNameEn: 'Название сайта (английский)',
      siteNameEnPlaceholder: 'TokenRouter',
      siteTitleZh: 'Заголовок сайта (китайский)',
      siteTitleZhPlaceholder: '统一的大模型接口',
      siteTitleEn: 'Заголовок сайта (английский)',
      siteTitleEnPlaceholder: 'The unified interface for AI models',
      siteSubtitleZh: 'Подзаголовок сайта (китайский)',
      siteSubtitleZhPlaceholder:
        '无需管理多个模型订阅，一站式接入 Claude、GPT、Gemini 等主流 AI 服务',
      siteSubtitleEn: 'Подзаголовок сайта (английский)',
      siteSubtitleEnPlaceholder:
        'No need to manage multiple subscriptions. Access Claude, GPT, Gemini and more with a single API key',
      apiBaseUrl: 'Базовый URL API',
      apiBaseUrlPlaceholder: 'https://api.example.com',
      apiBaseUrlHint:
        'Используется для функций «Использовать ключ» и «Импорт в CC Switch». Оставьте пустым, чтобы использовать URL текущего сайта.',
      tablePreferencesTitle: 'Глобальные настройки таблиц',
      tablePreferencesDescription:
        'Настройка поведения постраничной навигации по умолчанию для общих таблиц',
      tableDefaultPageSize: 'Строк на странице по умолчанию',
      tableDefaultPageSizeHint: 'Должно быть целым числом от 5 до 1000',
      tablePageSizeOptions: 'Варианты числа строк на странице',
      tablePageSizeOptionsPlaceholder: '10, 20, 50, 100',
      tablePageSizeOptionsHint:
        'Разделяйте целые числа от 5 до 1000 запятыми; при сохранении значения удаляются дубликаты и выполняется сортировка',
      usageRankingLimit: 'Число участников рейтинга использования',
      usageRankingLimitHint:
        'Определяет, сколько пользователей отображается на странице рейтинга использования для выбранного периода, от 1 до 100',
      tableDefaultPageSizeRangeError:
        'Число строк на странице по умолчанию должно быть от {min} до {max}',
      tablePageSizeOptionsFormatError:
        'Неверный формат вариантов. Введите целые числа от {min} до {max}, разделённые запятыми',
      usageRankingLimitRangeError:
        'Число участников рейтинга использования должно быть от {min} до {max}',
      customEndpoints: {
        title: 'Свои эндпоинты',
        description:
          'Добавьте дополнительные URL эндпоинтов API, чтобы пользователи могли быстро скопировать их на странице API-ключей',
        itemLabel: 'Эндпоинт #{n}',
        name: 'Название',
        namePlaceholder: 'например, совместимый с OpenAI',
        endpointUrl: 'URL эндпоинта',
        endpointUrlPlaceholder: 'https://api2.example.com',
        descriptionLabel: 'Описание',
        descriptionPlaceholder: 'например, поддерживает запросы в формате OpenAI',
        add: 'Добавить эндпоинт',
      },
      contactInfo: 'Контактная информация',
      contactInfoPlaceholder: 'например, QQ: 123456789',
      contactInfoHint:
        'Контактные данные поддержки, отображаются в профиле и меню пользователя в шапке.',
      docUrl: 'URL документации',
      docUrlPlaceholder: 'https://docs.example.com',
      docUrlHint:
        'Ссылка на сайт документации. Оставьте пустым, чтобы скрыть ссылку на документацию.',
      siteLogo: 'Логотип сайта',
      uploadImage: 'Загрузить изображение',
      remove: 'Удалить',
      logoHint: 'PNG, JPG или SVG. Не более 300 КБ. Рекомендуется квадратное изображение 80x80 px.',
      logoSizeError: 'Размер изображения превышает предел 300 КБ ({size} КБ)',
      logoTypeError: 'Выберите файл изображения',
      logoReadError: 'Не удалось прочитать файл изображения',
      homeContent: 'Содержимое главной страницы',
      homeContentPlaceholder:
        'Введите своё содержимое для главной страницы. Поддерживаются Markdown и HTML. Если указать URL, он будет показан в iframe.',
      homeContentHint:
        'Настройка содержимого главной страницы. Поддерживаются Markdown и HTML. Если указать URL (начинающийся с http:// или https://), он будет использован как src для iframe и встроит внешнюю страницу. После настройки стандартная информация о состоянии больше не отображается.',
      homeContentIframeWarning:
        'Примечание о режиме iframe: некоторые сайты запрещают встраивание через X-Frame-Options или политики безопасности CSP. Если страница пустая или показывает ошибку, проверьте, разрешает ли целевой сайт встраивание, или соберите собственное содержимое в режиме HTML.',
      hideCcsImportButton: 'Скрыть кнопку импорта в CCS',
      hideCcsImportButtonHint:
        'Когда функция включена, кнопка «Импорт в CCS» скрывается на странице API-ключей',
    },
    purchase: {
      title: 'Страница пополнения и подписки',
      description:
        'Показывать в боковом меню пункт «Пополнение / Подписка» и открывать настроенный URL в iframe',
      enabled: 'Показывать пункт «Пополнение / Подписка»',
      enabledHint: 'Показывать пункт, когда задан URL пополнения и подписки',
      url: 'URL пополнения и подписки',
      urlPlaceholder: 'https://example.com/purchase',
      urlHint: 'Должен быть абсолютным URL http(s)',
      iframeWarning:
        'Примечание об iframe: некоторые сайты запрещают встраивание через X-Frame-Options или CSP (frame-ancestors). Если страница пустая, предусмотрите альтернативу «Открыть в новой вкладке».',
      integrationDoc: 'Документация по интеграции оплаты',
      integrationDocHint: 'Описывает спецификацию эндпоинтов, идемпотентность и примеры кода',
    },
    customMenu: {
      title: 'Свои страницы меню',
      description:
        'Добавьте свои iframe-страницы в навигацию бокового меню. Каждая страница может быть видна обычным пользователям или администраторам.',
      itemLabel: 'Пункт меню #{n}',
      name: 'Название пункта меню',
      namePlaceholder: 'например, справочный центр',
      url: 'URL страницы',
      urlPlaceholder: 'https://example.com/page',
      iconSvg: 'Значок SVG',
      iconSvgPlaceholder: '<svg>...</svg>',
      iconPreview: 'Предпросмотр значка',
      uploadSvg: 'Загрузить SVG',
      removeSvg: 'Удалить',
      visibility: 'Кому видно',
      visibilityUser: 'Обычным пользователям',
      visibilityAdmin: 'Администраторам',
      add: 'Добавить пункт меню',
      remove: 'Удалить',
      moveUp: 'Переместить вверх',
      moveDown: 'Переместить вниз',
    },
    payment: {
      title: 'Настройки оплаты',
      description: 'Настройка параметров платёжной системы',
      configGuide: 'Руководство по настройке',
      enabled: 'Включить оплату',
      enabledHint: 'Включить или отключить платёжную систему',
      enabledPaymentTypes: 'Включённые провайдеры',
      enabledPaymentTypesHint: 'Отключение провайдера также отключит его экземпляры.',
      minAmount: 'Минимальная сумма',
      maxAmount: 'Максимальная сумма',
      dailyLimit: 'Дневной лимит',
      balanceRechargeMultiplier: 'Множитель пополнения баланса',
      balanceRechargeMultiplierHint:
        'Умножает числовую сумму пополнения при расчёте начисляемых {unitName}. Применяется к любой валюте оплаты.',
      balanceRechargePreview:
        'Пример: при пополнении на сумму 1 на баланс зачисляется {amount} {unitName} — в любой валюте оплаты.',
      subscriptionUsdToCnyRate: 'Курс доллара к юаню для подписок',
      subscriptionUsdToCnyRateHint:
        'Сколько юаней списывается за 1 доллар США цены плана на каналах с оплатой в юанях (например, 7.15). 0 или пустое значение = функция отключена, цена плана списывается как есть. Когда функция включена, все цены планов должны быть указаны в долларах США',
      subscriptionUsdToCnyRateDisabled: 'Отключено (цена списывается как есть)',
      rechargeFeeRate: 'Общая ставка комиссии по умолчанию',
      rechargeFeeRateHint:
        'Процент по умолчанию, используемый когда у способа оплаты нет отдельной настройки комиссии; 0 означает отсутствие комиссии',
      rechargeFeePreview: 'Предпросмотр: пополнение 100, комиссия {fee}',
      methodFeesTitle: 'Комиссии способов оплаты',
      methodFeesHint:
        'Когда отдельные настройки включены, способ использует фиксированную комиссию плюс ставку комиссии. Иначе применяется общая ставка по умолчанию.',
      paymentMethod: 'Способ оплаты',
      fixedFee: 'Фиксированная комиссия',
      feeRate: 'Ставка комиссии',
      methodFeeResult: 'Предпросмотр комиссии',
      methodFeeFallback: 'Используется общая ставка комиссии по умолчанию',
      methodFeePreview: 'Предпросмотр: комиссия {fee} со 100, к оплате {amount}',
      orderTimeout: 'Таймаут заказа',
      orderTimeoutHint: 'В минутах, минимум 1',
      maxPendingOrders: 'Максимум ожидающих заказов',
      cancelRateLimit: 'Ограничить частоту отмен',
      cancelRateLimitHint:
        'Когда функция включена, пользователи, превысившие лимит отмен в пределах временного окна, не могут создавать новые заказы',
      cancelRateLimitEvery: 'Каждые',
      cancelRateLimitAllowMax: 'допускается не более',
      cancelRateLimitTimes: 'отмен',
      cancelRateLimitWindow: 'Окно',
      cancelRateLimitUnit: 'Единица',
      cancelRateLimitMax: 'Максимум отмен',
      cancelRateLimitUnitMinute: 'Минуты',
      cancelRateLimitUnitHour: 'Часы',
      cancelRateLimitUnitDay: 'Дни',
      cancelRateLimitWindowMode: 'Режим окна',
      cancelRateLimitWindowModeRolling: 'Скользящее',
      cancelRateLimitWindowModeFixed: 'Фиксированное',
      alipayForceQRCode: 'Всегда показывать QR-код Alipay',
      alipayForceQRCodeHint:
        'Когда функция включена, пользователи мобильного Alipay всегда видят QR-код вместо перенаправления на страницу мобильной оплаты',
      alipayMobilePrecreateDeepLink: 'Переход в приложение при мобильном предсоздании Alipay',
      alipayMobilePrecreateDeepLinkHint:
        'Использовать официальный precreate Alipay на мобильных, открывать приложение Alipay и показывать динамический QR-код только если переход не удался. Эта опция имеет приоритет над «Всегда показывать QR-код Alipay»',
      helpText: 'Текст справки',
      helpImageUrl: 'URL изображения справки',
      manageProviders: 'Управление провайдерами',
      balancePaymentDisabled: 'Отключить пополнение баланса',
      noLimit: 'Пусто = без ограничений',
      helpImage: 'Изображение справки',
      helpImagePlaceholder: 'Загрузите изображение или укажите URL',
      helpTextHint: 'Поддерживается Markdown, включая ссылки, жирный текст и списки.',
      helpTextPlaceholder: 'Введите текст справки...',
      providerEasypay: 'EasyPay',
      providerAlipay: 'Alipay (напрямую)',
      providerWxpay: 'WeChat Pay (напрямую)',
      providerStripe: 'Stripe',
      providerAirwallex: 'Airwallex',
      typeDisabled: 'тип отключён',
      enableTypesFirst: 'Сначала включите хотя бы один тип оплаты выше',
      easypayRedirect: 'Перенаправление',
      paymentMode: 'Режим оплаты',
      modeRedirect: 'Перенаправление',
      modeQRCode: 'QR-код',
      modePopup: 'Всплывающее окно',
      validationNameRequired: 'Укажите название провайдера',
      validationTypesRequired: 'Выберите хотя бы один поддерживаемый тип оплаты',
      validationFieldRequired: 'Поле «{field}» обязательно',
      validationEasyPayCustomMethodRequired:
        'Для каждого своего метода EasyPay нужно указать и тип оплаты, и тип на вышестоящей стороне',
      validationEasyPayCustomMethodTypeInvalid:
        'Свои типы оплаты EasyPay могут содержать только строчные буквы, цифры, подчёркивания и дефисы',
      validationEasyPayCustomMethodUpstreamTypeInvalid:
        'Типы EasyPay на вышестоящей стороне могут содержать только строчные буквы, цифры, подчёркивания и дефисы',
      validationEasyPayCustomMethodReserved:
        'Свои типы оплаты EasyPay не могут использовать встроенные alipay или wxpay',
      validationEasyPayCustomMethodPrefixReserved:
        'Свои типы оплаты EasyPay не могут начинаться с alipay или wxpay',
      validationEasyPayCustomMethodDuplicate: 'Свои типы оплаты EasyPay должны быть уникальными',
      testConnection: 'Проверить соединение',
      testingConnection: 'Проверка...',
      connectionTestSuccess: 'Проверка соединения EasyPay прошла успешно',
      field_apiBase: 'Базовый URL API',
      field_notifyUrl: 'URL уведомлений',
      field_returnUrl: 'URL возврата',
      callbackBaseUrl: 'Базовый URL обратных вызовов',
      field_privateKey: 'Приватный ключ',
      field_publicKey: 'Публичный ключ',
      field_mpAppId: 'App ID официального аккаунта',
      field_mchId: 'Идентификатор продавца',
      field_apiV3Key: 'Ключ API v3',
      field_publicKeyId: 'Идентификатор публичного ключа',
      field_certSerial: 'Серийный номер сертификата',
      field_h5AppName: 'Название H5-приложения',
      field_h5AppUrl: 'URL H5-приложения',
      wxpayConfigHint:
        'WeChat Pay обычно требует только App ID. Заполняйте App ID официального аккаунта, название и URL H5-приложения только если ваш сценарий с официальным аккаунтом или H5 действительно этого требует.',
      wxpayAdvancedOptions: 'Расширенные настройки WeChat Pay',
      field_secretKey: 'Секретный ключ',
      field_clientId: 'Client ID',
      field_apiKey: 'API-ключ',
      field_publishableKey: 'Публикуемый ключ',
      field_webhookSecret: 'Секрет вебхука',
      field_countryCode: 'Код страны или региона',
      field_currency: 'Валюта оплаты',
      field_accountId: 'Airwallex Account ID',
      field_airwallexApiBaseHint:
        'Должен соответствовать среде API-ключа: используйте https://api-demo.airwallex.com/api/v1 для тестовых ключей и https://api.airwallex.com/api/v1 для рабочих. При смешивании сред возвращается credentials_invalid / Access Denied.',
      field_paymentCurrencyHint:
        'По умолчанию CNY. Stripe и Airwallex могут использовать HKD, USD или другую валюту из списка, поддерживаемого учётной записью; WeChat Pay, Alipay и EasyPay остаются в CNY.',
      field_accountIdHint:
        'Оставьте поле пустым, если вы не используете несколько аккаунтов, ключ уровня организации или платежи подключённых аккаунтов. API-ключ с областью одного аккаунта по умолчанию использует выбранный аккаунт.',
      field_cid: 'Идентификатор канала',
      field_cidAlipay: 'Идентификатор канала Alipay',
      field_cidWxpay: 'Идентификатор канала WeChat',
      easypayCustomMethods: 'Свои методы EasyPay',
      easypayCustomMethodsHint:
        'Добавьте методы, которые поддерживает этот эндпоинт EasyPay. Тип оплаты сохраняется в заказах TokenRouter; тип на вышестоящей стороне передаётся как тип EasyPay.',
      addCustomMethod: 'Добавить метод',
      customMethodType: 'Тип оплаты',
      customMethodUpstreamType: 'Тип на вышестоящей стороне',
      customMethodDisplayName: 'Отображаемое имя',
      customMethodDisplayNamePlaceholder: 'например, банковская карта',
      stripeWebhookHint: 'Настройте следующий URL как эндпоинт вебхука в панели Stripe:',
      stripeWebhookApiVersionHint:
        'Укажите для этого эндпоинта вебхука ту же версию API, что и у используемого SDK Stripe. Рекомендуется: {version}. Несовпадение может приводить к ошибкам разбора вебхуков.',
      airwallexWebhookHint:
        'Настройте следующий URL как эндпоинт вебхука в Airwallex. Выберите как минимум Payment Intent -> Succeeded (payment_intent.succeeded), желательно также Payment Intent -> Cancelled (payment_intent.cancelled). Используйте версию API по умолчанию для аккаунта или последнюю стабильную.',
      airwallexGuideSummary:
        'При создании API-ключа Airwallex с ограниченными правами выберите «Чтение и запись» для Payment Acceptance в разрешениях уровня аккаунта.',
      airwallexGuideNote:
        'Не выдавайте посторонние разрешения, такие как Spend, Payouts, Transfers, Funds Splits или POS Terminals, если они вам явно не нужны. Для вебхуков выберите как минимум payment_intent.succeeded, желательно также payment_intent.cancelled, и используйте версию API по умолчанию для аккаунта или последнюю стабильную.',
      limitsTitle: 'Лимиты',
      limitSingleMin: 'Минимум за заказ',
      limitSingleMax: 'Максимум за заказ',
      limitDaily: 'Дневной лимит',
      limitsHint:
        'Все поля пустые = используется общая конфигурация; заполнены частично = пустое поле означает отсутствие ограничения',
      limitsUseGlobal: 'Использовать общие',
      limitsNoLimit: 'Без ограничений',
      productNamePrefix: 'Префикс названия товара',
      productNameSuffix: 'Суффикс названия товара',
      preview: 'Предпросмотр',
      loadBalanceStrategy: 'Стратегия балансировки нагрузки',
      strategyRoundRobin: 'По кругу',
      strategyLeastAmount: 'Наименьшая сумма за день',
      providerManagement: 'Управление провайдерами',
      providerManagementDesc: 'Управление экземплярами платёжных провайдеров',
      createProvider: 'Добавить провайдера',
      editProvider: 'Изменить провайдера',
      deleteProvider: 'Удалить провайдера',
      deleteProviderConfirm: 'Удалить этого провайдера?',
      providerName: 'Название провайдера',
      providerKey: 'Тип провайдера',
      selectProviderKey: 'Выберите тип провайдера',
      providerConfig: 'Учётные данные',
      paymentGuideTrigger: 'Открыть руководство по оплате',
      guideOpenLabel: 'Включить: ',
      guideCallLabel: 'Вызов: ',
      guideFallbackLabel: 'Резерв: ',
      alipayGuideSummary:
        'На компьютерах предпочитается предсоздание QR-кода с переходом на кассу, на мобильных — оплата через WAP.',
      alipayGuideFaceToFaceTitle: 'Оплата по QR-коду / «лицом к лицу»',
      alipayGuideFaceToFaceOpen: 'Включите возможность оплаты по QR-коду или «лицом к лицу».',
      alipayGuideFaceToFaceCall:
        'Заказы с компьютера сначала вызывают alipay.trade.precreate и сразу отображают QR-код.',
      alipayGuideFaceToFaceFallback:
        'Если возможность недоступна или вызов не удался, сценарий автоматически переключается на оплату через сайт.',
      alipayGuidePagePayTitle: 'Оплата на сайте',
      alipayGuidePagePayOpen: 'Включите оплату на сайте.',
      alipayGuidePagePayCall:
        'Когда оплата «лицом к лицу» недоступна на компьютере, сценарий вызывает alipay.trade.page.pay и всё равно отображает возвращённую ссылку как QR-код.',
      alipayGuidePagePayFallback:
        'Ссылка на кассу остаётся доступной, чтобы пользователи могли вручную открыть страницу оплаты.',
      alipayGuideWapTitle: 'Оплата через WAP',
      alipayGuideWapOpen: 'Включите оплату на мобильных сайтах.',
      alipayGuideWapCall:
        'Заказы с мобильных сначала вызывают alipay.trade.wap.pay и переходят к оплате в Alipay.',
      alipayGuideWapFallback:
        'Если мобильная оплата недоступна или не удалась, фронтенд переключается на оплату по QR-коду и показывает уведомление.',
      wxpayGuideSummary:
        'На компьютерах предпочитается Native QR-код; на мобильных выбирается JSAPI или H5 в зависимости от контекста браузера.',
      wxpayGuideNote:
        'Текущая форма по умолчанию использует один общий App ID, что подходит для распространённого случая с одним субъектом: сайт, мобильное приложение и официальный аккаунт.',
      wxpayGuideNativeTitle: 'Оплата Native / по QR-коду',
      wxpayGuideNativeOpen: 'Включите возможность оплаты Native или по QR-коду.',
      wxpayGuideNativeCall:
        'Заказы с компьютера по умолчанию используют Native, а фронтенд отображает данные QR-кода.',
      wxpayGuideNativeFallback:
        'Мобильные сценарии также используют этот вариант, когда JSAPI или H5 применить нельзя.',
      wxpayGuideJsapiTitle: 'JSAPI / официальный аккаунт',
      wxpayGuideJsapiOpen:
        'Включите оплату через официальный аккаунт и убедитесь, что браузер открыт внутри WeChat и доступен OpenID.',
      wxpayGuideJsapiCall:
        'Внутри WeChat приложение вызывает JSAPI после авторизации и сразу запускает WeChat Pay.',
      wxpayGuideJsapiFallback:
        'Если конфигурация отсутствует, мост недоступен или запуск не удался, сценарий переключается на оплату по QR-коду.',
      wxpayGuideH5Title: 'Оплата H5',
      wxpayGuideH5Open: 'Включите оплату H5.',
      wxpayGuideH5Call:
        'В мобильных браузерах вне WeChat приложение вызывает оплату H5, когда доступен IP клиента.',
      wxpayGuideH5Fallback:
        'Если H5 недоступна или создание заказа не удалось, сценарий переключается на оплату по QR-коду.',
      noProviders: 'Экземпляры провайдеров не настроены',
      supportedTypes: 'Поддерживаемые типы оплаты',
      supportedTypesHint: 'Через запятую, например alipay,wxpay',
      refundEnabled: 'Разрешить возврат',
      allowUserRefund: 'Разрешить возврат пользователем',
      enableConflict:
        'У метода {method} уже есть включённый экземпляр провайдера: {provider}. Отключите существующий экземпляр перед переключением.',
    },
    balanceNotify: {
      title: 'Уведомление о низком балансе',
      description:
        'Отправлять уведомление по email, когда баланс пользователя опускается ниже порога',
      enabled: 'Включить уведомление о низком балансе',
      threshold: 'Порог по умолчанию',
      thresholdHint: 'Используется, когда пользователь не задал своё значение',
      thresholdPlaceholder: 'Введите сумму',
      rechargeUrl: 'URL страницы пополнения',
      rechargeUrlPlaceholder: 'https://example.com/payment',
      rechargeUrlHint: 'Если задано, в письме появится кнопка пополнения',
    },
    quotaNotify: {
      title: 'Уведомление о квоте провайдера',
      description:
        'Уведомлять администраторов, когда использование квоты провайдера достигает порога оповещения',
      enabled: 'Включить уведомление о квоте провайдера',
      emails: 'Email для уведомлений',
      emailsHint: 'Оставьте пустым, чтобы отключить уведомления',
      addEmail: 'Добавить email',
      emailPlaceholder: 'Введите адрес email',
    },
    subscriptionExpiryNotify: {
      title: 'Напоминание об окончании подписки',
      description:
        'Определяет, получают ли пользователи письма с напоминанием об окончании подписки.',
      enabled: 'Включить напоминание об окончании подписки',
      enabledHint:
        'Когда функция включена, система отправляет напоминания за 7, 3 и 1 день до окончания.',
    },
    smtp: {
      title: 'Настройки SMTP',
      description: 'Настройка отправки писем с кодами подтверждения',
      testConnection: 'Проверить соединение',
      testing: 'Проверка...',
      host: 'Хост SMTP',
      hostPlaceholder: 'smtp.gmail.com',
      port: 'Порт SMTP',
      portPlaceholder: '587',
      username: 'Имя пользователя SMTP',
      usernamePlaceholder: "your-email{'@'}gmail.com",
      password: 'Пароль SMTP',
      passwordPlaceholder: '********',
      passwordHint: 'Оставьте пустым, чтобы сохранить текущий пароль',
      passwordConfiguredPlaceholder: '********',
      passwordConfiguredHint:
        'Пароль задан. Оставьте поле пустым, чтобы сохранить текущее значение.',
      fromEmail: 'Email отправителя',
      fromEmailPlaceholder: "noreply{'@'}example.com",
      fromName: 'Имя отправителя',
      fromNamePlaceholder: 'TokenRouter',
      useTls: 'Использовать TLS',
      useTlsHint: 'Включить шифрование TLS для соединения SMTP',
    },
    testEmail: {
      title: 'Отправить тестовое письмо',
      description: 'Отправьте тестовое письмо, чтобы проверить настройки SMTP',
      recipientEmail: 'Email получателя',
      recipientEmailPlaceholder: "test{'@'}example.com",
      sendTestEmail: 'Отправить тестовое письмо',
      sending: 'Отправка...',
      enterRecipientHint: 'Введите адрес email получателя',
    },
    emailTemplates: {
      title: 'Шаблоны писем',
      description: 'Настройка темы и HTML-содержимого уведомлений для каждого события и языка.',
      event: 'Событие',
      locale: 'Язык',
      localeEn: 'Английский',
      localeZh: 'Китайский',
      subject: 'Тема',
      subjectPlaceholder: 'Введите тему письма',
      html: 'HTML-шаблон',
      htmlPlaceholder: 'Отредактируйте HTML-шаблон письма',
      placeholders: 'Доступные подстановки',
      placeholdersHelp:
        'Нажмите на подстановку, чтобы скопировать её. Серверная часть подставляет эти значения при отправке писем.',
      livePreview: 'Предпросмотр в реальном времени',
      previewSecurityHint:
        'HTML для предпросмотра формируется эндпоинтом предпросмотра серверной части и отображается в изолированном iframe с отключёнными скриптами.',
      preview: 'Предпросмотр / Обновить',
      previewing: 'Предпросмотр...',
      restoreOfficial: 'Восстановить официальный шаблон',
      restoring: 'Восстановление...',
      restoreConfirm:
        'Восстановить официальный шаблон для этого события и языка? Ваша версия будет заменена.',
      restoreSuccess: 'Официальный шаблон восстановлен',
      placeholderCopied: 'Подстановка скопирована',
      validationRequired: 'Тема и HTML-шаблон обязательны',
      empty: 'События или языки для шаблонов писем пока недоступны.',
      noPreview: 'Обновите предпросмотр, чтобы увидеть итоговую тему письма.',
      customized: 'Изменён',
    },
    opsMonitoring: {
      title: 'Мониторинг эксплуатации',
      description: 'Включить мониторинг эксплуатации для диагностики и контроля состояния',
      disabled: 'Мониторинг эксплуатации отключён',
      enabled: 'Включить мониторинг эксплуатации',
      enabledHint: 'Включить модуль мониторинга эксплуатации (только для администраторов)',
      realtimeEnabled: 'Включить мониторинг в реальном времени',
      realtimeEnabledHint: 'Включить передачу QPS и метрик в реальном времени (WebSocket)',
      metricsInterval: 'Интервал сбора метрик (секунды)',
      metricsIntervalHint:
        'Как часто собирать системные метрики и метрики запросов (от 60 до 3600 секунд)',
    },
    adminApiKey: {
      title: 'API-ключ администратора',
      description:
        'Глобальный API-ключ для интеграции внешних систем с полным доступом администратора',
      notConfigured: 'API-ключ администратора не настроен',
      configured: 'API-ключ администратора активен',
      currentKey: 'Текущий ключ',
      regenerate: 'Пересоздать',
      regenerating: 'Пересоздание...',
      delete: 'Удалить',
      deleting: 'Удаление...',
      create: 'Создать ключ',
      creating: 'Создание...',
      regenerateConfirm: 'Продолжить? Текущий ключ будет немедленно аннулирован.',
      deleteConfirm: 'Удалить API-ключ администратора? Внешние интеграции перестанут работать.',
      keyGenerated: 'Новый API-ключ администратора создан',
      keyDeleted: 'API-ключ администратора удалён',
      copyKey: 'Скопировать ключ',
      keyCopied: 'Ключ скопирован в буфер обмена',
      keyWarning: 'Этот ключ будет показан только один раз. Скопируйте его сейчас.',
      securityWarning:
        'Внимание: этот ключ даёт полный доступ администратора. Храните его в безопасности.',
      usage: 'Использование: добавьте в заголовок запроса — x-api-key: <ваш-админ-апи-ключ>',
    },
    overloadCooldown: {
      title: 'Пауза при перегрузке (529)',
      description:
        'Настройка стратегии приостановки планирования провайдера, когда вышестоящая сторона возвращает 529 (перегрузка)',
      enabled: 'Включить паузу при перегрузке',
      enabledHint:
        'Приостанавливать планирование провайдера при ошибках 529 и автоматически восстанавливать после паузы',
      cooldownMinutes: 'Длительность паузы (минуты)',
      cooldownMinutesHint: 'Как долго приостанавливать планирование провайдера (от 1 до 120 минут)',
      saveFailed: 'Не удалось сохранить настройки паузы при перегрузке',
    },
    openAI403Cooldown: {
      title: 'Пауза ChatGPT при ошибке 403',
      description:
        'Настройка временной приостановки планирования, когда провайдеры ChatGPT получают 403 от вышестоящей стороны',
      enabled: 'Включить паузу при 403',
      enabledHint:
        'Ненадолго приостанавливать планирование провайдеров ChatGPT при ошибках 403 и автоматически восстанавливать после паузы',
      cooldownMinutes: 'Длительность паузы (минуты)',
      cooldownMinutesHint:
        'Как долго приостанавливать планирование провайдеров ChatGPT (от 1 до 120 минут)',
      errorOnThresholdEnabled: 'Помечать ошибкой после порога',
      errorOnThresholdEnabledHint:
        'Переводить провайдера в состояние ошибки, когда число ответов 403 в пределах окна достигает порога',
      thresholdCount: 'Порог 403',
      thresholdCountHint:
        'Помечать ошибкой после такого числа ответов 403 в пределах окна (от 1 до 20)',
      thresholdWindowMinutes: 'Окно подсчёта (минуты)',
      thresholdWindowMinutesHint: 'Временное окно для подсчёта ответов 403 (от 1 до 1440 минут)',
      saveFailed: 'Не удалось сохранить настройки паузы ChatGPT при 403',
    },
    rateLimit429Cooldown: {
      title: 'Пауза по умолчанию при 429',
      description:
        'Настройка паузы провайдера по умолчанию, когда вышестоящая сторона возвращает 429 без явного времени сброса',
      enabled: 'Включить паузу по умолчанию при 429',
      enabledHint:
        'Приостанавливать планирование провайдера, когда в ответе 429 нет времени сброса, и автоматически восстанавливать после паузы',
      cooldownSeconds: 'Длительность паузы (секунды)',
      cooldownSecondsHint:
        'Длительность паузы по умолчанию (от 1 до 7200 секунд); явно указанное вышестоящей стороной время сброса всё равно имеет приоритет',
      saveFailed: 'Не удалось сохранить настройки паузы по умолчанию при 429',
    },
    streamTimeout: {
      title: 'Обработка таймаута потока',
      description:
        'Настройка стратегии работы с провайдером, когда истекает таймаут ответа вышестоящей стороны',
      enabled: 'Включить обработку таймаута потока',
      enabledHint:
        'Автоматически обрабатывать проблемных провайдеров при таймауте вышестоящей стороны',
      timeoutSeconds: 'Порог таймаута (секунды)',
      timeoutSecondsHint:
        'Если интервал между данными потока превышает это время, он считается таймаутом (от 30 до 300 с)',
      action: 'Действие',
      actionTempUnsched: 'Временно исключить из планирования',
      actionError: 'Пометить как ошибку',
      actionNone: 'Без действий',
      actionHint: 'Действие, применяемое к провайдеру после таймаута',
      tempUnschedMinutes: 'Длительность паузы (минуты)',
      tempUnschedMinutesHint:
        'Длительность временного исключения из планирования (от 1 до 60 минут)',
      thresholdCount: 'Порог срабатывания (количество)',
      thresholdCountHint: 'Число таймаутов до срабатывания действия (от 1 до 10)',
      thresholdWindowMinutes: 'Окно подсчёта (минуты)',
      thresholdWindowMinutesHint: 'Временное окно для подсчёта таймаутов (от 1 до 60 минут)',
      saveFailed: 'Не удалось сохранить настройки таймаута потока',
    },
    rectifier: {
      title: 'Корректор запросов',
      description:
        'Автоматически исправлять параметры запроса и повторять его, когда поддерживаемые вышестоящие стороны возвращают определённые ошибки подписи Thinking или бюджета. Эта глобальная настройка применяется ко всем подходящим сценариям повтора на шлюзе.',
      enabled: 'Включить корректор запросов',
      enabledHint: 'Главный переключатель — при отключении все функции коррекции выключаются',
      thinkingSignature: 'Корректор подписи Thinking',
      thinkingSignatureHint:
        'Автоматически удалять подписи и повторять запрос, когда вышестоящая сторона возвращает ошибки проверки подписи блока thinking',
      thinkingBudget: 'Корректор бюджета Thinking',
      thinkingBudgetHint:
        'Автоматически устанавливать бюджет 32000 и повторять запрос, когда вышестоящая сторона возвращает ошибку ограничения budget_tokens (≥1024)',
      apikeySignature: 'Корректор подписи для API-ключей',
      apikeySignatureHint:
        'Автоматически удалять подписи и повторять запрос, когда провайдеры с API-ключом получают ошибки, связанные с подписью (встроенные шаблоны применяются всегда)',
      apikeyPatterns: 'Свои шаблоны совпадений',
      apikeyPatternsHint:
        'Дополнительные ключевые слова для поиска в теле ответа (без учёта регистра). Встроенные шаблоны применяются всегда; используйте эти для дополнительных совпадений.',
      apikeyPatternPlaceholder: 'например, thinking_error',
      addPattern: 'Добавить шаблон',
      saveFailed: 'Не удалось сохранить настройки корректора',
    },
    betaPolicy: {
      title: 'Политика Beta',
      description:
        'Как обрабатывать возможности Beta при настройке передачи запросов к API Anthropic. Применяется только к эндпоинту /v1/messages.',
      action: 'Действие',
      actionPass: 'Передавать (без изменений)',
      actionFilter: 'Фильтровать (удалять)',
      actionBlock: 'Блокировать (отклонять)',
      scope: 'Область применения',
      scopeAll: 'Все провайдеры',
      scopeOAuth: 'Только OAuth',
      scopeAPIKey: 'Только API-ключ',
      scopeBedrock: 'Только Bedrock',
      errorMessage: 'Текст ошибки',
      errorMessagePlaceholder: 'Свой текст ошибки при блокировке',
      errorMessageHint: 'Оставьте пустым, чтобы использовать стандартное сообщение',
      saveFailed: 'Не удалось сохранить настройки политики Beta',
      modelWhitelist: 'Белый список моделей',
      modelWhitelistHint:
        'Оставьте пустым, чтобы применить ко всем моделям. Поддерживается точное совпадение и шаблон префикса (например, claude-opus-*)',
      modelPatternPlaceholder: 'например, claude-opus-* или claude-opus-4-6',
      addModelPattern: 'Добавить шаблон модели',
      removePattern: 'Удалить',
      fallbackAction: 'Действие для остальных моделей',
      fallbackActionHint: 'Действие для моделей, не подходящих под белый список',
      fallbackErrorMessagePlaceholder: 'Свой текст ошибки при блокировке моделей вне белого списка',
      quickPresets: 'Быстрые шаблоны',
      presetOpusOnly: 'Только Opus для 1M',
      presetOpusOnlyDesc: 'Передавать для Opus, остальные фильтровать',
      commonPatterns: 'Частые шаблоны',
    },
    openaiFastPolicy: {
      title: 'Политика Fast/Flex OpenAI',
      description:
        'Перехватывать, фильтровать или передавать запросы OpenAI fast(priority), ultrafast и flex на основе поля service_tier в теле запроса. Применяется только к шлюзу OpenAI.',
      empty: 'Правила не настроены. Нажмите кнопку ниже, чтобы добавить правило.',
      ruleHeader: 'Правило #{index}',
      removeRule: 'Удалить правило',
      addRule: 'Добавить правило',
      saveHint:
        'Сохраняется вместе с системными настройками (кнопка «Сохранить настройки» внизу страницы).',
      serviceTier: 'Совпадение service_tier',
      tierAll: 'Все значения tier',
      tierPriority: 'priority (fast)',
      tierUltrafast: 'ultrafast',
      tierFlex: 'flex',
      action: 'Действие',
      actionPass: 'Передавать (сохранять service_tier)',
      actionFilter: 'Фильтровать (удалять service_tier)',
      actionForcePriority: 'Принудительно priority (fast)',
      actionForceUltrafast: 'Принудительно Ultra Fast (ultrafast)',
      actionBlock: 'Блокировать (отклонять запрос)',
      scope: 'Область применения',
      scopeAll: 'Все провайдеры',
      scopeOAuth: 'Только OAuth',
      scopeAPIKey: 'Только API-ключ',
      scopeBedrock: 'Только Bedrock',
      userIds: 'Отдельные пользователи',
      userIdsHint:
        'Введите любую часть email пользователя для поиска. Оставьте пустым, чтобы применить ко всем пользователям TokenRouter. Для выбранных пользователей правило срабатывает на запросы с их API-ключами и имеет приоритет над общими правилами.',
      userSearchPlaceholder: 'Поиск по email пользователя',
      userSearchEmpty: 'Подходящие пользователи не найдены',
      userDeleted: '(удалён)',
      userIdFallback: 'Пользователь #{id}',
      removeUser: 'Удалить пользователя',
      errorMessage: 'Текст ошибки',
      errorMessagePlaceholder: 'Свой текст ошибки при блокировке',
      errorMessageHint: 'Оставьте пустым, чтобы использовать стандартное сообщение.',
      modelWhitelist: 'Целевые модели',
      modelWhitelistHint:
        'К моделям из этого списка применяется «Действие»; к моделям вне списка — действие для остальных моделей. Оставьте пустым, чтобы применить «Действие» ко всем моделям. Поддерживается точное совпадение и шаблон префикса (например, gpt-5.6*).',
      modelPatternPlaceholder: 'например, gpt-5.6-sol или gpt-5.6*',
      addModelPattern: 'Добавить целевую модель',
      fallbackAction: 'Действие для остальных моделей',
      fallbackActionHint: 'Применяется только к моделям вне целевого списка.',
      fallbackErrorMessagePlaceholder: 'Свой текст ошибки при блокировке остальных моделей',
      summaryTargetModels: 'Целевые модели',
      summaryAllModels: 'Все модели',
      summaryOtherModels: 'Остальные модели',
      summaryAction: {
        pass: 'Передавать',
        filter: 'Фильтровать',
        block: 'Блокировать',
        force_priority: 'Принудительно priority',
        force_ultrafast: 'Принудительно Ultra Fast',
      },
    },
    emailOAuth: {
      title: 'Вход через OAuth по email',
      description:
        'После включения OAuth по email для GitHub или Google система считывает подтверждённый адрес, выполняет вход для совпадающих пользователей, а отсутствующих проводит через завершение локальной регистрации.',
      githubHint:
        'Приложению OAuth GitHub нужны области read:user user:email. Используйте указанный ниже URL обратного вызова серверной части.',
      secretConfiguredPlaceholder:
        'Секрет задан. Оставьте поле пустым, чтобы сохранить текущее значение.',
      backendCallbackUrl: 'URL обратного вызова серверной части',
      generateAndCopy: 'Создать и скопировать',
      frontendCallbackUrl: 'URL обратного вызова фронтенда',
      googleHint:
        'Клиенту OAuth Google нужны области openid email profile и зарегистрированный в учётных данных URL обратного вызова серверной части.',
      googleGuide:
        'Руководство по настройке: Google Cloud Console → APIs & Services → OAuth consent screen, затем Credentials → Create Credentials → OAuth client ID, выберите Web application и добавьте указанный ниже URL в Authorized redirect URIs.',
      googleOneTapHint:
        'Показывать выбор аккаунта Google, управляемый браузером, неавторизованным пользователям на главной странице и странице входа.',
      copy: 'Скопировать',
      googleOneTapOriginHint:
        'Добавьте этот источник в Authorized JavaScript origins клиента OAuth в Google Cloud. В продакшене требуется HTTPS.',
      originCopied: 'Источник JavaScript скопирован.',
      callbackCopied: 'URL обратного вызова создан и скопирован.',
      githubGuideBeforeLink: 'Руководство по настройке: GitHub Settings → Developer settings →',
      githubGuideAfterLink:
        '→ New OAuth App. Укажите источник вашего сайта как Homepage URL, а URL обратного вызова серверной части ниже — как Authorization callback URL.',
    },
    homeFeaturedModels: {
      title: 'Избранные модели на главной',
      description:
        'Настройте отдельные карточки моделей в разделе «Supported AI Models» на главной странице; они показываются в указанном порядке, максимум 12. Когда список пуст, на главной остаются стандартные карточки категорий провайдеров.',
      clear: 'Очистить',
      add: 'Добавить модель',
      remove: 'Удалить модель',
      select: 'Выберите модель',
      empty: 'Модели недоступны (на маркетплейсе нет публичных групп)',
    },
    homeFooter: {
      groupIndex: 'Группа #{index}',
      title: 'Настройки подвала',
      description:
        'Настройте группы ссылок и дополнительный текст в подвале главной страницы (например, номер ICP). Когда всё пусто, в подвале остаётся только строка об авторских правах.',
      useDefault: 'Использовать стандартный шаблон',
      addGroup: 'Добавить группу',
      removeGroup: 'Удалить группу',
      groupTitle: 'Заголовок группы',
      groupTitlePlaceholder: 'Заголовок группы, например «Продукт»',
      addLink: 'Добавить ссылку',
      removeLink: 'Удалить ссылку',
      linkLabel: 'Подпись',
      linkUrl: 'URL',
      linkUrlPlaceholder: 'https://... или /models',
      extraText: 'Текст в подвале',
      extraTextPlaceholder:
        'например, номер ICP или свой текст об авторских правах, можно в несколько строк',
    },
    loginAgreement: {
      documentIndex: 'Документ #{index}',
      documentSlugDuplicate:
        'Маршруты документов соглашения о входе не могут повторяться: /legal/{id}',
      title: 'Соглашение о входе',
      description:
        'Определяет, должен ли пользователь на странице входа сначала принять документы политики в формате Markdown.',
      mode: 'Режим отображения',
      modeModal: 'Модальное окно',
      modeCheckbox: 'Флажок',
      modeCheckboxHint: 'Флажок отображается под кнопкой входа и блокирует все действия входа.',
      modeModalHint:
        'Модальное окно открывается на странице входа и блокирует все действия входа до принятия.',
      updatedAt: 'Дата обновления',
      updatedAtHint: 'Изменение даты или содержимого требует повторного согласия.',
      documents: 'Документы соглашения',
      documentsHint:
        'Заголовки документов можно менять, содержимое сохраняется в формате Markdown.',
      addDocument: 'Добавить документ',
      untitledDocument: 'Документ без названия',
      documentTitle: 'Заголовок документа',
      documentTitlePlaceholder: 'Например: Пользовательское соглашение',
      documentSlug: 'Маршрут документа',
      documentContent: 'Содержимое Markdown',
      documentContentPlaceholder: 'Напишите итоговое содержимое в формате Markdown.',
      documentsRequired: 'При включённом соглашении о входе нужен хотя бы один документ.',
      documentTitleRequired: 'Заголовок документа соглашения о входе не может быть пустым.',
    },
    wechatConnect: {
      title: 'WeChat Connect',
      description:
        'Настройка входа через WeChat Open Platform, официальный аккаунт или мини-программу.',
      enabledLabel: 'Включить WeChat Connect',
      enabledHint: 'Включите это, чтобы настроить обратные вызовы и авторизацию OAuth WeChat.',
      appIdLabel: 'App ID',
      appIdPlaceholder: 'WeChat App ID',
      appSecretLabel: 'App Secret',
      appSecretConfiguredPlaceholder:
        'Секрет задан. Оставьте поле пустым, чтобы сохранить текущее значение.',
      appSecretPlaceholder: 'WeChat App Secret',
      appSecretConfiguredHint:
        'Секрет задан. Оставьте поле пустым, чтобы сохранить текущее значение.',
      appSecretHint: 'Введите новый секрет, чтобы заменить текущие учётные данные WeChat.',
      modeLabel: 'Режим',
      openModeLabel: 'Использовать Open вне WeChat',
      openModeHint: 'Использовать авторизацию по QR-коду открытой платформы вне браузера WeChat.',
      mpModeLabel: 'Использовать официальный аккаунт внутри WeChat',
      mpModeHint: 'Использовать авторизацию официального аккаунта внутри браузера WeChat.',
      redirectUrlLabel: 'URL перенаправления',
      redirectUrlPlaceholder: 'https://your-site.com/api/v1/auth/oauth/wechat/callback',
      generateAndCopy: 'Создать и скопировать (текущий сайт)',
      redirectUrlSetAndCopied: 'URL перенаправления создан и скопирован в буфер обмена',
      frontendRedirectUrlLabel: 'URL перенаправления во фронтенде',
      frontendRedirectUrlPlaceholder: '/auth/wechat/callback',
      frontendRedirectUrlHint:
        'Обычно это путь обратного вызова во фронтенде; держите его согласованным с серверной частью.',
      pcApp: 'Приложение для ПК',
      pcAppHint:
        'Браузеры на компьютере выполняют вход по QR-коду через WeChat Open Platform. Это может работать одновременно с официальным аккаунтом или мобильным приложением.',
      pcAppId: 'App ID для ПК',
      pcAppIdPlaceholder: 'WeChat Open Platform PC App ID',
      pcAppSecret: 'App Secret для ПК',
      pcAppSecretPlaceholder: 'WeChat Open Platform PC App Secret',
      mp: 'Официальный аккаунт',
      mpHint: 'Доступно только внутри браузера WeChat. Вне WeChat отображается как недоступное.',
      mpAppId: 'App ID официального аккаунта',
      mpAppSecret: 'App Secret официального аккаунта',
      mobile: 'Мобильное приложение',
      mobileHint:
        'Нативные мобильные клиенты запускают авторизацию через SDK WeChat. Веб-интерфейс не запускает этот сценарий напрямую.',
      mobileAppId: 'App ID мобильного приложения',
      mobileAppSecret: 'App Secret мобильного приложения',
      unionIdHint:
        'Если приложение для ПК включено вместе с официальным аккаунтом или мобильным приложением, они должны относиться к одному аккаунту WeChat Open Platform, чтобы UnionID надёжно объединял учётные записи.',
      browserRedirectUrl: 'URL перенаправления в браузере',
      browserRedirectUrlHint:
        'Используется для обратных вызовов из браузера для приложения на ПК и официального аккаунта. Сценарии с нативным мобильным SDK не запускаются напрямую из этого браузерного обратного вызова.',
      mpMobileConflict: 'Официальный аккаунт и мобильное приложение нельзя включать одновременно.',
    },
    authSourceDefaults: {
      title: 'Значения по умолчанию для источников входа',
      description:
        'Настройка значений по умолчанию для баланса, параллелизма, подписок и правил начисления по каждому источнику.',
      requireEmailLabel: 'Требовать email при регистрации через сторонний сервис',
      requireEmailHint:
        'Когда функция включена, при регистрации через Linux DO, OIDC и WeChat необходимо указать email до создания учётной записи.',
      enabledHint:
        'Эти значения применяются, когда новый пользователь регистрируется через данный источник. Начисление при первой привязке применяется только когда существующий пользователь привязывает этот источник.',
      sources: {
        email: {
          title: 'Регистрация по email',
          description: 'Начисления квоты по умолчанию для регистрации по email и паролю.',
        },
        linuxdo: {
          title: 'Регистрация через Linux DO',
          description: 'Начисления квоты по умолчанию для регистрации через Linux DO.',
        },
        oidc: {
          title: 'Регистрация через OIDC',
          description: 'Начисления квоты по умолчанию для регистрации через OIDC.',
        },
        wechat: {
          title: 'Регистрация через WeChat',
          description: 'Начисления квоты по умолчанию для регистрации через WeChat.',
        },
        github: {
          title: 'GitHub',
          description: 'Регистрация или первая привязка через OAuth по email GitHub.',
        },
        google: {
          title: 'Google',
          description: 'Регистрация или первая привязка через OAuth по email Google.',
        },
        dingtalk: {
          title: 'DingTalk',
          description: 'Применяется при первой регистрации или первой привязке через DingTalk.',
        },
      },
      grantOnFirstBindLabel: 'Начислять при первой привязке',
      grantOnFirstBindHint:
        'Начислять права по умолчанию, когда существующий пользователь впервые привязывает этот источник.',
      defaultSubscriptionsLabel: 'Подписки по умолчанию',
      defaultSubscriptionsHint:
        'Применяется только к этому источнику входа. Оставьте пустым, чтобы не назначать подписки для этого источника.',
      noSourceSubscriptions: 'Подписки по умолчанию для отдельных источников не настроены.',
    },
    paymentVisibleMethods: {
      methodLabel: 'Видимый метод {title}',
      methodHint:
        'Определяет, показывается ли этот метод при оплате и какой ключ источника он использует.',
      sourceLabel: 'Источник оплаты',
      sourceHint:
        'Выберите явный источник перед включением метода. Методы без настроенного источника не показываются.',
      sourceRequiredError: 'Выберите источник оплаты, прежде чем включать {title}.',
    },
    openaiQuotaAutoPause: {
      title: 'Автопауза провайдеров OpenAI по квоте',
      description:
        'Когда провайдер OpenAI достигает порога использования за 5 ч или 7 дней, планировщик автоматически пропускает его и возобновляет работу после смены окна. Пороги отдельных провайдеров имеют приоритет над этим глобальным значением.',
      default5h: 'Порог использования за 5 ч по умолчанию (%)',
      default7d: 'Порог использования за 7 дней по умолчанию (%)',
      thresholdHint:
        'Значение от 0 до 100; оставьте пустым или укажите 0, чтобы отключить глобальный порог по умолчанию.',
      rangeError: 'Порог автопаузы по квоте OpenAI должен быть от 0 до 100',
    },
    usageRecords: {
      title: 'Записи об использовании',
      description:
        'Настройки записей об использовании и неудачных запросах, видимых конечным пользователям.',
    },
    user_error_view: {
      label: 'Разрешить пользователям видеть свои неудачные запросы',
      description:
        'Когда функция включена, пользователи видят на странице использования сокращённую версию своих неудачных запросов (без внутренних данных и данных вышестоящей стороны). Для получения данных требуется включённый мониторинг эксплуатации.',
    },
    saveSettings: 'Сохранить настройки',
    unsavedChanges: 'Есть несохранённые изменения',
    discardChanges: 'Отменить изменения',
    saving: 'Сохранение...',
    settingsSaved: 'Настройки сохранены',
    smtpConnectionSuccess: 'Соединение SMTP установлено',
    testEmailSent: 'Тестовое письмо отправлено',
    failedToLoad: 'Не удалось загрузить настройки',
    failedToSave: 'Не удалось сохранить настройки',
    failedToTestSmtp: 'Проверка соединения SMTP не удалась',
    failedToSendTestEmail: 'Не удалось отправить тестовое письмо',
  },
  // Error Passthrough Rules
  errorPassthrough: {
    title: 'Правила передачи ошибок',
    description: 'Настройка того, как ошибки вышестоящей стороны возвращаются клиентам',
    createRule: 'Создать правило',
    editRule: 'Изменить правило',
    deleteRule: 'Удалить правило',
    noRules: 'Правила не настроены',
    createFirstRule: 'Создайте первое правило передачи ошибок',
    allPlatforms: 'Все платформы',
    passthrough: 'Передавать как есть',
    custom: 'Свои значения',
    code: 'Код',
    body: 'Тело ответа',
    skipMonitoring: 'Пропускать мониторинг',

    // Columns
    columns: {
      priority: 'Приоритет',
      name: 'Название',
      conditions: 'Условия',
      platforms: 'Платформы',
      behavior: 'Поведение',
      status: 'Статус',
      actions: 'Действия',
    },

    // Match Mode
    matchMode: {
      any: 'Код ИЛИ ключевое слово',
      all: 'Код И ключевое слово',
      anyHint:
        'Код состояния совпадает с любым из кодов ошибок ИЛИ сообщение содержит любое из ключевых слов',
      allHint:
        'Код состояния совпадает с любым из кодов ошибок И сообщение содержит любое из ключевых слов',
    },

    // Form
    form: {
      name: 'Название правила',
      namePlaceholder: 'например, передача ошибки превышения контекста',
      priority: 'Приоритет',
      priorityHint: 'Меньшие значения имеют более высокий приоритет',
      description: 'Описание',
      descriptionPlaceholder: 'Опишите назначение этого правила...',
      matchConditions: 'Условия совпадения',
      errorCodes: 'Коды ошибок',
      errorCodesPlaceholder: '422, 400, 429',
      errorCodesHint: 'Разделяйте несколько кодов запятыми',
      keywords: 'Ключевые слова',
      keywordsPlaceholder:
        'По одному ключевому слову на строку\ncontext limit\nmodel not supported',
      keywordsHint: 'По одному ключевому слову на строку, без учёта регистра',
      matchMode: 'Режим совпадения',
      platforms: 'Платформы',
      platformsHint: 'Оставьте пустым, чтобы применить ко всем платформам',
      responseBehavior: 'Поведение ответа',
      passthroughCode: 'Передавать код состояния вышестоящей стороны',
      responseCode: 'Свой код состояния',
      passthroughBody: 'Передавать сообщение об ошибке вышестоящей стороны',
      customMessage: 'Своё сообщение об ошибке',
      customMessagePlaceholder: 'Сообщение об ошибке для возврата клиенту...',
      skipMonitoring: 'Пропускать мониторинг',
      skipMonitoringHint:
        'Когда функция включена, ошибки, подходящие под это правило, не записываются в мониторинг эксплуатации',
      enabled: 'Включить это правило',
    },

    // Messages
    nameRequired: 'Введите название правила',
    conditionsRequired: 'Настройте хотя бы один код ошибки или ключевое слово',
    ruleCreated: 'Правило создано',
    ruleUpdated: 'Правило обновлено',
    ruleDeleted: 'Правило удалено',
    deleteConfirm: 'Удалить правило «{name}»?',
    failedToLoad: 'Не удалось загрузить правила',
    failedToSave: 'Не удалось сохранить правило',
    failedToDelete: 'Не удалось удалить правило',
    failedToToggle: 'Не удалось переключить статус',
  },
  // TLS fingerprint routing
  tlsFingerprintRouters: {
    title: 'Маршрутизаторы TLS',
    description:
      'Сопоставлять значения User-Agent запросов по порядку и выбирать профили отпечатка TLS и заголовки клиента для вышестоящей стороны.',
    createRouter: 'Создать маршрутизатор',
    editRouter: 'Изменить маршрутизатор',
    deleteRouter: 'Удалить маршрутизатор',
    copyYaml: 'Скопировать YAML',
    copyYamlSuccess: 'YAML маршрутизатора TLS скопирован в буфер обмена',
    noRouters: 'Маршрутизаторы TLS не настроены',
    createFirstRouter: 'Создайте первый маршрутизатор TLS',
    columns: {
      name: 'Название',
      description: 'Описание',
      rules: 'Правила',
      status: 'Статус',
      actions: 'Действия',
    },
    form: {
      pasteYaml: 'Вставьте конфигурацию YAML',
      pasteYamlPlaceholder: 'Вставьте сюда конфигурацию YAML маршрутизатора TLS...',
      pasteYamlHint:
        'Вставьте скопированную конфигурацию YAML маршрутизатора TLS, чтобы заполнить форму автоматически.',
      parseYaml: 'Разобрать YAML',
      yamlParsed: 'YAML успешно разобран, поля заполнены автоматически',
      yamlParseFailed: 'Не удалось разобрать YAML: поле name не найдено',
      name: 'Название маршрутизатора',
      description: 'Описание',
      enabled: 'Включить маршрутизатор',
      chatgptOAuthTokenSettings: 'Запросы токена ChatGPT OAuth',
      chatgptOAuthTokenSettingsHint:
        'Применяется только к запросам обмена и обновления токена OpenAI/ChatGPT у провайдеров, привязанных к этому маршрутизатору TLS.',
      chatgptOAuthTokenUserAgent: 'User-Agent для токена',
      chatgptOAuthTokenUserAgentHint:
        'Для обмена и обновления токена пустое значение откатывается к User-Agent OpenAI Codex, когда включён профиль TLS для токена.',
      chatgptOAuthTokenProfile: 'Профиль TLS для токена',
      chatgptOAuthTokenProfileHint:
        'При отключении сохраняется текущее поведение запросов токена. Выбор профиля отправляет запросы обмена и обновления токена через DoWithTLS.',
      chatgptOAuthTokenProfileDisabled: 'Не включать отдельный профиль TLS для токена',
      codexInviteResetSettings: 'Запросы сброса приглашений Codex',
      codexInviteResetSettingsHint:
        'Применяется только к запросам статуса, отправки и использования сброса приглашений Codex Desktop у провайдеров, привязанных к этому маршрутизатору TLS.',
      codexInviteResetUserAgent: 'User-Agent для сброса приглашений',
      codexInviteResetUserAgentHint:
        'Оставьте пустым, чтобы использовать User-Agent Codex Desktop.',
      codexInviteResetProfile: 'Профиль TLS для сброса приглашений',
      codexInviteResetProfileHint:
        'При отключении используется профиль TLS провайдера. Выбор профиля отправляет запросы сброса приглашений через DoWithTLS.',
      codexInviteResetProfileDisabled: 'Использовать профиль TLS провайдера',
      rules: 'Правила маршрутизации',
      addRule: 'Добавить правило',
      noRules:
        'Правил пока нет. При отсутствии совпадений будет использоваться фиксированный профиль провайдера.',
      ruleTitle: 'Правило {index}',
      ruleName: 'Название правила',
      profile: 'Профиль TLS',
      matchType: 'Тип сопоставления',
      pattern: 'Шаблон',
      upstreamUserAgent: 'User-Agent для вышестоящей стороны',
      upstreamUserAgentHint:
        'Оставьте пустым, чтобы использовать свой User-Agent провайдера, входящий User-Agent или системное значение по умолчанию.',
      upstreamOriginator: 'Originator для вышестоящей стороны',
      upstreamOriginatorHint:
        'Оставьте пустым, чтобы использовать входящий originator или системное значение по умолчанию.',
      ruleEnabled: 'Включить правило',
      caseSensitive: 'Учитывать регистр',
      patternRequired: 'Укажите шаблон правила',
      regexInvalid: 'Некорректное регулярное выражение',
    },
    matchTypes: {
      contains: 'Содержит',
      prefix: 'Префикс',
      exact: 'Точное совпадение',
      regex: 'Регулярное выражение',
    },
    deleteConfirmMessage:
      'Удалить маршрутизатор TLS «{name}»? Провайдеры, которые его используют, вернутся к своему фиксированному профилю TLS.',
    createSuccess: 'Маршрутизатор TLS создан',
    updateSuccess: 'Маршрутизатор TLS обновлён',
    deleteSuccess: 'Маршрутизатор TLS удалён',
    loadFailed: 'Не удалось загрузить маршрутизаторы TLS',
    saveFailed: 'Не удалось сохранить маршрутизатор TLS',
    toggleFailed: 'Не удалось переключить маршрутизатор TLS',
    deleteFailed: 'Не удалось удалить маршрутизатор TLS',
  },
  // TLS Fingerprint Profiles
  tlsFingerprintProfiles: {
    title: 'Профили отпечатка TLS',
    description:
      'Управление профилями отпечатка TLS для имитации характеристик рукопожатия конкретных клиентов',
    createProfile: 'Создать профиль',
    editProfile: 'Изменить профиль',
    deleteProfile: 'Удалить профиль',
    copyYaml: 'Скопировать как YAML',
    noProfiles: 'Профили не настроены',
    createFirstProfile: 'Создайте первый профиль отпечатка TLS',

    columns: {
      name: 'Название',
      description: 'Описание',
      grease: 'GREASE',
      alpn: 'ALPN',
      actions: 'Действия',
    },

    collector: {
      title: 'Встроенный сборщик отпечатков',
      description:
        'Запустите временный сборщик HTTPS с этой страницы, чтобы получить реальные данные ClientHello от Claude Code и Codex CLI.',
      running: 'Работает',
      stopped: 'Остановлен',
      start: 'Запустить',
      stop: 'Остановить',
      listenAddress: 'Адрес прослушивания',
      publicBaseURL: 'Публичный URL',
      certificate: 'Сертификат',
      generatedCert: 'Временный самоподписанный CA',
      configuredCert: 'Настроенный сертификат',
      createSession: 'Создать сессию',
      refreshCaptures: 'Обновить захваты',
      captureURL: 'URL для захвата',
      expiresAt: 'Истекает',
      claudeCommand: 'Claude Code',
      codexCommand: 'Команда Codex CLI',
      codexConfig: 'Конфигурация Codex CLI',
      copyCA: 'Скопировать CA',
      downloadCA: 'Скачать CA',
      captures: 'Захваты',
      captureCount: 'Записей: {count}',
      noCaptures: 'Захватов пока нет. Запустите клиент командой выше, и результаты появятся здесь.',
      copyYaml: 'Скопировать YAML',
      applyCapture: 'Заполнить форму',
      unknownClient: 'Неизвестный клиент',
      statusFailed: 'Не удалось загрузить состояние сборщика',
      startSuccess: 'Сборщик запущен',
      stopSuccess: 'Сборщик остановлен',
      actionFailed: 'Не удалось выполнить действие сборщика',
      sessionCreated: 'Сессия захвата создана',
      sessionFailed: 'Не удалось создать сессию захвата',
      applied: 'Захват перенесён в форму',
      copied: 'Скопировано в буфер обмена',
      copyFailed: 'Не удалось скопировать',
    },

    form: {
      pasteYaml: 'Вставьте конфигурацию YAML',
      pasteYamlPlaceholder: 'Вставьте сюда вывод YAML из сборщика отпечатков TLS...',
      pasteYamlHint:
        'Вставьте YAML, скопированный из сборщика отпечатков TLS, чтобы заполнить все поля автоматически.',
      openCollector: 'Открыть сборщик',
      parseYaml: 'Разобрать YAML',
      yamlParsed: 'YAML успешно разобран, поля заполнены автоматически',
      yamlParseFailed: 'Не удалось разобрать YAML: поле name не найдено',
      name: 'Название профиля',
      namePlaceholder: 'например, macOS Node.js v24',
      description: 'Описание',
      descriptionPlaceholder: 'Необязательное описание этого профиля',
      enableGrease: 'Включить GREASE',
      enableGreaseHint: 'Вставлять значения GREASE в расширения ClientHello протокола TLS',
      cipherSuites: 'Наборы шифров',
      cipherSuitesHint: 'Шестнадцатеричные значения через запятую, например 0x1301, 0x1302, 0xc02c',
      curves: 'Эллиптические кривые',
      curvesHint: 'Идентификаторы кривых через запятую',
      pointFormats: 'Форматы точек',
      signatureAlgorithms: 'Алгоритмы подписи',
      alpnProtocols: 'Протоколы ALPN',
      alpnProtocolsHint: 'Через запятую, например h2, http/1.1',
      supportedVersions: 'Поддерживаемые версии TLS',
      keyShareGroups: 'Группы обмена ключами',
      pskModes: 'Режимы PSK',
      extensions: 'Расширения',
    },

    deleteConfirm: 'Удалить профиль',
    deleteConfirmMessage:
      'Удалить профиль «{name}»? Провайдеры, которые его используют, вернутся к встроенному значению по умолчанию.',
    createSuccess: 'Профиль создан',
    updateSuccess: 'Профиль обновлён',
    deleteSuccess: 'Профиль удалён',
    loadFailed: 'Не удалось загрузить профили',
    saveFailed: 'Не удалось сохранить профиль',
    deleteFailed: 'Не удалось удалить профиль',
  },
}
