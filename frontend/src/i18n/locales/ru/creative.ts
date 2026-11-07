// creative.ts supplies English text for image creation, editing, canvas
// controls, and run history. Prompts, selection guidance, generation parameters,
// and local-data messages describe the creative workspace states so its controls
// and validation errors share consistent wording.

export default {
  creative: {
    title: 'Творческая студия',
    description:
      'Создавайте и редактируйте изображения с помощью AI-моделей; файлы остаются только в этом браузере.',
    panel: {
      model: 'Модель',
      // Image selection guidance shared with generation validation when no reference image is selected.
      selectImageHint: 'Перед генерацией выберите изображение на холсте.',
      // Inpaint guidance describes selecting an image and painting its redraw area.
      maskHint:
        'Сначала выберите изображение на холсте, затем закрасьте кистью область для перерисовки.',
      imageSize: 'Размер изображения',
      aspectRatio: 'Соотношение сторон',
      quality: 'Качество',
      background: 'Фон',
      thinkingLevel: 'Уровень рассуждений',
      estimatedCost: '{cost} / изображение',
      uploadSource: 'Загрузить изображение',
      promptPlaceholder: 'Опишите изображение, которое хотите создать...',
      // Separate empty states describe an administrator-disabled studio and groups lacking image generation.
      studioDisabled:
        'Творческая студия отключена. Обратитесь к администратору, чтобы включить её.',
      noModelsAvailable:
        'Нет доступных моделей генерации изображений. Попросите администратора настроить группу с включённой генерацией изображений.',
    },
    composer: {
      send: 'Отправить',
      model: 'Модель',
      params: 'Параметры',
      operation: 'Операция',
      selectModel: 'Выберите модель',
      selectModelFirst: 'Сначала выберите модель.',
      autoRatio: 'Автоматически',
    },
    empty: {
      title: 'Начните с описания',
      description:
        'Введите запрос ниже, чтобы сгенерировать изображение, или загрузите своё для редактирования и закрашивания.',
      upload: 'Загрузить изображение',
      examples: {
        poster: {
          label: 'Ретро-плакат о путешествии',
          prompt:
            'Ретро-плакат о путешествии в стиле 1970-х: приморский городок на закате, тёплые оранжевые тона, зернистая текстура бумаги, крупный заголовок без засечек',
        },
        product: {
          label: 'Предметная съёмка',
          prompt:
            'Студийная съёмка флакона духов из матового стекла на светло-сером фоне, мягкий боковой свет, лёгкие блики, простая композиция',
        },
        illustration: {
          label: 'Плоская иллюстрация',
          prompt:
            'Рыжий кот читает книгу у окна, стиль плоской иллюстрации, приглушённые цвета, комнатные растения, послеполуденный свет',
        },
      },
    },
    operations: {
      generate: 'Генерация',
      edit: 'Редактирование',
      inpaint: 'Закрашивание',
    },
    operationsDesc: {
      generate: 'Создать изображение только по текстовому запросу',
      edit: 'Редактировать, взяв выбранное изображение с холста как образец',
      inpaint: 'Закрасить выбранное изображение, чтобы перерисовать эту область',
    },
    aspects: {
      '1x1': '1:1',
      '4x3': '4:3',
      '3x4': '3:4',
      '16x9': '16:9',
      '9x16': '9:16',
    },
    // Quality labels correspond to the generation quality values exposed by model settings.
    qualities: {
      low: 'Низкое',
      medium: 'Среднее',
      high: 'Высокое',
      auto: 'Авто',
    },
    backgrounds: {
      auto: 'Авто',
      opaque: 'Непрозрачный',
      transparent: 'Прозрачный',
    },
    thinkingLevels: {
      minimal: 'Минимальный',
      high: 'Высокий',
    },
    canvas: {
      brushSize: 'Размер кисти',
      shapeRound: 'Круглая кисть',
      shapeSquare: 'Квадратная кисть',
      // Box selection labels identify selection of canvas objects.
      boxSelect: 'Выделить, растянув рамку',
      // Painting controls distinguish drawing from pausing to pan or select another image.
      paintToggleOn: 'Начать рисовать',
      paintToggleOff: 'Приостановить рисование для панорамирования',
      // Undo reverses the newest retained mask operation, including clearing
      // all strokes.
      undoMask: 'Отменить последний штрих',
      clearMask: 'Очистить всю закраску',
      downloadSelected: 'Скачать выбранное изображение',
      removeSelected: 'Удалить выбранное изображение',
      reset: 'Очистить холст',
      backToDashboard: 'Вернуться в панель',
      settings: 'Настройки',
      // Inpaint selection guidance appears before an image is selected.
      inpaintPickHint: 'Нажмите на изображение на холсте, затем закрасьте область для перерисовки',
      // Reference selection guidance describes clicking an image or selecting several with a box.
      editPickHint:
        'Нажмите на изображение, чтобы взять его как образец, или растяните рамку, чтобы выбрать несколько',
      // Mask guidance identifies the painted area sent for redrawing.
      maskPaintHint:
        'Закрасьте область, которую нужно перерисовать. Фиолетовые штрихи уходят как маска.',
    },
    result: {
      actualCost: 'Фактическая стоимость: {cost}',
      missing: 'Отсутствует',
    },
    history: {
      noOutputs: 'Нет готовых изображений',
      title: 'История',
      elapsed: 'Заняло {time}',
      toggle: 'Показать или скрыть историю запусков',
      empty: 'Запусков пока нет.',
      importToCanvas: 'На холст',
      download: 'Скачать',
      copyPrompt: 'Скопировать запрос',
      promptCopied: 'Запрос скопирован',
      reuseParams: 'Подставить в панель',
      // Prompt availability refers to history stored by the submitting browser.
      promptUnavailable: 'В этом браузере не сохранён запрос для данного запуска.',
      // Short parameter labels fit the history sidebar.
      params: {
        group: 'Группа',
        size: 'Размер',
        ratio: 'Пропорции',
        thinking: 'Рассуждения',
        references: 'Образцы',
        count: 'Изображения',
      },
      clearData: 'Удалить локальные данные студии',
      clearSuccess: 'Локальные данные студии удалены.',
      confirmClearTitle: 'Удалить локальные данные студии?',
      confirmClearMessage:
        'Исходные изображения, маски, результаты, сцены холста, черновики и запросы, сохранённые локально, будут удалены без возможности восстановления, а рабочее пространство браузера сброшено. Прежняя история больше не будет отображаться в этом браузере (метаданные задач остаются на сервере), а новые запуски начнутся в новом рабочем пространстве. Уже созданные изображения восстановить нельзя. Баланс аккаунта не изменится.',
    },
    status: {
      queued: 'В очереди',
      running: 'Выполняется',
      provider_succeeded: 'Ожидает расчёта',
      settlement_pending: 'Идёт расчёт',
      release_pending: 'Снятие резерва',
      succeeded: 'Готово',
      failed: 'Ошибка',
      cancelled: 'Отменено',
      result_lost: 'Результат потерян',
      submitting: 'Отправка',
    },
    cropper: {
      title: 'Обрезать изображение',
      hint: 'Перетащите рамку, чтобы задать область обрезки. Обрезанное изображение хранится только в этом браузере.',
      confirm: 'Применить обрезку',
      skip: 'Не обрезать',
    },
    error: {
      loadModelsFailed: 'Не удалось загрузить модели студии. Попробуйте позже.',
      noModel: 'Сначала выберите модель.',
      operationNotSupported: 'Выбранная модель не поддерживает эту операцию.',
      promptTooLong: 'Запрос превышает ограничение сервера.',
      assetTooLarge: 'Изображение превышает ограничения сервера по размеру или формату.',
      sourceRequired: 'Для редактирования и закрашивания нужно хотя бы одно исходное изображение.',
      maskRequired:
        'Для закрашивания нужна маска. Выберите изображение и закрасьте область кистью.',
      referenceLimit: 'Эта модель принимает не более {max} изображений-образцов.',
      submitFailed: 'Не удалось отправить задачу. Повторите попытку.',
      historyFailed:
        'Не удалось загрузить историю запусков. Проверьте разрешения на локальное хранилище в браузере и повторите попытку.',
      workspaceUnavailable:
        'Рабочее пространство браузера недоступно. Разрешите сайту локальное хранилище и повторите попытку.',
      clearFailed: 'Не удалось удалить локальные данные.',
      loadImageFailed: 'Не удалось загрузить изображение.',
      quotaExceeded: 'Локальное хранилище заполнено. Скачайте свои файлы, чтобы продолжить.',
      dropInvalid: 'Не удалось распознать перетащенное изображение из истории.',
      dropUnsupported: 'Поддерживаются только изображения PNG, JPEG и WebP.',
      dropHistoryUnavailable: 'Это изображение из истории больше недоступно в данном браузере.',
    },
  },
}
