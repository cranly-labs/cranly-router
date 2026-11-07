import mcp from './mcp'
import modelAttributes from './modelAttributes'
import overview from './overview'
import pricing from './pricing'
import providers from './providers'
import resources from './resources'
import ops from './ops'
import settings from './settings'
import audit from './audit'
import plugins from './plugins'

// index.ts assembles the English administrator catalog and defines protocol-
// control messages. Imported domain dictionaries contribute their top-level
// namespaces in spread order, while protocol text describes native support,
// conversion choices, and image-tool policies used by configuration forms.

export default {
  plugins,
  protocols: {
    imageModes: {
      title: 'Режимы апстрима',
      hint: 'Запросы конвертируются автоматически. Непотоковые апстримы возвращают готовое изображение без промежуточных превью.',
      both: 'Оба режима',
      non_streaming: 'Только непотоковые',
      streaming: 'Только потоковые',
      generationMode: 'Режимы генерации изображений',
      editMode: 'Режимы редактирования изображений',
    },

    responsesWebSocketHTTPHint:
      'Если конвертация разрешена, клиент сохраняет WebSocket-соединение, а апстрим получает обычные HTTP-запросы Responses.',
    responsesWebSocket: 'Длинные диалоги (WebSocket)',
    responsesWebSocketHTTP: 'Конвертировать в обычный запрос Responses',
    nativeTitle: 'Нативные протоколы апстрима',
    nativeHint:
      'Показаны только нативные протоколы для этого типа провайдера. Пустой набор отключает новые вызовы.',
    loadError: 'Не удалось загрузить возможности протоколов. Откройте форму заново.',
    fallback: 'Конвертировать при недоступности',
    fallbackWhen: 'При недоступности',
    auto: 'Автоматическая конвертация',
    restricted: 'Упорядоченные цели конвертации',
    nativeOnly: 'Только нативные',
    imagePolicy: 'Генерация изображений в диалогах (Responses)',
    imagePolicyHint:
      'Управляет инструментами генерации изображений в диалогах. Отдельные эндпоинты генерации и редактирования изображений не затрагиваются.',
    imagePolicyOptions: {
      inherit: {
        label: 'Следовать настройкам провайдера и глобальным',
        description:
          'Использовать настройку провайдера для этой группы. Если у провайдера нет переопределения, применяется глобальная настройка.',
      },
      enabled: {
        label: 'Автоматически добавлять инструмент изображений',
        description:
          'Добавлять инструмент генерации изображений в обычные диалоги, кроме запросов Responses Lite. Инструменты изображений, переданные клиентом, сохраняются.',
      },
      disabled: {
        label: 'Оставить только инструменты изображений от клиента',
        description:
          'Не добавлять инструмент генерации изображений. Инструменты изображений, уже включённые в запрос клиента, сохраняются.',
      },
      block: {
        label: 'Удалять инструменты изображений',
        description:
          'Не добавлять инструменты изображений и удалять те, что включены в запрос клиента. Прямые вызовы моделей или эндпоинтов изображений не затрагиваются.',
      },
    },
    groupTitle: 'Управление протоколами',
    groupHint:
      'Сначала используется нативный протокол провайдера, затем разрешённые цели конвертации по порядку. В автоматическом режиме применяются маршруты, поддерживаемые сервером.',
  },
  ...modelAttributes,
  ...overview,
  ...pricing,
  ...providers,
  ...resources,
  ...ops,
  ...settings,
  ...audit,
  ...mcp,
}
