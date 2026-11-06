import { computed, ref } from 'vue'
import { fetchEventSource } from '@microsoft/fetch-event-source'
import { apiBaseUrl } from '@/config'

// 增量文本逐片更新会引发频繁布局，按 80ms 合并刷新
export const STREAM_FLUSH_INTERVAL = 80

// 六个状态覆盖一次生成的全部落点，没有“未知态”，UI 可以完全由状态派生
export const STREAM_STATUS = Object.freeze({
  IDLE: 'idle',
  CONNECTING: 'connecting',
  STREAMING: 'streaming',
  DONE: 'done',
  ABORTED: 'aborted',
  ERROR: 'error'
})

// 事件字母表：状态机只认这些输入。
// 回调与用户操作都先翻译成事件，再由迁移表决定走到哪、进状态时做什么。
export const STREAM_EVENT = Object.freeze({
  START: 'start',
  OPEN_OK: 'open-ok',
  OPEN_UNAUTHORIZED: 'open-unauthorized',
  OPEN_INVALID: 'open-invalid',
  CHUNK: 'chunk',
  DONE: 'done',
  CLOSE: 'close',
  SETTLED: 'settled',
  FAIL: 'fail',
  STOP: 'stop',
  ABORT: 'abort',
  RESET: 'reset'
})

/**
 * 一次流式生成的生命周期：连接、聚合、停止、失败。
 *
 * 边界：不碰消息列表、不碰滚动、不碰情绪面板、不碰 DOM。
 * 组件只通过回调拿到"当前累计文本 / 结束 / 中止 / 失败"。
 *
 * 状态机：idle -> connecting -> streaming -> done | aborted | error
 * 迁移表同时决定“走不走 / 走到哪 / 进状态时执行什么副作用”，
 * 回调只负责把原始信号翻译成事件，不再各自携带策略。
 * 表里没有的格子 = 显式忽略（例如停止之后又到达的 done / chunk）。
 * 另叠加 generation 代数守卫，在事件投递入口就把上一代事件过滤掉。
 */
export function useChatStream({
  transport = fetchEventSource,
  endpoint = `${apiBaseUrl}/psychological-chat/stream`
} = {}) {
  const status = ref(STREAM_STATUS.IDLE)
  const content = ref('')
  const error = ref(null)

  const isActive = computed(
    () =>
      status.value === STREAM_STATUS.CONNECTING || status.value === STREAM_STATUS.STREAMING
  )
  const isSettled = computed(
    () =>
      status.value === STREAM_STATUS.DONE ||
      status.value === STREAM_STATUS.ABORTED ||
      status.value === STREAM_STATUS.ERROR
  )

  let controller = null
  let buffer = ''
  let flushTimer = null
  // 代数守卫：停止或切换会话后，旧流的回调与已排队的 flush 一律失效。
  // 只靠 controller 判断挡不住已经排进定时器的那一片增量。
  let generation = 0
  let handlers = {}

  const clearFlushTimer = () => {
    if (flushTimer) {
      clearTimeout(flushTimer)
      flushTimer = null
    }
  }

  const flush = () => {
    clearFlushTimer()
    if (!buffer) return
    const pending = buffer
    buffer = ''
    content.value += pending
    handlers.onUpdate?.(content.value)
  }

  const discardBuffer = () => {
    buffer = ''
    clearFlushTimer()
  }

  // 静默掐掉在途连接：不迁移状态、不触发回调，由调用方决定去向
  const abandonActive = (shouldAbort) => {
    if (!shouldAbort) return
    const currentController = controller
    controller = null
    generation += 1
    discardBuffer()
    currentController?.abort()
  }

  const begin = (payload) => {
    const currentGeneration = ++generation
    const currentController = new AbortController()
    controller = currentController
    handlers = payload.handlers
    content.value = ''
    error.value = null
    discardBuffer()

    transport(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Token: payload.token,
        Accept: 'text/event-stream'
      },
      body: JSON.stringify({ sessionId: payload.sessionId, userMessage: payload.userMessage }),
      signal: currentController.signal,
      onopen: (response) => {
        if (currentGeneration !== generation) return

        if (response.status === 401 || response.status === 403) {
          dispatch(STREAM_EVENT.OPEN_UNAUTHORIZED, { generation: currentGeneration })
          return
        }

        const contentType = response.headers.get('Content-Type') || ''
        if (!response.ok || !contentType.includes('text/event-stream')) {
          dispatch(STREAM_EVENT.OPEN_INVALID, {
            generation: currentGeneration,
            message: '服务器响应异常，请稍后重试'
          })
          return
        }

        dispatch(STREAM_EVENT.OPEN_OK, { generation: currentGeneration })
      },
      onmessage: (event) => {
        if (currentGeneration !== generation) return

        const raw = event.data.trim()
        if (!raw) return

        if (event.event === 'done') {
          dispatch(STREAM_EVENT.DONE, { generation: currentGeneration })
          return
        }

        try {
          const payload = JSON.parse(raw)
          const ok = String(payload.code) === '200'
          if (ok && payload.data && payload.data.content) {
            dispatch(STREAM_EVENT.CHUNK, {
              generation: currentGeneration,
              text: payload.data.content
            })
          } else {
            dispatch(STREAM_EVENT.FAIL, {
              generation: currentGeneration,
              message: payload.message || 'AI回复失败'
            })
          }
        } catch (err) {
          console.error('解析流式数据异常:', err)
        }
      },
      onerror: (err) => {
        if (currentGeneration !== generation) return
        // 主动中止不是错误：由 STOP / ABORT 负责收尾
        if (err?.name === 'AbortError') return
        console.error('SSE 连接异常:', err)
        dispatch(STREAM_EVENT.FAIL, {
          generation: currentGeneration,
          message: '网络连接中断或AI回复失败'
        })
        // 抛出让 fetch-event-source 停止重连
        throw err
      },
      onclose: () => {
        if (currentGeneration !== generation) return
        dispatch(STREAM_EVENT.CLOSE, { generation: currentGeneration })
      }
    })
      .catch(() => {
        // 中止与错误已由统一回调处理，这里只消费 Promise rejection
      })
      .finally(() => {
        if (currentGeneration !== generation) return
        dispatch(STREAM_EVENT.SETTLED, { generation: currentGeneration })
      })
  }

  // 生成中再次发起：先静默掐掉上一代，再开新连接
  const restart = (payload) => {
    abandonActive(true)
    begin(payload)
  }

  const append = (payload) => {
    if (!payload?.text) return
    buffer += payload.text
    if (flushTimer) return
    const currentGeneration = generation
    flushTimer = setTimeout(() => {
      flushTimer = null
      if (currentGeneration === generation) flush()
    }, STREAM_FLUSH_INTERVAL)
  }

  // 收到 done 事件：落盘缓冲、通知组件、主动关流
  const finish = () => {
    flush()
    handlers.onDone?.()
    controller?.abort()
  }

  // 连接被关闭 / Promise 落定但还在生成中：只落盘，不回调
  const settleActive = () => {
    flush()
    controller = null
  }

  // 终态之后 Promise 才落定：只清引用，不再触发任何行为
  const settle = () => {
    controller = null
  }

  const failWith = (payload) => {
    flush()
    const message = payload?.message || 'AI回复失败'
    error.value = message
    handlers.onError?.(message, content.value)
    controller?.abort()
  }

  const unauthorized = () => {
    discardBuffer()
    const message = '登录已过期，请重新登录'
    error.value = message
    handlers.onUnauthorized?.()
    controller?.abort()
  }

  // 用户主动停止：保留已生成内容，通知组件收尾
  const stopActive = () => {
    const currentController = controller
    controller = null
    flush()
    generation += 1
    currentController?.abort()
    handlers.onAbort?.(content.value)
  }

  // 硬重置：掐掉在途流并回到 idle
  const resetAll = (_payload, from) => {
    abandonActive(from === STREAM_STATUS.CONNECTING || from === STREAM_STATUS.STREAMING)
    content.value = ''
    error.value = null
    handlers = {}
  }

  // 迁移表：(状态, 事件) -> { 目标状态, 进入目标状态时执行的副作用 }
  // 未列出的格子视为显式忽略，不再需要在各回调里散写 isActive 判断。
  const table = Object.freeze({
    [STREAM_STATUS.IDLE]: {
      [STREAM_EVENT.START]: { to: STREAM_STATUS.CONNECTING, effect: begin },
      [STREAM_EVENT.RESET]: { to: STREAM_STATUS.IDLE, effect: resetAll },
      [STREAM_EVENT.SETTLED]: { to: STREAM_STATUS.IDLE, effect: settle }
    },
    [STREAM_STATUS.CONNECTING]: {
      [STREAM_EVENT.START]: { to: STREAM_STATUS.CONNECTING, effect: restart },
      [STREAM_EVENT.OPEN_OK]: { to: STREAM_STATUS.STREAMING },
      [STREAM_EVENT.OPEN_UNAUTHORIZED]: { to: STREAM_STATUS.ERROR, effect: unauthorized },
      [STREAM_EVENT.OPEN_INVALID]: { to: STREAM_STATUS.ERROR, effect: failWith },
      [STREAM_EVENT.CHUNK]: { to: STREAM_STATUS.STREAMING, effect: append },
      [STREAM_EVENT.DONE]: { to: STREAM_STATUS.DONE, effect: finish },
      [STREAM_EVENT.CLOSE]: { to: STREAM_STATUS.DONE, effect: flush },
      [STREAM_EVENT.SETTLED]: { to: STREAM_STATUS.DONE, effect: settleActive },
      [STREAM_EVENT.FAIL]: { to: STREAM_STATUS.ERROR, effect: failWith },
      [STREAM_EVENT.STOP]: { to: STREAM_STATUS.ABORTED, effect: stopActive },
      [STREAM_EVENT.ABORT]: { to: STREAM_STATUS.ABORTED, effect: () => abandonActive(true) },
      [STREAM_EVENT.RESET]: { to: STREAM_STATUS.IDLE, effect: resetAll }
    },
    [STREAM_STATUS.STREAMING]: {
      [STREAM_EVENT.START]: { to: STREAM_STATUS.CONNECTING, effect: restart },
      [STREAM_EVENT.CHUNK]: { to: STREAM_STATUS.STREAMING, effect: append },
      [STREAM_EVENT.DONE]: { to: STREAM_STATUS.DONE, effect: finish },
      [STREAM_EVENT.CLOSE]: { to: STREAM_STATUS.DONE, effect: flush },
      [STREAM_EVENT.SETTLED]: { to: STREAM_STATUS.DONE, effect: settleActive },
      [STREAM_EVENT.FAIL]: { to: STREAM_STATUS.ERROR, effect: failWith },
      [STREAM_EVENT.STOP]: { to: STREAM_STATUS.ABORTED, effect: stopActive },
      [STREAM_EVENT.ABORT]: { to: STREAM_STATUS.ABORTED, effect: () => abandonActive(true) },
      [STREAM_EVENT.RESET]: { to: STREAM_STATUS.IDLE, effect: resetAll }
    },
    [STREAM_STATUS.DONE]: {
      [STREAM_EVENT.START]: { to: STREAM_STATUS.CONNECTING, effect: begin },
      [STREAM_EVENT.RESET]: { to: STREAM_STATUS.IDLE, effect: resetAll },
      [STREAM_EVENT.SETTLED]: { to: STREAM_STATUS.DONE, effect: settle }
    },
    [STREAM_STATUS.ABORTED]: {
      [STREAM_EVENT.START]: { to: STREAM_STATUS.CONNECTING, effect: begin },
      [STREAM_EVENT.RESET]: { to: STREAM_STATUS.IDLE, effect: resetAll },
      [STREAM_EVENT.SETTLED]: { to: STREAM_STATUS.ABORTED, effect: settle }
    },
    [STREAM_STATUS.ERROR]: {
      [STREAM_EVENT.START]: { to: STREAM_STATUS.CONNECTING, effect: begin },
      [STREAM_EVENT.RESET]: { to: STREAM_STATUS.IDLE, effect: resetAll },
      [STREAM_EVENT.SETTLED]: { to: STREAM_STATUS.ERROR, effect: settle }
    }
  })

  // 状态迁移与副作用的唯一入口
  const dispatch = (event, payload = {}) => {
    // 代数守卫：上一代的事件连迁移表都进不了
    if (payload.generation !== undefined && payload.generation !== generation) return false

    const from = status.value
    const edge = table[from]?.[event]
    if (!edge) return false

    status.value = edge.to
    edge.effect?.(payload, from)
    return true
  }

  const start = ({
    sessionId,
    userMessage,
    token,
    onUpdate,
    onDone,
    onAbort,
    onError,
    onUnauthorized
  }) => {
    dispatch(STREAM_EVENT.START, {
      sessionId,
      userMessage,
      token,
      handlers: { onUpdate, onDone, onAbort, onError, onUnauthorized }
    })
  }

  // 用户主动停止：保留已生成内容，通知组件收尾
  const stop = () => dispatch(STREAM_EVENT.STOP)

  // 静默中止：切换会话、卸载组件时用，不触发任何回调
  const abort = () => {
    dispatch(STREAM_EVENT.ABORT)
  }

  const reset = () => {
    dispatch(STREAM_EVENT.RESET)
  }

  return {
    status,
    content,
    error,
    isActive,
    isSettled,
    start,
    stop,
    abort,
    reset
  }
}
