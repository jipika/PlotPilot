import { createDiscreteApi } from 'naive-ui'
import { llmControlApi, type LLMRuntimeSummary } from '@/api/llmControl'
import { useAppSettingsShellStore } from '@/stores/appSettingsShellStore'

const { dialog } = createDiscreteApi(['dialog'])

let cachedRuntime: LLMRuntimeSummary | null = null
let cacheAt = 0
const CACHE_MS = 4000

export function invalidateLlmRuntimeCache(): void {
  cachedRuntime = null
  cacheAt = 0
}

export async function fetchLlmRuntime(): Promise<LLMRuntimeSummary> {
  const now = Date.now()
  if (cachedRuntime && now - cacheAt < CACHE_MS) {
    return cachedRuntime
  }
  const panel = await llmControlApi.getPanel()
  cachedRuntime = panel.runtime
  cacheAt = now
  return panel.runtime
}

export function isLlmRuntimeReady(runtime: LLMRuntimeSummary): boolean {
  return !runtime.using_mock
}

/** @returns true = 已配置可继续；false = 已弹窗拦截 */
export async function ensureLlmConfigured(): Promise<boolean> {
  const runtime = await fetchLlmRuntime()
  if (isLlmRuntimeReady(runtime)) {
    return true
  }

  const shell = useAppSettingsShellStore()
  const reason =
    runtime.reason ||
    '未配置可用的 API Key 与模型名，无法使用 AI 生成。请在「应用设置 → 模型引擎」中完成配置。'

  return new Promise((resolve) => {
    dialog.warning({
      title: '请先配置大模型',
      content: reason,
      positiveText: '去配置',
      negativeText: '取消',
      onPositiveClick: () => {
        shell.open('engine')
        resolve(false)
      },
      onNegativeClick: () => {
        resolve(false)
      },
    })
  })
}

export function parseLlmNotConfiguredDetail(detail: unknown): string | null {
  if (detail == null) return null
  if (typeof detail === 'object' && detail !== null) {
    const d = detail as { code?: string; message?: string }
    if (d.code === 'LLM_NOT_CONFIGURED') {
      return d.message || '请先配置大模型'
    }
  }
  if (typeof detail === 'string' && detail.includes('LLM_NOT_CONFIGURED')) {
    return detail
  }
  return null
}
