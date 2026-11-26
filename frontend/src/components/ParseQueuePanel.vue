<template>
  <section class="queue-panel" data-testid="parse-queue-panel">
    <div class="queue-head">
      <div>
        <h3>样本批次解析队列</h3>
        <p>队列只确认实际写成的样本；领取与结果都会写入该样本批次的逐条处理记录。</p>
      </div>
      <div class="queue-controls">
        <input
          v-model="batchFilter"
          class="batch-input"
          data-testid="parse-batch-filter"
          placeholder="按样本/上传批次过滤"
          @keyup.enter="loadQueue"
        />
        <button class="btn" data-testid="refresh-parse-queue" type="button" @click="loadQueue">刷新/确认可领取项</button>
      </div>
    </div>

    <p v-if="message" class="ok-text" data-testid="parse-message">{{ message }}</p>
    <p v-if="error" class="error-text" data-testid="parse-error">{{ error }}</p>

    <table class="data-table" data-testid="parse-queue-table">
      <thead>
        <tr>
          <th>样本</th>
          <th>实际样本批次</th>
          <th>页面</th>
          <th>队列状态/原因</th>
          <th>领取单</th>
          <th>结果次数</th>
          <th>结果内容</th>
          <th>操作</th>
          <th>逐条处理记录</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="item in items"
          :key="String(item.sample_id)"
          :data-testid="`queue-row-${item.sample_id}`"
          :data-focus="String(item.sample_id) === focusSample ? 'true' : 'false'"
          :class="{ focused: String(item.sample_id) === focusSample }"
        >
          <td><RouterLink :to="`/sample_store?focus=${item.sample_id}`">#{{ item.sample_id }}</RouterLink></td>
          <td data-testid="actual-sample-batch">{{ item.sample_batch }}</td>
          <td>{{ item.page_key || '—' }}</td>
          <td>
            <span :class="['tag', item.claimable ? 'ok' : item.status === '已解析' ? 'ok' : 'warn']">
              {{ item.status }}
            </span>
            <div class="muted" data-testid="claim-reason">{{ item.blocked_reason || '可领取' }}</div>
          </td>
          <td data-testid="claim-id-cell">{{ item.claim_id || '—' }}</td>
          <td data-testid="result-count-cell">
            {{ item.result_count || 0 }}
            <span v-if="(item.result_delivery_count || 0) > 1" class="muted">
              （送达回执 {{ item.result_delivery_count }}）
            </span>
          </td>
          <td>
            <input
              v-model="resultValues[item.sample_id]"
              class="result-input"
              :data-testid="`result-value-${item.sample_id}`"
              placeholder="解析价格，如 99.5"
            />
          </td>
          <td class="row-actions">
            <button
              v-if="item.claimable"
              class="link"
              type="button"
              :data-testid="`claim-${item.sample_id}`"
              @click="claim(item)"
            >
              领取
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`submit-result-${item.sample_id}`"
              @click="submitResult(item)"
            >
              提交结果
            </button>
          </td>
          <td>
            <ul class="record-list" data-testid="processing-records">
              <li v-for="record in item.processing_records || []" :key="String(record.id || record.at + record.type)">
                {{ record.message }} <span class="muted">{{ record.claim_id || '' }}</span>
              </li>
            </ul>
          </td>
        </tr>
        <tr v-if="!items.length">
          <td colspan="9" class="empty-state">暂无解析队列项</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { nextTick, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

import { getJson, postJson } from '@/api/backend-service'

type QueueItem = {
  sample_id: number
  sample_batch: string
  page_key?: string
  status: string
  claimable: boolean
  blocked_reason?: string
  claim_id?: string
  result_count?: number
  result_delivery_count?: number
  processing_records?: Array<Record<string, any>>
  [key: string]: unknown
}

const route = useRoute()
const items = ref<QueueItem[]>([])
const batchFilter = ref(typeof route.query.batch === 'string' ? route.query.batch : '')
const focusSample = ref(typeof route.query.focus === 'string' ? route.query.focus : '')
const resultValues = ref<Record<number, string>>({})
const message = ref('')
const error = ref('')

async function loadQueue() {
  error.value = ''
  const query = batchFilter.value.trim() ? `?batch_id=${encodeURIComponent(batchFilter.value.trim())}` : ''
  const data = await getJson<{ items: QueueItem[] }>(`/api/sample_store/parse-queue${query}`)
  items.value = data.items || []
}

async function claim(item: QueueItem) {
  error.value = ''
  try {
    const result = await postJson<{ ok: boolean; claim_id?: string; message?: string; continued?: boolean }>(
      '/api/sample_store/parse-queue/claim',
      {
        batch_id: batchFilter.value || undefined,
        sample_id: item.sample_id,
        page_key: item.page_key,
      },
    )
    if (!result.ok) throw new Error(result.message || '领取失败')
    message.value = result.continued ? '已继续原来那次领取' : `已确认领取单 ${result.claim_id}`
    await loadQueue()
  } catch (e) {
    error.value = e instanceof Error ? e.message : '领取失败'
  }
}

async function submitResult(item: QueueItem) {
  error.value = ''
  const raw = resultValues.value[item.sample_id] || '99.00'
  const numeric = Number(raw)
  const parseResult = Number.isFinite(numeric) && raw.trim() !== '' ? { price: numeric } : { value: raw }
  try {
    const result = await postJson<{ ok: boolean; duplicate?: boolean; message?: string }>(
      '/api/sample_store/results',
      {
        batch_id: batchFilter.value || item.batch_id,
        claim_id: item.claim_id,
        sample_id: item.sample_id,
        page_key: item.page_key,
        result: parseResult,
      },
    )
    if (!result.ok) throw new Error(result.message || '结果提交失败')
    message.value = result.duplicate ? '相同结果重提：仍只保留一份解析结果' : '解析结果已写入样本批次记录'
    await loadQueue()
  } catch (e) {
    error.value = e instanceof Error ? e.message : '结果提交失败'
  }
}

onMounted(() => {
  loadQueue()
    .then(async () => {
      if (focusSample.value) {
        await nextTick()
        document.querySelector(`[data-testid="queue-row-${focusSample.value}"]`)?.scrollIntoView({
          behavior: 'smooth', block: 'center',
        })
      }
    })
    .catch((e: unknown) => {
      error.value = e instanceof Error ? e.message : '队列读取失败'
    })
})
</script>

<style scoped>
.queue-panel { background: #fff; border: 1px solid var(--border, #e4e7ec); border-radius: 10px; padding: 16px; margin: 14px 0; }
.queue-head { display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; align-items: flex-start; }
.queue-head h3 { margin: 0 0 4px; }
.queue-head p { margin: 0; color: #667085; font-size: 13px; }
.queue-controls { display: flex; gap: 8px; }
.batch-input { min-width: 240px; padding: 7px 10px; border: 1px solid #d0d5dd; border-radius: 6px; }
.result-input { width: 110px; padding: 4px 6px; border: 1px solid #d0d5dd; border-radius: 4px; }
.tag { display: inline-block; padding: 2px 8px; border-radius: 999px; background: #f2f4f7; font-size: 12px; }
.tag.ok { background: #dcfae6; color: #067647; }
.tag.warn { background: #fef0c7; color: #b54708; }
.muted { color: #667085; font-size: 12px; }
.ok-text { color: #067647; font-size: 13px; }
.error-text { color: #b42318; font-size: 13px; }
.record-list { margin: 0; padding-left: 16px; max-width: 260px; font-size: 12px; }
tr.focused { outline: 2px solid #1f6feb; background: #f0f7ff; }
</style>
