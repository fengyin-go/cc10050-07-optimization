<template>
  <section class="recovery-panel" data-testid="batch-intake-panel">
    <div class="recovery-head">
      <div>
        <h3>整批接收与缺页续办<span v-if="taskId" class="muted">（任务 #{{ taskId }}）</span></h3>
        <p>同批页面逐项留回执；中断重提幂等，缺页可多选后一次补传。</p>
      </div>
      <div class="recovery-controls">
        <input
          v-model="batchId"
          data-testid="batch-id-input"
          class="batch-input"
          placeholder="输入批次号，如 JIA-YI-BING-1"
          @keyup.enter="loadBatch"
        />
        <button class="btn" data-testid="load-batch" type="button" @click="loadBatch">打开/续办批次</button>
        <button class="btn primary" data-testid="build-recovery-demo" type="button" :disabled="busy" @click="buildDemo">
          构造甲乙丙中断现场
        </button>
      </div>
    </div>

    <p v-if="message" class="recovery-message" data-testid="batch-message">{{ message }}</p>
    <p v-if="error" class="error-text" data-testid="batch-error">{{ error }}</p>

    <div v-if="taskBatches.length" class="task-batch-list" data-testid="task-batch-list">
      <button
        v-for="item in taskBatches"
        :key="String(item.batch_id)"
        type="button"
        class="btn ghost batch-chip"
        :data-testid="`task-batch-${item.batch_id}`"
        @click="selectTaskBatch(String(item.batch_id))"
      >
        {{ item.batch_id }}（{{ item.status }}）
      </button>
    </div>

    <template v-if="batch">
      <div class="batch-summary" data-testid="batch-summary">
        <strong>{{ batch.batch_id }}</strong>
        <span>状态：{{ batch.status }}</span>
        <span>应到 {{ batch.expected_count }}</span>
        <span>已送达 {{ batch.received_count }}</span>
        <span>已签收 {{ batch.signed_count }}</span>
        <span>缺页 {{ batch.missing_count }}</span>
        <button class="btn ghost" data-testid="select-unsigned-missing" type="button" @click="selectUnsignedMissing">
          仅选未签收缺页
        </button>
        <button
          class="btn primary"
          data-testid="submit-selected-missing"
          type="button"
          :disabled="busy || selectedMissing.length === 0"
          @click="submitSelectedMissing"
        >
          一次补传选中缺项（{{ selectedMissing.length }}）
        </button>
        <button class="btn" data-testid="sign-all-delivered" type="button" :disabled="busy" @click="signAllDelivered">
          逐条签收全部已送达
        </button>
      </div>

      <table class="data-table checklist-table" data-testid="checklist-table">
        <thead>
          <tr>
            <th>补传选择</th>
            <th>页面</th>
            <th>送达清单</th>
            <th>核实</th>
            <th>签收</th>
            <th>解析转交</th>
            <th>样本库</th>
            <th>缺页原因/说明</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in batch.checklist || []"
            :key="String(item.id)"
            :data-testid="`checklist-row-${item.page_key}`"
            :data-delivered="String(item.delivered)"
            :data-signed="String(item.signed)"
          >
            <td>
              <input
                v-if="item.missing"
                type="checkbox"
                :data-testid="`missing-select-${item.page_key}`"
                :value="item.page_key"
                v-model="selectedMissing"
              />
              <span v-else>—</span>
            </td>
            <td>
              <strong>{{ item.page_key }}</strong>
              <div class="muted">位置 {{ item.position }}</div>
              <input
                v-if="item.missing && selectedMissing.includes(item.page_key)"
                v-model="replacementContent[item.page_key]"
                :data-testid="`missing-content-${item.page_key}`"
                class="content-input"
                :placeholder="`${item.page_key} 的完整页面内容`"
              />
            </td>
            <td>
              <span :class="['tag', item.delivered ? 'ok' : 'bad']" :data-testid="`delivery-${item.page_key}`">
                {{ item.delivery_status }}
              </span>
              <div class="muted">重提 {{ item.submit_count }} 次/样本 {{ sampleCount(item) }}</div>
            </td>
            <td>
              <span :class="['tag', item.verification_status === '已核实' ? 'ok' : 'warn']">
                {{ item.verification_status }}
              </span>
            </td>
            <td>
              <span :class="['tag', item.signed ? 'ok' : '']" :data-testid="`sign-status-${item.page_key}`">
                {{ item.sign_status }}
              </span>
            </td>
            <td>
              <span :class="['tag', item.forwarded_to_parser ? 'ok' : '']" data-testid="handover-status">
                {{ item.forwarded_to_parser ? '已转交解析' : '未转交' }}
              </span>
              <div class="muted">{{ item.parse_status }}</div>
            </td>
            <td>
              <RouterLink
                v-if="item.sample_id"
                class="link"
                :to="`/sample_store?focus=${item.sample_id}&batch=${encodeURIComponent(batch.batch_id)}`"
                :data-testid="`open-sample-${item.sample_id}`"
              >
                打开样本 #{{ item.sample_id }}
              </RouterLink>
              <span v-else class="muted">无样本</span>
            </td>
            <td class="reason-cell" :data-testid="`reason-${item.page_key}`">
              {{ item.missing_reason || item.blocked_reason || '—' }}
            </td>
            <td class="row-actions">
              <button
                v-if="item.delivered && !item.signed"
                class="link"
                type="button"
                :data-testid="`sign-${item.page_key}`"
                @click="signPage(item.page_key)"
              >
                签收
              </button>
              <button
                v-if="item.verification_status === '待核实'"
                class="link"
                type="button"
                :data-testid="`verify-${item.page_key}`"
                @click="verifyPage(item.page_key)"
              >
                幂等核实
              </button>
            </td>
          </tr>
        </tbody>
      </table>
      <p class="muted">重新打开时仅递交未签收页：{{ (batch.unsigned_pages || []).join('、') || '无' }}</p>
    </template>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import { getJson, postJson } from '@/api/backend-service'

type BatchLike = {
  batch_id: string
  status?: string
  expected_count?: number
  received_count?: number
  signed_count?: number
  missing_count?: number
  unsigned_pages?: string[]
  checklist?: Array<Record<string, any>>
  [key: string]: unknown
}

const props = defineProps<{ taskId?: number | null }>()
const route = useRoute()
const initialBatch = typeof route.query.batch === 'string'
  ? route.query.batch
  : (typeof window !== 'undefined' ? window.sessionStorage.getItem('price-watch:last-batch') || '' : '')
const batchId = ref(initialBatch)
const batch = ref<BatchLike | null>(null)
const taskBatches = ref<BatchLike[]>([])
const selectedMissing = ref<string[]>([])
const replacementContent = ref<Record<string, string>>({})
const busy = ref(false)
const message = ref('')
const error = ref('')

function sampleCount(item: Record<string, any>): number | string {
  return item.sample_id ? 1 : 0
}

function rememberBatch(id: string) {
  batchId.value = id
  try {
    window.sessionStorage.setItem('price-watch:last-batch', id)
  } catch {
    // 隐私模式等场景不影响接口流程。
  }
}

async function loadTaskBatches(taskId: number) {
  const data = await getJson<{ items: BatchLike[] }>(`/api/price_crawler/batches?task_id=${taskId}`)
  taskBatches.value = data.items || []
  return taskBatches.value
}

async function openForTask(taskId: number) {
  try {
    const items = await loadTaskBatches(taskId)
    if (items.length) {
      await selectTaskBatch(String(items[0].batch_id))
    } else {
      message.value = `任务 #${taskId} 暂无上传批次，可先构造演练现场或通过接收接口建批`
    }
  } catch (e) {
    error.value = e instanceof Error ? e.message : '任务批次读取失败'
  }
}

async function selectTaskBatch(id: string) {
  rememberBatch(id)
  await loadBatch()
}

defineExpose({ openForTask })

watch(() => props.taskId, (value) => {
  if (value) void openForTask(value)
})

onMounted(() => {
  if (batchId.value) void loadBatch()
})

async function buildDemo() {
  busy.value = true
  error.value = ''
  message.value = ''
  try {
    const result = await postJson<{ batch_id: string; batch: BatchLike; message?: string }>(
      '/api/price_crawler/recovery-demo',
      {},
    )
    rememberBatch(result.batch_id)
    batch.value = result.batch
    message.value = result.message || '演练现场已构造'
    seedContents()
  } catch (e) {
    error.value = e instanceof Error ? e.message : '构造失败'
  } finally {
    busy.value = false
  }
}

async function loadBatch() {
  if (!batchId.value.trim()) return
  busy.value = true
  error.value = ''
  const id = encodeURIComponent(batchId.value.trim())
  try {
    rememberBatch(batchId.value.trim())
    batch.value = await getJson<BatchLike>(`/api/price_crawler/batches/${id}`)
    seedContents()
    message.value = `已重新打开批次，未签收页：${batch.value.unsigned_pages?.join('、') || '无'}`
  } catch (e) {
    error.value = e instanceof Error ? e.message : '批次读取失败'
  } finally {
    busy.value = false
  }
}

function seedContents() {
  for (const item of batch.value?.checklist || []) {
    if (item.missing && !replacementContent.value[item.page_key]) {
      replacementContent.value[item.page_key] = `<html>${item.page_key}补齐后的完整价格页</html>`
    }
  }
}

function selectUnsignedMissing() {
  selectedMissing.value = (batch.value?.checklist || [])
    .filter((item) => item.missing && !item.signed)
    .map((item) => String(item.page_key))
  seedContents()
}

async function submitSelectedMissing() {
  if (!batch.value || selectedMissing.value.length === 0) return
  busy.value = true
  error.value = ''
  try {
    const key = encodeURIComponent(batch.value.batch_id)
    const pages = selectedMissing.value.map((pageKey) => ({
      page_key: pageKey,
      content: replacementContent.value[pageKey] || `<html>${pageKey}补齐后的完整价格页</html>`,
    }))
    batch.value = await postJson<BatchLike>(`/api/price_crawler/batches/${key}/missing`, {
      page_keys: selectedMissing.value,
      pages,
    })
    message.value = `已只提交缺项：${selectedMissing.value.join('、')}`
    selectedMissing.value = []
  } catch (e) {
    error.value = e instanceof Error ? e.message : '补传失败'
  } finally {
    busy.value = false
  }
}

async function signPage(pageKey: string) {
  if (!batch.value) return
  const key = encodeURIComponent(batch.value.batch_id)
  try {
    batch.value = await postJson<BatchLike>(`/api/price_crawler/batches/${key}/sign`, {
      page_keys: [pageKey],
    })
    message.value = `${pageKey} 已逐项签收，可从其样本项打开样本库`
  } catch (e) {
    error.value = e instanceof Error ? e.message : '签收失败'
  }
}

async function signAllDelivered() {
  if (!batch.value) return
  const key = encodeURIComponent(batch.value.batch_id)
  try {
    batch.value = await postJson<BatchLike>(`/api/price_crawler/batches/${key}/sign`, {
      sign_all_received: true,
    })
    message.value = '已逐条确认所有已送达页面'
  } catch (e) {
    error.value = e instanceof Error ? e.message : '签收失败'
  }
}

async function verifyPage(pageKey: string) {
  if (!batch.value) return
  const key = encodeURIComponent(batch.value.batch_id)
  try {
    await postJson(`/api/price_crawler/batches/${key}/verify`, { page_key: pageKey, verified: true })
    await loadBatch()
    message.value = `${pageKey} 的原写入已核实，重复请求没有生成额外样本`
  } catch (e) {
    error.value = e instanceof Error ? e.message : '核实失败'
  }
}
</script>

<style scoped>
.recovery-panel {
  background: #fff;
  border: 1px solid var(--border, #e4e7ec);
  border-radius: 10px;
  padding: 16px;
  margin: 14px 0;
}
.recovery-head { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; flex-wrap: wrap; }
.recovery-head h3 { margin: 0 0 4px; }
.recovery-head p { margin: 0; color: #667085; font-size: 13px; }
.recovery-controls { display: flex; gap: 8px; flex-wrap: wrap; }
.batch-input { min-width: 260px; padding: 7px 10px; border: 1px solid #d0d5dd; border-radius: 6px; }
.batch-summary { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; margin: 14px 0; font-size: 13px; }
.checklist-table { margin-top: 8px; }
.muted { color: #667085; font-size: 12px; margin-top: 2px; }
.tag { display: inline-block; padding: 2px 8px; border-radius: 999px; background: #f2f4f7; font-size: 12px; }
.tag.ok { background: #dcfae6; color: #067647; }
.tag.warn { background: #fef0c7; color: #b54708; }
.tag.bad { background: #fee4e2; color: #b42318; }
.reason-cell { max-width: 240px; font-size: 12px; color: #b42318; }
.content-input { width: 180px; margin-top: 6px; padding: 4px 6px; border: 1px solid #d0d5dd; border-radius: 4px; }
.recovery-message { color: #067647; font-size: 13px; }
.task-batch-list { display: flex; gap: 8px; flex-wrap: wrap; margin: 10px 0; }
.batch-chip { font-size: 12px; }
.error-text { color: #b42318; font-size: 13px; }
</style>
