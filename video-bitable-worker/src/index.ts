interface Env {
  FEISHU_APP_TOKEN: string;
  FEISHU_TABLE_ID: string;
  VERCEL_CONSUMER_URL: string;
  VIDEO_ANALYSIS_QUEUE: Queue;
  RATE_LIMIT_QPS?: string; // 可选：控频（默认 1）
  TOKEN_PROXY_SERVICE?: Fetcher; // Service Binding：获取飞书 tenant_access_token（可选）
  VERCEL_PROTECTION_BYPASS?: string; // 可选：Vercel 保护绕过 Token
}

type FeishuRecord = { record_id: string; fields: Record<string, any> };

function createCors(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    },
  });
}

function extractText(value: any): string {
  if (value == null) return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (Array.isArray(value)) {
    const parts = value.map((v) => {
      if (v && typeof v === 'object') {
        return v.text || v.name || v.value || '';
      }
      return String(v ?? '');
    }).filter(Boolean);
    return parts.join(' ');
  }
  if (typeof value === 'object') {
    return value.text || value.name || value.value || value.date || '';
  }
  return '';
}

function parseBeijingDateOnly(text: string): string | null {
  // 输入示例：2025/08/10 13:42:43
  if (!text || typeof text !== 'string') return null;
  const m = text.match(/^(\d{4})\/(\d{2})\/(\d{2})/);
  if (!m) return null;
  return `${m[1]}-${m[2]}-${m[3]}`; // YYYY-MM-DD
}

function getYesterdayDateStrInBeijing(now = new Date()): string {
  // 将 now 视为本地时间，转换到北京时区的日期（简化：按 UTC 偏移 +8 计算）
  const utc = now.getTime() + now.getTimezoneOffset() * 60000;
  const beijingNow = new Date(utc + 8 * 3600000);
  const y = new Date(beijingNow.getTime() - 24 * 3600000);
  const yyyy = y.getUTCFullYear();
  const mm = String(y.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(y.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

async function searchVideoRecordsToProcess(env: Env, accessToken: string): Promise<FeishuRecord[]> {
  const url = `https://open.feishu.cn/open-apis/bitable/v1/apps/${env.FEISHU_APP_TOKEN}/tables/${env.FEISHU_TABLE_ID}/records/search`;
  // 仅按“是否发起分析为空”初筛，日期逻辑在本地二次过滤（Time 为文本）
  const payload = {
    filter: {
      conjunction: 'and',
      conditions: [
        { field_name: 'Video ID', operator: 'isNotEmpty', value: [] },
        { field_name: '是否发起分析', operator: 'isEmpty', value: [] },
      ],
    },
    page_size: 500,
  };

  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json; charset=utf-8',
    },
    body: JSON.stringify(payload),
  });
  const data = await resp.json<any>();
  if (!resp.ok || data.code !== 0) {
    throw new Error(`Feishu search failed: ${resp.status} ${data.msg || ''}`);
  }
  return data.data?.items || [];
}

async function getFieldsMeta(env: Env, accessToken: string): Promise<Array<{ field_id: string; field_name: string; type: number }>> {
  const url = `https://open.feishu.cn/open-apis/bitable/v1/apps/${env.FEISHU_APP_TOKEN}/tables/${env.FEISHU_TABLE_ID}/fields`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  const data = await resp.json<any>();
  if (!resp.ok || data.code !== 0) throw new Error(`Feishu list fields failed: ${resp.status} ${data.msg || ''}`);
  return data.data?.items || [];
}

async function ensureTextField(env: Env, accessToken: string, fieldName: string): Promise<void> {
  const fields = await getFieldsMeta(env, accessToken);
  if (fields.some((f) => f.field_name === fieldName)) return;
  const url = `https://open.feishu.cn/open-apis/bitable/v1/apps/${env.FEISHU_APP_TOKEN}/tables/${env.FEISHU_TABLE_ID}/fields`;
  const payload = { field_name: fieldName, type: 1 }; // 1: 文本
  const resp = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(payload),
  });
  const data = await resp.json<any>();
  if (!resp.ok || data.code !== 0) throw new Error(`Create field '${fieldName}' failed: ${resp.status} ${data.msg || ''}`);
}

async function batchUpdateRecords(env: Env, accessToken: string, updates: Array<{ record_id: string; fields: Record<string, any> }>) {
  if (updates.length === 0) return;
  const url = `https://open.feishu.cn/open-apis/bitable/v1/apps/${env.FEISHU_APP_TOKEN}/tables/${env.FEISHU_TABLE_ID}/records/batch_update`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify({ records: updates }),
  });
  const data = await resp.json<any>();
  if (!resp.ok || data.code !== 0) {
    throw new Error(`Feishu batch_update failed: ${resp.status} ${data.msg || ''}`);
  }
}

async function getAccessTokenViaProxy(env: Env): Promise<string> {
  // 使用 Service Binding 调用你已绑定的 Token 代理服务
  if (!env.TOKEN_PROXY_SERVICE) throw new Error('TOKEN_PROXY_SERVICE 未绑定，且请求体未提供 accessToken');
  const resp = await env.TOKEN_PROXY_SERVICE.fetch('http://proxy/token');
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`Token proxy failed: ${resp.status} ${text}`);
  }
  const data = await resp.json<any>().catch(() => ({}));
  const token = data.tenant_access_token || data.access_token || data.token;
  if (!token) throw new Error('Token proxy returned no tenant_access_token');
  return token;
}

async function fetchRecordById(env: Env, accessToken: string, recordId: string): Promise<FeishuRecord | null> {
  const url = `https://open.feishu.cn/open-apis/bitable/v1/apps/${env.FEISHU_APP_TOKEN}/tables/${env.FEISHU_TABLE_ID}/records/${recordId}`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  const data = await resp.json<any>().catch(() => ({}));
  if (!resp.ok || data.code !== 0) return null;
  const rec = data.data?.record;
  if (!rec) return null;
  return { record_id: rec.record_id, fields: rec.fields || {} };
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (request.method === 'OPTIONS') return createCors({}, 200);
    if (request.method !== 'POST') return createCors({ error: 'Use POST' }, 405);

    try {
      const body = await request.json<any>().catch(() => ({}));
      const accessToken: string = body.accessToken || (await getAccessTokenViaProxy(env));

      // 1) 搜索候选记录
      const candidates = await searchVideoRecordsToProcess(env, accessToken);

      const yesterday = getYesterdayDateStrInBeijing();
      const needProcess: FeishuRecord[] = candidates.filter((rec) => {
        const timeTextRaw = rec.fields['Time'];
        const timeText = extractText(timeTextRaw).trim();
        const dateOnly = parseBeijingDateOnly(timeText);
        const statusVal = extractText(rec.fields['是否发起分析']).trim();
        return dateOnly === yesterday && !statusVal;
      });

      // 调试模式：返回筛选前后关键信息，便于定位字段名或日期格式问题
      if (body && body.debug === true) {
        // 支持按指定 record_id 精准预览：body.debugRecordIds = ["rec...", "rec..."]
        const debugIdsRaw = Array.isArray(body.debugRecordIds) ? body.debugRecordIds : undefined;
        const debugIdSet = debugIdsRaw ? new Set(debugIdsRaw.map((v: any) => String(v))) : undefined;
        let picked: FeishuRecord[] = [];
        if (debugIdSet) {
          // 先从本页 candidates 命中
          picked = candidates.filter((rec) => debugIdSet.has(String(rec.record_id)));
          // 若仍缺失，则逐个直接从飞书按 record_id 拉取
          const missing = Array.from(debugIdSet).filter((id) => !picked.some((p) => String(p.record_id) === String(id)));
          for (const id of missing) {
            const rec = await fetchRecordById(env, accessToken, id);
            if (rec) picked.push(rec);
          }
        } else {
          picked = candidates.slice(0, 10);
        }

        const preview = picked.map((rec) => {
          const timeText = extractText(rec.fields['Time']).trim();
          const dateOnly = parseBeijingDateOnly(timeText);
          const videoId = extractText(rec.fields['Video ID']).trim();
          const statusVal = extractText(rec.fields['是否发起分析']).trim();
          const equalsYesterday = dateOnly === yesterday;
          const reason_date_mismatch = !(equalsYesterday);
          const reason_status_not_empty = !!statusVal;
          const reason_videoid_empty = !videoId;
          return {
            record_id: rec.record_id,
            keys: Object.keys(rec.fields || {}),
            Time: timeText,
            dateOnly,
            yesterday,
            equalsYesterday,
            videoId,
            status: statusVal,
            reasons_not_selected: {
              date_mismatch: reason_date_mismatch,
              status_not_empty: reason_status_not_empty,
              video_id_empty: reason_videoid_empty,
            },
          };
        });
        return createCors({ success: true, candidates: candidates.length, selected: needProcess.length, preview });
      }

      if (needProcess.length === 0) {
        return createCors({ success: true, message: 'No records to enqueue' });
      }

      // 2) 入队前确保存在“是否发起分析”字段，并写入=是
      try {
        await ensureTextField(env, accessToken, '是否发起分析');
        const updates = needProcess.map((r) => ({ record_id: r.record_id, fields: { '是否发起分析': '是' } }));
        await batchUpdateRecords(env, accessToken, updates);
      } catch (e) {
        // 字段创建或写入失败不阻塞入队，但会影响状态标记
      }

      // 3) 推送到队列
      for (const rec of needProcess) {
        const videoId = extractText(rec.fields['Video ID']).trim();
        if (!videoId) continue;
        const payload = { feishuRecordId: rec.record_id, videoId, env: { FEISHU_APP_TOKEN: env.FEISHU_APP_TOKEN, FEISHU_TABLE_ID: env.FEISHU_TABLE_ID }, accessToken };
        ctx.waitUntil(env.VIDEO_ANALYSIS_QUEUE.send(payload, { contentType: 'json' }));
      }

      return createCors({ success: true, enqueued: needProcess.length });
    } catch (e: any) {
      return createCors({ success: false, error: e.message }, 500);
    }
  },

  async queue(batch: MessageBatch<any>, env: Env, ctx: ExecutionContext) {
    // 按原项目思路：在 Worker 端控频串行转发到 Vercel
    const qps = Math.max(1, Number(env.RATE_LIMIT_QPS || '1'));
    const intervalMs = Math.floor(1000 / qps);

    for (const message of batch.messages) {
      try {
        const payload = { messages: [{ id: message.id, body: message.body, timestamp: message.timestamp }] };
        const headers: Record<string, string> = { 'Content-Type': 'application/json' };
        if (env.VERCEL_PROTECTION_BYPASS) {
          headers['x-vercel-protection-bypass'] = env.VERCEL_PROTECTION_BYPASS;
        }
        await fetch(env.VERCEL_CONSUMER_URL, {
          method: 'POST',
          headers,
          body: JSON.stringify(payload),
        });
      } catch (_) {
        // 转发失败不重试，交由 Vercel/表回填失败原因；保持吞吐稳定
      } finally {
        message.ack();
        // 简单节流：消息之间等待 intervalMs 毫秒
        if (intervalMs > 0) {
          await new Promise((r) => setTimeout(r, intervalMs));
        }
      }
    }
  },
};


