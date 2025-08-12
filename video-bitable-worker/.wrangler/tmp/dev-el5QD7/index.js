var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// .wrangler/tmp/bundle-JqbbgI/checked-fetch.js
var urls = /* @__PURE__ */ new Set();
function checkURL(request, init) {
  const url = request instanceof URL ? request : new URL(
    (typeof request === "string" ? new Request(request, init) : request).url
  );
  if (url.port && url.port !== "443" && url.protocol === "https:") {
    if (!urls.has(url.toString())) {
      urls.add(url.toString());
      console.warn(
        `WARNING: known issue with \`fetch()\` requests to custom HTTPS ports in published Workers:
 - ${url.toString()} - the custom port will be ignored when the Worker is published using the \`wrangler deploy\` command.
`
      );
    }
  }
}
__name(checkURL, "checkURL");
globalThis.fetch = new Proxy(globalThis.fetch, {
  apply(target, thisArg, argArray) {
    const [request, init] = argArray;
    checkURL(request, init);
    return Reflect.apply(target, thisArg, argArray);
  }
});

// src/index.ts
function createCors(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }
  });
}
__name(createCors, "createCors");
function extractText(value) {
  if (value == null) return "";
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value)) {
    const parts = value.map((v) => {
      if (v && typeof v === "object") {
        return v.text || v.name || v.value || "";
      }
      return String(v ?? "");
    }).filter(Boolean);
    return parts.join(" ");
  }
  if (typeof value === "object") {
    return value.text || value.name || value.value || value.date || "";
  }
  return "";
}
__name(extractText, "extractText");
function parseBeijingDateOnly(text) {
  if (!text || typeof text !== "string") return null;
  const m = text.match(/^(\d{4})\/(\d{2})\/(\d{2})/);
  if (!m) return null;
  return `${m[1]}-${m[2]}-${m[3]}`;
}
__name(parseBeijingDateOnly, "parseBeijingDateOnly");
function getYesterdayDateStrInBeijing(now = /* @__PURE__ */ new Date()) {
  const utc = now.getTime() + now.getTimezoneOffset() * 6e4;
  const beijingNow = new Date(utc + 8 * 36e5);
  const y = new Date(beijingNow.getTime() - 24 * 36e5);
  const yyyy = y.getUTCFullYear();
  const mm = String(y.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(y.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}
__name(getYesterdayDateStrInBeijing, "getYesterdayDateStrInBeijing");
async function searchVideoRecordsToProcess(env, accessToken) {
  const url = `https://open.feishu.cn/open-apis/bitable/v1/apps/${env.FEISHU_APP_TOKEN}/tables/${env.FEISHU_TABLE_ID}/records/search`;
  const payload = {
    filter: {
      conjunction: "and",
      conditions: [
        { field_name: "Video ID", operator: "isNotEmpty", value: [] }
      ]
    },
    page_size: 500
  };
  const resp = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json; charset=utf-8"
    },
    body: JSON.stringify(payload)
  });
  const data = await resp.json();
  if (!resp.ok || data.code !== 0) {
    throw new Error(`Feishu search failed: ${resp.status} ${data.msg || ""}`);
  }
  return data.data?.items || [];
}
__name(searchVideoRecordsToProcess, "searchVideoRecordsToProcess");
async function getFieldsMeta(env, accessToken) {
  const url = `https://open.feishu.cn/open-apis/bitable/v1/apps/${env.FEISHU_APP_TOKEN}/tables/${env.FEISHU_TABLE_ID}/fields`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  const data = await resp.json();
  if (!resp.ok || data.code !== 0) throw new Error(`Feishu list fields failed: ${resp.status} ${data.msg || ""}`);
  return data.data?.items || [];
}
__name(getFieldsMeta, "getFieldsMeta");
async function ensureTextField(env, accessToken, fieldName) {
  const fields = await getFieldsMeta(env, accessToken);
  if (fields.some((f) => f.field_name === fieldName)) return;
  const url = `https://open.feishu.cn/open-apis/bitable/v1/apps/${env.FEISHU_APP_TOKEN}/tables/${env.FEISHU_TABLE_ID}/fields`;
  const payload = { field_name: fieldName, type: 1 };
  const resp = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json; charset=utf-8" },
    body: JSON.stringify(payload)
  });
  const data = await resp.json();
  if (!resp.ok || data.code !== 0) throw new Error(`Create field '${fieldName}' failed: ${resp.status} ${data.msg || ""}`);
}
__name(ensureTextField, "ensureTextField");
async function batchUpdateRecords(env, accessToken, updates) {
  if (updates.length === 0) return;
  const url = `https://open.feishu.cn/open-apis/bitable/v1/apps/${env.FEISHU_APP_TOKEN}/tables/${env.FEISHU_TABLE_ID}/records/batch_update`;
  const resp = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json; charset=utf-8" },
    body: JSON.stringify({ records: updates })
  });
  const data = await resp.json();
  if (!resp.ok || data.code !== 0) {
    throw new Error(`Feishu batch_update failed: ${resp.status} ${data.msg || ""}`);
  }
}
__name(batchUpdateRecords, "batchUpdateRecords");
async function getAccessTokenViaProxy(env) {
  if (!env.TOKEN_PROXY_SERVICE) throw new Error("TOKEN_PROXY_SERVICE \u672A\u7ED1\u5B9A\uFF0C\u4E14\u8BF7\u6C42\u4F53\u672A\u63D0\u4F9B accessToken");
  const resp = await env.TOKEN_PROXY_SERVICE.fetch("http://proxy/token");
  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`Token proxy failed: ${resp.status} ${text}`);
  }
  const data = await resp.json().catch(() => ({}));
  const token = data.tenant_access_token || data.access_token || data.token;
  if (!token) throw new Error("Token proxy returned no tenant_access_token");
  return token;
}
__name(getAccessTokenViaProxy, "getAccessTokenViaProxy");
var src_default = {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") return createCors({}, 200);
    if (request.method !== "POST") return createCors({ error: "Use POST" }, 405);
    try {
      const body = await request.json().catch(() => ({}));
      const accessToken = body.accessToken || await getAccessTokenViaProxy(env);
      const candidates = await searchVideoRecordsToProcess(env, accessToken);
      const yesterday = getYesterdayDateStrInBeijing();
      const needProcess = candidates.filter((rec) => {
        const timeTextRaw = rec.fields["Time"];
        const timeText = extractText(timeTextRaw).trim();
        const dateOnly = parseBeijingDateOnly(timeText);
        const statusVal = extractText(rec.fields["\u662F\u5426\u53D1\u8D77\u5206\u6790"]).trim();
        return dateOnly === yesterday && !statusVal;
      });
      if (body && body.debug === true) {
        const preview = candidates.slice(0, 10).map((rec) => {
          const timeText = extractText(rec.fields["Time"]).trim();
          const dateOnly = parseBeijingDateOnly(timeText);
          const videoId = extractText(rec.fields["Video ID"]).trim();
          const statusVal = extractText(rec.fields["\u662F\u5426\u53D1\u8D77\u5206\u6790"]).trim();
          return {
            record_id: rec.record_id,
            keys: Object.keys(rec.fields || {}),
            Time: timeText,
            dateOnly,
            yesterday,
            equalsYesterday: dateOnly === yesterday,
            videoId,
            status: statusVal
          };
        });
        return createCors({ success: true, candidates: candidates.length, selected: needProcess.length, preview });
      }
      if (needProcess.length === 0) {
        return createCors({ success: true, message: "No records to enqueue" });
      }
      try {
        await ensureTextField(env, accessToken, "\u662F\u5426\u53D1\u8D77\u5206\u6790");
        const updates = needProcess.map((r) => ({ record_id: r.record_id, fields: { "\u662F\u5426\u53D1\u8D77\u5206\u6790": "\u662F" } }));
        await batchUpdateRecords(env, accessToken, updates);
      } catch (e) {
      }
      for (const rec of needProcess) {
        const videoId = extractText(rec.fields["Video ID"]).trim();
        if (!videoId) continue;
        const payload = { feishuRecordId: rec.record_id, videoId, env: { FEISHU_APP_TOKEN: env.FEISHU_APP_TOKEN, FEISHU_TABLE_ID: env.FEISHU_TABLE_ID }, accessToken };
        ctx.waitUntil(env.VIDEO_ANALYSIS_QUEUE.send(payload, { contentType: "json" }));
      }
      return createCors({ success: true, enqueued: needProcess.length });
    } catch (e) {
      return createCors({ success: false, error: e.message }, 500);
    }
  },
  async queue(batch, env, ctx) {
    const qps = Math.max(1, Number(env.RATE_LIMIT_QPS || "1"));
    const intervalMs = Math.floor(1e3 / qps);
    for (const message of batch.messages) {
      try {
        const payload = { messages: [{ id: message.id, body: message.body, timestamp: message.timestamp }] };
        const headers = { "Content-Type": "application/json" };
        if (env.VERCEL_PROTECTION_BYPASS) {
          headers["x-vercel-protection-bypass"] = env.VERCEL_PROTECTION_BYPASS;
        }
        await fetch(env.VERCEL_CONSUMER_URL, {
          method: "POST",
          headers,
          body: JSON.stringify(payload)
        });
      } catch (_) {
      } finally {
        message.ack();
        if (intervalMs > 0) {
          await new Promise((r) => setTimeout(r, intervalMs));
        }
      }
    }
  }
};

// node_modules/wrangler/templates/middleware/middleware-ensure-req-body-drained.ts
var drainBody = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } finally {
    try {
      if (request.body !== null && !request.bodyUsed) {
        const reader = request.body.getReader();
        while (!(await reader.read()).done) {
        }
      }
    } catch (e) {
      console.error("Failed to drain the unused request body.", e);
    }
  }
}, "drainBody");
var middleware_ensure_req_body_drained_default = drainBody;

// node_modules/wrangler/templates/middleware/middleware-miniflare3-json-error.ts
function reduceError(e) {
  return {
    name: e?.name,
    message: e?.message ?? String(e),
    stack: e?.stack,
    cause: e?.cause === void 0 ? void 0 : reduceError(e.cause)
  };
}
__name(reduceError, "reduceError");
var jsonError = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } catch (e) {
    const error = reduceError(e);
    return Response.json(error, {
      status: 500,
      headers: { "MF-Experimental-Error-Stack": "true" }
    });
  }
}, "jsonError");
var middleware_miniflare3_json_error_default = jsonError;

// .wrangler/tmp/bundle-JqbbgI/middleware-insertion-facade.js
var __INTERNAL_WRANGLER_MIDDLEWARE__ = [
  middleware_ensure_req_body_drained_default,
  middleware_miniflare3_json_error_default
];
var middleware_insertion_facade_default = src_default;

// node_modules/wrangler/templates/middleware/common.ts
var __facade_middleware__ = [];
function __facade_register__(...args) {
  __facade_middleware__.push(...args.flat());
}
__name(__facade_register__, "__facade_register__");
function __facade_invokeChain__(request, env, ctx, dispatch, middlewareChain) {
  const [head, ...tail] = middlewareChain;
  const middlewareCtx = {
    dispatch,
    next(newRequest, newEnv) {
      return __facade_invokeChain__(newRequest, newEnv, ctx, dispatch, tail);
    }
  };
  return head(request, env, ctx, middlewareCtx);
}
__name(__facade_invokeChain__, "__facade_invokeChain__");
function __facade_invoke__(request, env, ctx, dispatch, finalMiddleware) {
  return __facade_invokeChain__(request, env, ctx, dispatch, [
    ...__facade_middleware__,
    finalMiddleware
  ]);
}
__name(__facade_invoke__, "__facade_invoke__");

// .wrangler/tmp/bundle-JqbbgI/middleware-loader.entry.ts
var __Facade_ScheduledController__ = class ___Facade_ScheduledController__ {
  constructor(scheduledTime, cron, noRetry) {
    this.scheduledTime = scheduledTime;
    this.cron = cron;
    this.#noRetry = noRetry;
  }
  static {
    __name(this, "__Facade_ScheduledController__");
  }
  #noRetry;
  noRetry() {
    if (!(this instanceof ___Facade_ScheduledController__)) {
      throw new TypeError("Illegal invocation");
    }
    this.#noRetry();
  }
};
function wrapExportedHandler(worker) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return worker;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  const fetchDispatcher = /* @__PURE__ */ __name(function(request, env, ctx) {
    if (worker.fetch === void 0) {
      throw new Error("Handler does not export a fetch() function.");
    }
    return worker.fetch(request, env, ctx);
  }, "fetchDispatcher");
  return {
    ...worker,
    fetch(request, env, ctx) {
      const dispatcher = /* @__PURE__ */ __name(function(type, init) {
        if (type === "scheduled" && worker.scheduled !== void 0) {
          const controller = new __Facade_ScheduledController__(
            Date.now(),
            init.cron ?? "",
            () => {
            }
          );
          return worker.scheduled(controller, env, ctx);
        }
      }, "dispatcher");
      return __facade_invoke__(request, env, ctx, dispatcher, fetchDispatcher);
    }
  };
}
__name(wrapExportedHandler, "wrapExportedHandler");
function wrapWorkerEntrypoint(klass) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return klass;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  return class extends klass {
    #fetchDispatcher = /* @__PURE__ */ __name((request, env, ctx) => {
      this.env = env;
      this.ctx = ctx;
      if (super.fetch === void 0) {
        throw new Error("Entrypoint class does not define a fetch() function.");
      }
      return super.fetch(request);
    }, "#fetchDispatcher");
    #dispatcher = /* @__PURE__ */ __name((type, init) => {
      if (type === "scheduled" && super.scheduled !== void 0) {
        const controller = new __Facade_ScheduledController__(
          Date.now(),
          init.cron ?? "",
          () => {
          }
        );
        return super.scheduled(controller);
      }
    }, "#dispatcher");
    fetch(request) {
      return __facade_invoke__(
        request,
        this.env,
        this.ctx,
        this.#dispatcher,
        this.#fetchDispatcher
      );
    }
  };
}
__name(wrapWorkerEntrypoint, "wrapWorkerEntrypoint");
var WRAPPED_ENTRY;
if (typeof middleware_insertion_facade_default === "object") {
  WRAPPED_ENTRY = wrapExportedHandler(middleware_insertion_facade_default);
} else if (typeof middleware_insertion_facade_default === "function") {
  WRAPPED_ENTRY = wrapWorkerEntrypoint(middleware_insertion_facade_default);
}
var middleware_loader_entry_default = WRAPPED_ENTRY;
export {
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=index.js.map
