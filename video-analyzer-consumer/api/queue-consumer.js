import { GoogleGenAI, Type } from "@google/genai";
import fetch from 'node-fetch';
import FormData from 'form-data';

const WORKER_URL = process.env.WORKER_URL || 'https://jolly-dream-33e8.1170731839.workers.dev/';

async function fetchVideoBufferById(videoId) {
  // 参照示意代码：POST { ids: [videoId] }，返回数组结果，解析出 downloadUrl 然后二次下载
  const resp = await fetch(WORKER_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ids: [String(videoId)] }),
    timeout: 30000,
  });
  if (!resp.ok) throw new Error(`Worker fetch failed: ${resp.status}`);
  let arr;
  try {
    arr = await resp.json();
  } catch (e) {
    const text = await resp.text().catch(() => '');
    throw new Error(`Downloader returned non-JSON: ${text?.slice(0,200)}`);
  }
  const item = Array.isArray(arr) ? arr.find(x => String(x.videoId) === String(videoId)) : null;
  if (!item) throw new Error('Downloader returned no item for this videoId');
  if (item && item.error) throw new Error(`Downloader error: ${item.error}`);
  const url = item.downloadUrl || item.url || item.download_url || item.nowm || item.play || item.playAddr;
  if (!url) throw new Error('No downloadUrl (or compatible field) from downloader');
  const mp4 = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 30000 });
  if (!mp4.ok) throw new Error(`Download mp4 failed: ${mp4.status}`);
  return Buffer.from(await mp4.arrayBuffer());
}

async function uploadVideoToFeishu(buffer, filename, accessToken, appToken) {
  const uploadUrl = 'https://open.feishu.cn/open-apis/drive/v1/medias/upload_all';
  const form = new FormData();
  form.append('file_name', filename);
  form.append('parent_type', 'bitable_file');
  form.append('parent_node', appToken);
  form.append('size', String(buffer.length));
  form.append('file', buffer, { filename, contentType: 'video/mp4' });

  const resp = await fetch(uploadUrl, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      ...form.getHeaders(),
    },
    body: form,
  });

  const text = await resp.text();
  let data;
  try { data = JSON.parse(text); } catch (_) {
    throw new Error(`Feishu medias upload non-JSON response: ${resp.status} ${resp.statusText} ${text?.slice(0,200)}`);
  }
  if (!resp.ok || data.code !== 0) {
    throw new Error(`Feishu medias upload failed: ${resp.status} ${data.msg || text?.slice(0,200)}`);
  }
  return data.data?.file_token || data.data?.file?.file_token;
}

// Removed legacy AI schema/functions for old "视频脚本/视频标签/视频得分" pipeline

const NEW_ANALYSIS_RESPONSE_SCHEMA = {
  type: Type.OBJECT,
  properties: {
    detected_language_code: { type: Type.STRING },
    subtitle_groups: {
      type: Type.ARRAY,
      items: {
        type: Type.OBJECT,
        properties: {
          startTime: { type: Type.STRING },
          endTime: { type: Type.STRING },
          text: { type: Type.STRING },
          translation: { type: Type.STRING },
          summary: { type: Type.STRING },
          purpose: { type: Type.STRING },
          isVisuallyStrong: { type: Type.BOOLEAN },
          visualStrengthReason: { type: Type.STRING, nullable: true },
          highlightSummary: { type: Type.STRING, nullable: true },
        },
        required: [
          'startTime',
          'endTime',
          'text',
          'translation',
          'summary',
          'purpose',
          'isVisuallyStrong',
        ],
      },
    },
    overall_score: { type: Type.INTEGER },
    score_rationale: { type: Type.STRING },
    // 三大板块评估（非必填，中文键名的 checklist，放宽约束以提升容错）
    panel_evaluation: {
      type: Type.OBJECT,
      properties: {
        hook: {
          type: Type.OBJECT,
          properties: {
            score: { type: Type.INTEGER },
            analysis: { type: Type.STRING },
            checklist: {
              type: Type.OBJECT,
              properties: {
                '效果前置': { type: Type.BOOLEAN },
                '效果前置说明': { type: Type.STRING },
                '质地诱惑': { type: Type.BOOLEAN },
                '质地诱惑说明': { type: Type.STRING },
                '问题特写': { type: Type.BOOLEAN },
                '问题特写说明': { type: Type.STRING },
                '灵魂发问': { type: Type.BOOLEAN },
                '灵魂发问说明': { type: Type.STRING },
                '反差剧情': { type: Type.BOOLEAN },
                '反差剧情说明': { type: Type.STRING },
              },
            },
          },
        },
        pitch: {
          type: Type.OBJECT,
          properties: {
            score: { type: Type.INTEGER },
            analysis: { type: Type.STRING },
            checklist: {
              type: Type.OBJECT,
              properties: {
                '光线与画质达标': { type: Type.BOOLEAN },
                '光线与画质说明': { type: Type.STRING },
                '手法专业流畅': { type: Type.BOOLEAN },
                '手法专业流畅说明': { type: Type.STRING },
                '过程观感舒适': { type: Type.BOOLEAN },
                '过程观感舒适说明': { type: Type.STRING },
                '对比真实性高': { type: Type.BOOLEAN },
                '对比真实性说明': { type: Type.STRING },
                '场景化植入具体': { type: Type.BOOLEAN },
                '场景化植入说明': { type: Type.STRING },
                '表达自然有人味': { type: Type.BOOLEAN },
                '表达自然说明': { type: Type.STRING },
                '感官细节充分': { type: Type.BOOLEAN },
                '感官细节说明': { type: Type.STRING },
                '信任状有呈现': { type: Type.BOOLEAN },
                '信任状说明': { type: Type.STRING },
              },
            },
          },
        },
        close: {
          type: Type.OBJECT,
          properties: {
            score: { type: Type.INTEGER },
            analysis: { type: Type.STRING },
            checklist: {
              type: Type.OBJECT,
              properties: {
                '视觉CTA明显': { type: Type.BOOLEAN },
                '视觉CTA说明': { type: Type.STRING },
                '口播CTA清晰': { type: Type.BOOLEAN },
                '口播CTA说明': { type: Type.STRING },
                '字幕CTA明确': { type: Type.BOOLEAN },
                '字幕CTA说明': { type: Type.STRING },
                '营造紧迫稀缺': { type: Type.BOOLEAN },
                '紧迫稀缺说明': { type: Type.STRING },
                '提供风险对冲': { type: Type.BOOLEAN },
                '风险对冲说明': { type: Type.STRING },
              },
            },
          },
        },
      },
    },
    // 三支柱（消费者感知）评估与红旗
    consumer_pillars: {
      type: Type.OBJECT,
      properties: {
        pillar1_authenticity_trust: {
          type: Type.OBJECT,
          properties: {
            score: { type: Type.INTEGER },
            analysis: { type: Type.STRING },
            checklist: { type: Type.ARRAY, items: { type: Type.OBJECT, properties: { name: { type: Type.STRING }, hit: { type: Type.BOOLEAN }, notes: { type: Type.STRING } } } },
          },
        },
        pillar2_value_persuasion: {
          type: Type.OBJECT,
          properties: {
            score: { type: Type.INTEGER },
            analysis: { type: Type.STRING },
            checklist: { type: Type.ARRAY, items: { type: Type.OBJECT, properties: { name: { type: Type.STRING }, hit: { type: Type.BOOLEAN }, notes: { type: Type.STRING } } } },
          },
        },
        pillar3_conversion_readiness: {
          type: Type.OBJECT,
          properties: {
            score: { type: Type.INTEGER },
            analysis: { type: Type.STRING },
            checklist: { type: Type.ARRAY, items: { type: Type.OBJECT, properties: { name: { type: Type.STRING }, hit: { type: Type.BOOLEAN }, notes: { type: Type.STRING } } } },
          },
        },
      },
    },
    red_flags: {
      type: Type.ARRAY,
      items: { type: Type.OBJECT, properties: { name: { type: Type.STRING }, hit: { type: Type.BOOLEAN }, severity: { type: Type.STRING }, notes: { type: Type.STRING } } },
    },
    // V3.0 标签体系（非必填）：按白名单标签选择，每个子类最多2个，并附带一句中文依据（basis）
    v3_labeling: {
      type: Type.OBJECT,
      properties: {
        // 维度一：创作者人设与定位
        creator_persona: {
          type: Type.OBJECT,
          properties: {
            labels: { type: Type.ARRAY, items: { type: Type.STRING } },
            basis: { type: Type.STRING },
          },
        },
        // 维度二：视听呈现策略
        visual_audio: {
          type: Type.OBJECT,
          properties: {
            appearance_scene: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            audio_speech: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            rhythm_bgm: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            emotion_style: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
          },
        },
        // 维度三：内容策略与剧本
        content_script: {
          type: Type.OBJECT,
          properties: {
            classic_patterns: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            narrative_framework: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
          },
        },
        // 维度四：产品展示焦点
        product_showcase: {
          type: Type.OBJECT,
          properties: {
            core_selling_points: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            demonstration_methods: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
          },
        },
        // 维度五：情绪价值与心理挂钩
        emotional_hooks: {
          type: Type.OBJECT,
          properties: {
            pain_shortcuts: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            value_surprise: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            sensory_emotion: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
          },
        },
        // 维度六：本土化与文化融合
        localization: {
          type: Type.OBJECT,
          properties: {
            language_humor: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            culture_trends: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            aesthetics_scenes: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
          },
        },
        // 维度七：TikTok平台生态玩法
        tiktok_ecosystem: {
          type: Type.OBJECT,
          properties: {
            traffic_features: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            video_techniques: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            interaction_guidance: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            traffic_path: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
          },
        },
        // 维度八：商业转化策略
        commercial_conversion: {
          type: Type.OBJECT,
          properties: {
            effect_value: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            trust_urgency: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            experience_cta: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
            conversion_path: {
              type: Type.OBJECT,
              properties: { labels: { type: Type.ARRAY, items: { type: Type.STRING } }, basis: { type: Type.STRING } },
            },
          },
        },
      },
    },
    video_structure: {
      type: Type.OBJECT,
      properties: {
        golden_3s_hook_analysis: { type: Type.STRING },
        segments: {
        type: Type.ARRAY,
        items: {
          type: Type.OBJECT,
          properties: {
              timeRange: { type: Type.STRING },
              partName: { type: Type.STRING },
              description: { type: Type.STRING },
            },
            required: ['timeRange', 'partName', 'description'],
          },
        },
      },
      required: ['golden_3s_hook_analysis', 'segments'],
    },
    script_analysis: {
      type: Type.OBJECT,
      properties: {
        marketing_stage_summary: { type: Type.STRING },
        negativeSummary: { type: Type.STRING },
        strengths: { type: Type.ARRAY, items: { type: Type.STRING } },
        weaknesses: { type: Type.ARRAY, items: { type: Type.STRING } },
        suggestions: { type: Type.ARRAY, items: { type: Type.STRING } },
      },
      required: ['marketing_stage_summary', 'negativeSummary', 'strengths', 'weaknesses', 'suggestions'],
    },
    video_tags: {
      type: Type.OBJECT,
      properties: {
        videoType: { type: Type.STRING },
        emotionalTone: { type: Type.STRING },
        coreContentAngle: { type: Type.STRING },
        otherTags: { type: Type.ARRAY, items: { type: Type.STRING } },
      },
      required: ['videoType', 'emotionalTone', 'coreContentAngle', 'otherTags'],
    },
  },
  required: ['detected_language_code', 'subtitle_groups', 'script_analysis', 'consumer_pillars'],
};

// 双阶段最小化 Schema：阶段A（保持字幕与结构，保证飞书两字段内容不变）
const SCHEMA_STAGE_A = {
  type: Type.OBJECT,
  properties: {
    detected_language_code: { type: Type.STRING },
    subtitle_groups: NEW_ANALYSIS_RESPONSE_SCHEMA.properties.subtitle_groups,
    overall_score: { type: Type.INTEGER },
    score_rationale: { type: Type.STRING },
    panel_evaluation: NEW_ANALYSIS_RESPONSE_SCHEMA.properties.panel_evaluation,
    video_structure: NEW_ANALYSIS_RESPONSE_SCHEMA.properties.video_structure,
    script_analysis: NEW_ANALYSIS_RESPONSE_SCHEMA.properties.script_analysis,
    video_tags: NEW_ANALYSIS_RESPONSE_SCHEMA.properties.video_tags,
    // 扩展：真人口播（当镜口述）判定增强（可选返回，向后兼容）
    on_camera_speech: { type: Type.BOOLEAN },
    on_camera_speech_basis: { type: Type.STRING },
    on_camera_speech_segments: { type: Type.ARRAY, items: { type: Type.STRING } },
  },
  required: ['detected_language_code', 'subtitle_groups', 'script_analysis', 'video_structure'],
};

// 阶段B（仅三支柱/红旗/V3标签，避免扩大状态空间）
const SCHEMA_STAGE_B = {
  type: Type.OBJECT,
  properties: {
    consumer_pillars: NEW_ANALYSIS_RESPONSE_SCHEMA.properties.consumer_pillars,
    red_flags: NEW_ANALYSIS_RESPONSE_SCHEMA.properties.red_flags,
    v3_labeling: NEW_ANALYSIS_RESPONSE_SCHEMA.properties.v3_labeling,
  },
  required: ['consumer_pillars'],
};

// 统一处理：剥离 Markdown 代码块围栏
function stripCodeFences(text) {
  try {
    if (typeof text !== 'string') return '';
    const m = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
    return (m && m[1] ? m[1] : text).trim();
  } catch (_) { return ''; }
}

// 统一处理：从结果对象中尽可能稳健地提取文本；并输出可观测性快照
function extractTextWithSnapshot(result, feishuRecordId, stageLabel = 'Stage') {
  try {
    // 优先通道
    if (result && typeof result.text === 'string' && result.text.trim()) {
      return stripCodeFences(result.text);
    }
    if (result && result.response && typeof result.response.text === 'function') {
      const t = result.response.text();
      if (t && typeof t === 'string' && t.trim()) return stripCodeFences(t);
    }

    // 遍历 candidates[*].content.parts[*].text
    const candidates = (result && result.response && Array.isArray(result.response.candidates))
      ? result.response.candidates
      : (Array.isArray(result?.candidates) ? result.candidates : []);

    const collected = [];
    const snapshot = {
      candidatesCount: Array.isArray(candidates) ? candidates.length : 0,
      candidates: [],
    };
    for (let i = 0; i < candidates.length; i++) {
      const c = candidates[i] || {};
      const parts = (c.content && Array.isArray(c.content.parts)) ? c.content.parts : [];
      const partInfo = [];
      for (let j = 0; j < parts.length; j++) {
        const p = parts[j] || {};
        const hasText = typeof p.text === 'string' && p.text.trim().length > 0;
        if (hasText) collected.push(p.text);
        const typeGuess = hasText ? 'text' : (Object.keys(p)[0] || 'unknown');
        partInfo.push({ idx: j, type: typeGuess, hasText, textLen: hasText ? p.text.length : 0 });
      }
      snapshot.candidates.push({
        idx: i,
        finishReason: c.finishReason || c.finish_reason || null,
        safety: c.safetyRatings || c.safety || null,
        partsCount: parts.length,
        parts: partInfo,
      });
    }

    try { console.log(`[DEBUG] Record ${feishuRecordId}: ${stageLabel} snapshot`, JSON.stringify(snapshot)); } catch (_) {}

    // 选择最可能是 JSON 的文本
    let chosen = '';
    for (const t of collected) {
      const stripped = stripCodeFences(t);
      const s = stripped.trim();
      if (!s) continue;
      if (s.startsWith('{') || s.startsWith('[')) { chosen = s; break; }
      if (!chosen) chosen = s; // 兜底取第一个非空
    }
    return chosen || '';
  } catch (e) {
    try { console.warn(`[WARN] Record ${feishuRecordId}: ${stageLabel} extract error: ${e.message}`); } catch (_) {}
    return '';
  }
}

async function generateWithRetry(genAI, req, feishuRecordId, stageLabel = 'Stage') {
  // 首次尝试
  let result = await genAI.models.generateContent(req);
  let rawText = extractTextWithSnapshot(result, feishuRecordId, stageLabel);
  if (rawText) return rawText;
  try { console.warn(`[WARN] Record ${feishuRecordId}: ${stageLabel} empty content on first attempt, retrying once...`); } catch (_) {}
  // 保持相同 schema/温度，重试一次
  const result2 = await genAI.models.generateContent(req);
  rawText = extractTextWithSnapshot(result2, feishuRecordId, stageLabel);
  if (!rawText) {
    try {
      const snapshot2 = {
        hasResponse: !!result2?.response,
        candidateCount: Array.isArray(result2?.response?.candidates) ? result2.response.candidates.length : (Array.isArray(result2?.candidates) ? result2.candidates.length : 0),
        finishReasons: (result2?.response?.candidates || result2?.candidates || []).map(c => c?.finishReason || c?.finish_reason || null),
      };
      console.error(`[ERROR] Record ${feishuRecordId}: ${stageLabel} empty content after retry`, JSON.stringify(snapshot2));
    } catch (_) {}
  }
  return rawText || '';
}

async function analyzeStageA(genAI, buffer, feishuRecordId, model = 'gemini-2.5-flash') {
  const systemInstruction = `You are an expert short-form video script analyst for TikTok skincare. Return JSON strictly matching the provided schema for Stage A (basic analysis). All analysis in Simplified Chinese.

Scoring rule (MANDATORY): All scores MUST be integers in the range 0-100. DO NOT use 10-point or 1-point scales. DO NOT return decimals.`;
  const prompt = '请进行阶段A基础分析，并严格按schema返回JSON（简体中文）。评分必须是0-100的整数；不得使用10分制或1分制；不得返回小数。\n\n额外要求（可选但建议返回）：\n- on_camera_speech（boolean）：是否“真人口播”（必须同时满足：真人出镜 + 现场收音/同期声；若为画外配音则为false）。\n- on_camera_speech_basis（string）：一句中文依据（为何判断为当镜口述/非当镜口述）。\n- on_camera_speech_segments（string[]）：当镜口述的大致时间段，如 \'00:03-00:06\'。';
  const videoPart = { inlineData: { data: buffer.toString('base64'), mimeType: 'video/mp4' } };
  const contents = { parts: [videoPart, { text: prompt }] };
  const req = {
    model,
    contents,
    config: { responseMimeType: 'application/json', temperature: 0.0, responseSchema: SCHEMA_STAGE_A, systemInstruction },
  };
  const rawText = await generateWithRetry(genAI, req, feishuRecordId, 'StageA');
  if (!rawText) throw new Error('StageA: Empty content (after retry)');
  try { return JSON.parse(rawText); } catch (e) { throw new Error(`StageA parse failed. Head: ${rawText.slice(0,200)}...`); }
}

async function analyzeStageB(genAI, buffer, feishuRecordId, model = 'gemini-2.5-flash') {
  const systemInstruction = `你是短视频消费者感知评估专家。仅进行“阶段B：三支柱/红旗/V3标签”分析；全部用简体中文；严格匹配 schema。

评分标尺（务必遵守）：
- 三支柱每柱先根据 checklist 计算命中率 base = round(hits/total*100)。允许±10分的质量微调（择其一）：
  - 正向：证据链代表性强/表达自然人话/场景具体/CTA自然清晰（+10 封顶）
  - 负向：证据薄弱但语言空喊/对比可疑/硬广腔（-10 封顶）
- 一致性约束：
  - 若 analysis 明显正向且命中率 ≥ 30%，分数不得 < 20
  - 若 analysis 明显负向且命中率 ≤ 20%，分数不得 > 60
- 请在每柱返回 score 与一句 score_basis（含 命中X/Y 与微调因子），checklist 使用数组项 {name, hit, notes}。

红旗：返回数组 [{name, hit, severity, notes}]；name 限定：不公平对比/医疗化或夸大承诺/纯广告感强或噪声大或SKU混乱/全程无实拍或素材堆叠/合规_医疗暗示或虚假承诺。

V3标签：仅输出命中标签，扁平到 v3_labels_flat 的“[维度]--[二级维度]-[TAG]”；并在 v3_label_bases 返回 {label,basis}。

评分规则（强制）：所有分数必须是0-100的整数；不得使用10分制或1分制；不得返回小数。`;
  const prompt = '请进行阶段B分析（三支柱/红旗/V3标签），并严格按schema返回JSON（简体中文）。三支柱请按命中率→微调→一致性约束的流程得出分数，并返回 score_basis。';
  const videoPart = { inlineData: { data: buffer.toString('base64'), mimeType: 'video/mp4' } };
  const contents = { parts: [videoPart, { text: prompt }] };
  const req = {
    model,
    contents,
    config: { responseMimeType: 'application/json', temperature: 0.0, responseSchema: SCHEMA_STAGE_B, systemInstruction },
  };
  const rawText = await generateWithRetry(genAI, req, feishuRecordId, 'StageB');
  if (!rawText) throw new Error('StageB: Empty content (after retry)');
  try { return JSON.parse(rawText); } catch (e) { throw new Error(`StageB parse failed. Head: ${rawText.slice(0,200)}...`); }
}

async function postProcessAndScore(merged, feishuRecordId) {
  // 复用 handler 中的后处理逻辑，保持一致性
  let raw = JSON.stringify(merged);
  const parsed = JSON.parse(raw);
  // 功能性分
  const pe = parsed?.panel_evaluation || {};
  const hs = typeof pe?.hook?.score === 'number' ? pe.hook.score : null;
  const ps = typeof pe?.pitch?.score === 'number' ? pe.pitch.score : null;
  const cs = typeof pe?.close?.score === 'number' ? pe.close.score : null;
  if (hs !== null && ps !== null && cs !== null) parsed.functional_score = Math.round(hs*0.4 + ps*0.4 + cs*0.2);
  // 感知分
  const cp = parsed?.consumer_pillars || {};
  const p1 = typeof cp?.pillar1_authenticity_trust?.score === 'number' ? cp.pillar1_authenticity_trust.score : null;
  const p2 = typeof cp?.pillar2_value_persuasion?.score === 'number' ? cp.pillar2_value_persuasion.score : null;
  const p3 = typeof cp?.pillar3_conversion_readiness?.score === 'number' ? cp.pillar3_conversion_readiness.score : null;
  if (p1!==null && p2!==null && p3!==null) parsed.perception_score = Math.round(p1*0.5 + p2*0.3 + p3*0.2);
  // 融合
  const fs = parsed.functional_score; const ps2 = parsed.perception_score;
  let fused = null; if (typeof fs==='number' && typeof ps2==='number') fused = Math.round(fs*0.5 + ps2*0.5); else if (typeof fs==='number') fused = fs; else if (typeof ps2==='number') fused = ps2;
  // 红旗
  let penalty = 0; let cap = 100;
  const rfArr = Array.isArray(parsed.red_flags) ? parsed.red_flags : [];
  const findHit = (name) => rfArr.find(x=>x&&x.name===name && x.hit===true);
  const applyPenalty=(item,r)=>{ if(!item) return; const sev=String(item.severity||'').toLowerCase(); if(sev.includes('high')) penalty+=r.high; else if(sev.includes('mid')) penalty+=r.mid; else if(sev.includes('low')) penalty+=r.low; };
  const unfair=findHit('不公平对比'); applyPenalty(unfair,{low:-10,mid:-15,high:-20}); if(unfair) cap=Math.min(cap,60);
  const medical=findHit('医疗化或夸大承诺'); applyPenalty(medical,{low:-15,mid:-20,high:-30}); if(medical) cap=Math.min(cap,50);
  const adlike=findHit('纯广告感强或噪声大或SKU混乱'); applyPenalty(adlike,{low:-5,mid:-10,high:-15});
  const noLive=findHit('全程无实拍或素材堆叠'); applyPenalty(noLive,{low:-10,mid:-15,high:-20}); if(noLive) cap=Math.min(cap,60);
  const compliance=findHit('合规_医疗暗示或虚假承诺'); if(compliance) cap=Math.min(cap,50);
  if (typeof fused==='number') {
    const after=Math.max(0,Math.min(100,fused+penalty));
    parsed.fused_pre_penalty = fused;
    parsed.penalty_total = penalty;
    parsed.cap_applied = cap;
    parsed.final_score=Math.min(after,cap);
    parsed.overall_score=parsed.final_score;
  }
  return parsed;
}

async function runFullAnalysisWithModel(genAI, buffer, feishuRecordId, modelName) {
  const a = await analyzeStageA(genAI, buffer, feishuRecordId, modelName);
  let b = {};
  try {
    b = await analyzeStageB(genAI, buffer, feishuRecordId, modelName);
  } catch (e) {
    try { console.warn(`[WARN] Record ${feishuRecordId}: StageB failed on ${modelName}: ${e.message}`); } catch (_) {}
    b = { degrade: true };
  }
  const merged = { ...a, ...b };
  const scored = await postProcessAndScore(merged, feishuRecordId);
  return scored;
}

async function analyzeVideoWithSchema(genAI, buffer, feishuRecordId) {
  const systemInstruction = `You are an expert short-form video script analyst for TikTok, specializing in the skincare niche. Your task is to analyze a video and provide a structured, critical evaluation.

**Analysis Rules & Output Schema:**

1.  **Language:**
    * **Detect Language:** You MUST identify the primary spoken language of the video and return its BCP-47 code (e.g., "en-US", "th-TH", "zh-CN") in the \`detected_language_code\` field.
    * **Keep Original Language:** Extract all subtitles in their original spoken language.
    * **Translation:** Provide a concise **Simplified Chinese** translation for each subtitle line. THIS IS A MANDATORY REQUIREMENT.
    * **Analysis Language:** All analysis (summaries, purposes, critiques, etc.) must be in Simplified Chinese.

2.  **Scoring (0-100):**
    * **Context:** The score must reflect the video's potential for success **specifically within the TikTok skincare short-video context.**
    * **Negative Bias:** Be critical. Overly generic content, poor structure, or a weak hook should be heavily penalized. A score of 50-60 is average. A score above 85 requires exceptional quality.
    * **Score Rationale:** Briefly justify the score in one sentence, referencing the video's structure and hook.

3.  **Video Structure Analysis:**
    * **Golden 3s Hook Analysis:** Critically evaluate the first 3 seconds. Focus **only on weaknesses**. Is the hook clear? Is it attention-grabbing? What could be improved? If it's good, briefly state why, but still find a point of improvement.
    * **Content Segmentation:** Break the video into logical parts (e.g., Hook, Problem Intro, Product Showcase, CTA). For each part, provide the \`timeRange\` (e.g., "00:00 - 00:05"), \`partName\` (e.g., "钩子 / Hook"), and a brief \`description\`.

4.  **Subtitle Segmentation:**
    * **Semantic Grouping:** Do NOT split subtitles mid-sentence. Group them into semantically complete thoughts or sentences. Each group should have a \`startTime\` and \`endTime\`.
    * **Per-Segment Analysis:** For each subtitle group:
        * \`summary\`: A brief summary of the line's content.
        * \`purpose\`: The marketing purpose of the line (e.g., "建立信任", "制造紧迫感").
        * \`isVisuallyStrong\`: \`true\` ONLY if the visual composition for this specific segment is exceptionally good (e.g., great lighting, creative transition, strong emotional acting). Be very selective. Most segments should be \`false\`.
        * \`visualStrengthReason\`: **(MANDATORY if \`isVisuallyStrong\` is true)** A short tag explaining WHY it's strong (e.g., "构图出色", "情感表达强烈"). Cannot be null if \`isVisuallyStrong\` is true.
        * \`highlightSummary\`: **(MANDATORY if \`isVisuallyStrong\` is true)** A brief summary of what is happening in this high-quality clip. Cannot be null if \`isVisuallyStrong\` is true.

5.  **Overall Script Evaluation:**
    * \`marketing_stage_summary\`: Identify the primary marketing stage this script is suited for (Awareness, Interest, or Conversion) and briefly explain why.
    * \`negativeSummary\`: A single, critical sentence that summarizes the video's biggest weakness.
    * \`strengths\`: List 2-3 key strengths.
    * \`weaknesses\`: List 2-3 key weaknesses.
    * \`suggestions\`: Provide 2-3 actionable suggestions for improvement.

6.  **Video Tagging:**
    * \`videoType\`: Classify as either "口播视频" (presenter-led) or "配音视频" (voice-over).
    * \`emotionalTone\`: Describe the dominant emotional tone (e.g., "情绪饱满", "专业冷静", "焦虑不安").
    * \`coreContentAngle\`: Identify the single, most central topic or angle of the video (e.g., "产品成分深度解析", "痘痘肌急救指南", "热门产品吐槽").
    * \`otherTags\`: Provide 2-3 other relevant tags (e.g., "干货分享", "剧情演绎", "前后对比").

7.  **中文三大板块评分与检查清单（panel_evaluation）**：
    请返回 \`panel_evaluation\`，包含 \`hook\`（抓人能力）、\`pitch\`（种草能力）、\`close\`（转化能力）。每个对象包含：
    - \`score\`：0-100 的整数分。
    - \`analysis\`：中文总结，概述该板块的亮点与问题。
    - \`checklist\`：使用以下“中文键名”的布尔检查项，并为每项提供对应“说明”字符串（键名以“说明”结尾）。所有输出必须使用简体中文。

    抓人能力（Hook）Checklist：
    - 视觉冲击钩：
      - \`效果前置\`（boolean），\`效果前置说明\`（string）
      - \`质地诱惑\`（boolean），\`质地诱惑说明\`（string）
      - \`问题特写\`（boolean），\`问题特写说明\`（string）
    - 情景/痛点共鸣钩：
      - \`灵魂发问\`（boolean），\`灵魂发问说明\`（string）
      - \`反差剧情\`（boolean），\`反差剧情说明\`（string）

    种草能力（Pitch）Checklist：
    - 产品展示的可信度：
      - \`光线与画质达标\`（boolean），\`光线与画质说明\`（string）
      - \`手法专业流畅\`（boolean），\`手法专业流畅说明\`（string）
      - \`过程观感舒适\`（boolean），\`过程观感舒适说明\`（string）
      - \`对比真实性高\`（boolean），\`对比真实性说明\`（string）
    - 价值传递的说服力：
      - \`场景化植入具体\`（boolean），\`场景化植入说明\`（string）
      - \`表达自然有人味\`（boolean），\`表达自然说明\`（string）
      - \`感官细节充分\`（boolean），\`感官细节说明\`（string）
      - \`信任状有呈现\`（boolean），\`信任状说明\`（string）

    转化能力（Close）Checklist：
    - 行动号召（CTA）清晰度：
      - \`视觉CTA明显\`（boolean），\`视觉CTA说明\`（string）
      - \`口播CTA清晰\`（boolean），\`口播CTA说明\`（string）
      - \`字幕CTA明确\`（boolean），\`字幕CTA说明\`（string）
    - 降低决策门槛：
      - \`营造紧迫稀缺\`（boolean），\`紧迫稀缺说明\`（string）
      - \`提供风险对冲\`（boolean），\`风险对冲说明\`（string）

8.  **综合分计算规则（中文）**：
    - 请计算并返回 \`overall_score\`（整数），计算方式为：\`overall_score = round(hook.score * 0.4 + pitch.score * 0.4 + close.score * 0.2)\`。
    - 同时返回 \`score_rationale\`：用一句中文解释影响分数的最核心原因（引用钩子/主体/收尾中的关键要点）。

9.  **V3.0 标签体系选择规则（中文输出）**：
    - 你必须从以下白名单中选择标签，每个“子类”最多选择2个标签；若不确定可留空。所有输出字段名与文本都必须是简体中文。
    - 输出字段 \`v3_labeling\` 的结构包含以下键，并为每个“子类”附带一句中文 \`basis\`（来源依据），简要说明判断原因。
    - 白名单（仅可从下列集合中选）：
      - 创作者人设与定位/creator_persona.labels：
        - 专业权威型（皮肤科医生/药剂师、专业化妆师、成分党）
        - 亲和陪伴型（邻家闺蜜、泰式搞笑人、Vlogger）
        - 偶像向往型（颜值博主/Hi-So、明星名人）
        - 特定人群型（学生党、熟龄肌、大码自信美）
      - 视听呈现策略/appearance_scene.labels：真人出镜、仅手部出镜、产品空镜、沉浸式场景、专业影棚、户外自然光
      - 视听呈现策略/audio_speech.labels：人声口播(画外音)、现场收音(同期声)、无人声、口播风格:OMG式安利、口播风格:快语速带货、口播风格:聊天式分享、口播风格:ASMR耳语
      - 视听呈现策略/rhythm_bgm.labels：节奏紧凑/强卡点、节奏缓慢/沉浸式、BGM:TikTok热门BGM、BGM:泰语流行歌曲、BGM:氛围感纯音乐
      - 视听呈现策略/emotion_style.labels：视频情绪:情绪高昂、视频情绪:情绪平和、视频情绪:情绪专业、视觉风格:Cleanfit极简、视觉风格:Y2K复古、视觉风格:泰式甜美风
      - 内容策略与剧本/classic_patterns.labels：沉浸式护肤、妆前妆后对比、好物测评/红黑榜、保姆级教程、VLOG种草、挑战跟风、GRWM、GUWM
      - 内容策略与剧本/narrative_framework.labels：成分深扒/科普、P.A.S.结构、神话破解、对比测试
      - 产品展示焦点/core_selling_points.labels：成分故事、科技原理、独特肤感、最终妆效
      - 产品展示焦点/demonstration_methods.labels：高清质地特写、手臂/上脸试色、持久度/防水测试、包装美学展示
      - 情绪价值与心理挂钩/pain_shortcuts.labels：戳中痛点(痘肌/毛孔)、懒人必备
      - 情绪价值与心理挂钩/value_surprise.labels：惊天反差、空瓶记/铁皮、平替/大牌同款
      - 情绪价值与心理挂钩/sensory_emotion.labels：解压治愈、FOMO、知识获得感
      - 本土化与文化融合/language_humor.labels：泰语口语化表达、泰式英语夹杂、泰式幽默/玩梗
      - 本土化与文化融合/culture_trends.labels：泰国节日/事件、泰国影视/明星同款、社会热点
      - 本土化与文化融合/aesthetics_scenes.labels：符合泰国审美、本土生活场景
      - TikTok平台生态玩法/traffic_features.labels：使用热门BGM/音效、使用热门滤镜/特效
      - TikTok平台生态玩法/video_techniques.labels：卡点/转场运镜、绿幕/画中画特效
      - TikTok平台生态玩法/interaction_guidance.labels：引导评论区互动、使用投票/问答贴纸、引导合拍/Stitch
      - TikTok平台生态玩法/traffic_path.labels：挂小黄车/引流链接、直播预告引流、引导至主页Linktree
      - 商业转化策略/effect_value.labels：强效用展示、价格优势/促销
      - 商业转化策略/trust_urgency.labels：信任状/背书、制造稀缺/紧迫感
      - 商业转化策略/experience_cta.labels：开箱/沉浸式体验、行动号召(CTA)
      - 商业转化策略/conversion_path.labels：站内闭环、引流电商、引流私域

10. **消费者感知三支柱与红旗（中文输出）**：
    - 三支柱分别打分（0-100），并产出 checklist（布尔+说明）与简要 analysis：
      - 真实与信任（pillar1_authenticity_trust，权重50%）
      - 价值与说服（pillar2_value_persuasion，权重30%）
      - 转化准备度（pillar3_conversion_readiness，权重20%）
    - 红旗 red_flags：如下项若命中，请提供 hit/severity(notes)：
      - 不公平对比（角度/光线不一致或滤镜/美颜开着）
      - 医疗化/夸大承诺（如“秒变”“治愈”“永久根除”）
      - 纯广告感强/信息噪声大/SKU混乱
      - 全程无实拍/全靠素材堆叠
      - 合规（医疗暗示/虚假承诺）
    - 生成感知分 perception_score（0-100）与一句话 perception_rationale。

**Input:**
The system will provide only a video file (no keyframe images). Base your visual analysis (\`isVisuallyStrong\`, etc.) solely on the video content.

**Output:**
You MUST return a single JSON object matching the provided schema. Do not add any extra text or explanations.`;

  const prompt = '请分析这个视频，并根据schema返回JSON。所有输出必须是简体中文。三大板块（Hook/Pitch/Close）需评分与中文checklist。V3标签每子类最多2个并附一句中文依据。消费者感知三支柱（真实与信任/价值与说服/转化准备度）需评分与中文analysis，并以数组形式返回checklist（每项包含name/hit/notes）。红旗以数组返回（name/hit/severity/notes）。请计算perception_score与一句中文perception_rationale。严禁输出白名单之外的标签。';

  const videoPart = { inlineData: { data: buffer.toString('base64'), mimeType: 'video/mp4' } };
  const contents = { parts: [videoPart, { text: prompt }] };

  const result = await genAI.models.generateContent({
    model: 'gemini-2.5-flash',
    contents,
    config: {
      responseMimeType: 'application/json',
      temperature: 0.0,
      systemInstruction,
    },
  });

  console.log(`[DEBUG] Record ${feishuRecordId}: analyzeVideoWithSchema - Gemini raw result: ${JSON.stringify(result, null, 2)}`);

  let rawText;
  if (result?.text) {
    rawText = result.text.trim();
  } else if (result.response) {
    rawText = result.response.text();
  } else if (result.candidates?.[0]?.content?.parts?.[0]?.text) {
    console.warn(`[WARN] Record ${feishuRecordId}: analyzeVideoWithSchema - Missing response. Fallback to candidates.`);
    rawText = result.candidates[0].content.parts[0].text;
    const m = rawText.match(/```(?:json)?\n([\s\S]*?)\n```/);
    if (m && m[1]) rawText = m[1];
  } else {
    throw new Error('analyzeVideoWithSchema: Gemini returned no content');
  }

  if (!rawText) throw new Error('analyzeVideoWithSchema: Empty content');

  // 打印 Gemini 返回的原始文本内容（未经解析的完整字符串）
  try {
    console.log(`[DEBUG] Record ${feishuRecordId}: analyzeVideoWithSchema - Gemini raw text (as-is):\n${rawText}`);
  } catch (_) {
    // 忽略日志异常
  }

  try {
    const parsed = JSON.parse(rawText);
    // 打印解析后的 JSON（与 schema 对应）
    try {
      console.log(`[DEBUG] Record ${feishuRecordId}: analyzeVideoWithSchema - Parsed JSON:\n${JSON.stringify(parsed, null, 2)}`);
    } catch (_) {}
    // 服务端重算功能性分与综合分（降低波动）
    try {
      const pe = parsed?.panel_evaluation || {};
      let hRaw = typeof pe?.hook?.score === 'number' ? pe.hook.score : null;
      let pRaw = typeof pe?.pitch?.score === 'number' ? pe.pitch.score : null;
      let cRaw = typeof pe?.close?.score === 'number' ? pe.close.score : null;

      // 分制识别：若疑似 10 分制或 1 分制则整体放大
      const maxDim = Math.max(
        hRaw ?? -Infinity,
        pRaw ?? -Infinity,
        cRaw ?? -Infinity,
      );
      if (maxDim > -Infinity) {
        if (maxDim > 0 && maxDim <= 1) {
          if (hRaw !== null) hRaw *= 100;
          if (pRaw !== null) pRaw *= 100;
          if (cRaw !== null) cRaw *= 100;
        } else if (maxDim > 0 && maxDim <= 10) {
          if (hRaw !== null) hRaw *= 10;
          if (pRaw !== null) pRaw *= 10;
          if (cRaw !== null) cRaw *= 10;
        }
      }

      const clampInt = (v) => {
        if (typeof v !== 'number' || Number.isNaN(v)) return null;
        return Math.max(0, Math.min(100, Math.round(v)));
      };

      // 统计 checklist 命中率（支持对象或数组两种格式）
      const statsFromChecklist = (cl) => {
        if (!cl) return { hits: 0, total: 0 };
        if (Array.isArray(cl)) {
          let hits = 0, total = 0;
          for (const item of cl) {
            if (!item || typeof item !== 'object') continue;
            if (typeof item.hit === 'boolean') {
              total++;
              if (item.hit) hits++;
            }
          }
          return { hits, total };
        }
        if (typeof cl === 'object') {
          let hits = 0, total = 0;
          for (const [k, v] of Object.entries(cl)) {
            if (k.endsWith('说明')) continue;
            if (v === true || v === false) {
              total++;
              if (v === true) hits++;
            }
          }
          return { hits, total };
        }
        return { hits: 0, total: 0 };
      };

      const correctFunctionalDim = (raw, cl) => {
        let used = clampInt(raw);
        const { hits, total } = statsFromChecklist(cl);
        if (total > 0) {
          const base = Math.round((hits / total) * 100);
          // 与 base 相差过大则回正（保守：不做额外微调）
          if (used === null || Math.abs(used - base) > 15) used = clampInt(base);
          // 一致性护栏
          const rate = hits / total;
          if (rate >= 0.3 && used < 20) used = 20;
          if (rate <= 0.2 && used > 60) used = 60;
        }
        return used;
      };

      const hUsed = correctFunctionalDim(hRaw, pe?.hook?.checklist);
      const pUsed = correctFunctionalDim(pRaw, pe?.pitch?.checklist);
      const cUsed = correctFunctionalDim(cRaw, pe?.close?.checklist);

      if (hUsed !== null && pUsed !== null && cUsed !== null) {
        const functionalScore = Math.round(hUsed * 0.4 + pUsed * 0.4 + cUsed * 0.2);
        parsed.functional_score = functionalScore;
        parsed.functional_components = { hook_raw: clampInt(hRaw), hook_used: hUsed, pitch_raw: clampInt(pRaw), pitch_used: pUsed, close_raw: clampInt(cRaw), close_used: cUsed };
        try { console.log(`[DEBUG] Record ${feishuRecordId}: FS detail`, parsed.functional_components); } catch (_) {}
      }
      // 感知分：若模型给出三支柱得分，则计算 perception_score（并加入兜底回正）
      const cp = parsed?.consumer_pillars || {};
      const correctPillar = (pillar) => {
        if (!pillar) return null;
        const score = typeof pillar.score === 'number' ? pillar.score : null;
        const cl = Array.isArray(pillar.checklist) ? pillar.checklist : [];
        const total = cl.length || 1;
        const hits = cl.filter(i => i && i.hit === true).length;
        const base = Math.round((hits / total) * 100);
        // 质量微调无法从JSON推断，保守±0
        let corrected = base;
        // 一致性兜底：若命中≥30%，不过低于20分
        if (hits / total >= 0.3) corrected = Math.max(corrected, 20);
        // 若命中≤20%，不过高于60分
        if (hits / total <= 0.2) corrected = Math.min(corrected, 60);
        // 若模型分存在且与 corrected 差距 ≤15，取模型分；否则采用 corrected
        if (score !== null && Math.abs(score - corrected) <= 15) return score;
        return corrected;
      };
      const p1 = correctPillar(cp?.pillar1_authenticity_trust);
      const p2 = correctPillar(cp?.pillar2_value_persuasion);
      const p3 = correctPillar(cp?.pillar3_conversion_readiness);
      if (p1 !== null && p2 !== null && p3 !== null) {
        const perceptionScore = Math.round(p1 * 0.5 + p2 * 0.3 + p3 * 0.2);
        parsed.perception_score = perceptionScore;
      }
      // 融合分：功能性与感知各 50%（若其中之一缺失，则退化为另一个）
      const fs = typeof parsed.functional_score === 'number' ? parsed.functional_score : null;
      const ps2 = typeof parsed.perception_score === 'number' ? parsed.perception_score : null;
      let fused = null;
      if (fs !== null && ps2 !== null) fused = Math.round(fs * 0.5 + ps2 * 0.5);
      else if (fs !== null) fused = fs;
      else if (ps2 !== null) fused = ps2;

      // 红旗扣分与上限
      let penalty = 0; // -30..0
      let cap = 100;
      const rfRaw = parsed?.red_flags;
      const rfArr = Array.isArray(rfRaw)
        ? rfRaw
        : (rfRaw && typeof rfRaw === 'object')
          ? Object.keys(rfRaw).map((k) => ({ name: k, ...(rfRaw[k] || {}) }))
          : [];
      const findHit = (name) => rfArr.find((x) => x && x.name === name && x.hit === true);
      const applyPenalty = (item, ranges) => {
        if (!item) return;
        const sev = String(item.severity || '').toLowerCase();
        if (sev.includes('high')) penalty += ranges.high;
        else if (sev.includes('mid')) penalty += ranges.mid;
        else if (sev.includes('low')) penalty += ranges.low;
      };
      // 不公平对比
      const unfair = findHit('不公平对比');
      applyPenalty(unfair, { low: -10, mid: -15, high: -20 });
      if (unfair) cap = Math.min(cap, 60);
      // 医疗化或夸大承诺
      const medical = findHit('医疗化或夸大承诺');
      applyPenalty(medical, { low: -15, mid: -20, high: -30 });
      if (medical) cap = Math.min(cap, 50);
      // 纯广告感强或噪声大或SKU混乱
      const adlike = findHit('纯广告感强或噪声大或SKU混乱');
      applyPenalty(adlike, { low: -5, mid: -10, high: -15 });
      // 全程无实拍或素材堆叠
      const noLive = findHit('全程无实拍或素材堆叠');
      applyPenalty(noLive, { low: -10, mid: -15, high: -20 });
      if (noLive) cap = Math.min(cap, 60);
      // 合规_医疗暗示或虚假承诺
      const compliance = findHit('合规_医疗暗示或虚假承诺');
      if (compliance) cap = Math.min(cap, 50);

      if (typeof fused === 'number') {
        const afterPenalty = Math.max(0, Math.min(100, fused + penalty));
        parsed.fused_pre_penalty = fused;
        parsed.penalty_total = penalty;
        parsed.cap_applied = cap;
        parsed.final_score = Math.min(afterPenalty, cap);
        parsed.overall_score = parsed.final_score; // 向后兼容：覆盖 overall_score
        try {
          const rfDebug = rfArr && rfArr.length ? rfArr.filter(x => x && x.hit === true) : [];
          console.log(`[DEBUG] Record ${feishuRecordId}: scoring detail`, {
            functional_score: parsed.functional_score,
            perception_score: parsed.perception_score,
            fused_pre_penalty: parsed.fused_pre_penalty,
            penalty_total: parsed.penalty_total,
            cap_applied: parsed.cap_applied,
            final_score: parsed.final_score,
            red_flags_hits: rfDebug,
          });
        } catch (_) {}
      }
    } catch (_) {}
    return parsed;
  } catch (e) {
    throw new Error(`analyzeVideoWithSchema: JSON parse failed. Head: ${rawText.slice(0, 500)}...`);
  }
}

async function getFieldMeta(appToken, tableId, accessToken) {
  const resp = await fetch(`https://open.feishu.cn/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/fields`, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  const text = await resp.text();
  try {
    const data = JSON.parse(text);
    if (data.code !== 0) throw new Error(`Get fields meta failed: ${data.msg}`);
    return data.data?.items || [];
  } catch (e) {
    throw new Error(`Get fields meta failed: ${resp.status} ${resp.statusText} ${text?.slice(0,200)}`);
  }
}

async function ensureTextField(appToken, tableId, accessToken, fieldName) {
  const fields = await getFieldMeta(appToken, tableId, accessToken);
  if (fields.some((f) => f.field_name === fieldName)) return;
  const resp = await fetch(`https://open.feishu.cn/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/fields`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify({ field_name: fieldName, type: 1 }),
  });
  const text = await resp.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error(`Create field '${fieldName}' failed: ${resp.status} ${resp.statusText} ${text?.slice(0,200)}`);
  }
  if (data.code !== 0) throw new Error(`Create field '${fieldName}' failed: ${data.msg}`);
}

async function getOrCreateMultiSelectField(appToken, tableId, accessToken) {
  const fields = await getFieldMeta(appToken, tableId, accessToken);
  const primaryName = '视频标签';
  const altName = '视频标签（多选）';
  const found = fields.find(f => f.field_name === primaryName);
  if (found && Number(found.type) === 20) {
    return { fieldId: found.field_id, fieldName: primaryName };
  }
  // 若存在同名但非多选，尝试寻找备用名
  const alt = fields.find(f => f.field_name === altName && Number(f.type) === 20);
  if (alt) {
    return { fieldId: alt.field_id, fieldName: altName };
  }
  // 创建一个新的多选字段
  const createResp = await fetch(`https://open.feishu.cn/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/fields`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify({ field_name: altName, type: 20, property: { options: [] } })
  });
  const createText = await createResp.text();
  let created;
  try { created = JSON.parse(createText); } catch (_) {
    throw new Error(`Create multi-select field failed: ${createResp.status} ${createResp.statusText} ${createText?.slice(0,200)}`);
  }
  if (created.code !== 0) {
    throw new Error(`Create multi-select field failed: ${created.msg}`);
  }
  const newField = created.data || {};
  return { fieldId: newField.field_id, fieldName: altName };
}

// 确保存在数字类型字段
async function ensureNumberField(appToken, tableId, accessToken, fieldName) {
  const fields = await getFieldMeta(appToken, tableId, accessToken);
  const exists = fields.find((f) => f.field_name === fieldName);
  if (exists) return;
  const resp = await fetch(`https://open.feishu.cn/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/fields`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify({ field_name: fieldName, type: 2 }), // 2: 数字
  });
  const text = await resp.text();
  let data;
  try { data = JSON.parse(text); } catch (_) {
    throw new Error(`Create number field '${fieldName}' failed: ${resp.status} ${resp.statusText} ${text?.slice(0,200)}`);
  }
  if (data.code !== 0) throw new Error(`Create number field '${fieldName}' failed: ${data.msg}`);
}

async function ensureMultiSelectOptions(appToken, tableId, fieldId, tagNames, accessToken) {
  // 拉取当前 options（容错：若返回非 JSON，则退化为用列表接口获取字段属性）
  let current = [];
  try {
    const resp = await fetch(`https://open.feishu.cn/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/fields/${fieldId}`, {
      headers: { Authorization: `Bearer ${accessToken}` },
    });
    const text = await resp.text();
    const data = JSON.parse(text);
    if (data.code !== 0) throw new Error(`Get field detail failed: ${data.msg}`);
    current = (data.data?.property?.options || []).map(o => ({ id: o.id, name: o.name }));
  } catch (e) {
    const fields = await getFieldMeta(appToken, tableId, accessToken);
    const tagField = fields.find(f => f.field_id === fieldId);
    current = (tagField?.property?.options || []).map(o => ({ id: o.id, name: o.name }));
  }

  const existsMap = new Map(current.map(o => [o.name, o.id]));
  const needCreate = tagNames.filter(name => !existsMap.has(name));
  if (needCreate.length === 0) return tagNames.map(n => existsMap.get(n));

  // 追加新选项
  const newOptions = current.concat(needCreate.map((name) => ({ name })));
  const updateResp = await fetch(`https://open.feishu.cn/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/fields/${fieldId}`, {
    method: 'PUT',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify({
      field_name: '视频标签',
      type: 20, // 多选类型
      property: { options: newOptions },
    }),
  });
  const updatedText = await updateResp.text();
  let updated;
  try {
    updated = JSON.parse(updatedText);
  } catch (e) {
    throw new Error(`Update multi-select options failed: ${updateResp.status} ${updateResp.statusText} ${updatedText?.slice(0,200)}`);
  }
  if (updated.code !== 0) throw new Error(`Update multi-select options failed: ${updated.msg}`);

  const latest = updated.data?.property?.options || [];
  const latestMap = new Map(latest.map(o => [o.name, o.id]));
  return tagNames.map(n => latestMap.get(n)).filter(Boolean);
}

async function updateRecord(appToken, tableId, recordId, fields, accessToken) {
  const resp = await fetch(`https://open.feishu.cn/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/records/${recordId}`, {
    method: 'PUT',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify({ fields }),
  });
  const text = await resp.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error(`Update record failed: ${resp.status} ${resp.statusText} ${text?.slice(0,200)}`);
  }
  if (data.code !== 0) throw new Error(`Update record failed: ${data.msg}`);
}

function formatSubtitlesAndAnalysis(result) {
  const groups = Array.isArray(result?.subtitle_groups) ? result.subtitle_groups : [];
  if (!groups.length) return '';

  const purposeIcon = (purpose) => {
    const p = String(purpose || '').toLowerCase();
    if (!p) return '';
    if (p.includes('建立信任') || p.includes('trust')) return '🤝 ';
    if (p.includes('制造紧迫感') || p.includes('紧迫') || p.includes('urgency')) return '⏰ ';
    if (p.includes('吸引注意') || p.includes('钩子') || p.includes('hook')) return '🎣 ';
    if (p.includes('引导互动') || p.includes('互动') || p.includes('engagement')) return '💬 ';
    if (p.includes('行动号召') || p.includes('cta') || p.includes('下单') || p.includes('购买')) return '👉 ';
    if (p.includes('证明') || p.includes('背书') || p.includes('evidence') || p.includes('proof')) return '✅ ';
    return '';
  };

  const lines = [];
  lines.push('## 字幕与分析');
  const highlightCount = groups.filter(g => g?.isVisuallyStrong).length;
  lines.push(`- 片段数: ${groups.length} | 高光片段: ${highlightCount}`);

  groups.forEach((segment, idx) => {
    const start = segment.startTime || '??:??';
    const end = segment.endTime || '??:??';
    const isStrong = !!segment.isVisuallyStrong;
    const badge = isStrong ? ' 🌟 高光' : '';
    lines.push('', `### [${start} - ${end}]${badge}`);

    if (isStrong && segment.visualStrengthReason) {
      lines.push(`- 高光理由: ${segment.visualStrengthReason}`);
    }
    if (isStrong && segment.highlightSummary) {
      lines.push(`- 高光片段: ${segment.highlightSummary}`);
    }

    if (segment.text) lines.push(`- 原文: ${segment.text}`);
    if (segment.translation) lines.push(`- 译文: ${segment.translation}`);
    if (segment.summary) lines.push(`- 总结: ${segment.summary}`);
    if (segment.purpose) lines.push(`- 目的: ${purposeIcon(segment.purpose)}${segment.purpose}`);

    if (idx < groups.length - 1) {
      lines.push('', '---');
    }
  });

  return lines.join('\n');
}

function formatVideoStructureAndAnalysis(result) {
  const vs = result?.video_structure;
  if (!vs) return '';

  const getIconForPart = (name = '') => {
    const n = String(name).toLowerCase();
    if (/(hook|钩子)/.test(n)) return '🎣';
    if (/(pain|problem|痛点|问题)/.test(n)) return '❓';
    if (/(product|展示|方案|演示|种草|推荐)/.test(n)) return '🧴';
    if (/(proof|evidence|背书|案例|证明)/.test(n)) return '✅';
    if (/(compare|对比|前后|before|after)/.test(n)) return '🔀';
    if (/(transition|转场|过渡|节奏)/.test(n)) return '⏩';
    if (/(summary|总结|复盘|收束)/.test(n)) return '🧾';
    if (/(cta|call to action|行动|号召|购买|下单)/.test(n)) return '👉';
    return '📌';
  };

  const escapePipes = (text) => String(text ?? '').replace(/\|/g, '\\|');

  const lines = [];
  lines.push('## 视频结构与分析');
  lines.push('', '### 黄金3秒分析（不足与建议）');
  lines.push(vs.golden_3s_hook_analysis || '（无）');

  const segs = Array.isArray(vs.segments) ? vs.segments : [];
  if (segs.length > 0) {
    lines.push('', '---', '', '### 内容结构');
    if (segs.length <= 12) {
      // 表格模式
      lines.push('| 时间段 | 模块 | 描述 |');
      lines.push('| --- | --- | --- |');
      for (const seg of segs) {
        const time = seg?.timeRange || '00:00 - 00:00';
        const name = seg?.partName || '';
        const icon = getIconForPart(name);
        const desc = escapePipes(seg?.description || '（无）');
        lines.push(`| ${escapePipes(time)} | ${icon} ${escapePipes(name)} | ${desc} |`);
      }
    } else {
      // 列表模式
      for (const seg of segs) {
        const time = seg?.timeRange || '00:00 - 00:00';
        const name = seg?.partName || '';
        const icon = getIconForPart(name);
        lines.push('', `#### [${time}] ${icon} ${name}`.trim());
        lines.push(`- 描述: ${seg?.description || '（无）'}`);
      }
    }
  }

  return lines.join('\n');
}

function formatOverallScriptEvaluation(result) {
  const lines = [];
  lines.push('## 整体脚本评估');

  // 顶部得分与理由
  // 展示原始分（未扣分、未限顶）
  const deriveFused = () => {
    const fs = result?.functional_score;
    const ps = result?.perception_score;
    if (typeof fs === 'number' && typeof ps === 'number') return Math.round(fs * 0.5 + ps * 0.5);
    if (typeof fs === 'number') return fs;
    if (typeof ps === 'number') return ps;
    return null;
  };
  const score = (typeof result?.fused_pre_penalty === 'number')
    ? result.fused_pre_penalty
    : (deriveFused() ?? (result?.overall_score ?? result?.final_score));
  const rationale = result?.score_rationale;
  if (typeof score === 'number') {
    lines.push(`- **综合得分**: ${score}/100`);
  }
  if (rationale) {
    lines.push(`- **评分理由**: ${rationale}`);
  }

  // 分数拆解（功能性/感知/红旗）
  const fs = result?.functional_score;
  const ps = result?.perception_score;
  if (typeof fs === 'number' || typeof ps === 'number') {
    lines.push('', '### 分数拆解');
    if (typeof fs === 'number') lines.push(`- 功能性分: ${fs}/100（由 Hook/Pitch/Close 加权 40/40/20）`);
    if (typeof ps === 'number') lines.push(`- 感知分: ${ps}/100（由 真实与信任/价值与说服/转化准备度 加权 50/30/20）`);
    const fusedPre = result?.fused_pre_penalty;
    const pen = result?.penalty_total;
    const cap = result?.cap_applied;
    // 精简展示：不展示 融合前分/分数上限/融合后分，仅保留必要信息
    if (typeof pen === 'number' && pen !== 0) lines.push(`- 红旗扣分: ${pen}`);
  }

  // 新增：质量评级与优质度
  const qi = typeof result?.quality_index === 'number' ? result.quality_index : null;
  const qr = result?.quality_rating;
  const oc = (typeof result?.on_camera_speech === 'boolean') ? result.on_camera_speech : (typeof result?.on_camera_speech_final === 'boolean' ? result.on_camera_speech_final : null);
  const ocBasis = result?.on_camera_speech_basis || result?.quality_components?.on_camera_speech_basis || '';
  const missSummary = Array.isArray(result?.quality_components?.missing_summary) ? result.quality_components.missing_summary : [];
  if (qi !== null || qr || oc !== null) {
    lines.push('', '### 质量评级与优质度');
    if (qr) lines.push(`- 质量评级: ${qr}`);
    if (qi !== null) lines.push(`- 优质度指数: ${qi}/100`);
    // 明确基础分来源以避免误解（默认为综合原始分 fused_pre_penalty）
    const baseForQuality = (typeof result?.fused_pre_penalty === 'number') ? result.fused_pre_penalty : deriveFused();
    if (typeof baseForQuality === 'number') lines.push(`- 基础分: ${baseForQuality}/100`);
    if (oc !== null) lines.push(`- 真人口播: ${oc ? '是' : '否'}${ocBasis ? `（依据：${ocBasis}）` : ''}`);
    const missParts = missSummary.filter(Boolean).slice(0, 3);
    if (missParts.length) lines.push(`- 关键缺失: ${missParts.join('、')}`);
    const cmp = result?.quality_components;
    if (cmp) {
      // 加分与扣分分别列出具体项
      const pos = Array.isArray(cmp.positive_reasons) ? cmp.positive_reasons : [];
      const hl = Array.isArray(cmp.highlight_reasons) ? cmp.highlight_reasons : [];
      const negCritical = Array.isArray(cmp.critical_reasons) ? cmp.critical_reasons : [];
      const negAnti = Array.isArray(cmp.anti_reasons) ? cmp.anti_reasons : [];
      const unfairSingle = typeof cmp.unfair_penalty === 'number' && cmp.unfair_penalty !== 0 ? `不公平对比（${cmp.unfair_penalty}）` : '';
      const medicalSingle = typeof cmp.medical_penalty === 'number' && cmp.medical_penalty !== 0 ? `医疗化/夸大承诺（${cmp.medical_penalty}）` : '';

      const addItems = [...pos, ...hl];
      const subItems = [...negCritical, ...negAnti];

      if (addItems.length) {
        lines.push('- 加分项:');
        addItems.forEach((t) => lines.push(`  - ${t}`));
      }
      if (subItems.length || unfairSingle || medicalSingle) {
        lines.push('- 减分项:');
        subItems.forEach((t) => lines.push(`  - ${t}`));
        if (unfairSingle || medicalSingle) {
          lines.push('  - 单列标注:');
          if (unfairSingle) lines.push(`    - ${unfairSingle}`);
          if (medicalSingle) lines.push(`    - ${medicalSingle}`);
        }
      }
    }
  }

  // 新增：三大板块评估（若存在）
  const pe = result?.panel_evaluation || {};
  const renderPanel = (key, title) => {
    const panel = pe?.[key];
    if (!panel) return;
    const panelScore = typeof panel.score === 'number' ? panel.score : null;
    lines.push('', `### ${title}${panelScore !== null ? ` — 评分: ${panelScore}/100` : ''}`);
    if (panel.analysis) {
      lines.push(panel.analysis);
    }
    // checklist：将 true 归为“达成要点”，false 归为“待改进要点”
    const checklist = panel.checklist && typeof panel.checklist === 'object' ? panel.checklist : null;
    if (checklist) {
      const achieved = [];
      const toImprove = [];
      for (const [k, v] of Object.entries(checklist)) {
        // 将成对的“说明”键分离，用主键名展示
        if (k.endsWith('说明')) continue;
        const explain = checklist[`${k}说明`];
        const text = explain ? `${k}（${explain}）` : k;
        if (v === true) achieved.push(text);
        else if (v === false) toImprove.push(text);
      }
      if (achieved.length) {
        lines.push('', '- 达成要点:');
        achieved.forEach((t) => lines.push(`  - ${t}`));
      }
      if (toImprove.length) {
        lines.push('', '- 待改进要点:');
        toImprove.forEach((t) => lines.push(`  - ${t}`));
      }
    }
  };
  renderPanel('hook', '抓人能力（Hook）');
  renderPanel('pitch', '种草能力（Pitch）');
  renderPanel('close', '转化能力（Close）');

  // 三支柱（消费者感知）输出
  const cp = result?.consumer_pillars || {};
  const renderPillar = (p, title) => {
    if (!p) return;
    const pScore = typeof p.score === 'number' ? p.score : null;
    lines.push('', `### ${title}${pScore !== null ? ` — 评分: ${pScore}/100` : ''}`);
    if (p.analysis) lines.push(p.analysis);
    const clArr = Array.isArray(p.checklist) ? p.checklist : null;
    if (clArr) {
      const achieved = [];
      const toImprove = [];
      for (const item of clArr) {
        const name = item?.name || '';
        const explain = item?.notes || '';
        const text = explain ? `${name}（${explain}）` : name;
        if (item?.hit === true) achieved.push(text);
        else if (item?.hit === false) toImprove.push(text);
      }
      if (achieved.length) {
        lines.push('', '- 达成要点:');
        achieved.forEach((t) => lines.push(`  - ${t}`));
      }
      if (toImprove.length) {
        lines.push('', '- 待改进要点:');
        toImprove.forEach((t) => lines.push(`  - ${t}`));
      }
    }
  };
  renderPillar(cp.pillar1_authenticity_trust, '真实与信任（支柱1）');
  renderPillar(cp.pillar2_value_persuasion, '价值与说服（支柱2）');
  renderPillar(cp.pillar3_conversion_readiness, '转化准备度（支柱3）');

  const sa = result?.script_analysis || {};

  // 适用营销阶段
  if (sa.marketing_stage_summary) {
    lines.push('', '### 适用营销阶段');
    lines.push(`- ${sa.marketing_stage_summary}`);
  }

  // 核心问题
  if (sa.negativeSummary) {
    lines.push('', '### 核心问题');
    lines.push(`- ⚠️ ${sa.negativeSummary}`);
  }

  // 优点
  if (Array.isArray(sa.strengths) && sa.strengths.length) {
    lines.push('', '### 优点');
    sa.strengths.forEach((s) => lines.push(`- ${s}`));
  }

  // 缺点
  if (Array.isArray(sa.weaknesses) && sa.weaknesses.length) {
    lines.push('', '### 缺点');
    sa.weaknesses.forEach((w) => lines.push(`- ${w}`));
  }

  // 优化建议
  if (Array.isArray(sa.suggestions) && sa.suggestions.length) {
    lines.push('', '### 优化建议');
    sa.suggestions.forEach((s) => lines.push(`- ${s}`));
  }

  // 标签信息
  const vt = result?.video_tags;
  if (vt && (vt.videoType || vt.emotionalTone || vt.coreContentAngle || (Array.isArray(vt.otherTags) && vt.otherTags.length))) {
    lines.push('', '### 标签信息');
    if (vt.videoType) lines.push(`- 视频类型: ${vt.videoType}`);
    if (vt.emotionalTone) lines.push(`- 情绪基调: ${vt.emotionalTone}`);
    if (vt.coreContentAngle) lines.push(`- 核心角度: ${vt.coreContentAngle}`);
    if (Array.isArray(vt.otherTags) && vt.otherTags.length) lines.push(`- 其他标签: ${vt.otherTags.join(' · ')}`);
  }

  // 标签命中（V3.0融合版，扁平输出仅命中项）
  const lb = result?.v3_labeling;
  if (lb && typeof lb === 'object') {
    const flatLines = [];
    const tryPush = (dimension, sub, node) => {
      if (!node || typeof node !== 'object') return;
      const labels = Array.isArray(node.labels) ? node.labels : [];
      const basis = typeof node.basis === 'string' && node.basis.trim() ? node.basis.trim() : '';
      for (const tag of labels) {
        flatLines.push(`- [${dimension}]--[${sub}]-[${tag}]`);
        if (basis) flatLines.push(`  - 依据: ${basis}`);
      }
    };
    // 维度一
    tryPush('创作者人设与定位', '人设', lb.creator_persona);
    // 维度二
    const va = lb.visual_audio || {};
    tryPush('视听呈现策略', '出镜与场景', va.appearance_scene);
    tryPush('视听呈现策略', '音频与口播', va.audio_speech);
    tryPush('视听呈现策略', '节奏与BGM', va.rhythm_bgm);
    tryPush('视听呈现策略', '情绪与风格', va.emotion_style);
    // 维度三
    const cs = lb.content_script || {};
    tryPush('内容策略与剧本', '经典内容套路', cs.classic_patterns);
    tryPush('内容策略与剧本', '叙事框架', cs.narrative_framework);
    // 维度四
    const ps = lb.product_showcase || {};
    tryPush('产品展示焦点', '核心卖点', ps.core_selling_points);
    tryPush('产品展示焦点', '展示手法', ps.demonstration_methods);
    // 维度五
    const eh = lb.emotional_hooks || {};
    tryPush('情绪价值与心理挂钩', '痛点与捷径', eh.pain_shortcuts);
    tryPush('情绪价值与心理挂钩', '价值与惊喜', eh.value_surprise);
    tryPush('情绪价值与心理挂钩', '感官与情感', eh.sensory_emotion);
    // 维度六
    const loc = lb.localization || {};
    tryPush('本土化与文化融合', '语言与幽默', loc.language_humor);
    tryPush('本土化与文化融合', '文化与热点', loc.culture_trends);
    tryPush('本土化与文化融合', '审美与场景', loc.aesthetics_scenes);
    // 维度七
    const eco = lb.tiktok_ecosystem || {};
    tryPush('TikTok平台生态玩法', '流量功能利用', eco.traffic_features);
    tryPush('TikTok平台生态玩法', '视频技法', eco.video_techniques);
    tryPush('TikTok平台生态玩法', '互动引导', eco.interaction_guidance);
    tryPush('TikTok平台生态玩法', '流量路径', eco.traffic_path);
    // 维度八
    const com = lb.commercial_conversion || {};
    tryPush('商业转化策略', '效果与价值', com.effect_value);
    tryPush('商业转化策略', '信任与紧迫', com.trust_urgency);
    tryPush('商业转化策略', '体验与号召', com.experience_cta);
    tryPush('商业转化策略', '转化路径', com.conversion_path);

    if (flatLines.length) {
      lines.push('', '### 标签命中（V3.0融合版）');
      lines.push(...flatLines);
    }
  }

  // 红旗与合规（若命中）
  const rf = Array.isArray(result?.red_flags) ? result.red_flags.filter(x => x && x.hit) : [];
  if (rf.length) {
    lines.push('', '### 红旗与合规');
    for (const item of rf) {
      const name = item?.name || '未知红旗';
      const sev = item?.severity || '';
      const notes = item?.notes || '';
      lines.push(`- ${name}${sev ? `（${sev}）` : ''}${notes ? `：${notes}` : ''}`);
    }
  }

  return lines.join('\n');
}

// ===== 质量信号与评分（优质度指数/评级） =====
function timeStrToSeconds(s) {
  if (!s || typeof s !== 'string') return null;
  const parts = s.trim().split(':').map((v) => Number(v));
  if (parts.some((v) => Number.isNaN(v))) return null;
  if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
  if (parts.length === 2) return parts[0] * 60 + parts[1];
  if (parts.length === 1) return parts[0];
  return null;
}

function getArraySafe(v) { return Array.isArray(v) ? v : []; }
function getLabels(node) {
  if (!node || typeof node !== 'object') return [];
  const arr = Array.isArray(node.labels) ? node.labels : [];
  return arr.filter((x) => typeof x === 'string');
}

function includesAny(text, kws) {
  const t = String(text || '').toLowerCase();
  return kws.some((k) => t.includes(k));
}

function detectStructurePresence(result) {
  const segs = getArraySafe(result?.video_structure?.segments);
  const names = segs.map((s) => String(s?.partName || '')).filter(Boolean);
  const hasByName = (kws) => names.some((n) => includesAny(n, kws));
  const pe = result?.panel_evaluation || {};
  const closeCl = pe?.close?.checklist || {};
  const pitchCl = pe?.pitch?.checklist || {};
  const hookCl = pe?.hook?.checklist || {};

  const hook = hasByName(['hook', '钩', '开场']) || Object.values(hookCl).some((v) => v === true);
  const painOrScene = hasByName(['痛点', '问题', '场景', 'problem', 'pain', 'scene']) || (hookCl['灵魂发问'] === true || hookCl['反差剧情'] === true);
  const showcase = hasByName(['展示', '演示', '方案', '种草', 'product', 'showcase', 'demo']) || (pitchCl['手法专业流畅'] === true || pitchCl['过程观感舒适'] === true);
  const proof = hasByName(['证明', '背书', '对比', '案例', 'proof', 'compare', 'before', 'after']) || (pitchCl['对比真实性高'] === true);
  const cta = hasByName(['cta', '行动', '号召', '购买', '下单', 'link', '关注', 'call to action']) || (closeCl['视觉CTA明显'] === true || closeCl['口播CTA清晰'] === true || closeCl['字幕CTA明确'] === true);

  return { hook, painOrScene, showcase, proof, cta };
}

function computeQualitySignals(result) {
  const pe = result?.panel_evaluation || {};
  const closeCl = pe?.close?.checklist || {};
  const pitchCl = pe?.pitch?.checklist || {};
  const hookScore = typeof pe?.hook?.score === 'number' ? pe.hook.score : null;
  const closeScore = typeof pe?.close?.score === 'number' ? pe.close.score : null;

  const visualAudio = result?.v3_labeling?.visual_audio || {};
  const appearance = getLabels(visualAudio.appearance_scene);
  const audioSpeech = getLabels(visualAudio.audio_speech);
  const rhythm = getLabels(visualAudio.rhythm_bgm);
  const emotion = getLabels(visualAudio.emotion_style);

  const contentScript = result?.v3_labeling?.content_script || {};
  const narrative = getLabels(contentScript.narrative_framework);
  const demoMethods = getLabels(result?.v3_labeling?.product_showcase?.demonstration_methods);

  const onCameraProvided = typeof result?.on_camera_speech === 'boolean' ? result.on_camera_speech : null;
  const isPersonOnCam = appearance.includes('真人出镜');
  const hasVoiceOver = audioSpeech.some((l) => l.includes('人声口播') || l.toLowerCase().includes('voice'));
  const hasSync = audioSpeech.some((l) => l.includes('现场收音') || l.includes('同期声'));
  const onCameraDerived = isPersonOnCam && hasSync && !hasVoiceOver;
  const onCameraFinal = onCameraProvided !== null ? onCameraProvided : onCameraDerived;

  const emotionNatural = pitchCl['表达自然有人味'] === true;
  const emotionNotNatural = pitchCl['表达自然有人味'] === false;
  const emotionHigh = emotion.some((l) => l.includes('视频情绪:情绪高昂'));

  const qualityPass = pitchCl['光线与画质达标'] === true;
  const qualityFail = pitchCl['光线与画质达标'] === false;

  const groups = getArraySafe(result?.subtitle_groups);
  let strongCount = 0; let durationTotal = 0; let durationCount = 0;
  for (const g of groups) {
    if (g?.isVisuallyStrong) strongCount++;
    const a = timeStrToSeconds(g?.startTime);
    const b = timeStrToSeconds(g?.endTime);
    if (a !== null && b !== null && b > a) { durationTotal += (b - a); durationCount++; }
  }
  const strongRatio = groups.length > 0 ? (strongCount / groups.length) : 0;
  const avgSegDur = durationCount > 0 ? (durationTotal / durationCount) : null;

  const structure = detectStructurePresence(result);
  const ctaHits = [closeCl['视觉CTA明显'] === true, closeCl['口播CTA清晰'] === true, closeCl['字幕CTA明确'] === true].filter(Boolean).length;

  const rfArr = Array.isArray(result?.red_flags) ? result.red_flags : [];
  const getRF = (name) => rfArr.find((x) => x && x.name === name && x.hit === true) || null;
  const unfair = getRF('不公平对比');
  const medical = getRF('医疗化或夸大承诺');
  const adlike = getRF('纯广告感强或噪声大或SKU混乱');
  const noLive = getRF('全程无实拍或素材堆叠');

  return {
    structure,
    hookScore, closeScore,
    isPersonOnCam, hasVoiceOver, hasSync, onCameraFinal,
    emotionNatural, emotionNotNatural, emotionHigh,
    qualityPass, qualityFail,
    strongCount, strongRatio, avgSegDur,
    demoMethods, narrative, rhythm,
    ctaHits,
    rf: { unfair, medical, adlike, noLive },
  };
}

function penaltyFromSeverity(node, map) {
  if (!node) return 0;
  const sev = String(node.severity || '').toLowerCase();
  if (sev.includes('high')) return map.high;
  if (sev.includes('mid')) return map.mid;
  if (sev.includes('low')) return map.low;
  return 0;
}

function computeQualityIndexAndRating(result) {
  const signals = computeQualitySignals(result);
  const base = (typeof result?.fused_pre_penalty === 'number')
    ? result.fused_pre_penalty
    : (() => {
        const fs = typeof result?.functional_score === 'number' ? result.functional_score : null;
        const ps = typeof result?.perception_score === 'number' ? result.perception_score : null;
        if (fs !== null && ps !== null) return Math.round(fs * 0.5 + ps * 0.5);
        if (fs !== null) return fs;
        if (ps !== null) return ps;
        return 0;
      })();

  // 准备明细容器
  const positiveReasons = [];
  const highlightReasons = [];
  const criticalReasons = [];
  const antiReasons = [];

  // 缺失项扣分
  let criticalPenalty = 0;
  const miss = [];
  if (!signals.structure.hook) { criticalPenalty += 25; miss.push('缺Hook'); criticalReasons.push('缺Hook -25'); }
  if (!signals.structure.cta) { criticalPenalty += 25; miss.push('缺CTA'); criticalReasons.push('缺CTA -25'); }
  if (!signals.structure.painOrScene) { criticalPenalty += 10; miss.push('缺痛点/场景'); criticalReasons.push('缺痛点/场景 -10'); }
  if (!signals.structure.showcase) { criticalPenalty += 10; miss.push('缺展示/方案'); criticalReasons.push('缺展示/方案 -10'); }
  if (!signals.structure.proof) { criticalPenalty += 10; miss.push('缺证据/对比'); criticalReasons.push('缺证据/对比 -10'); }
  if (typeof signals.hookScore === 'number' && signals.hookScore < 40) { criticalPenalty += 5; criticalReasons.push('Hook质量低(<40) -5'); }
  if (typeof signals.closeScore === 'number' && signals.closeScore < 40) { criticalPenalty += 5; criticalReasons.push('Close质量低(<40) -5'); }

  // 反模式扣分（排除：不公平对比/医疗化，改为单列标注）
  let antiPatternPenalty = 0;
  const adlikeP = penaltyFromSeverity(signals.rf.adlike, { low: 5, mid: 10, high: 15 });
  if (adlikeP > 0) { antiPatternPenalty += adlikeP; antiReasons.push(`纯广告感强/噪声大或SKU混乱 -${adlikeP}`); }
  const noLiveP = penaltyFromSeverity(signals.rf.noLive, { low: 10, mid: 15, high: 20 });
  if (noLiveP > 0) { antiPatternPenalty += noLiveP; antiReasons.push(`全程无实拍或素材堆叠 -${noLiveP}`); }

  const unfairPenalty = -penaltyFromSeverity(signals.rf.unfair, { low: 10, mid: 15, high: 20 });
  const medicalPenalty = -penaltyFromSeverity(signals.rf.medical, { low: 15, mid: 20, high: 30 });

  // 正向加分与其它扣分
  let positive = 0;
  // 结构覆盖正向：痛点/展示/证据/CTA 各 +2；黄金3秒无致命缺陷 +2（简化为无“严重/致命/失败”等词）
  if (signals.structure.painOrScene) { positive += 2; positiveReasons.push('结构覆盖-痛点/场景 +2'); }
  if (signals.structure.showcase) { positive += 2; positiveReasons.push('结构覆盖-展示/方案 +2'); }
  if (signals.structure.proof) { positive += 2; positiveReasons.push('结构覆盖-证据/对比 +2'); }
  if (signals.structure.cta) { positive += 2; positiveReasons.push('结构覆盖-CTA +2'); }
  const g3 = String(result?.video_structure?.golden_3s_hook_analysis || '').toLowerCase();
  const badG3 = ['致命', '严重', '失败', '完全没有', '极差'].some((k) => g3.includes(k));
  if (!badG3 && g3) { positive += 2; positiveReasons.push('黄金3秒无致命缺陷 +2'); }

  // 真人口播（当镜口述）正负
  if (signals.isPersonOnCam) { positive += 3; positiveReasons.push('真人出镜 +3'); }
  if (signals.hasSync) { positive += 2; positiveReasons.push('同期声 +2'); }
  if (signals.isPersonOnCam && signals.hasSync && !signals.hasVoiceOver) { positive += 2; positiveReasons.push('当镜口述 +2'); }
  if (signals.isPersonOnCam && signals.hasVoiceOver) { criticalPenalty += 5; criticalReasons.push('真人出镜但画外音 -5'); }
  if (!signals.isPersonOnCam && !signals.hasSync) { criticalPenalty += 10; criticalReasons.push('无真人且无当镜口述 -10'); }

  // 情绪（仅当镜口述前提）
  if (signals.onCameraFinal) {
    if (signals.emotionNatural) { positive += 5; positiveReasons.push('表达自然有人味 +5'); }
    if (signals.emotionNotNatural) { criticalPenalty += 10; criticalReasons.push('表达不自然 -10'); }
    if (signals.emotionHigh) { positive += 3; positiveReasons.push('情绪高昂 +3'); }
  }

  // 画质
  if (signals.qualityPass) { positive += 5; positiveReasons.push('画质达标 +5'); }
  if (signals.qualityFail) { criticalPenalty += 15; criticalReasons.push('画质不达标 -15'); }
  if (signals.demoMethods.includes('高清质地特写')) { positive += 3; positiveReasons.push('高清质地特写 +3'); }
  if (signals.strongRatio >= 0.15) { positive += 2; positiveReasons.push('强视觉片段占比≥15% +2'); }
  if (signals.strongRatio > 0 && signals.strongRatio < 0.05) { criticalPenalty += 5; criticalReasons.push('强视觉片段占比<5% -5'); }

  // 节奏
  const tight = signals.rhythm.some((l) => l.includes('节奏紧凑') || l.includes('强卡点'));
  if (tight) { positive += 5; positiveReasons.push('节奏紧凑/强卡点 +5'); }
  if (typeof signals.avgSegDur === 'number' && signals.avgSegDur >= 2 && signals.avgSegDur <= 6) { positive += 3; positiveReasons.push('平均片段2-6秒 +3'); }
  if (!tight && typeof signals.avgSegDur === 'number' && signals.avgSegDur > 6) { const p = (signals.avgSegDur > 10 ? 10 : 5); criticalPenalty += p; criticalReasons.push(`节奏偏慢(平均片段>${signals.avgSegDur>10? '10':'6'}s) -${p}`); }

  // CTA 完备
  if (signals.ctaHits === 3) { positive += 5; positiveReasons.push('CTA三项全命中 +5'); }

  // 亮点加分（封顶 +10）：高光≥3；高清特写+对比测试；CTA三项全命中
  let highlight = 0; const HIGHLIGHT_CAP = 10;
  if (signals.strongCount >= 3) { highlight += 5; highlightReasons.push('高光片段≥3 +5'); }
  if (signals.demoMethods.includes('高清质地特写') && signals.narrative.includes('对比测试')) { highlight += 5; highlightReasons.push('高清特写+对比测试 +5'); }
  if (signals.ctaHits === 3) { highlight += 5; highlightReasons.push('CTA三项全命中 +5'); }
  if (highlight > HIGHLIGHT_CAP) highlight = HIGHLIGHT_CAP;

  const qualityIndexRaw = base + positive + highlight - criticalPenalty - antiPatternPenalty;
  const qualityIndex = Math.max(0, Math.min(100, Math.round(qualityIndexRaw)));

  // 评级（默认二档）
  let rating = '普通';
  const passStructure = signals.structure.hook && signals.structure.cta;
  if (qualityIndex >= 80 && passStructure && criticalPenalty <= 10) rating = '优秀';

  const components = {
    base,
    positive_bonus: positive,
    highlight_bonus: highlight,
    critical_omission_penalty: criticalPenalty,
    anti_pattern_penalty: antiPatternPenalty,
    unfair_penalty: unfairPenalty, // 单列，不计入quality_index
    medical_penalty: medicalPenalty, // 单列，不计入quality_index
    missing_summary: miss,
    on_camera_speech_basis: result?.on_camera_speech_basis || (signals.onCameraFinal ? '真人出镜+同期声，且非画外音' : (signals.hasVoiceOver ? '检测到画外音' : '未满足当镜口述条件')),
    positive_reasons: positiveReasons,
    highlight_reasons: highlightReasons,
    critical_reasons: criticalReasons,
    anti_reasons: antiReasons,
  };

  return {
    quality_index: qualityIndex,
    quality_rating: rating,
    on_camera_speech_final: signals.onCameraFinal,
    quality_components: components,
  };
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method Not Allowed' });
  try {
    const { messages } = req.body || {};
    if (!Array.isArray(messages) || messages.length === 0) return res.json({ success: true, message: 'No messages' });
    const { body } = messages[0];
    const { feishuRecordId, videoId, env, accessToken } = body || {};
    const returnMarkdown = !!(body && (body.returnMarkdown || body.returnOverallEvaluation));
    const disableFeishu = !!(body && body.disableFeishu);
    // 参数校验：若禁用飞书，仅需 videoId；否则保持原校验
    if (!videoId) return res.status(200).json({ success: true, message: 'Skip: missing videoId' });
    if (!disableFeishu && (!feishuRecordId || !env)) return res.status(200).json({ success: true, message: 'Skip: missing body' });

    console.log(`[INFO] Received analysis task for feishuRecordId: ${feishuRecordId}, videoId: ${videoId}`);

    const genAI = new GoogleGenAI(process.env.GEMINI_API_KEY);

    let buffer;
    try {
      buffer = await fetchVideoBufferById(videoId);
    } catch (e) {
      // 下载失败：可选写失败原因到 是否发起分析（若未禁用飞书）
      console.error(`[ERROR] Record ${feishuRecordId}: Failed to download video. Error: ${e.message}`);
      if (accessToken && !disableFeishu) {
        await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, { '是否发起分析': `下载失败: ${e.message}` }, accessToken);
      }
      return res.json({ success: false, error: e.message });
    }

    // 输入前置校验：记录视频字节数与 mime；过小或过大提前失败，避免推给模型
    try {
      const size = buffer?.length || 0;
      console.log(`[INFO] Record ${feishuRecordId}: Video buffer size=${size} bytes, mime=video/mp4`);
      // 经验阈值：小于 8KB 视为无效；大于 150MB 拒绝
      const MIN_BYTES = 8 * 1024;
      const MAX_BYTES = 150 * 1024 * 1024;
      if (size < MIN_BYTES) {
        const msg = `视频无效：体积过小(${size} bytes)`;
        if (accessToken && !disableFeishu) {
          await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, { '是否发起分析': msg }, accessToken);
        }
        return res.json({ success: false, error: msg });
      }
      if (size > MAX_BYTES) {
        const msg = `视频超限：体积过大(${Math.round(size/1024/1024)} MB)`;
        if (accessToken && !disableFeishu) {
          await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, { '是否发起分析': msg }, accessToken);
        }
        return res.json({ success: false, error: msg });
      }
    } catch (_) {}

    // 上传视频到飞书附件字段：TK视频内容（不阻塞后续分析）
    if (accessToken && !disableFeishu) {
      try {
        const filename = `${videoId}.mp4`;
        console.log(`[INFO] Record ${feishuRecordId}: Uploading video to Feishu (medias.upload_all) as '${filename}'...`);
        const fileToken = await uploadVideoToFeishu(buffer, filename, accessToken, env.FEISHU_APP_TOKEN);
        if (fileToken) {
          await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, {
            'TK视频内容': [ { file_token: fileToken, name: filename } ]
          }, accessToken);
          console.log(`[INFO] Record ${feishuRecordId}: Video attachment uploaded and record updated.`);
        }
      } catch (e) {
        console.error(`[ERROR] Record ${feishuRecordId}: Failed to upload video attachment: ${e.message}`);
      }
    }

    // 分析（支持 StageA 空内容的模型级 fallback）
    let result;
    let primaryError = null;
    try {
      console.log(`[INFO] Record ${feishuRecordId}: Starting analysis with gemini-2.5-flash...`);
      result = await runFullAnalysisWithModel(genAI, buffer, feishuRecordId, 'gemini-2.5-flash');
      console.log(`[INFO] Record ${feishuRecordId}: Analysis finished on gemini-2.5-flash.`);
    } catch (e) {
      primaryError = e;
      const msg = String(e && e.message || '');
      const isStageAEmpty = msg.includes('StageA: Empty content (after retry)');
      if (isStageAEmpty) {
        try { console.info(`[INFO] Record ${feishuRecordId}: Fallback to gemini-2.5-pro due to StageA empty content.`); } catch (_) {}
        try {
          result = await runFullAnalysisWithModel(genAI, buffer, feishuRecordId, 'gemini-2.5-pro');
          console.log(`[INFO] Record ${feishuRecordId}: Analysis finished on gemini-2.5-pro.`);
        } catch (e2) {
          console.error(`[ERROR] Record ${feishuRecordId}: Fallback gemini-2.5-pro also failed: ${e2.message}`);
          if (accessToken && !disableFeishu) {
            await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, { '是否发起分析': `分析失败: ${e2.message}` }, accessToken);
          }
          return res.json({ success: false, error: e2.message });
        }
      } else {
        console.error(`[ERROR] Record ${feishuRecordId}: Analysis failed without fallback condition: ${e.message}`);
        if (accessToken && !disableFeishu) {
          await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, { '是否发起分析': `分析失败: ${e.message}` }, accessToken);
        }
        return res.json({ success: false, error: e.message });
      }
    }

    // --- Step 2: Compute quality and update the record ---
    // 计算优质度指数与评级（不影响综合得分展示）
    const quality = computeQualityIndexAndRating(result);
    result.quality_index = quality.quality_index;
    result.quality_rating = quality.quality_rating;
    if (typeof result.on_camera_speech !== 'boolean') result.on_camera_speech = quality.on_camera_speech_final;
    result.quality_components = quality.quality_components;

    const subtitlesAndAnalysisText = formatSubtitlesAndAnalysis(result);
    const structureAndAnalysisText = formatVideoStructureAndAnalysis(result);
    const overallEvaluationText = formatOverallScriptEvaluation(result);
    // 扣分：原始分(融合前) - 最终分（含扣分与上限）
    const deriveFusedForUpdate = () => {
      const fs = typeof result?.functional_score === 'number' ? result.functional_score : null;
      const ps = typeof result?.perception_score === 'number' ? result.perception_score : null;
      if (fs !== null && ps !== null) return Math.round(fs * 0.5 + ps * 0.5);
      if (fs !== null) return fs;
      if (ps !== null) return ps;
      return null;
    };
    const originalScore = (typeof result?.fused_pre_penalty === 'number') ? result.fused_pre_penalty : deriveFusedForUpdate();
    const finalScore = (typeof result?.final_score === 'number') ? result.final_score : (typeof result?.overall_score === 'number' ? result.overall_score : null);
    const deduction = (typeof originalScore === 'number' && typeof finalScore === 'number') ? Math.max(0, originalScore - finalScore) : null;

    // 确保目标字段存在
    if (accessToken && !disableFeishu) {
      await ensureTextField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '字幕与分析');
      await ensureTextField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '视频结构与分析');
      await ensureTextField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '整体脚本评估');
      // 可选：确保状态字段存在
      await ensureTextField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '是否发起分析');
      // 确保“扣分”为数字列（若不存在则创建）
      await ensureNumberField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '扣分');
      // 质量相关字段
      await ensureNumberField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '优质度指数');
      await ensureNumberField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '缺失项扣分');
      await ensureNumberField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '反模式扣分');
      await ensureNumberField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '亮点加分');
      await ensureNumberField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '不公平对比扣分');
      await ensureNumberField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '医疗化或夸大承诺扣分');
      await ensureTextField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '质量评级');
      await ensureTextField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '真人口播判定');
    }

    const fieldsToUpdate = {
      '字幕与分析': subtitlesAndAnalysisText,
      '视频结构与分析': structureAndAnalysisText,
      '整体脚本评估': overallEvaluationText,
      '是否发起分析': '已分析',
    };
    if (typeof deduction === 'number') {
      fieldsToUpdate['扣分'] = deduction;
    }
    // 写入质量相关字段
    if (typeof result?.quality_index === 'number') fieldsToUpdate['优质度指数'] = result.quality_index;
    if (typeof result?.quality_components?.critical_omission_penalty === 'number') fieldsToUpdate['缺失项扣分'] = result.quality_components.critical_omission_penalty;
    if (typeof result?.quality_components?.anti_pattern_penalty === 'number') fieldsToUpdate['反模式扣分'] = result.quality_components.anti_pattern_penalty;
    if (typeof result?.quality_components?.highlight_bonus === 'number') fieldsToUpdate['亮点加分'] = result.quality_components.highlight_bonus;
    if (typeof result?.quality_components?.unfair_penalty === 'number') fieldsToUpdate['不公平对比扣分'] = result.quality_components.unfair_penalty;
    if (typeof result?.quality_components?.medical_penalty === 'number') fieldsToUpdate['医疗化或夸大承诺扣分'] = result.quality_components.medical_penalty;
    if (result?.quality_rating) fieldsToUpdate['质量评级'] = result.quality_rating;
    if (typeof result?.on_camera_speech === 'boolean') fieldsToUpdate['真人口播判定'] = result.on_camera_speech ? '是' : '否';

        if (accessToken && !disableFeishu) {
          await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, fieldsToUpdate, accessToken);
    }

    if (returnMarkdown) {
      return res.json({ success: true, overallEvaluationMarkdown: overallEvaluationText });
    }
    return res.json({ success: true });
  } catch (error) {
    console.error('[FATAL] Unhandled error in handler:', error);
    return res.status(500).json({ error: error.message });
  }
};

