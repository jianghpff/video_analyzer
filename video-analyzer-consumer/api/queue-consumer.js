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
  const arr = await resp.json();
  const item = Array.isArray(arr) ? arr.find(x => String(x.videoId) === String(videoId)) : null;
  if (!item || !item.downloadUrl) throw new Error('No downloadUrl for this videoId');
  const mp4 = await fetch(item.downloadUrl, { headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 30000 });
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
  required: ['detected_language_code', 'subtitle_groups', 'script_analysis'],
};

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

**Input:**
The system will provide only a video file (no keyframe images). Base your visual analysis (\`isVisuallyStrong\`, etc.) solely on the video content.

**Output:**
You MUST return a single JSON object matching the provided schema. Do not add any extra text or explanations.`;

  const prompt = '请分析这个视频，并根据schema返回JSON。';

  const videoPart = { inlineData: { data: buffer.toString('base64'), mimeType: 'video/mp4' } };
  const contents = { parts: [videoPart, { text: prompt }] };

  const result = await genAI.models.generateContent({
    model: 'gemini-2.5-flash',
    contents,
    config: {
      responseMimeType: 'application/json',
      temperature: 0.2,
      responseSchema: NEW_ANALYSIS_RESPONSE_SCHEMA,
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
  const score = result?.overall_score;
  const rationale = result?.score_rationale;
  if (typeof score === 'number') {
    lines.push(`- **综合得分**: ${score}/100`);
  }
  if (rationale) {
    lines.push(`- **评分理由**: ${rationale}`);
  }

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

  return lines.join('\n');
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method Not Allowed' });
  try {
    const { messages } = req.body || {};
    if (!Array.isArray(messages) || messages.length === 0) return res.json({ success: true, message: 'No messages' });
    const { body } = messages[0];
    const { feishuRecordId, videoId, env, accessToken } = body || {};
    if (!feishuRecordId || !videoId || !env) return res.status(200).json({ success: true, message: 'Skip: missing body' });

    console.log(`[INFO] Received analysis task for feishuRecordId: ${feishuRecordId}, videoId: ${videoId}`);

    const genAI = new GoogleGenAI(process.env.GEMINI_API_KEY);

    let buffer;
    try {
      buffer = await fetchVideoBufferById(videoId);
    } catch (e) {
      // 下载失败：写失败原因到 是否发起分析
      console.error(`[ERROR] Record ${feishuRecordId}: Failed to download video. Error: ${e.message}`);
      if (accessToken) {
        await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, { '是否发起分析': `下载失败: ${e.message}` }, accessToken);
      }
      return res.json({ success: false, error: e.message });
    }

    // 上传视频到飞书附件字段：TK视频内容（不阻塞后续分析）
    if (accessToken) {
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

    // 分析
    let result;
    try {
      console.log(`[INFO] Record ${feishuRecordId}: Starting analysis...`);
      // result = await analyzeSingleVideo(genAI, buffer, feishuRecordId);
      result = await analyzeVideoWithSchema(genAI, buffer, feishuRecordId);
      console.log(`[INFO] Record ${feishuRecordId}: Analysis finished.`);
    } catch (e) {
      console.error(`[ERROR] Record ${feishuRecordId}: Analysis failed. Error: ${e.message}`);
      if (accessToken) {
        await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, { '是否发起分析': `分析失败: ${e.message}` }, accessToken);
      }
      return res.json({ success: false, error: e.message });
    }

    // --- Step 2: Update the record ---
    const subtitlesAndAnalysisText = formatSubtitlesAndAnalysis(result);
    const structureAndAnalysisText = formatVideoStructureAndAnalysis(result);
    const overallEvaluationText = formatOverallScriptEvaluation(result);

    // 确保目标文本字段存在
    if (accessToken) {
      await ensureTextField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '字幕与分析');
      await ensureTextField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '视频结构与分析');
      await ensureTextField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '整体脚本评估');
      // 可选：确保状态字段存在
      await ensureTextField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken, '是否发起分析');
    }

    const fieldsToUpdate = {
      '字幕与分析': subtitlesAndAnalysisText,
      '视频结构与分析': structureAndAnalysisText,
      '整体脚本评估': overallEvaluationText,
      '是否发起分析': '已分析',
    };

        if (accessToken) {
          await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, fieldsToUpdate, accessToken);
    }

    return res.json({ success: true });
  } catch (error) {
    console.error('[FATAL] Unhandled error in handler:', error);
    return res.status(500).json({ error: error.message });
  }
};

