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

async function analyzeStageA(genAI, buffer, feishuRecordId) {
  const systemInstruction = `You are an expert short-form video script analyst for TikTok skincare. Return JSON strictly matching the provided schema for Stage A (basic analysis). All analysis in Simplified Chinese.`;
  const prompt = '请进行阶段A基础分析，并严格按schema返回JSON（简体中文）';
  const videoPart = { inlineData: { data: buffer.toString('base64'), mimeType: 'video/mp4' } };
  const contents = { parts: [videoPart, { text: prompt }] };
  const result = await genAI.models.generateContent({
    model: 'gemini-2.5-flash',
    contents,
    config: { responseMimeType: 'application/json', temperature: 0.0, responseSchema: SCHEMA_STAGE_A, systemInstruction },
  });
  let rawText = result?.text?.trim() || (result.response && result.response.text()) || result?.candidates?.[0]?.content?.parts?.[0]?.text || '';
  if (!rawText) throw new Error('StageA: Empty content');
  try { return JSON.parse(rawText); } catch (e) { throw new Error(`StageA parse failed. Head: ${rawText.slice(0,200)}...`); }
}

async function analyzeStageB(genAI, buffer, feishuRecordId) {
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

V3标签：仅输出命中标签，扁平到 v3_labels_flat 的“[维度]--[二级维度]-[TAG]”；并在 v3_label_bases 返回 {label,basis}。`;
  const prompt = '请进行阶段B分析（三支柱/红旗/V3标签），并严格按schema返回JSON（简体中文）。三支柱请按命中率→微调→一致性约束的流程得出分数，并返回 score_basis。';
  const videoPart = { inlineData: { data: buffer.toString('base64'), mimeType: 'video/mp4' } };
  const contents = { parts: [videoPart, { text: prompt }] };
  const result = await genAI.models.generateContent({
    model: 'gemini-2.5-flash',
    contents,
    config: { responseMimeType: 'application/json', temperature: 0.0, responseSchema: SCHEMA_STAGE_B, systemInstruction },
  });
  let rawText = result?.text?.trim() || (result.response && result.response.text()) || result?.candidates?.[0]?.content?.parts?.[0]?.text || '';
  if (!rawText) throw new Error('StageB: Empty content');
  try { return JSON.parse(rawText); } catch (e) { throw new Error(`StageB parse failed. Head: ${rawText.slice(0,200)}...`); }
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
      const hs = typeof pe?.hook?.score === 'number' ? pe.hook.score : null;
      const ps = typeof pe?.pitch?.score === 'number' ? pe.pitch.score : null;
      const cs = typeof pe?.close?.score === 'number' ? pe.close.score : null;
      if (hs !== null && ps !== null && cs !== null) {
        const functionalScore = Math.round(hs * 0.4 + ps * 0.4 + cs * 0.2);
        parsed.functional_score = functionalScore;
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
        parsed.final_score = Math.min(afterPenalty, cap);
        parsed.overall_score = parsed.final_score; // 向后兼容：覆盖 overall_score
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
  const score = result?.final_score ?? result?.overall_score;
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

  return lines.join('\n');
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

    // 分析
    let result;
    try {
      console.log(`[INFO] Record ${feishuRecordId}: Starting analysis...`);
      const a = await analyzeStageA(genAI, buffer, feishuRecordId);
      let b = {};
      try { b = await analyzeStageB(genAI, buffer, feishuRecordId); } catch (e) { console.warn(`[WARN] StageB failed: ${e.message}`); b = { degrade: true }; }
      // 融合：A 为基础（保证字幕/结构用于飞书两字段），B 为三支柱等
      result = { ...a, ...b };
      // 后处理与打分（沿用原逻辑）
      result = await (async () => {
        const fake = { response: { text: () => JSON.stringify(result) } };
        const merged = await (async () => {
          // 复用解析与打分逻辑：借用 analyzeVideoWithSchema 的后半段
          let raw = JSON.stringify(result);
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
          if (typeof fused==='number') { const after=Math.max(0,Math.min(100,fused+penalty)); parsed.final_score=Math.min(after,cap); parsed.overall_score=parsed.final_score; }
          return parsed;
        })();
        return merged;
      })();
      console.log(`[INFO] Record ${feishuRecordId}: Analysis finished.`);
    } catch (e) {
      console.error(`[ERROR] Record ${feishuRecordId}: Analysis failed. Error: ${e.message}`);
      if (accessToken && !disableFeishu) {
        await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, { '是否发起分析': `分析失败: ${e.message}` }, accessToken);
      }
      return res.json({ success: false, error: e.message });
    }

    // --- Step 2: Update the record ---
    const subtitlesAndAnalysisText = formatSubtitlesAndAnalysis(result);
    const structureAndAnalysisText = formatVideoStructureAndAnalysis(result);
    const overallEvaluationText = formatOverallScriptEvaluation(result);

    // 确保目标文本字段存在
    if (accessToken && !disableFeishu) {
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

