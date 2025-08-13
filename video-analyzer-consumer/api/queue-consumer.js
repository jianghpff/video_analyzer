import { GoogleGenAI, Type } from "@google/genai";
import fetch from 'node-fetch';

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

const ANALYSIS_RESPONSE_SCHEMA = {
  type: Type.OBJECT,
  properties: {
    "script": {
      type: Type.ARRAY,
      items: {
        type: Type.OBJECT,
        properties: {
          "startTime": { type: Type.STRING },
          "endTime": { type: Type.STRING },
          "transcription": { type: Type.STRING },
          "screen_description": { type: Type.STRING },
          "summary": { type: Type.STRING }
        },
        required: ["startTime", "endTime", "transcription", "screen_description", "summary"]
      }
    },
    "score": {
      type: Type.OBJECT,
      properties: {
        "opening_appeal": { type: Type.OBJECT, properties: { "score": { type: Type.NUMBER }, "reasoning": { type: Type.STRING } }, required: ["score", "reasoning"] },
        "product_highlights": { type: Type.OBJECT, properties: { "score": { type: Type.NUMBER }, "reasoning": { type: Type.STRING } }, required: ["score", "reasoning"] },
        "use_case_scenarios": { type: Type.OBJECT, properties: { "score": { type: Type.NUMBER }, "reasoning": { type: Type.STRING } }, required: ["score", "reasoning"] },
        "call_to_action": { type: Type.OBJECT, properties: { "score": { type: Type.NUMBER }, "reasoning": { type: Type.STRING } }, required: ["score", "reasoning"] },
        "fluency_and_emotion": { type: Type.OBJECT, properties: { "score": { type: Type.NUMBER }, "reasoning": { type: Type.STRING } }, required: ["score", "reasoning"] }
      },
      required: ["opening_appeal", "product_highlights", "use_case_scenarios", "call_to_action", "fluency_and_emotion"]
    },
    "tags": {
      type: Type.ARRAY,
      items: { type: Type.STRING }
    }
  },
  required: ["script", "score", "tags"]
};

const VALID_TAGS_TEXT = `
一、视频特征类: 真人出镜+口播, 配音, 无配音, 搭配BGM, 节奏紧凑, 节奏缓慢, 视频情绪高, 视频情绪中等, 视频情绪低
二、内容套路类: 沉浸式护肤, 妆前妆后, 好物测评, 保姆级教程, 成分深扒, VLOG种草, 挑战跟风
三、情绪钩子类: 戳中痛点, 解压治愈, 惊天反差, 懒人必备, 空瓶记, 平替/大牌同款
四、口播节奏类: OMG式安利, 快语速带货, 聊天式分享, ASMR耳语
五、本土化与文化契合度: 泰式幽默/玩梗, 泰国节日/热点, 泰语口语化表达, 符合泰国审美, 明星/网红同款
六、TikTok平台特性: 使用热门BGM/音效, 利用热门滤镜/特效, 卡点/转场运镜, 引导评论区互动, 挂小黄车/引流链接
七、视频商业化成熟度: 强效用展示, 价格优势/促销, 信任状/背书, 制造稀缺/紧迫感, 开箱/沉浸式体验
`;

async function translateScript(genAI, script) {
  if (!Array.isArray(script) || script.length === 0) return [];
  const toTranslate = script.map((s, id) => ({ id, ...s }));
  const prompt = `你是一位专业的翻译家。请将以下JSON数组中每个对象的 "transcription", "screen_description", "summary" 字段都翻译成流畅、准确的中文。严格保持JSON结构和 "id" 不变。待翻译内容: ${JSON.stringify(toTranslate)}`;
  const result = await genAI.models.generateContent({
    model: "gemini-2.5-pro",
    contents: [{ role: 'user', parts: [{ text: prompt }] }],
    generationConfig: {
      responseMimeType: "application/json",
      responseSchema: {
        type: Type.ARRAY,
        items: {
          type: Type.OBJECT,
          properties: {
            id: { type: Type.NUMBER },
            startTime: { type: Type.STRING },
            endTime: { type: Type.STRING },
            transcription: { type: Type.STRING },
            screen_description: { type: Type.STRING },
            summary: { type: Type.STRING },
          },
          required: ["id", "startTime", "endTime", "transcription", "screen_description", "summary"],
        }
      }
    },
  });

  const rawText = result.response.text();
  if (!rawText) throw new Error("translateScript: Gemini returned empty content.");

  try {
    return JSON.parse(rawText);
  } catch (e) {
    throw new Error(`translateScript: Failed to parse JSON. Response started with: "${rawText.slice(0, 100)}..."`);
  }
}

async function validateAndCorrectTags(genAI, generatedTags) {
  if (!Array.isArray(generatedTags)) {
    return [];
  }

  const validTags = VALID_TAGS_TEXT.split('\n').flatMap(line => line.split(': ')[1]?.split(', ') || []).map(tag => tag.trim());
  const validTagsSet = new Set(validTags);
  
  const invalidTags = generatedTags.filter(tag => !validTagsSet.has(tag));
  const alreadyValidTags = generatedTags.filter(tag => validTagsSet.has(tag));

  if (invalidTags.length > 0) {
    console.warn(`Found invalid tags: ${invalidTags.join(', ')}. Attempting to correct...`);
    const correctionPrompt = `
      你是一位专业的标签校准专家。请参考以下“标签体系”，将“待纠正标签”列表中的每一个标签，映射到体系中最接近的一个正确标签上。

      - 你的输出必须是一个JSON数组，数组中的每个元素都是一个字符串，即纠正后的标签。
      - 数组的长度必须与输入的“待纠正标签”列表完全一致。
      - 严格按照原始顺序进行映射。
      - 禁止自创任何标签，所有返回的标签都必须严格存在于“标签体系”中。

      标签体系:
      ${VALID_TAGS_TEXT}

      待纠正标签:
      ${JSON.stringify(invalidTags)}
    `;

    const correctionResult = await genAI.models.generateContent({
      model: "gemini-2.5-pro",
      contents: [{ role: 'user', parts: [{ text: correctionPrompt }] }],
      generationConfig: {
        responseMimeType: "application/json",
        responseSchema: {
          type: Type.ARRAY,
          items: { type: Type.STRING }
        }
      },
    });

    try {
      const rawText = correctionResult.response.text();
      if (!rawText) return generatedTags; // 如果没有返回，则不进行修正
      
      const correctedArray = JSON.parse(rawText);
      if (Array.isArray(correctedArray)) {
        return [...alreadyValidTags, ...correctedArray];
      }
      return generatedTags;
    } catch (e) {
      console.error("Failed to parse corrected tags from Gemini, returning original.", e);
      return generatedTags;
    }
  }
  return generatedTags;
}

async function analyzeSingleVideo(genAI, buffer) {
  const prompt = `
    作为一名顶级的电商视频分析专家，你的任务是分析所提供的视频，并评估其销售转化潜力。

    请以JSON格式提供你的分析报告。JSON对象内的所有字符串值都必须是中文，但"transcription"字段除外，它应为原文。

    脚本生成指南：
    - 【强制要求】必须根据视频内容的逻辑和节奏，将其合理地切分成多个连续的时间片段。
    - 【强制要求】对于每一个片段，都必须同时提供“transcription”（原文）、“screen_description”（画面描述）和“summary”（中文总结）。
    - 【强制要求】“screen_description”字段绝对不能留空，必须有详细的画面描述。

    评分指南：
    - 对每个维度的评分范围为0到100。

    标签生成指南：
    - 【强制要求】你必须从下方的“标签体系”中，挑选出3-7个最符合视频特征的中文标签。
    - 【强制要求】你的选择必须严格来自于“标签体系”，禁止自创任何标签。
    - 【强制要求】输出的必须是只包含中文标签字符串的JSON数组。

    --- 标签体系 ---
${VALID_TAGS_TEXT}
    --------------------

    最后提醒：分析视频，并以中文提供JSON输出（"transcription"除外）。
  `;

  const videoPart = { inlineData: { data: buffer.toString("base64"), mimeType: "video/mp4" } };
  const contents = [{ role: 'user', parts: [{ text: prompt }, videoPart] }];

  const safetySettings = [
    { category: "HARM_CATEGORY_HARASSMENT", threshold: "BLOCK_NONE" },
    { category: "HARM_CATEGORY_HATE_SPEECH", threshold: "BLOCK_NONE" },
    { category: "HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold: "BLOCK_NONE" },
    { category: "HARM_CATEGORY_DANGEROUS_CONTENT", threshold: "BLOCK_NONE" },
  ];

  const result = await genAI.models.generateContent({
    model: "gemini-2.5-pro",
    contents: contents,
    generationConfig: {
      responseMimeType: "application/json",
      responseSchema: ANALYSIS_RESPONSE_SCHEMA,
    },
    safetySettings,
  });
  
  const rawText = result.response.text();
  if (!rawText) throw new Error("analyzeSingleVideo: Gemini returned empty content.");

  try {
    const analysis = JSON.parse(rawText);
    const scores = analysis.score;
    const finalScore = (scores.opening_appeal.score * 0.2) +
                       (scores.product_highlights.score * 0.3) +
                       (scores.use_case_scenarios.score * 0.2) +
                       (scores.call_to_action.score * 0.2) +
                       (scores.fluency_and_emotion.score * 0.1);

    const translatedScript = await translateScript(genAI, analysis.script);

    return {
      script: translatedScript,
      score: Math.round(finalScore),
      tags: analysis.tags,
    };
  } catch (e) {
    throw new Error(`analyzeSingleVideo: Failed to parse or process analysis. Raw response: "${rawText.slice(0, 100)}...". Error: ${e.message}`);
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

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method Not Allowed' });
  try {
    const { messages } = req.body || {};
    if (!Array.isArray(messages) || messages.length === 0) return res.json({ success: true, message: 'No messages' });
    const { body } = messages[0];
    const { feishuRecordId, videoId, env, accessToken } = body || {};
    if (!feishuRecordId || !videoId || !env) return res.status(200).json({ success: true, message: 'Skip: missing body' });

    const genAI = new GoogleGenAI(process.env.GEMINI_API_KEY);

    let buffer;
    try {
      buffer = await fetchVideoBufferById(videoId);
    } catch (e) {
      // 下载失败：写失败原因到 是否发起分析
      if (accessToken) {
        await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, { '是否发起分析': `下载失败: ${e.message}` }, accessToken);
      }
      return res.json({ success: false, error: e.message });
    }

    // 分析
    let result;
    try {
      result = await analyzeSingleVideo(genAI, buffer);
    } catch (e) {
      if (accessToken) {
        await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, { '是否发起分析': `分析失败: ${e.message}` }, accessToken);
      }
      return res.json({ success: false, error: e.message });
    }

    // 处理标签（规范化、截断到10字）
    const rawTags = Array.isArray(result.tags) ? result.tags : [];
    const correctedTags = await validateAndCorrectTags(genAI, rawTags);
    
    const cleanTags = Array.from(new Set(correctedTags.map((t) => String(t).trim()).filter(Boolean)))
      .map((t) => (t.length > 10 ? t.slice(0, 10) : t));

    // 确定用于写入标签的目标字段（若原“视频标签”不是多选，则自动创建“视频标签（多选）”）
    let tagTarget = { fieldId: null, fieldName: '视频标签' };
    if (accessToken) {
      try {
        tagTarget = await getOrCreateMultiSelectField(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, accessToken);
      } catch (e) {
        // 若创建失败，则仍尝试用原显示名写文本（不推荐，但保证流程不中断）
        tagTarget = { fieldId: null, fieldName: '视频标签' };
      }
    }

    // --- Step 1: Ensure options exist ---
    if (accessToken && tagTarget.fieldId && cleanTags.length > 0) {
      console.log(`[DEBUG] Record ${feishuRecordId}: About to ensure options for tags: ${JSON.stringify(cleanTags)}`);
      await ensureMultiSelectOptions(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, tagTarget.fieldId, cleanTags, accessToken);
      console.log(`[DEBUG] Record ${feishuRecordId}: Successfully ensured options.`);
      
      // 为解决飞书API最终一致性问题，在更新选项后增加3秒延迟
      console.log(`[DEBUG] Record ${feishuRecordId}: Waiting 3 seconds for API propagation...`);
      await new Promise(r => setTimeout(r, 3000));
    }

    // --- Step 2: Update the record ---
    let formattedScript = "";
    if (Array.isArray(result.script)) {
        formattedScript = result.script.map(segment => {
            const header = `### 🕒 ${segment.startTime || '??:??'} - ${segment.endTime || '??:??'}`;
            const transcriptionText = `**口播内容**: ${segment.transcription || '（无）'}`;
            const screenDescriptionText = `**画面描述**: ${segment.screen_description || '（无）'}`;
            const summaryText = `**片段总结**: ${segment.summary || '（无）'}`;
            return `${header}\n${transcriptionText}\n${screenDescriptionText}\n${summaryText}`;
        }).join('\n\n---\n\n');
    } else {
        // Fallback for old format or unexpected string
        formattedScript = String(result.script || '');
    }
    const fieldsToUpdate = {
      '视频脚本': formattedScript,
      '视频得分': String(Math.max(0, Math.min(100, Math.round(result.score))))
    };
    
    if (cleanTags.length > 0) {
      fieldsToUpdate[tagTarget.fieldName] = cleanTags;
      try {
        console.log(`[DEBUG] Record ${feishuRecordId}: About to update record with fields: ${JSON.stringify(fieldsToUpdate)}`);
        if (accessToken) {
          await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, fieldsToUpdate, accessToken);
        }
        console.log(`[DEBUG] Record ${feishuRecordId}: Successfully updated record with multi-select tags.`);
      } catch (e) {
        console.error(`[DEBUG] Record ${feishuRecordId}: FAILED to update with multi-select format. Error: ${e.message}`);
        console.log(`[DEBUG] Record ${feishuRecordId}: Falling back to writing tags as plain text.`);
        
        // 多选写入失败，回退到文本写入
        delete fieldsToUpdate[tagTarget.fieldName];
        fieldsToUpdate[tagTarget.fieldName] = cleanTags.join(' / ');
        
        try {
          console.log(`[DEBUG] Record ${feishuRecordId}: About to update record with fallback text format: ${JSON.stringify(fieldsToUpdate)}`);
          if (accessToken) {
            await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, fieldsToUpdate, accessToken);
          }
          console.log(`[DEBUG] Record ${feishuRecordId}: Successfully updated record with fallback text format.`);
        } catch (e2) {
          console.error(`[DEBUG] Record ${feishuRecordId}: FAILED to update with fallback text format. Error: ${e2.message}`);
        }
      }
    } else {
      // 只写入脚本和得分
      if (accessToken) {
        await updateRecord(env.FEISHU_APP_TOKEN, env.FEISHU_TABLE_ID, feishuRecordId, fieldsToUpdate, accessToken);
      }
    }

    return res.json({ success: true });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: error.message });
  }
};

