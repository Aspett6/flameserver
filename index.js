import 'dotenv/config';
import express from 'express';
import cors from 'cors';

const app = express();

const PORT = process.env.PORT || 3001;
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || '';
const DEEPSEEK_BASE_URL = (process.env.DEEPSEEK_BASE_URL || 'https://api.deepseek.com').replace(/\/$/, '');
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || 'deepseek-v4-flash';
const CLIENT_ORIGIN = process.env.CLIENT_ORIGIN || '*';
const DASHSCOPE_API_KEY = process.env.DASHSCOPE_API_KEY || '';
const DASHSCOPE_BASE_URL = (process.env.DASHSCOPE_BASE_URL || 'https://dashscope.aliyuncs.com/compatible-mode/v1').replace(/\/$/, '');
const VISION_MODEL = process.env.VISION_MODEL || 'qwen-vl-plus';

// CORS：'*' 表示放行所有来源；否则用逗号分隔的域名列表
const corsOptions = {
  origin: CLIENT_ORIGIN === '*' ? true : CLIENT_ORIGIN.split(',').map((s) => s.trim()),
};

app.use(cors(corsOptions));
app.use(express.json({ limit: '10mb' }));

app.get('/api/health', (_req, res) => {
  res.json({ ok: true, model: DEEPSEEK_MODEL, hasKey: Boolean(DEEPSEEK_API_KEY), hasVisionKey: Boolean(DASHSCOPE_API_KEY) });
});

// ===== 全局统计：后端单点真源，所有访客看到同一数值 =====
const STATS_BASE = {
  userCount: 15000,
  gameCount: 28000,
  shareCount: 8500,
  knowledgeCount: 45000,
};
const globalStats = { ...STATS_BASE };

// 服务端按节奏缓慢自增，模拟真实增长（所有访客读到一致的当前值）
setInterval(() => {
  globalStats.userCount += 1;
  globalStats.gameCount += 1 + Math.floor(Math.random() * 3);
  globalStats.shareCount += Math.random() < 0.35 ? 1 : 0;
  globalStats.knowledgeCount += 5 + Math.floor(Math.random() * 11);
}, 8000);

app.get('/api/stats', (_req, res) => {
  res.json({ ok: true, stats: globalStats });
});

const SYSTEM_PROMPT = `你是「FireSeer」，一名消防应急科普领域的智能诊断专家。你会根据用户在火灾逃生互动模拟中的作答数据，生成一份专业、易懂、有针对性的中文深度分析。

请严格遵守以下要求：
1. 使用纯文本输出，不要使用任何 Markdown 标记（不要出现 #、*、-、反引号等符号）。
2. 用小标题分段，格式如【行为画像】【关键风险】【隐患溯源】【整改建议】【补学路径】。
3. 语气亲切、鼓励，适合中小学生及家长阅读，但结论要专业、可落地。
4. 针对用户的实际作答（正确的和错误的选择）给出具体点评，不要泛泛而谈。
5. 全文控制在 400 字以内。`;

function buildMessages(report) {
  const r = report || {};
  const metricsText = `险情识别 ${r.metrics?.awareness ?? '-'}、冷静处置 ${r.metrics?.calm ?? '-'}、即时行动 ${r.metrics?.action ?? '-'}、提醒协同 ${r.metrics?.teamwork ?? '-'}、逃生判断 ${r.metrics?.escape ?? '-'}`;
  const gapsText = (r.gaps || []).map((g) => `${g.title}：${g.desc}`).join('；') || '无明显盲区';
  const traceText = (r.traceability || []).map((t) => `${t.title}（权重 ${Math.round(t.weight)}）`).join('、');
  const fireProb = typeof r.fireProbability === 'number' ? r.fireProbability.toFixed(2) : (r.fireProbability ?? '-');

  const user = `请为以下用户生成深度诊断分析：

- 玩家昵称：${r.name ?? '玩家'}
- 挑战场景：${r.sceneTitle ?? '-'}
- 互动总分：${r.finalScore ?? '-'}
- 行为画像：${r.persona ?? '-'}
- 动态风险等级：${r.riskLevel ?? '-'}
- 火灾发生概率：${fireProb}%
- 事故严重程度：${r.severity ?? '-'}
- 能力雷达：${metricsText}
- 重点排查链路：${traceText}
- 关键隐患：${gapsText}

请据此生成分析。`;

  return [
    { role: 'system', content: SYSTEM_PROMPT },
    { role: 'user', content: user },
  ];
}

app.post('/api/diagnose', async (req, res) => {
  if (!DEEPSEEK_API_KEY) {
    return res.status(500).json({ error: '服务器未配置 DEEPSEEK_API_KEY，请检查后端环境变量。' });
  }
  const { report } = req.body || {};
  if (!report) {
    return res.status(400).json({ error: '缺少 report 数据。' });
  }

  // 先向上游发起流式请求，确认成功后再向客户端开启 SSE
  let upstream;
  try {
    upstream = await fetch(`${DEEPSEEK_BASE_URL}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${DEEPSEEK_API_KEY}`,
      },
      body: JSON.stringify({
        model: DEEPSEEK_MODEL,
        messages: buildMessages(report),
        stream: true,
        temperature: 0.7,
      }),
    });
  } catch (err) {
    return res.status(502).json({ error: `无法连接 DeepSeek 服务：${err.message}` });
  }

  if (!upstream.ok || !upstream.body) {
    const text = await upstream.text().catch(() => '');
    return res.status(upstream.status).json({ error: `DeepSeek 请求失败（${upstream.status}）：${text.slice(0, 300)}` });
  }

  // 开启 SSE 响应
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let buffer = '';

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith('data:')) continue;
        const data = trimmed.slice(5).trim();
        if (data === '[DONE]') continue;

        let json;
        try {
          json = JSON.parse(data);
        } catch {
          continue;
        }

        const delta = json.choices?.[0]?.delta;
        if (!delta) continue;

        // 只转发正文与思考内容，不暴露 token 用量等冗余字段
        const out = {};
        if (typeof delta.reasoning_content === 'string' && delta.reasoning_content) {
          out.reasoning_content = delta.reasoning_content;
        }
        if (typeof delta.content === 'string' && delta.content) {
          out.content = delta.content;
        }
        if (Object.keys(out).length) {
          res.write(`data: ${JSON.stringify({ choices: [{ delta: out }] })}\n\n`);
        }
      }
    }

    res.write('data: [DONE]\n\n');
    res.end();
  } catch (err) {
    try {
      res.write(`data: ${JSON.stringify({ error: `流式传输中断：${err.message}` })}\n\n`);
      res.write('data: [DONE]\n\n');
    } catch {
      /* 客户端已断开 */
    }
    res.end();
  }
});

const VISION_PROMPT = `你是一名专业的消防安全检查专家。请仔细观察这张照片，识别其中可能存在的火灾安全隐患。

请严格以 JSON 格式输出（不要包含任何其他文字或 Markdown），结构如下：
{"hazards":[{"name":"隐患名称","desc":"具体说明","level":"高/中/低"}],"risk":"整体风险等级（高/中/低）","advice":"总体整改建议"}

要求：
1. hazards 数组列出所有识别到的隐患，每条包含 name、desc、level 三个字段。
2. 若照片中没有明显火灾隐患，hazards 返回空数组，并在 advice 中给出 1-2 条日常消防提醒。
3. 只输出 JSON 本身。`;

app.post('/api/vision', async (req, res) => {
  if (!DASHSCOPE_API_KEY) {
    return res.status(500).json({ error: '服务器未配置 DASHSCOPE_API_KEY（视觉识别需阿里云百炼 Key），请检查后端环境变量。' });
  }
  const { image } = req.body || {};
  if (!image || typeof image !== 'string') {
    return res.status(400).json({ error: '缺少 image（base64 图片数据）。' });
  }
  // 兼容纯 base64 或带 data:image 前缀的两种格式
  const dataUrl = image.startsWith('data:') ? image : `data:image/jpeg;base64,${image}`;

  try {
    const upstream = await fetch(`${DASHSCOPE_BASE_URL}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${DASHSCOPE_API_KEY}`,
      },
      body: JSON.stringify({
        model: VISION_MODEL,
        messages: [
          {
            role: 'user',
            content: [
              { type: 'image_url', image_url: { url: dataUrl } },
              { type: 'text', text: VISION_PROMPT },
            ],
          },
        ],
      }),
    });

    if (!upstream.ok) {
      const text = await upstream.text().catch(() => '');
      return res.status(upstream.status).json({ error: `视觉模型请求失败（${upstream.status}）：${text.slice(0, 300)}` });
    }

    const data = await upstream.json();
    const content = data.choices?.[0]?.message?.content;
    if (!content) {
      return res.status(502).json({ error: '视觉模型返回为空。' });
    }

    const text = typeof content === 'string' ? content : JSON.stringify(content);

    // 尝试解析结构化 JSON，失败则回退为原始文本由前端展示
    let parsed = null;
    try {
      parsed = JSON.parse(text);
    } catch {
      const m = text.match(/\{[\s\S]*\}/);
      if (m) {
        try { parsed = JSON.parse(m[0]); } catch { /* 忽略 */ }
      }
    }

    res.json({ ok: true, text, parsed });
  } catch (err) {
    return res.status(502).json({ error: `无法连接视觉模型服务：${err.message}` });
  }
});

app.listen(PORT, () => {
  console.log(`萌焰逃生记 后端已启动：http://localhost:${PORT}`);
  console.log(`模型：${DEEPSEEK_MODEL} ｜ API Key：${DEEPSEEK_API_KEY ? '已配置' : '未配置'}`);
});
