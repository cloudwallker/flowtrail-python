export const SAMPLE_WORKFLOW = {
  workflow: {
    name: '平台接入 · 文本推理示例',
    nodes: [
      { id: 'prepare', type: 'text', depends_on: [], config: { template: '请介绍 ${inputs.topic}，并给出一个平台接入场景。' } },
      { id: 'infer', type: 'inference', depends_on: ['prepare'], config: { prompt: '${prepare.output}', mode: 'mock' } },
      { id: 'publish', type: 'text', depends_on: ['infer'], config: { template: '推理结果：${infer.output}' } },
    ],
  },
  inputs: { topic: '多租户资源隔离' },
};

export const statusLabels = {
  queued: '排队中', running: '运行中', succeeded: '已成功', failed: '已失败',
  interrupted: '已中断', pending: '待执行', skipped: '已跳过',
};

export function parseSSE(source) {
  return source.replace(/\r\n?/g, '\n').split('\n\n').flatMap(block => {
    let event = 'message';
    let id = '';
    const data = [];
    for (const line of block.split('\n')) {
      if (!line || line.startsWith(':')) continue;
      const separator = line.indexOf(':');
      const key = separator < 0 ? line : line.slice(0, separator);
      const value = separator < 0 ? '' : line.slice(separator + 1).replace(/^ /, '');
      if (key === 'data') data.push(value);
      if (key === 'event') event = value;
      if (key === 'id' && !value.includes('\0')) id = value;
    }
    if (!data.length) return [];
    const raw = data.join('\n');
    let payload;
    try { payload = JSON.parse(raw); } catch { payload = { data: raw }; }
    if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) payload = { data: payload };
    return [{ ...payload, type: payload.type ?? event, seq: payload.seq ?? id }];
  });
}

export function errorMessage(payload, fallback = '请求失败', token = '') {
  const detail = payload?.detail ?? payload?.message ?? fallback;
  const message = Array.isArray(detail)
    ? detail.map(item => `${(item.loc ?? []).join('.')}: ${item.msg ?? '参数无效'}`).join('；')
    : typeof detail === 'string' ? detail : JSON.stringify(detail);
  return token ? message.split(token).join('[已隐藏令牌]') : message;
}

export function parseWorkflow(source) {
  let document;
  try { document = JSON.parse(source); } catch { throw new Error('JSON 格式不正确，请检查引号、逗号与括号。'); }
  if (!document || typeof document !== 'object' || Array.isArray(document)) throw new Error('请求需要是 JSON 对象。');
  if (typeof document.workflow?.name !== 'string' || !document.workflow.name.trim() || !Array.isArray(document.workflow.nodes) || !document.workflow.nodes.length) {
    throw new Error('请提供 workflow.name 和至少一个 workflow.nodes 节点。');
  }
  return document;
}

export function formatBytes(value) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes)) return '—';
  if (bytes < 1024) return `${bytes} B`;
  const unit = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), 3);
  return `${(bytes / 1024 ** unit).toFixed(unit === 1 ? 1 : 2)} ${['B', 'KB', 'MB', 'GB'][unit]}`;
}

export function createApi(token) {
  async function request(path, options = {}) {
    const { responseType = 'json', ...init } = options;
    const headers = new Headers(init.headers);
    if (token) headers.set('Authorization', `Bearer ${token}`);
    const timeout = AbortSignal.timeout(15000);
    const signal = init.signal ? AbortSignal.any([init.signal, timeout]) : timeout;
    let response;
    try { response = await fetch(path, { ...init, headers, signal, cache: 'no-store' }); }
    catch (error) {
      if (error.name === 'AbortError') throw error;
      throw new Error(error.name === 'TimeoutError' ? '请求超时，请检查后端服务后重试。' : '无法连接服务，请确认 Python 后端正在 8000 端口运行。');
    }
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      const fallback = response.status === 401 ? '身份验证失败，请检查服务令牌。'
        : response.status === 403 ? '当前角色没有执行此操作的权限。'
          : `请求失败（HTTP ${response.status}）`;
      throw new Error(errorMessage(payload, fallback, token));
    }
    if (response.status === 204) return null;
    if (responseType === 'blob') return response.blob();
    if (responseType === 'text') return response.text();
    return response.json();
  }
  return {
    request,
    me: signal => request('/api/me', { signal }),
    runs: signal => request('/api/runs', { signal }),
    run: (id, signal) => request(`/api/runs/${encodeURIComponent(id)}`, { signal }),
    createRun: body => request('/api/runs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
    resume: id => request(`/api/runs/${encodeURIComponent(id)}/resume`, { method: 'POST' }),
    events: async (id, signal) => parseSSE(await request(`/api/runs/${encodeURIComponent(id)}/events?follow=false`, { signal, responseType: 'text' })),
    quota: signal => request('/api/quota', { signal }),
    objects: signal => request('/api/objects', { signal }),
    upload: file => request(`/api/objects/${encodeURIComponent(file.name)}`, { method: 'PUT', body: file, headers: { 'Content-Type': 'application/octet-stream' } }),
    remove: name => request(`/api/objects/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    download: name => request(`/api/objects/${encodeURIComponent(name)}`, { responseType: 'blob' }),
  };
}
