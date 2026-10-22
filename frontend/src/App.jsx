import React, { useEffect, useMemo, useRef, useState } from 'react';
import { createApi, formatBytes, parseWorkflow, SAMPLE_WORKFLOW, statusLabels } from './api.mjs';

function Icon({ name, size = 20, ...props }) {
  const paths = {
    flow: <><rect x="3" y="3" width="6" height="6" rx="1.5" /><rect x="15" y="15" width="6" height="6" rx="1.5" /><path d="M9 6h6a3 3 0 0 1 3 3v6M6 9v9h9" /></>,
    grid: <><rect x="3" y="3" width="7" height="7" rx="1.5" /><rect x="14" y="3" width="7" height="7" rx="1.5" /><rect x="3" y="14" width="7" height="7" rx="1.5" /><rect x="14" y="14" width="7" height="7" rx="1.5" /></>,
    storage: <><ellipse cx="12" cy="5" rx="8" ry="3" /><path d="M4 5v14c0 4 16 4 16 0V5M4 12c0 4 16 4 16 0" /></>,
    key: <><circle cx="8" cy="8" r="5" /><path d="m12 12 9 9m-4-4 3-3m-6 0 3-3" /></>,
    play: <path d="m8 4 12 8-12 8Z" />,
    refresh: <><path d="M20 8a8 8 0 1 0 0 8M20 3v5h-5" /></>,
    check: <path d="m5 12 4 4L19 6" />,
    arrow: <path d="M4 12h16m-6-6 6 6-6 6" />,
    code: <><path d="m8 6-6 6 6 6m8-12 6 6-6 6m-3-14-2 16" /></>,
    upload: <><path d="M12 16V3m-5 5 5-5 5 5M4 15v5h16v-5" /></>,
    download: <><path d="M12 3v13m-5-5 5 5 5-5M4 16v5h16v-5" /></>,
    trash: <><path d="M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7m4-7v7" /></>,
    clock: <><circle cx="12" cy="12" r="9" /><path d="M12 7v5l3 2" /></>,
    terminal: <><rect x="3" y="4" width="18" height="16" rx="3" /><path d="m7 9 3 3-3 3m6 0h4" /></>,
    close: <path d="m6 6 12 12M6 18 18 6" />,
    shield: <><path d="m12 3 8 3v6c0 4-8 9-8 9s-8-5-8-9V6Z" /><path d="m8 11 3 3 5-5" /></>,
  };
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}>{paths[name] ?? paths.flow}</svg>;
}

function Status({ value }) {
  return <span className={`status status-${value ?? 'pending'}`}><span className="status-dot" />{statusLabels[value] ?? value ?? '待执行'}</span>;
}

function Empty({ icon = 'flow', title, children }) {
  return <div className="empty"><span className="empty-icon"><Icon name={icon} size={26} /></span><strong>{title}</strong><p>{children}</p></div>;
}

function readableTime(value) {
  if (!value) return '—';
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? String(value) : new Intl.DateTimeFormat('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(parsed);
}

function Output({ value }) {
  return <pre className="output">{typeof value === 'string' ? value : JSON.stringify(value, null, 2)}</pre>;
}

export default function App() {
  const [draftToken, setDraftToken] = useState('');
  const [token, setToken] = useState('');
  const [identity, setIdentity] = useState(null);
  const [connecting, setConnecting] = useState(false);
  const [connectionError, setConnectionError] = useState('');
  const [overviewError, setOverviewError] = useState('');
  const [detailError, setDetailError] = useState('');
  const [notice, setNotice] = useState(null);
  const [runs, setRuns] = useState([]);
  const [objects, setObjects] = useState([]);
  const [quota, setQuota] = useState(null);
  const [selected, setSelected] = useState('');
  const [run, setRun] = useState(null);
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [tick, setTick] = useState(0);
  const [busy, setBusy] = useState('');
  const [dsl, setDsl] = useState(JSON.stringify(SAMPLE_WORKFLOW, null, 2));
  const [dslError, setDslError] = useState('');
  const [file, setFile] = useState(null);
  const [activeSection, setActiveSection] = useState('overview');
  const fileInput = useRef(null);
  const session = useRef(0);
  const connection = useRef(null);
  const api = useMemo(() => createApi(token), [token]);
  const connected = Boolean(identity && token);
  const writable = connected && ['admin', 'operator'].includes(identity.role);
  const successful = runs.filter(item => item.status === 'succeeded').length;
  const active = runs.filter(item => ['queued', 'running'].includes(item.status)).length;
  const usedPercent = quota?.limit_bytes > 0 ? Math.min(100, quota.used_bytes / quota.limit_bytes * 100) : 0;

  async function connect(event) {
    event.preventDefault();
    const candidate = draftToken.trim();
    if (!candidate) { setConnectionError('请输入后端启动时配置的服务令牌。'); return; }
    const current = ++session.current;
    connection.current?.abort();
    const controller = new AbortController();
    connection.current = controller;
    setConnecting(true);
    setConnectionError('');
    try {
      const candidateApi = createApi(candidate);
      const [nextIdentity] = await Promise.all([
        candidateApi.me(controller.signal),
        candidateApi.request('/healthz', { signal: controller.signal }),
      ]);
      if (session.current !== current) return;
      setIdentity(nextIdentity);
      setToken(candidate);
      setDraftToken('');
      setNotice({ type: 'success', text: `已连接租户 ${nextIdentity.tenant_id}，数据来自 Python 后端。` });
    } catch (error) {
      if (error.name !== 'AbortError' && session.current === current) setConnectionError(error.message);
    } finally { if (session.current === current) setConnecting(false); }
  }

  function disconnect() {
    ++session.current;
    connection.current?.abort();
    setToken(''); setDraftToken(''); setIdentity(null); setRuns([]); setObjects([]); setQuota(null);
    setSelected(''); setRun(null); setEvents([]); setNotice(null); setBusy('');
    setConnectionError(''); setOverviewError(''); setDetailError(''); setFile(null);
    if (fileInput.current) fileInput.current.value = '';
  }

  useEffect(() => {
    if (!connected) return;
    const controller = new AbortController();
    let fetching = false;
    const refresh = async () => {
      if (fetching) return;
      fetching = true;
      setLoading(true);
      try {
        const [nextRuns, nextObjects, nextQuota] = await Promise.all([api.runs(controller.signal), api.objects(controller.signal), api.quota(controller.signal)]);
        if (controller.signal.aborted) return;
        setRuns(nextRuns.items); setObjects(nextObjects.items); setQuota(nextQuota);
        setSelected(current => nextRuns.items.some(item => item.id === current) ? current : nextRuns.items[0]?.id ?? '');
        setOverviewError('');
      } catch (error) {
        if (!controller.signal.aborted) setOverviewError(error.message);
      } finally { fetching = false; if (!controller.signal.aborted) setLoading(false); }
    };
    refresh();
    const timer = setInterval(refresh, 5000);
    return () => { controller.abort(); clearInterval(timer); };
  }, [api, connected, tick]);

  useEffect(() => {
    setRun(null); setEvents([]); setDetailError('');
    if (!connected || !selected) { setDetailLoading(false); return; }
    const controller = new AbortController();
    let fetching = false;
    const refresh = async () => {
      if (fetching) return;
      fetching = true;
      setDetailLoading(true);
      try {
        const [nextRun, nextEvents] = await Promise.all([api.run(selected, controller.signal), api.events(selected, controller.signal)]);
        if (controller.signal.aborted) return;
        setRun(nextRun); setEvents(nextEvents); setDetailError('');
      } catch (error) { if (!controller.signal.aborted) setDetailError(error.message); }
      finally { fetching = false; if (!controller.signal.aborted) setDetailLoading(false); }
    };
    refresh();
    const timer = setInterval(refresh, 1800);
    return () => { controller.abort(); clearInterval(timer); };
  }, [api, connected, selected, tick]);

  useEffect(() => () => { connection.current?.abort(); }, []);

  async function action(label, operation, onSuccess) {
    const current = session.current;
    setBusy(label); setNotice(null);
    try {
      const result = await operation();
      if (session.current !== current) return;
      onSuccess?.(result);
      setTick(value => value + 1);
    } catch (error) {
      if (session.current === current) setNotice({ type: 'error', text: error.message });
    } finally { if (session.current === current) setBusy(''); }
  }

  function submitWorkflow() {
    let body;
    try { body = parseWorkflow(dsl); setDslError(''); }
    catch (error) { setDslError(error.message); return; }
    action('run', () => api.createRun(body), result => {
      setSelected(result.id);
      setNotice({ type: 'success', text: '工作流已提交，节点状态与执行事件将自动刷新。' });
      document.getElementById('runs')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  }

  function download(object) {
    action(`download:${object.name}`, () => api.download(object.name), blob => {
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url; anchor.download = object.name; anchor.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      setNotice({ type: 'success', text: `已下载 ${object.name}` });
    });
  }

  function navigate(section) {
    setActiveSection(section);
    document.getElementById(section)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  return <div className="app-shell">
    <aside className="sidebar" aria-label="主导航">
      <a className="brand" href="#overview" onClick={() => setActiveSection('overview')}><span className="brand-mark"><Icon name="flow" size={24} /></span><span>FlowTrail<small>AI PLATFORM</small></span></a>
      <div className="workspace"><span className="workspace-avatar">F</span><div><strong>研发工作空间</strong><small>Python Edition</small></div><span className="workspace-dot" /></div>
      <div className="nav-label">工作空间</div>
      <nav>{[['overview', 'grid', '平台概览'], ['runs', 'flow', '工作流运行'], ['composer', 'code', '工作流编排'], ['storage', 'storage', '对象存储']].map(([id, icon, label]) => <button key={id} className={`nav-item ${activeSection === id ? 'active' : ''}`} onClick={() => navigate(id)} aria-current={activeSection === id ? 'page' : undefined}><Icon name={icon} />{label}{id === 'runs' && connected && <span className="nav-count">{runs.length}</span>}</button>)}</nav>
      <div className="sidebar-bottom"><div className="sidebar-card"><Icon name="shield" /><strong>多租户隔离</strong><p>身份、工作流与对象存储<br />在同一租户边界内管理。</p></div><div className="edition"><span className="tiny-dot" />LOCAL DEVELOPMENT<span>v0.1</span></div></div>
    </aside>

    <main className="main" id="overview">
      <header className="topbar"><div className="breadcrumb">工作空间 <span>/</span> <strong>平台概览</strong></div><span className={`connection-state ${connected ? 'online' : ''}`}><span className="status-dot" />{connected ? '后端已连接' : '等待连接'}</span></header>
      <div className="content">
        <div className="page-heading"><div><div className="eyebrow">WORKSPACE OVERVIEW</div><h1>让每一次 AI 任务，有迹可循。</h1><p>编排推理任务、追踪运行事件，管理租户内的存储资源。</p></div><button className="button secondary refresh-button" disabled={!connected || loading} onClick={() => setTick(value => value + 1)}><Icon name="refresh" size={17} className={loading ? 'spin' : ''} />{loading ? '同步中' : '刷新数据'}</button></div>

        <section className={`connection-panel ${connected ? 'is-connected' : ''}`} aria-labelledby="connection-title">
          <span className="connection-icon"><Icon name={connected ? 'shield' : 'key'} size={22} /></span>
          <div className="connection-copy"><h2 id="connection-title">{connected ? '租户身份已验证' : '连接你的 AI 平台'}</h2><p>{connected ? '当前会话已通过 Bearer Token 认证。' : '输入 Python 后端配置的服务令牌，读取当前租户的真实运行数据。'}</p></div>
          {connected ? <div className="identity"><span className="identity-field"><small>TENANT</small><strong>{identity.tenant_id}</strong></span><span className="role">{identity.role}</span><button className="button quiet" onClick={disconnect}>断开连接</button></div>
            : <form className="connection-form" onSubmit={connect}><label className="sr-only" htmlFor="service-token">服务令牌</label><input id="service-token" type="password" autoComplete="off" value={draftToken} onChange={event => setDraftToken(event.target.value)} placeholder="粘贴服务令牌" disabled={connecting} aria-describedby="token-help" /><button className="button primary" disabled={connecting}>{connecting ? '验证中…' : '连接平台'}<Icon name="arrow" size={16} /></button></form>}
          {!connected && <p id="token-help" className="token-help">令牌仅保留在页面内存，刷新后需重新连接。</p>}
          {connectionError && <p className="inline-error connection-error" role="alert">{connectionError}</p>}
        </section>

        {(notice || overviewError) && <div className={`notice ${(notice?.type === 'error' || overviewError) ? 'notice-error' : 'notice-success'}`} role={notice?.type === 'error' || overviewError ? 'alert' : 'status'}><Icon name={notice?.type === 'error' || overviewError ? 'terminal' : 'check'} size={18} /><span>{overviewError || notice?.text}</span>{!overviewError && <button className="icon-button" aria-label="关闭提示" onClick={() => setNotice(null)}><Icon name="close" size={16} /></button>}</div>}

        <section className="stats" aria-label="平台运行统计">
          {[{ label: '工作流运行', value: connected ? runs.length : '—', meta: '当前租户最近 100 次以内', icon: 'flow', color: 'teal' }, { label: '执行中', value: connected ? active : '—', meta: '当前列表中排队与运行的任务', icon: 'clock', color: 'blue' }, { label: '成功运行', value: connected ? successful : '—', meta: connected && runs.length ? `${Math.round(successful / runs.length * 100)}% 当前列表运行成功率` : '等待执行数据', icon: 'check', color: 'green' }, { label: '对象存储', value: quota ? formatBytes(quota.used_bytes) : '—', meta: quota ? `配额 ${formatBytes(quota.limit_bytes)}` : '租户独立存储配额', icon: 'storage', color: 'orange' }].map(stat => <article className="stat" key={stat.label}><div className="stat-top"><span>{stat.label}</span><span className={`stat-icon ${stat.color}`}><Icon name={stat.icon} size={19} /></span></div><strong>{stat.value}</strong><p>{stat.meta}</p></article>)}
        </section>

        <div className="execution-layout" id="runs">
          <section className="panel run-list-panel" aria-labelledby="runs-title"><div className="panel-heading"><div><h2 id="runs-title">工作流运行 <span className="count">{runs.length}</span></h2><p>查看任务状态与执行记录</p></div><span className="live-label"><span className={connected ? 'tiny-dot' : 'tiny-dot muted'} />{connected ? '自动同步' : '未连接'}</span></div>
            {runs.length ? <div className="run-list">{runs.map(item => <button className={`run-item ${selected === item.id ? 'selected' : ''}`} key={item.id} onClick={() => setSelected(item.id)} aria-pressed={selected === item.id}><div className="run-item-top"><strong>{item.name}</strong><Status value={item.status} /></div><div className="run-item-bottom"><code>{item.id.slice(0, 12)}</code><time>{readableTime(item.created_at)}</time></div></button>)}</div> : <Empty title={loading ? '正在加载运行记录' : connected ? '从第一条工作流开始' : '等待平台连接'}>{connected ? '在下方编排区提交示例，运行将显示在这里。' : '连接后查看租户内的工作流与运行状态。'}</Empty>}
          </section>

          <section className="panel detail-panel" aria-labelledby="detail-title"><div className="panel-heading"><div><h2 id="detail-title">执行详情</h2><p>{run ? run.name : '节点状态与可恢复执行'}</p></div>{run && <Status value={run.status} />}</div>
            {detailError && <p className="inline-error detail-error" role="alert">{detailError}</p>}
            {run ? <div className="run-detail"><div className="run-meta"><span>RUN ID <code title={run.id}>{run.id}</code></span><button className="button small secondary" disabled={!writable || busy !== '' || !['failed', 'interrupted'].includes(run.status)} onClick={() => action('resume', () => api.resume(run.id), () => setNotice({ type: 'success', text: '恢复请求已提交，已成功节点将从检查点复用。' }))}><Icon name="refresh" size={15} />{busy === 'resume' ? '恢复中…' : '恢复运行'}</button></div>
              <div className="node-track">{run.nodes?.map((node, index) => <div key={node.id} className={`node-card node-${node.status}`}><div className="node-index">{node.status === 'succeeded' ? <Icon name="check" size={17} /> : String(index + 1).padStart(2, '0')}</div><div className="node-title"><strong>{node.id}</strong><small>{node.type}</small></div><Status value={node.status} /></div>)}</div>
              {run.error && <p className="inline-error" role="alert">{run.error}</p>}
              <div className="node-outputs">{run.nodes?.map(node => <details key={node.id}><summary><span><Icon name="code" size={15} />{node.id} <small>第 {node.attempt ?? 0} 次执行</small></span><span>{node.error ? '错误详情' : '输出详情'}</span></summary>{node.error && <p className="inline-error">{node.error}</p>}{node.output != null ? <Output value={node.output} /> : <p className="output-empty">该节点尚无输出。</p>}</details>)}</div>
              <div className="detail-footer"><Icon name="clock" size={14} />更新于 {readableTime(run.updated_at)}<span>{detailLoading ? '同步中' : '每 1.8 秒刷新'}</span></div>
            </div> : <Empty icon="grid" title={detailLoading ? '正在读取执行详情' : '选择一次运行'}>查看各节点状态、输出，以及失败后的恢复入口。</Empty>}
          </section>
        </div>

        <div className="authoring-layout">
          <section className="panel composer-panel" id="composer" aria-labelledby="composer-title"><div className="panel-heading"><div><h2 id="composer-title">工作流编排</h2><p>用 JSON 定义节点、依赖关系与输入参数</p></div><span className="subtle-badge">DAG DSL</span></div><div className="editor-toolbar"><span><span className="file-dot" />workflow.json</span><button className="text-button" onClick={() => { setDsl(JSON.stringify(SAMPLE_WORKFLOW, null, 2)); setDslError(''); }}>填入示例</button></div><label htmlFor="workflow-editor" className="sr-only">工作流 JSON 请求</label><textarea id="workflow-editor" className="code-editor" spellCheck="false" value={dsl} onChange={event => setDsl(event.target.value)} aria-describedby="workflow-hint" aria-invalid={Boolean(dslError)} />{dslError && <p className="inline-error editor-error" role="alert">{dslError}</p>}<div className="composer-footer"><p id="workflow-hint">示例使用 <code>mock</code> 推理，无需模型密钥。<br />{connected && !writable ? '当前为只读角色，可查看工作流与运行结果。' : '提交后由 Python 引擎按依赖调度执行。'}</p><button className="button primary" disabled={!writable || busy !== ''} onClick={submitWorkflow}><Icon name="play" size={16} />{busy === 'run' ? '提交中…' : '运行工作流'}</button></div></section>

          <section className="panel event-panel" aria-labelledby="events-title"><div className="panel-heading"><div><h2 id="events-title">执行事件</h2><p>持久化事件回放 · 自动刷新</p></div><span className="count">{events.length}</span></div>{events.length ? <ol className="event-list">{events.map((event, index) => <li key={`${event.seq}-${index}`} className={event.type?.includes('FAILED') ? 'event-failed' : ''}><span className="event-bullet" /><div className="event-main"><div className="event-heading"><code>{event.type}</code><small>#{event.seq}</small></div><span className="event-time">{readableTime(event.created_at)}{event.node_id ? ` · ${event.node_id}` : ''}</span>{event.data != null && <details><summary>查看事件数据</summary><Output value={event.data} /></details>}</div></li>)}</ol> : <Empty icon="terminal" title="等待执行事件">任务提交后，可追踪节点开始、成功、失败与恢复过程。</Empty>}<div className="event-note"><Icon name="shield" size={14} />通过认证请求读取 SSE 事件回放</div></section>
        </div>

        <section className="panel storage-panel" id="storage" aria-labelledby="storage-title"><div className="panel-heading"><div><h2 id="storage-title">对象存储 <span className="count">{objects.length}</span></h2><p>按租户管理文件、检查配额与访问权限</p></div><span className="subtle-badge"><Icon name="shield" size={14} />租户隔离</span></div><div className="storage-toolbar"><div className="quota"><div><span>存储配额</span><strong>{quota ? `${formatBytes(quota.used_bytes)} / ${formatBytes(quota.limit_bytes)}` : '连接后读取'}</strong></div><div className="quota-track" role="progressbar" aria-label="已使用存储配额" aria-valuenow={Math.round(usedPercent)} aria-valuemin="0" aria-valuemax="100"><span style={{ width: `${usedPercent}%` }} /></div></div><div className="upload-controls"><label className={`button secondary file-picker ${!writable ? 'disabled' : ''}`}><Icon name="upload" size={16} /><span>{file ? file.name : '选择文件'}</span><input ref={fileInput} type="file" disabled={!writable || busy !== ''} onChange={event => setFile(event.target.files?.[0] ?? null)} aria-label="选择待上传文件" /></label><button className="button primary" disabled={!writable || !file || busy !== ''} onClick={() => action('upload', () => api.upload(file), () => { setNotice({ type: 'success', text: `已上传 ${file.name}` }); setFile(null); if (fileInput.current) fileInput.current.value = ''; })}>{busy === 'upload' ? '上传中…' : '上传对象'}</button></div></div>
          {objects.length ? <div className="table-scroll"><table><thead><tr><th scope="col">对象名称</th><th scope="col">大小</th><th scope="col">SHA-256</th><th scope="col">更新时间</th><th scope="col" className="actions-column">操作</th></tr></thead><tbody>{objects.map(object => <tr key={object.name}><td><span className="object-name"><Icon name="storage" size={17} />{object.name}</span></td><td>{formatBytes(object.size_bytes)}</td><td><code title={object.sha256}>{object.sha256?.slice(0, 12) ?? '—'}</code></td><td>{readableTime(object.updated_at)}</td><td><div className="row-actions"><button className="icon-button" aria-label={`下载 ${object.name}`} disabled={busy !== ''} onClick={() => download(object)}><Icon name="download" size={17} /></button><button className="icon-button danger" aria-label={`删除 ${object.name}`} disabled={!writable || busy !== ''} onClick={() => action(`delete:${object.name}`, () => api.remove(object.name), () => setNotice({ type: 'success', text: `已删除 ${object.name}` }))}><Icon name="trash" size={17} /></button></div></td></tr>)}</tbody></table></div> : <Empty icon="storage" title={connected ? '当前租户还没有对象' : '连接后管理存储资源'}>{connected ? '选择一个文件上传，服务端会校验权限与剩余配额。' : '对象列表、文件下载和配额信息由后端按租户返回。'}</Empty>}
        </section>
        <footer className="page-footer"><span>FlowTrail <span className="footer-dot">·</span> Python AI 平台工程实践</span><span>FastAPI / React / SQLite</span></footer>
      </div>
    </main>
  </div>;
}


