import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import express from "express";
import helmet from "helmet";
import cors from "cors";
import morgan from "morgan";
import rateLimit from "express-rate-limit";
import crypto from "crypto";

const app = express();

// =========================================================
// ENV
// =========================================================

const PORT = Number(process.env.PORT || 3001);
const API_AUTH_TOKEN = process.env.API_AUTH_TOKEN || "";
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

const META_APP_ID = process.env.FACEBOOK_APP_ID || process.env.META_APP_ID || "";
const META_APP_SECRET = process.env.FACEBOOK_APP_SECRET || process.env.META_APP_SECRET || "";
const META_GRAPH_VERSION = process.env.META_GRAPH_VERSION || "v25.0";
const META_OAUTH_REDIRECT_URI =
  process.env.FACEBOOK_OAUTH_REDIRECT_URI ||
  process.env.META_OAUTH_REDIRECT_URI ||
  "https://vyrexads-backend.onrender.com/auth/facebook/callback";

const FRONTEND_RETURN_URL =
  process.env.FRONTEND_RETURN_URL || "http://localhost:8080/analytics";

// =========================================================
// N8N RELAY - ancien serveur actif conservé
// =========================================================

const N8N_BASE_URL = process.env.N8N_BASE_URL || "";

const N8N_CONTENT_EXAMPLE_WEBHOOK_PATH =
  process.env.N8N_CONTENT_EXAMPLE_WEBHOOK_PATH || "";
const N8N_TEMPLATE_REGEN_WEBHOOK_PATH =
  process.env.N8N_TEMPLATE_REGEN_WEBHOOK_PATH || "";
const N8N_CONTENT_REGENERATE_WEBHOOK_PATH =
  process.env.N8N_CONTENT_REGENERATE_WEBHOOK_PATH || "";
const N8N_CONTENT_IMAGE_WEBHOOK_PATH =
  process.env.N8N_CONTENT_IMAGE_WEBHOOK_PATH || "";
const N8N_CONTENT_CARROUSEL_WEBHOOK_PATH =
  process.env.N8N_CONTENT_CARROUSEL_WEBHOOK_PATH || "";
const N8N_CONTENT_PROMPT_WEBHOOK_PATH =
  process.env.N8N_CONTENT_PROMPT_WEBHOOK_PATH || "";
const N8N_COMPANY_PRODUCT_WEBHOOK_PATH =
  process.env.N8N_COMPANY_PRODUCT_WEBHOOK_PATH || "";
const N8N_DESCRIPTION_POST_WEBHOOK_PATH =
  process.env.N8N_DESCRIPTION_POST_WEBHOOK_PATH || "";
const N8N_COMPETITOR_ANALYSIS_WEBHOOK_PATH =
  process.env.N8N_COMPETITOR_ANALYSIS_WEBHOOK_PATH || "";

if (!N8N_BASE_URL) {
  console.warn("[env] Missing N8N_BASE_URL - /relay/* endpoints will fail until it is set");
}



const META_OAUTH_SCOPES =
  process.env.FACEBOOK_OAUTH_SCOPES ||
  process.env.META_OAUTH_SCOPES ||
  [
    "ads_read",
    "business_management",
    "pages_show_list",
    "pages_read_engagement",
    "pages_read_user_content",
    "read_insights",
    "leads_retrieval",
    "instagram_basic",
    "instagram_manage_insights",
  ].join(",");

const OAUTH_STATE_SECRET =
  process.env.OAUTH_STATE_SECRET || "dev_state_secret_change_me";

if (!API_AUTH_TOKEN) console.warn("[env] Missing API_AUTH_TOKEN");
if (!SUPABASE_URL) console.warn("[env] Missing SUPABASE_URL");
if (!SUPABASE_SERVICE_ROLE_KEY) console.warn("[env] Missing SUPABASE_SERVICE_ROLE_KEY");

// =========================================================
// EXPRESS MIDDLEWARE
// =========================================================

app.use(helmet());
app.use(express.json({ limit: "10mb" }));
app.use(
  cors({
    origin: (origin, cb) => {
      if (!origin) return cb(null, true);
      if (ALLOWED_ORIGINS.length === 0) return cb(null, true);
      if (ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
      return cb(new Error("CORS blocked: origin not allowed"), false);
    },
    credentials: true,
  })
);
app.use(morgan("tiny"));
app.use(
  rateLimit({
    windowMs: 60_000,
    max: 800,
    standardHeaders: true,
    legacyHeaders: false,
  })
);

function requireAuth(
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) {
  const token = req.header("x-api-token") || req.header("x-api-auth");
  if (!token || token !== API_AUTH_TOKEN) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

// =========================================================
// RELAY ROUTES - conservées depuis le serveur actif
// =========================================================

function unwrapPayload(body: any) {
  if (body && typeof body === "object" && "payload" in body) return body.payload;
  return body;
}

async function relayToN8N(
  webhookPath: string,
  payload: unknown,
  timeoutMs = 60_000
) {
  if (!N8N_BASE_URL) {
    return {
      status: 500,
      contentType: "application/json",
      body: JSON.stringify({ error: "Missing N8N_BASE_URL in backend env" }),
    };
  }

  if (!webhookPath) {
    return {
      status: 500,
      contentType: "application/json",
      body: JSON.stringify({ error: "Missing webhook path in backend env" }),
    };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const url = `${N8N_BASE_URL}${webhookPath}`;
    console.log("[relay] calling n8n:", url, `(timeout ${timeoutMs}ms)`);

    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    const contentType = r.headers.get("content-type") || "";
    const text = await r.text();
    return { status: r.status, contentType, body: text };
  } finally {
    clearTimeout(timer);
  }
}

function sendRelayedResponse(
  out: { status: number; contentType: string; body: string },
  res: express.Response
) {
  res.status(out.status);
  if (out.contentType.includes("application/json")) {
    try {
      return res.json(JSON.parse(out.body));
    } catch {
      return res.type("application/json").send(out.body);
    }
  }
  return res.type(out.contentType || "text/plain").send(out.body);
}

function relayRoute(webhookPath: string, timeoutMs = 600_000) {
  return async (req: express.Request, res: express.Response) => {
    try {
      const payload = unwrapPayload(req.body);
      const out = await relayToN8N(webhookPath, payload, timeoutMs);
      return sendRelayedResponse(out, res);
    } catch (e: any) {
      const msg = e?.name === "AbortError" ? "n8n timeout" : e?.message || String(e);
      return res.status(502).json({ error: "Bad Gateway", details: msg });
    }
  };
}

app.post("/relay/content-example", requireAuth, relayRoute(N8N_CONTENT_EXAMPLE_WEBHOOK_PATH, 600_000));
app.post("/relay/template-regenerate", requireAuth, relayRoute(N8N_TEMPLATE_REGEN_WEBHOOK_PATH, 600_000));
app.post("/relay/content-regenerate", requireAuth, relayRoute(N8N_CONTENT_REGENERATE_WEBHOOK_PATH, 600_000));
app.post("/relay/content-regenerate-image", requireAuth, relayRoute(N8N_CONTENT_IMAGE_WEBHOOK_PATH, 600_000));
app.post("/relay/content-regenerate-carrousel", requireAuth, relayRoute(N8N_CONTENT_CARROUSEL_WEBHOOK_PATH, 600_000));
app.post("/relay/content-prompt", requireAuth, relayRoute(N8N_CONTENT_PROMPT_WEBHOOK_PATH, 600_000));
app.post("/relay/description-post", requireAuth, relayRoute(N8N_DESCRIPTION_POST_WEBHOOK_PATH, 600_000));
app.post("/relay/company-product", requireAuth, relayRoute(N8N_COMPANY_PRODUCT_WEBHOOK_PATH, 600_000));
app.post("/relay/competitor-analysis", requireAuth, relayRoute(N8N_COMPETITOR_ANALYSIS_WEBHOOK_PATH, 600_000));


// =========================================================
// TYPES
// =========================================================

type Json = Record<string, any>;

type SyncRequestBody = {
  owner_id?: string;
  account_ids?: string[];
  page_ids?: string[];
  since?: string;
  until?: string;
  limit?: number;
  include_metrics?: boolean;
  include_breakdowns?: boolean;
  include_organic?: boolean;
  include_leads?: boolean;
  include_catalogs?: boolean;
  breakdowns?: string[];
};

type SyncCounters = Record<string, number>;

function inc(counters: SyncCounters, key: string, amount = 1) {
  counters[key] = (counters[key] || 0) + amount;
}

// =========================================================
// SMALL HELPERS
// =========================================================

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function todayISODate() {
  return new Date().toISOString().slice(0, 10);
}

function daysAgoISODate(days: number) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().slice(0, 10);
}

function parseDateOrFallback(value: any, fallback: string) {
  if (!value || typeof value !== "string") return fallback;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return fallback;
  return value;
}

function toNumber(value: any, fallback: number | null = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toBigIntNumber(value: any, fallback = 0) {
  const n = toNumber(value, fallback);
  return n === null ? fallback : Math.trunc(n);
}

function asArray(value: any) {
  return Array.isArray(value) ? value : [];
}

function asObject(value: any) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function minorToAmount(minor: any) {
  const n = toNumber(minor, null);
  if (n === null) return null;
  return n / 100;
}

function normalizeAccountId(accountIdOrAct: string) {
  return String(accountIdOrAct || "").replace(/^act_/, "");
}

function act(accountIdOrAct: string) {
  const clean = normalizeAccountId(accountIdOrAct);
  return `act_${clean}`;
}

function hashJson(value: any) {
  return crypto.createHash("sha256").update(JSON.stringify(value || {})).digest("hex");
}

function base64UrlEncode(buffer: Buffer) {
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function generatePkceVerifier() {
  return base64UrlEncode(crypto.randomBytes(32));
}

function generatePkceChallenge(verifier: string) {
  return base64UrlEncode(crypto.createHash("sha256").update(verifier).digest());
}

function safeMetaTime(value: any) {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "number" && Number.isFinite(value)) {
    const ms = value > 10_000_000_000 ? value : value * 1000;
    return new Date(ms).toISOString();
  }

  if (typeof value === "string" && /^\d+$/.test(value)) {
    const n = Number(value);
    const ms = n > 10_000_000_000 ? n : n * 1000;
    return new Date(ms).toISOString();
  }

  if (typeof value !== "string") return null;
  const t = Date.parse(value);
  return Number.isFinite(t) ? new Date(t).toISOString() : null;
}

function getActionValue(actions: any, actionType: string) {
  const found = asArray(actions).find((a) => a?.action_type === actionType);
  return toNumber(found?.value, null);
}

function destinationUrlFromCreative(creative: any) {
  const oss = asObject(creative?.object_story_spec);
  const linkData = asObject(oss?.link_data);
  const videoData = asObject(oss?.video_data);
  const cta = asObject(linkData?.call_to_action || videoData?.call_to_action || creative?.call_to_action);
  const ctaValue = asObject(cta?.value);

  return (
    creative?.link_url ||
    linkData?.link ||
    videoData?.link ||
    ctaValue?.link ||
    ctaValue?.link_url ||
    null
  );
}

function primaryTextFromCreative(creative: any) {
  const afs = asObject(creative?.asset_feed_spec);
  const oss = asObject(creative?.object_story_spec);
  return (
    creative?.body ||
    asArray(afs?.bodies)?.[0]?.text ||
    oss?.link_data?.message ||
    oss?.video_data?.message ||
    null
  );
}

function titleFromCreative(creative: any) {
  const afs = asObject(creative?.asset_feed_spec);
  const oss = asObject(creative?.object_story_spec);
  return (
    creative?.title ||
    asArray(afs?.titles)?.[0]?.text ||
    oss?.link_data?.name ||
    oss?.video_data?.title ||
    null
  );
}

function descriptionFromCreative(creative: any) {
  const afs = asObject(creative?.asset_feed_spec);
  const oss = asObject(creative?.object_story_spec);
  const linkData = asObject(oss?.link_data);
  const videoData = asObject(oss?.video_data);

  return (
    linkData?.description ||
    videoData?.link_description ||
    asArray(afs?.descriptions)?.[0]?.text ||
    null
  );
}

function pageIdFromCreative(creative: any) {
  const oss = asObject(creative?.object_story_spec);
  return oss?.page_id || creative?.page_id || null;
}

function instagramActorIdFromCreative(creative: any) {
  const oss = asObject(creative?.object_story_spec);
  return creative?.instagram_actor_id || oss?.instagram_actor_id || null;
}

function callToActionTypeFromCreative(creative: any) {
  const afs = asObject(creative?.asset_feed_spec);
  const oss = asObject(creative?.object_story_spec);
  const linkData = asObject(oss?.link_data);
  const videoData = asObject(oss?.video_data);
  return (
    creative?.call_to_action_type ||
    linkData?.call_to_action?.type ||
    videoData?.call_to_action?.type ||
    asArray(afs?.call_to_action_types)?.[0] ||
    null
  );
}

// =========================================================
// OAUTH STATE
// =========================================================

function signState(payload: Record<string, unknown>) {
  const json = JSON.stringify(payload);
  const sig = crypto
    .createHmac("sha256", OAUTH_STATE_SECRET)
    .update(json)
    .digest("hex");
  return Buffer.from(`${json}.${sig}`).toString("base64url");
}

function verifyState(state: string) {
  const raw = Buffer.from(state, "base64url").toString("utf8");
  const idx = raw.lastIndexOf(".");
  if (idx === -1) return null;

  const json = raw.slice(0, idx);
  const sig = raw.slice(idx + 1);
  const expected = crypto
    .createHmac("sha256", OAUTH_STATE_SECRET)
    .update(json)
    .digest("hex");

  if (sig !== expected) return null;

  try {
    return JSON.parse(json) as Record<string, unknown>;
  } catch {
    return null;
  }
}

// =========================================================
// SUPABASE REST HELPERS
// =========================================================

function assertSupabaseEnv() {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }
}

function supabaseHeaders(extra?: Record<string, string>) {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    "Content-Type": "application/json",
    ...extra,
  };
}

async function supabaseRpc<T = any>(fn: string, body: Json): Promise<T> {
  assertSupabaseEnv();

  const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${fn}`, {
    method: "POST",
    headers: supabaseHeaders(),
    body: JSON.stringify(body),
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Supabase RPC ${fn} failed: ${res.status} ${text}`);
  }

  if (!text) return null as T;
  try {
    return JSON.parse(text) as T;
  } catch {
    return text as T;
  }
}

async function supabaseSelect<T = any>(table: string, query: URLSearchParams) {
  assertSupabaseEnv();

  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${query.toString()}`, {
    method: "GET",
    headers: supabaseHeaders({ Accept: "application/json" }),
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Supabase select ${table} failed: ${res.status} ${text}`);
  }

  return (text ? JSON.parse(text) : []) as T[];
}

async function supabaseUpsert(table: string, rows: Json[], onConflict: string) {
  assertSupabaseEnv();
  if (!rows.length) return 0;

  let total = 0;
  const chunkSize = 300;

  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const url = `${SUPABASE_URL}/rest/v1/${table}?on_conflict=${encodeURIComponent(onConflict)}`;

    const res = await fetch(url, {
      method: "POST",
      headers: supabaseHeaders({
        Prefer: "resolution=merge-duplicates,return=minimal",
      }),
      body: JSON.stringify(chunk),
    });

    const text = await res.text();
    if (!res.ok) {
      throw new Error(`Supabase upsert ${table} failed: ${res.status} ${text}`);
    }

    total += chunk.length;
  }

  return total;
}

async function supabaseInsert(table: string, rows: Json[]) {
  assertSupabaseEnv();
  if (!rows.length) return 0;

  let total = 0;
  const chunkSize = 300;

  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);

    const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
      method: "POST",
      headers: supabaseHeaders({ Prefer: "return=minimal" }),
      body: JSON.stringify(chunk),
    });

    const text = await res.text();
    if (!res.ok) {
      throw new Error(`Supabase insert ${table} failed: ${res.status} ${text}`);
    }

    total += chunk.length;
  }

  return total;
}

async function supabaseDelete(table: string, query: URLSearchParams) {
  assertSupabaseEnv();

  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${query.toString()}`, {
    method: "DELETE",
    headers: supabaseHeaders({ Prefer: "return=minimal" }),
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Supabase delete ${table} failed: ${res.status} ${text}`);
  }
}

async function deleteMetricRange(table: string, params: {
  owner_id: string;
  account_id?: string;
  page_id?: string;
  ig_user_id?: string;
  level?: string;
  since: string;
  until: string;
  breakdown_type?: string;
}) {
  const q = new URLSearchParams();
  q.set("owner_id", `eq.${params.owner_id}`);
  q.set("provider", "eq.facebook");
  q.set("date_start", `gte.${params.since}`);
  q.append("date_start", `lte.${params.until}`);

  if (params.account_id) q.set("account_id", `eq.${params.account_id}`);
  if (params.page_id) q.set("page_id", `eq.${params.page_id}`);
  if (params.ig_user_id) q.set("ig_user_id", `eq.${params.ig_user_id}`);
  if (params.level) q.set("level", `eq.${params.level}`);
  if (params.breakdown_type) q.set("breakdown_type", `eq.${params.breakdown_type}`);

  await supabaseDelete(table, q);
}

// =========================================================
// TOKEN HELPERS - COMPATIBLE AVEC TES RPC VAULT
// =========================================================

async function upsertProviderTokenVault(params: {
  owner_id: string;
  provider: string;
  token: Json;
}) {
  await supabaseRpc("upsert_provider_token_admin_json", {
    p_owner_id: params.owner_id,
    p_provider: params.provider,
    p_token: params.token,
  });
}

async function getProviderToken(owner_id: string, provider: string) {
  const data = await supabaseRpc<any>("get_provider_token", {
    p_owner_id: owner_id,
    p_provider: provider,
  });

  const token = Array.isArray(data)
    ? data[0]?.decrypted_secret ?? data[0] ?? null
    : data?.decrypted_secret ?? data ?? null;

  if (!token) throw new Error(`No token found for provider=${provider}`);
  return token;
}

function getAccessTokenFromStoredToken(token: any) {
  let parsed = token;

  if (typeof parsed === "string") {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      throw new Error("Stored token is not valid JSON");
    }
  }

  const accessToken =
    parsed?.access_token ||
    parsed?.raw_token?.access_token ||
    parsed?.long_lived?.access_token ||
    parsed?.short_lived?.access_token ||
    "";

  if (!accessToken || typeof accessToken !== "string") {
    throw new Error("access_token not found in stored token");
  }

  return accessToken;
}

function facebookPageProviderKey(page_id: string) {
  return `facebook_page:${page_id}`;
}

async function getMetaUserAccessToken(owner_id: string) {
  const token = await getProviderToken(owner_id, "facebook");
  return getAccessTokenFromStoredToken(token);
}

async function getFacebookPageAccessToken(owner_id: string, page_id: string) {
  const token = await getProviderToken(owner_id, facebookPageProviderKey(page_id));
  return getAccessTokenFromStoredToken(token);
}

// =========================================================
// META GRAPH HELPERS
// =========================================================

async function fetchJsonWithRetry(url: string, retries = 3): Promise<any> {
  let lastText = "";

  for (let attempt = 0; attempt <= retries; attempt++) {
    const res = await fetch(url, {
      method: "GET",
      headers: { Accept: "application/json" },
    });

    const text = await res.text();
    lastText = text;

    let json: any = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }

    const errorCode = json?.error?.code;
    const isRetryable =
      res.status >= 500 ||
      res.status === 429 ||
      errorCode === 1 ||
      errorCode === 2 ||
      errorCode === 4 ||
      errorCode === 17 ||
      errorCode === 32 ||
      errorCode === 613;

    if (res.ok && !json?.error) return json;

    if (!isRetryable || attempt === retries) {
      throw new Error(`Meta Graph error: ${res.status} ${text}`);
    }

    await sleep(600 * Math.pow(2, attempt));
  }

  throw new Error(`Meta Graph error: ${lastText}`);
}

async function graphGet(
  path: string,
  accessToken: string,
  query?: Record<string, string | number | boolean | null | undefined>
) {
  const url = new URL(
    `https://graph.facebook.com/${META_GRAPH_VERSION}/${path.replace(/^\/+/, "")}`
  );

  for (const [key, value] of Object.entries(query || {})) {
    if (value === undefined || value === null) continue;
    url.searchParams.set(key, String(value));
  }

  if (accessToken) url.searchParams.set("access_token", accessToken);
  return fetchJsonWithRetry(url.toString());
}

async function graphGetAll(
  path: string,
  accessToken: string,
  query?: Record<string, string | number | boolean | null | undefined>,
  maxPages = 50
) {
  const firstUrl = new URL(
    `https://graph.facebook.com/${META_GRAPH_VERSION}/${path.replace(/^\/+/, "")}`
  );

  for (const [key, value] of Object.entries(query || {})) {
    if (value === undefined || value === null) continue;
    firstUrl.searchParams.set(key, String(value));
  }

  if (accessToken) firstUrl.searchParams.set("access_token", accessToken);

  const rows: any[] = [];
  let nextUrl: string | null = firstUrl.toString();
  let page = 0;

  while (nextUrl && page < maxPages) {
    const json = await fetchJsonWithRetry(nextUrl);

    if (Array.isArray(json?.data)) rows.push(...json.data);
    else if (json && !json.data) rows.push(json);

    nextUrl = json?.paging?.next || null;
    page += 1;
  }

  return rows;
}

async function graphGetAllSafe(
  label: string,
  path: string,
  accessToken: string,
  query?: Record<string, string | number | boolean | null | undefined>,
  maxPages = 50
) {
  try {
    return await graphGetAll(path, accessToken, query, maxPages);
  } catch (e: any) {
    console.warn(`[meta][skip] ${label}:`, e?.message || e);
    return [];
  }
}

async function graphGetSafe(
  label: string,
  path: string,
  accessToken: string,
  query?: Record<string, string | number | boolean | null | undefined>
) {
  try {
    return await graphGet(path, accessToken, query);
  } catch (e: any) {
    console.warn(`[meta][skip] ${label}:`, e?.message || e);
    return null;
  }
}

// =========================================================
// META FIELDS
// =========================================================

const AD_ACCOUNT_FIELDS = [
  "id",
  "account_id",
  "name",
  "currency",
  "timezone_id",
  "timezone_name",
  "timezone_offset_hours_utc",
  "account_status",
  "disable_reason",
  "business{id,name}",
  "amount_spent",
  "balance",
  "spend_cap",
  "funding_source",
  "is_prepay_account",
  "is_tax_id_required",
  "created_time",
].join(",");

const CAMPAIGN_FIELDS = [
  "id",
  "account_id",
  "name",
  "objective",
  "status",
  "effective_status",
  "configured_status",
  "buying_type",
  "bid_strategy",
  "daily_budget",
  "lifetime_budget",
  "budget_remaining",
  "spend_cap",
  "special_ad_categories",
  "special_ad_category_country",
  "promoted_object",
  "source_campaign_id",
  "is_skadnetwork_attribution",
  "start_time",
  "stop_time",
  "created_time",
  "updated_time",
].join(",");

const ADSET_FIELDS = [
  "id",
  "account_id",
  "campaign_id",
  "name",
  "status",
  "effective_status",
  "configured_status",
  "daily_budget",
  "lifetime_budget",
  "budget_remaining",
  "bid_amount",
  "bid_strategy",
  "billing_event",
  "optimization_goal",
  "optimization_sub_event",
  "destination_type",
  "pacing_type",
  "attribution_spec",
  "targeting",
  "promoted_object",
  "start_time",
  "end_time",
  "created_time",
  "updated_time",
].join(",");

const CREATIVE_FIELDS = [
  "id",
  "name",
  "status",
  "object_type",
  "object_story_id",
  "effective_object_story_id",
  "object_story_spec",
  "asset_feed_spec",
  "image_hash",
  "image_url",
  "thumbnail_url",
  "video_id",
  "body",
  "title",
  "link_url",
  "template_url",
  "url_tags",
  "call_to_action_type",
  "instagram_actor_id",
  "degrees_of_freedom_spec",
  "creative_sourcing_spec",
].join(",");

const AD_FIELDS = [
  "id",
  "account_id",
  "campaign_id",
  "adset_id",
  "name",
  "status",
  "effective_status",
  "configured_status",
  "preview_shareable_link",
  "tracking_specs",
  "conversion_specs",
  "recommendations",
  "issues_info",
  "ad_review_feedback",
  "created_time",
  "updated_time",
  `creative{${CREATIVE_FIELDS}}`,
].join(",");

const PAGE_FIELDS = [
  "id",
  "name",
  "category",
  "link",
  "picture{url}",
  "fan_count",
  "followers_count",
  "instagram_business_account{id,username,name,profile_picture_url,biography,website,followers_count,follows_count,media_count}",
  "tasks",
  "access_token",
].join(",");

const PAGE_POST_FIELDS = [
  "id",
  "message",
  "story",
  "created_time",
  "permalink_url",
  "full_picture",
  "status_type",
  "is_published",
].join(",");

const IG_ACCOUNT_FIELDS = [
  "id",
  "username",
  "name",
  "profile_picture_url",
  "biography",
  "website",
  "followers_count",
  "follows_count",
  "media_count",
].join(",");

const IG_MEDIA_FIELDS = [
  "id",
  "caption",
  "media_type",
  "media_product_type",
  "media_url",
  "thumbnail_url",
  "permalink",
  "timestamp",
  "like_count",
  "comments_count",
  "children{id,media_type,media_url,thumbnail_url,permalink,timestamp}",
].join(",");

const INSIGHT_FIELDS = [
  "date_start",
  "date_stop",
  "account_id",
  "campaign_id",
  "campaign_name",
  "adset_id",
  "adset_name",
  "ad_id",
  "ad_name",
  "impressions",
  "reach",
  "frequency",
  "spend",
  "clicks",
  "unique_clicks",
  "ctr",
  "unique_ctr",
  "cpc",
  "cpm",
  "cpp",
  "inline_link_clicks",
  "inline_link_click_ctr",
  "cost_per_inline_link_click",
  "outbound_clicks",
  "outbound_clicks_ctr",
  "cost_per_outbound_click",
  "quality_ranking",
  "engagement_rate_ranking",
  "conversion_rate_ranking",
  "actions",
  "action_values",
  "cost_per_action_type",
  "conversions",
  "conversion_values",
  "purchase_roas",
  "website_purchase_roas",
  "mobile_app_purchase_roas",
  "video_play_actions",
  "video_view_per_impression",
  "video_15_sec_watched_actions",
  "video_30_sec_watched_actions",
  "video_avg_time_watched_actions",
  "video_p25_watched_actions",
  "video_p50_watched_actions",
  "video_p75_watched_actions",
  "video_p95_watched_actions",
  "video_p100_watched_actions",
  "video_thruplay_watched_actions",
  "cost_per_thruplay",
  "cost_per_15_sec_video_view",
  "cost_per_2_sec_continuous_video_view",
].join(",");

const BREAKDOWN_INSIGHT_FIELDS = [
  "date_start",
  "date_stop",
  "account_id",
  "campaign_id",
  "adset_id",
  "ad_id",
  "impressions",
  "reach",
  "spend",
  "clicks",
  "ctr",
  "cpc",
  "cpm",
  "actions",
  "action_values",
  "conversions",
].join(",");

const DEFAULT_BREAKDOWNS = [
  "age,gender",
  "country",
  "region",
  "publisher_platform,platform_position",
  "device_platform",
  "impression_device",
];

// =========================================================
// MAPPERS
// =========================================================

function mapAdAccount(owner_id: string, a: any) {
  const account_id = normalizeAccountId(a.account_id || a.id);
  return {
    owner_id,
    provider: "facebook",
    account_id,
    name: a.name ?? null,
    currency: a.currency ?? null,
    timezone_id: toNumber(a.timezone_id, null),
    timezone_name: a.timezone_name ?? null,
    timezone_offset_hours_utc: toNumber(a.timezone_offset_hours_utc, null),
    account_status: toNumber(a.account_status, null),
    disable_reason: toNumber(a.disable_reason, null),
    business_id: a.business?.id ?? null,
    business_name: a.business?.name ?? null,
    owner_business: asObject(a.owner_business),
    amount_spent: toNumber(a.amount_spent, null),
    balance: toNumber(a.balance, null),
    spend_cap: toNumber(a.spend_cap, null),
    funding_source: a.funding_source ?? null,
    is_prepay_account: typeof a.is_prepay_account === "boolean" ? a.is_prepay_account : null,
    is_tax_id_required: typeof a.is_tax_id_required === "boolean" ? a.is_tax_id_required : null,
    created_time: safeMetaTime(a.created_time),
    raw: a,
    last_synced_at: new Date().toISOString(),
  };
}

function mapCampaign(owner_id: string, account_id: string, c: any) {
  return {
    owner_id,
    provider: "facebook",
    account_id,
    campaign_id: c.id,
    name: c.name ?? null,
    objective: c.objective ?? null,
    status: c.status ?? null,
    effective_status: c.effective_status ?? null,
    configured_status: c.configured_status ?? null,
    buying_type: c.buying_type ?? null,
    bid_strategy: c.bid_strategy ?? null,
    daily_budget: minorToAmount(c.daily_budget),
    daily_budget_minor: toBigIntNumber(c.daily_budget, 0) || null,
    lifetime_budget: minorToAmount(c.lifetime_budget),
    lifetime_budget_minor: toBigIntNumber(c.lifetime_budget, 0) || null,
    budget_remaining: minorToAmount(c.budget_remaining),
    budget_remaining_minor: toBigIntNumber(c.budget_remaining, 0) || null,
    spend_cap: minorToAmount(c.spend_cap),
    spend_cap_minor: toBigIntNumber(c.spend_cap, 0) || null,
    special_ad_categories: asArray(c.special_ad_categories),
    special_ad_category_country: asArray(c.special_ad_category_country),
    promoted_object: asObject(c.promoted_object),
    source_campaign_id: c.source_campaign_id ?? null,
    is_skadnetwork_attribution: typeof c.is_skadnetwork_attribution === "boolean" ? c.is_skadnetwork_attribution : null,
    start_time: safeMetaTime(c.start_time),
    stop_time: safeMetaTime(c.stop_time),
    created_time: safeMetaTime(c.created_time),
    updated_time_meta: safeMetaTime(c.updated_time),
    raw: c,
    last_synced_at: new Date().toISOString(),
  };
}

function mapAdSet(owner_id: string, account_id: string, s: any) {
  return {
    owner_id,
    provider: "facebook",
    account_id,
    campaign_id: s.campaign_id,
    adset_id: s.id,
    name: s.name ?? null,
    status: s.status ?? null,
    effective_status: s.effective_status ?? null,
    configured_status: s.configured_status ?? null,
    daily_budget: minorToAmount(s.daily_budget),
    daily_budget_minor: toBigIntNumber(s.daily_budget, 0) || null,
    lifetime_budget: minorToAmount(s.lifetime_budget),
    lifetime_budget_minor: toBigIntNumber(s.lifetime_budget, 0) || null,
    budget_remaining: minorToAmount(s.budget_remaining),
    budget_remaining_minor: toBigIntNumber(s.budget_remaining, 0) || null,
    bid_amount: minorToAmount(s.bid_amount),
    bid_amount_minor: toBigIntNumber(s.bid_amount, 0) || null,
    bid_strategy: s.bid_strategy ?? null,
    billing_event: s.billing_event ?? null,
    optimization_goal: s.optimization_goal ?? null,
    optimization_sub_event: s.optimization_sub_event ?? null,
    destination_type: s.destination_type ?? null,
    pacing_type: asArray(s.pacing_type),
    attribution_spec: asArray(s.attribution_spec),
    targeting: asObject(s.targeting),
    promoted_object: asObject(s.promoted_object),
    start_time: safeMetaTime(s.start_time),
    end_time: safeMetaTime(s.end_time),
    created_time: safeMetaTime(s.created_time),
    updated_time_meta: safeMetaTime(s.updated_time),
    raw: s,
    last_synced_at: new Date().toISOString(),
  };
}

function mapAd(owner_id: string, account_id: string, ad: any) {
  return {
    owner_id,
    provider: "facebook",
    account_id,
    campaign_id: ad.campaign_id ?? null,
    adset_id: ad.adset_id ?? null,
    ad_id: ad.id,
    creative_id: ad.creative?.id ?? null,
    name: ad.name ?? null,
    status: ad.status ?? null,
    effective_status: ad.effective_status ?? null,
    configured_status: ad.configured_status ?? null,
    preview_shareable_link: ad.preview_shareable_link ?? null,
    tracking_specs: asArray(ad.tracking_specs),
    conversion_specs: asArray(ad.conversion_specs),
    recommendations: asArray(ad.recommendations),
    issues_info: asArray(ad.issues_info),
    ad_review_feedback: asObject(ad.ad_review_feedback),
    created_time: safeMetaTime(ad.created_time),
    updated_time_meta: safeMetaTime(ad.updated_time),
    raw: ad,
    last_synced_at: new Date().toISOString(),
  };
}

function mapCreative(owner_id: string, account_id: string, creative: any) {
  if (!creative?.id) return null;

  return {
    owner_id,
    provider: "facebook",
    account_id,
    ad_account_id_act: act(account_id),
    creative_id: creative.id,
    name: creative.name ?? null,
    status: creative.status ?? null,
    object_type: creative.object_type ?? null,
    object_story_id: creative.object_story_id ?? null,
    effective_object_story_id: creative.effective_object_story_id ?? null,
    page_id: pageIdFromCreative(creative),
    instagram_actor_id: instagramActorIdFromCreative(creative),
    image_hash: creative.image_hash ?? null,
    image_url:
      creative?.image_url ||
      creative?.thumbnail_url ||
      asObject(creative?.object_story_spec)?.link_data?.picture ||
      null,

    thumbnail_url:
      creative?.thumbnail_url ||
      asObject(creative?.object_story_spec)?.video_data?.image_url ||
      null,

    video_id:
      creative?.video_id ||
      asObject(creative?.object_story_spec)?.video_data?.video_id ||
      asArray(asObject(creative?.asset_feed_spec)?.videos)?.[0]?.video_id ||
      null,
    body: primaryTextFromCreative(creative),
    title: titleFromCreative(creative),
    description: descriptionFromCreative(creative),
    link_url: creative.link_url ?? null,
    template_url: creative.template_url ?? null,
    url_tags: creative.url_tags ?? null,
    call_to_action_type: callToActionTypeFromCreative(creative),
    destination_url: destinationUrlFromCreative(creative),
    object_story_spec: asObject(creative.object_story_spec),
    asset_feed_spec: asObject(creative.asset_feed_spec),
    degrees_of_freedom_spec: asObject(creative.degrees_of_freedom_spec),
    creative_sourcing_spec: asObject(creative.creative_sourcing_spec),
    raw: creative,
    last_synced_at: new Date().toISOString(),
  };
}

function mapPage(owner_id: string, p: any) {
  return {
    owner_id,
    provider: "facebook",
    page_id: p.id,
    name: p.name ?? null,
    category: p.category ?? null,
    link: p.link ?? null,
    picture_url: p.picture?.data?.url ?? null,
    fan_count: toBigIntNumber(p.fan_count, 0) || null,
    followers_count: toBigIntNumber(p.followers_count, 0) || null,
    instagram_business_account_id: p.instagram_business_account?.id ?? null,
    tasks: asArray(p.tasks),
    raw: { ...p, access_token: undefined },
    last_synced_at: new Date().toISOString(),
  };
}

function mapIgAccount(owner_id: string, page_id: string | null, ig: any) {
  return {
    owner_id,
    provider: "facebook",
    ig_user_id: ig.id,
    page_id,
    username: ig.username ?? null,
    name: ig.name ?? null,
    biography: ig.biography ?? null,
    website: ig.website ?? null,
    profile_picture_url: ig.profile_picture_url ?? null,
    followers_count: toBigIntNumber(ig.followers_count, 0) || null,
    follows_count: toBigIntNumber(ig.follows_count, 0) || null,
    media_count: toBigIntNumber(ig.media_count, 0) || null,
    raw: ig,
    last_synced_at: new Date().toISOString(),
  };
}

function mapPagePost(owner_id: string, page_id: string, post: any) {
  return {
    owner_id,
    provider: "facebook",
    page_id,
    post_id: post.id,
    message: post.message ?? null,
    story: post.story ?? null,
    type: post.type ?? null,
    status_type: post.status_type ?? null,
    is_published: typeof post.is_published === "boolean" ? post.is_published : null,
    permalink_url: post.permalink_url ?? null,
    permalink: post.permalink_url ?? null,
    full_picture: post.full_picture ?? null,
    attachments: asArray(post.attachments?.data),
    from_data: asObject(post.from),
    admin_creator: asObject(post.admin_creator),
    created_time: safeMetaTime(post.created_time),
    updated_time_meta: safeMetaTime(post.updated_time),
    raw: post,
    last_synced_at: new Date().toISOString(),
  };
}

function mapIgMedia(owner_id: string, page_id: string | null, ig_user_id: string, media: any) {
  return {
    owner_id,
    provider: "facebook",
    ig_user_id,
    page_id,
    media_id: media.id,
    caption: media.caption ?? null,
    media_type: media.media_type ?? null,
    media_product_type: media.media_product_type ?? null,
    media_url: media.media_url ?? null,
    thumbnail_url: media.thumbnail_url ?? null,
    permalink: media.permalink ?? null,
    children: asArray(media.children?.data),
    timestamp_meta: safeMetaTime(media.timestamp),
    raw: media,
    last_synced_at: new Date().toISOString(),
  };
}

function mapInsightBase(owner_id: string, level: string, account_id: string, row: any) {
  const actions = asArray(row.actions);
  return {
    owner_id,
    provider: "facebook",
    level,
    date_start: row.date_start,
    date_stop: row.date_stop,
    account_id,
    campaign_id: row.campaign_id ?? null,
    adset_id: row.adset_id ?? null,
    ad_id: row.ad_id ?? null,
    impressions: toBigIntNumber(row.impressions, 0),
    reach: toBigIntNumber(row.reach, 0),
    frequency: toNumber(row.frequency, null),
    spend: toNumber(row.spend, 0),
    clicks: toBigIntNumber(row.clicks, 0),
    unique_clicks: toBigIntNumber(row.unique_clicks, 0) || null,
    ctr: toNumber(row.ctr, null),
    unique_ctr: toNumber(row.unique_ctr, null),
    cpc: toNumber(row.cpc, null),
    cpm: toNumber(row.cpm, null),
    cpp: toNumber(row.cpp, null),
    inline_link_clicks: toBigIntNumber(row.inline_link_clicks, 0) || null,
    inline_link_click_ctr: toNumber(row.inline_link_click_ctr, null),
    cost_per_inline_link_click: toNumber(row.cost_per_inline_link_click, null),
    outbound_clicks: asArray(row.outbound_clicks),
    outbound_clicks_ctr: asArray(row.outbound_clicks_ctr),
    cost_per_outbound_click: asArray(row.cost_per_outbound_click),
    landing_page_views: getActionValue(actions, "landing_page_view"),
    quality_ranking: row.quality_ranking ?? null,
    engagement_rate_ranking: row.engagement_rate_ranking ?? null,
    conversion_rate_ranking: row.conversion_rate_ranking ?? null,
    actions,
    action_values: asArray(row.action_values),
    cost_per_action_type: asArray(row.cost_per_action_type),
    conversions: asArray(row.conversions),
    conversion_values: asArray(row.conversion_values),
    purchase_roas: asArray(row.purchase_roas),
    website_purchase_roas: asArray(row.website_purchase_roas),
    mobile_app_purchase_roas: asArray(row.mobile_app_purchase_roas),
    raw: row,
    fetched_at: new Date().toISOString(),
  };
}

function mapVideoInsight(owner_id: string, level: string, account_id: string, row: any) {
  const videoFields = [
    "video_play_actions",
    "video_15_sec_watched_actions",
    "video_30_sec_watched_actions",
    "video_avg_time_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_p100_watched_actions",
    "video_thruplay_watched_actions",
    "cost_per_thruplay",
    "cost_per_15_sec_video_view",
    "cost_per_2_sec_continuous_video_view",
  ];

  const hasVideo = videoFields.some((f) => asArray(row[f]).length > 0) || row.video_view_per_impression;
  if (!hasVideo) return null;

  return {
    owner_id,
    provider: "facebook",
    level,
    date_start: row.date_start,
    date_stop: row.date_stop,
    account_id,
    campaign_id: row.campaign_id ?? null,
    adset_id: row.adset_id ?? null,
    ad_id: row.ad_id ?? null,
    video_play_actions: asArray(row.video_play_actions),
    video_view_per_impression: toNumber(row.video_view_per_impression, null),
    video_15_sec_watched_actions: asArray(row.video_15_sec_watched_actions),
    video_30_sec_watched_actions: asArray(row.video_30_sec_watched_actions),
    video_avg_time_watched_actions: asArray(row.video_avg_time_watched_actions),
    video_p25_watched_actions: asArray(row.video_p25_watched_actions),
    video_p50_watched_actions: asArray(row.video_p50_watched_actions),
    video_p75_watched_actions: asArray(row.video_p75_watched_actions),
    video_p95_watched_actions: asArray(row.video_p95_watched_actions),
    video_p100_watched_actions: asArray(row.video_p100_watched_actions),
    video_thruplay_watched_actions: asArray(row.video_thruplay_watched_actions),
    cost_per_thruplay: asArray(row.cost_per_thruplay),
    cost_per_15_sec_video_view: asArray(row.cost_per_15_sec_video_view),
    cost_per_2_sec_continuous_video_view: asArray(row.cost_per_2_sec_continuous_video_view),
    raw: row,
    fetched_at: new Date().toISOString(),
  };
}

function mapActionInsights(owner_id: string, level: string, account_id: string, row: any) {
  const valuesByType = new Map<string, number>();
  const costsByType = new Map<string, number>();

  for (const item of asArray(row.action_values)) {
    if (item?.action_type) valuesByType.set(item.action_type, toNumber(item.value, 0) || 0);
  }
  for (const item of asArray(row.cost_per_action_type)) {
    if (item?.action_type) costsByType.set(item.action_type, toNumber(item.value, 0) || 0);
  }

  return asArray(row.actions).map((a) => ({
    owner_id,
    provider: "facebook",
    level,
    date_start: row.date_start,
    date_stop: row.date_stop,
    account_id,
    campaign_id: row.campaign_id ?? null,
    adset_id: row.adset_id ?? null,
    ad_id: row.ad_id ?? null,
    action_type: a.action_type,
    action_destination: a.action_destination ?? "",
    action_device: a.action_device ?? "",
    conversion_destination: a.conversion_destination ?? "",
    value: toNumber(a.value, 0) || 0,
    action_value: valuesByType.get(a.action_type) ?? null,
    cost_per_action: costsByType.get(a.action_type) ?? null,
    raw: a,
    fetched_at: new Date().toISOString(),
  }));
}

function mapBreakdownInsight(
  owner_id: string,
  level: string,
  account_id: string,
  breakdownType: string,
  row: any
) {
  const dims = breakdownType.split(",").map((d) => d.trim()).filter(Boolean);
  const breakdown: Json = {};

  for (const dim of dims) {
    breakdown[dim] = row[dim] ?? null;
  }

  return {
    owner_id,
    provider: "facebook",
    level,
    date_start: row.date_start,
    date_stop: row.date_stop,
    account_id,
    campaign_id: row.campaign_id ?? null,
    adset_id: row.adset_id ?? null,
    ad_id: row.ad_id ?? null,
    breakdown_type: breakdownType,
    breakdown,
    breakdown_hash: hashJson(breakdown),
    impressions: toBigIntNumber(row.impressions, 0),
    reach: toBigIntNumber(row.reach, 0),
    spend: toNumber(row.spend, 0),
    clicks: toBigIntNumber(row.clicks, 0),
    ctr: toNumber(row.ctr, null),
    cpc: toNumber(row.cpc, null),
    cpm: toNumber(row.cpm, null),
    actions: asArray(row.actions),
    action_values: asArray(row.action_values),
    conversions: asArray(row.conversions),
    raw: row,
    fetched_at: new Date().toISOString(),
  };
}

// =========================================================
// CORE SYNC FUNCTIONS
// =========================================================

async function syncPages(owner_id: string, userAccessToken: string, counters: SyncCounters, pageIds?: string[]) {
  const pages = await graphGetAll("me/accounts", userAccessToken, {
    fields: PAGE_FIELDS,
    limit: 100,
  });

  const filtered = pageIds?.length ? pages.filter((p) => pageIds.includes(String(p.id))) : pages;

  const pageRows = filtered.map((p) => mapPage(owner_id, p));
  await supabaseUpsert("meta_pages", pageRows, "owner_id,provider,page_id");
  inc(counters, "pages_synced", pageRows.length);

  const igRows = filtered
    .map((p) => p.instagram_business_account ? mapIgAccount(owner_id, p.id, p.instagram_business_account) : null)
    .filter(Boolean) as Json[];

  await supabaseUpsert("meta_instagram_accounts", igRows, "owner_id,provider,ig_user_id");
  inc(counters, "instagram_accounts_synced", igRows.length);

  for (const p of filtered) {
    if (!p.access_token) continue;
    await upsertProviderTokenVault({
      owner_id,
      provider: facebookPageProviderKey(p.id),
      token: {
        provider: "facebook_page",
        page_id: p.id,
        page_name: p.name ?? null,
        access_token: p.access_token,
        tasks: asArray(p.tasks),
        stored_at: new Date().toISOString(),
      },
    });
    inc(counters, "page_tokens_stored");
  }

  return filtered;
}

async function syncBusinessesAndCatalogs(owner_id: string, userAccessToken: string, counters: SyncCounters, includeCatalogs = true) {
  const businesses = await graphGetAllSafe("businesses", "me/businesses", userAccessToken, {
    fields: "id,name,verification_status,created_time,updated_time",
    limit: 100,
  });

  const businessRows = businesses.map((b) => ({
    owner_id,
    provider: "facebook",
    business_id: b.id,
    name: b.name ?? null,
    verification_status: b.verification_status ?? null,
    created_time: safeMetaTime(b.created_time),
    updated_time_meta: safeMetaTime(b.updated_time),
    raw: b,
    last_synced_at: new Date().toISOString(),
  }));

  await supabaseUpsert("meta_businesses", businessRows, "owner_id,provider,business_id");
  inc(counters, "businesses_synced", businessRows.length);

  if (!includeCatalogs) return businesses;

  for (const b of businesses) {
    const catalogs = await graphGetAllSafe(
      `business_catalogs_${b.id}`,
      `${b.id}/owned_product_catalogs`,
      userAccessToken,
      { fields: "id,name,vertical,business", limit: 100 },
      10
    );

    const catalogRows = catalogs.map((cat) => ({
      owner_id,
      provider: "facebook",
      catalog_id: cat.id,
      business_id: b.id,
      name: cat.name ?? null,
      vertical: cat.vertical ?? null,
      raw: cat,
      last_synced_at: new Date().toISOString(),
    }));

    await supabaseUpsert("meta_catalogs", catalogRows, "owner_id,provider,catalog_id");
    inc(counters, "catalogs_synced", catalogRows.length);

    for (const cat of catalogs) {
      const productSets = await graphGetAllSafe(
        `product_sets_${cat.id}`,
        `${cat.id}/product_sets`,
        userAccessToken,
        { fields: "id,name,filter", limit: 100 },
        10
      );

      const productSetRows = productSets.map((ps) => ({
        owner_id,
        provider: "facebook",
        catalog_id: cat.id,
        product_set_id: ps.id,
        name: ps.name ?? null,
        filter: asObject(ps.filter),
        raw: ps,
        last_synced_at: new Date().toISOString(),
      }));

      await supabaseUpsert("meta_product_sets", productSetRows, "owner_id,provider,product_set_id");
      inc(counters, "product_sets_synced", productSetRows.length);

      const products = await graphGetAllSafe(
        `products_${cat.id}`,
        `${cat.id}/products`,
        userAccessToken,
        {
          fields: [
            "id",
            "retailer_id",
            "name",
            "description",
            "brand",
            "category",
            "price",
            "currency",
            "availability",
            "condition",
            "image_url",
            "url",
            "custom_label_0",
            "custom_label_1",
            "custom_label_2",
            "custom_label_3",
            "custom_label_4",
          ].join(","),
          limit: 100,
        },
        10
      );

      const productRows = products.map((p) => ({
        owner_id,
        provider: "facebook",
        catalog_id: cat.id,
        product_id: p.id,
        retailer_id: p.retailer_id ?? null,
        name: p.name ?? null,
        description: p.description ?? null,
        brand: p.brand ?? null,
        category: p.category ?? null,
        price: toNumber(p.price, null),
        currency: p.currency ?? null,
        availability: p.availability ?? null,
        condition: p.condition ?? null,
        image_url: p.image_url ?? null,
        url: p.url ?? null,
        custom_label_0: p.custom_label_0 ?? null,
        custom_label_1: p.custom_label_1 ?? null,
        custom_label_2: p.custom_label_2 ?? null,
        custom_label_3: p.custom_label_3 ?? null,
        custom_label_4: p.custom_label_4 ?? null,
        raw: p,
        last_synced_at: new Date().toISOString(),
      }));

      await supabaseUpsert("meta_products", productRows, "owner_id,provider,product_id");
      inc(counters, "products_synced", productRows.length);
    }
  }

  return businesses;
}

async function syncOrganicForPages(owner_id: string, pages: any[], counters: SyncCounters) {
  for (const p of pages) {
    const page_id = p.id;
    let pageToken = p.access_token || "";

    if (!pageToken) {
      try {
        pageToken = await getFacebookPageAccessToken(owner_id, page_id);
      } catch {
        console.warn(`[meta][organic] no page token for page=${page_id}`);
        continue;
      }
    }

    const posts = await graphGetAllSafe(
      `page_posts_${page_id}`,
      `${page_id}/posts`,
      pageToken,
      { fields: PAGE_POST_FIELDS, limit: 100 },
      20
    );

    const postRows = posts.map((post) => mapPagePost(owner_id, page_id, post));
    await supabaseUpsert("meta_page_posts", postRows, "owner_id,provider,post_id");
    inc(counters, "page_posts_synced", postRows.length);

    for (const post of posts) {
      await syncPagePostMetrics(owner_id, page_id, post.id, pageToken, counters);
    }

    const igId = p.instagram_business_account?.id;
    if (igId) {
      const ig = await graphGetSafe(`ig_account_${igId}`, igId, pageToken, { fields: IG_ACCOUNT_FIELDS });
      if (ig?.id) {
        await supabaseUpsert("meta_instagram_accounts", [mapIgAccount(owner_id, page_id, ig)], "owner_id,provider,ig_user_id");
        inc(counters, "instagram_accounts_synced");
      }

      const media = await graphGetAllSafe(
        `ig_media_${igId}`,
        `${igId}/media`,
        pageToken,
        { fields: IG_MEDIA_FIELDS, limit: 100 },
        20
      );

      const mediaRows = media.map((m) => mapIgMedia(owner_id, page_id, igId, m));
      await supabaseUpsert("meta_instagram_media", mediaRows, "owner_id,provider,media_id");
      inc(counters, "instagram_media_synced", mediaRows.length);

      for (const m of media) {
        await syncInstagramMediaMetrics(owner_id, igId, m.id, pageToken, counters);
      }
    }
  }
}

async function syncPagePostMetrics(owner_id: string, page_id: string, post_id: string, pageToken: string, counters: SyncCounters) {
const metrics = [
  "post_total_media_view_unique",
  "post_clicks",
  "post_reactions_by_type_total",
  "post_video_views",
  "post_video_avg_time_watched",
  "post_video_complete_views_organic",
  "post_video_views_organic",
  "post_video_views_autoplayed",
  "post_video_views_clicked_to_play",
  "post_video_retention_graph",
].join(",");
  const json = await graphGetSafe(`page_post_metrics_${post_id}`, `${post_id}/insights`, pageToken, { metric: metrics, period: "day" });
  if (!json?.data) return;

  const byDate = new Map<string, Json>();

  for (const metric of asArray(json.data)) {
    for (const v of asArray(metric.values)) {
      const endDate = String(v.end_time || todayISODate()).slice(0, 10);
      const row = byDate.get(endDate) || {
        owner_id,
        provider: "facebook",
        page_id,
        post_id,
        date_start: endDate,
        date_stop: endDate,
        raw: {},
        fetched_at: new Date().toISOString(),
      };

      row.raw[metric.name] = v;
      const value = v.value;

      if (metric.name === "post_impressions") row.impressions = toBigIntNumber(value, 0);
      if (metric.name === "post_total_media_view_unique") row.impressions_unique = toBigIntNumber(value, 0);
      if (metric.name === "post_engaged_users") row.engaged_users = toBigIntNumber(value, 0);
      if (metric.name === "post_clicks") row.clicks = toBigIntNumber(value, 0);
      if (metric.name === "post_video_views") row.video_views = toBigIntNumber(value, 0);
      if (metric.name === "post_video_avg_time_watched") row.video_avg_time_watched = toNumber(value, null);
      if (metric.name === "post_negative_feedback") row.negative_feedback = toBigIntNumber(value, 0);
      if (metric.name === "post_reactions_by_type_total") {
        const obj = asObject(value);
        row.reactions_total = Object.values(obj).reduce((sum: number, n: any) => sum + (toNumber(n, 0) || 0), 0);
        row.likes_count = toBigIntNumber(obj.like, 0) || null;
      }

      byDate.set(endDate, row);
    }
  }

  const rows = Array.from(byDate.values());
  await supabaseUpsert("meta_page_post_metrics_daily", rows, "owner_id,provider,page_id,post_id,date_start,date_stop");
  inc(counters, "page_post_metric_rows_synced", rows.length);
}

async function syncInstagramMediaMetrics(owner_id: string, ig_user_id: string, media_id: string, pageToken: string, counters: SyncCounters) {
  const metrics = "reach,views,likes,comments,shares,saved,total_interactions";
  const json = await graphGetSafe(`ig_media_metrics_${media_id}`, `${media_id}/insights`, pageToken, { metric: metrics });
  if (!json?.data) return;

  const row: Json = {
    owner_id,
    provider: "facebook",
    ig_user_id,
    media_id,
    date_start: todayISODate(),
    date_stop: todayISODate(),
    raw: json,
    fetched_at: new Date().toISOString(),
  };

  for (const metric of asArray(json.data)) {
    const value = metric.values?.[0]?.value ?? metric.total_value?.value ?? null;
    if (metric.name === "reach") row.reach = toBigIntNumber(value, 0);
    if (metric.name === "impressions") row.impressions = toBigIntNumber(value, 0);
    if (metric.name === "views") row.views = toBigIntNumber(value, 0);
    if (metric.name === "plays") row.plays = toBigIntNumber(value, 0);
    if (metric.name === "likes") row.likes = toBigIntNumber(value, 0);
    if (metric.name === "comments") row.comments = toBigIntNumber(value, 0);
    if (metric.name === "shares") row.shares = toBigIntNumber(value, 0);
    if (metric.name === "saved") row.saves = toBigIntNumber(value, 0);
    if (metric.name === "replies") row.replies = toBigIntNumber(value, 0);
    if (metric.name === "profile_visits") row.profile_visits = toBigIntNumber(value, 0);
    if (metric.name === "follows") row.follows = toBigIntNumber(value, 0);
    if (metric.name === "total_interactions") row.total_interactions = toBigIntNumber(value, 0);
    if (metric.name === "navigation") row.navigation = asObject(value);
  }

  await supabaseUpsert("meta_instagram_media_metrics_daily", [row], "owner_id,provider,ig_user_id,media_id,date_start,date_stop");
  inc(counters, "instagram_media_metric_rows_synced");
}

async function syncAdAccounts(owner_id: string, userAccessToken: string, counters: SyncCounters, accountIds?: string[]) {
  const accounts = await graphGetAll("me/adaccounts", userAccessToken, {
    fields: AD_ACCOUNT_FIELDS,
    limit: 100,
  });

  const filtered = accountIds?.length
    ? accounts.filter((a) => accountIds.includes(normalizeAccountId(a.account_id || a.id)))
    : accounts;

  const rows = filtered.map((a) => mapAdAccount(owner_id, a));
  await supabaseUpsert("meta_ad_accounts", rows, "owner_id,provider,account_id");
  inc(counters, "ad_accounts_synced", rows.length);

  return filtered.map((a) => normalizeAccountId(a.account_id || a.id));
}

async function syncAdsMetadataForAccount(owner_id: string, account_id: string, userAccessToken: string, counters: SyncCounters) {
  const accountAct = act(account_id);

  const [campaigns, adsets, ads, accountCreatives] = await Promise.all([
    graphGetAllSafe(`campaigns_${account_id}`, `${accountAct}/campaigns`, userAccessToken, { fields: CAMPAIGN_FIELDS, limit: 100 }, 50),
    graphGetAllSafe(`adsets_${account_id}`, `${accountAct}/adsets`, userAccessToken, { fields: ADSET_FIELDS, limit: 100 }, 50),
    graphGetAllSafe(`ads_${account_id}`, `${accountAct}/ads`, userAccessToken, { fields: AD_FIELDS, limit: 100 }, 50),
    graphGetAllSafe(`adcreatives_${account_id}`, `${accountAct}/adcreatives`, userAccessToken, { fields: CREATIVE_FIELDS, limit: 100 }, 50),
  ]);

  const campaignRows = campaigns.map((c) => mapCampaign(owner_id, account_id, c));
  const adsetRows = adsets.map((s) => mapAdSet(owner_id, account_id, s));
  const adRows = ads.map((ad) => mapAd(owner_id, account_id, ad));

  const creativeMap = new Map<string, Json>();
  for (const c of accountCreatives) {
    const row = mapCreative(owner_id, account_id, c);
    if (row) creativeMap.set(row.creative_id, row);
  }
  for (const ad of ads) {
    const row = mapCreative(owner_id, account_id, ad.creative);
    if (row) creativeMap.set(row.creative_id, row);
  }

  await supabaseUpsert("meta_ad_campaigns", campaignRows, "owner_id,provider,campaign_id");
  await supabaseUpsert("meta_ad_sets", adsetRows, "owner_id,provider,adset_id");
  await supabaseUpsert("meta_ads", adRows, "owner_id,provider,ad_id");
  await supabaseUpsert("meta_ad_creatives", Array.from(creativeMap.values()), "owner_id,provider,creative_id");

  inc(counters, "campaigns_synced", campaignRows.length);
  inc(counters, "adsets_synced", adsetRows.length);
  inc(counters, "ads_synced", adRows.length);
  inc(counters, "creatives_synced", creativeMap.size);
}

async function syncAudiencesForAccount(owner_id: string, account_id: string, userAccessToken: string, counters: SyncCounters) {
  const accountAct = act(account_id);

  const customAudiences = await graphGetAllSafe(
    `custom_audiences_${account_id}`,
    `${accountAct}/customaudiences`,
    userAccessToken,
    {
      fields: [
        "id",
        "name",
        "description",
        "subtype",
        "approximate_count",
        "delivery_status",
        "operation_status",
        "permission_for_actions",
        "lookalike_spec",
        "retention_days",
        "rule",
        "data_source",
        "time_created",
        "time_updated",
      ].join(","),
      limit: 100,
    },
    20
  );

  const savedAudiences = await graphGetAllSafe(
    `saved_audiences_${account_id}`,
    `${accountAct}/saved_audiences`,
    userAccessToken,
    { fields: "id,name,description,targeting,time_created,time_updated,run_status", limit: 100 },
    20
  );

  const rows = [
    ...customAudiences.map((a) => ({
      owner_id,
      provider: "facebook",
      account_id,
      audience_id: a.id,
      name: a.name ?? null,
      description: a.description ?? null,
      audience_type: "custom_audience",
      subtype: a.subtype ?? null,
      approximate_count: toBigIntNumber(a.approximate_count, 0) || null,
      retention_days: toNumber(a.retention_days, null),
      delivery_status: asObject(a.delivery_status),
      operation_status: asObject(a.operation_status),
      permission_for_actions: asObject(a.permission_for_actions),
      lookalike_spec: asObject(a.lookalike_spec),
      rule: asObject(a.rule),
      data_source: asObject(a.data_source),
      time_created: safeMetaTime(a.time_created),
      time_updated: safeMetaTime(a.time_updated),
      raw: a,
      last_synced_at: new Date().toISOString(),
    })),
    ...savedAudiences.map((a) => ({
      owner_id,
      provider: "facebook",
      account_id,
      audience_id: a.id,
      name: a.name ?? null,
      description: a.description ?? null,
      audience_type: "saved_audience",
      subtype: null,
      approximate_count: null,
      retention_days: null,
      delivery_status: {},
      operation_status: a.run_status ? { run_status: a.run_status } : {},
      permission_for_actions: {},
      lookalike_spec: {},
      rule: asObject(a.targeting),
      data_source: {},
      time_created: safeMetaTime(a.time_created),
      time_updated: safeMetaTime(a.time_updated),
      raw: a,
      last_synced_at: new Date().toISOString(),
    })),
  ];

  await supabaseUpsert("meta_audiences", rows, "owner_id,provider,audience_id");
  inc(counters, "audiences_synced", rows.length);
}

async function syncPixelsAndConversionsForAccount(owner_id: string, account_id: string, userAccessToken: string, counters: SyncCounters) {
  const accountAct = act(account_id);

  const pixels = await graphGetAllSafe(
    `pixels_${account_id}`,
    `${accountAct}/adspixels`,
    userAccessToken,
    {
      fields: "id,name,owner_ad_account,is_created_by_business,automatic_matching_fields,creation_time,last_fired_time",
      limit: 100,
    },
    10
  );

  const pixelRows = pixels.map((p) => ({
    owner_id,
    provider: "facebook",
    pixel_id: p.id,
    account_id,
    business_id: null,
    name: p.name ?? null,
    owner_ad_account_id: p.owner_ad_account?.id ?? null,
    is_created_by_business: typeof p.is_created_by_business === "boolean" ? p.is_created_by_business : null,
    automatic_matching_fields: asArray(p.automatic_matching_fields),
    event_stats: {},
    creation_time: safeMetaTime(p.creation_time),
    last_fired_time: safeMetaTime(p.last_fired_time),
    raw: p,
    last_synced_at: new Date().toISOString(),
  }));

  await supabaseUpsert("meta_pixels", pixelRows, "owner_id,provider,pixel_id");
  inc(counters, "pixels_synced", pixelRows.length);

  const customConversions = await graphGetAllSafe(
    `custom_conversions_${account_id}`,
    `${accountAct}/customconversions`,
    userAccessToken,
    {
      fields: "id,name,description,event_source_type,event_name,custom_event_type,default_conversion_value,rule,is_archived,creation_time,last_fired_time,pixel",
      limit: 100,
    },
    10
  );

  const conversionRows = customConversions.map((cc) => ({
    owner_id,
    provider: "facebook",
    custom_conversion_id: cc.id,
    account_id,
    pixel_id: cc.pixel?.id ?? null,
    name: cc.name ?? null,
    description: cc.description ?? null,
    event_source_type: cc.event_source_type ?? null,
    event_name: cc.event_name ?? null,
    custom_event_type: cc.custom_event_type ?? null,
    default_conversion_value: toNumber(cc.default_conversion_value, null),
    rule: asObject(cc.rule),
    is_archived: typeof cc.is_archived === "boolean" ? cc.is_archived : null,
    creation_time: safeMetaTime(cc.creation_time),
    last_fired_time: safeMetaTime(cc.last_fired_time),
    raw: cc,
    last_synced_at: new Date().toISOString(),
  }));

  await supabaseUpsert("meta_custom_conversions", conversionRows, "owner_id,provider,custom_conversion_id");
  inc(counters, "custom_conversions_synced", conversionRows.length);
}

async function syncLeadFormsAndLeads(owner_id: string, pages: any[], counters: SyncCounters) {
  for (const p of pages) {
    const page_id = p.id;
    let pageToken = p.access_token || "";

    if (!pageToken) {
      try {
        pageToken = await getFacebookPageAccessToken(owner_id, page_id);
      } catch {
        continue;
      }
    }

    const forms = await graphGetAllSafe(
      `lead_forms_${page_id}`,
      `${page_id}/leadgen_forms`,
      pageToken,
      {
        fields: "id,name,status,locale,questions,privacy_policy,thank_you_page,leads_count,expired_leads_count,created_time",
        limit: 100,
      },
      10
    );

    const formRows = forms.map((f) => ({
      owner_id,
      provider: "facebook",
      page_id,
      form_id: f.id,
      name: f.name ?? null,
      status: f.status ?? null,
      locale: f.locale ?? null,
      questions: asArray(f.questions),
      privacy_policy: asObject(f.privacy_policy),
      thank_you_page: asObject(f.thank_you_page),
      leads_count: toBigIntNumber(f.leads_count, 0) || null,
      expired_leads_count: toBigIntNumber(f.expired_leads_count, 0) || null,
      created_time: safeMetaTime(f.created_time),
      raw: f,
      last_synced_at: new Date().toISOString(),
    }));

    await supabaseUpsert("meta_lead_forms", formRows, "owner_id,provider,form_id");
    inc(counters, "lead_forms_synced", formRows.length);

    for (const f of forms) {
      const leads = await graphGetAllSafe(
        `leads_${f.id}`,
        `${f.id}/leads`,
        pageToken,
        {
          fields: "id,created_time,field_data,ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,platform,is_organic",
          limit: 100,
        },
        20
      );

      const leadRows = leads.map((l) => ({
        owner_id,
        provider: "facebook",
        lead_id: l.id,
        form_id: f.id,
        page_id,
        account_id: null,
        campaign_id: l.campaign_id ?? null,
        campaign_name: l.campaign_name ?? null,
        adset_id: l.adset_id ?? null,
        adset_name: l.adset_name ?? null,
        ad_id: l.ad_id ?? null,
        ad_name: l.ad_name ?? null,
        platform: l.platform ?? null,
        is_organic: typeof l.is_organic === "boolean" ? l.is_organic : null,
        field_data: asArray(l.field_data),
        created_time: safeMetaTime(l.created_time),
        raw: l,
        last_synced_at: new Date().toISOString(),
      }));

      await supabaseUpsert("meta_leads", leadRows, "owner_id,provider,lead_id");
      inc(counters, "leads_synced", leadRows.length);
    }
  }
}

async function syncInsightsForAccount(owner_id: string, account_id: string, userAccessToken: string, counters: SyncCounters, since: string, until: string) {
  const accountAct = act(account_id);
  const levels = ["account", "campaign", "adset", "ad"];

  for (const level of levels) {
    const rows = await graphGetAllSafe(
      `insights_${level}_${account_id}`,
      `${accountAct}/insights`,
      userAccessToken,
      {
        level,
        fields: INSIGHT_FIELDS,
        time_range: JSON.stringify({ since, until }),
        time_increment: 1,
        limit: 500,
      },
      50
    );

    if (!rows.length) continue;

    await deleteMetricRange("meta_ad_action_metrics_daily", { owner_id, account_id, level, since, until });

    const baseRows = rows.map((r) => mapInsightBase(owner_id, level, account_id, r));
    const videoRows = rows.map((r) => mapVideoInsight(owner_id, level, account_id, r)).filter(Boolean) as Json[];
    const actionRows = rows.flatMap((r) => mapActionInsights(owner_id, level, account_id, r));

    await supabaseUpsert("meta_ad_metrics_daily", baseRows, "owner_id,provider,level,date_start,date_stop,account_id,entity_id");
    await supabaseUpsert("meta_ad_video_metrics_daily", videoRows, "owner_id,provider,level,date_start,date_stop,account_id,entity_id");
    await supabaseInsert("meta_ad_action_metrics_daily", actionRows);

    inc(counters, `insight_${level}_rows_synced`, baseRows.length);
    inc(counters, "video_metric_rows_synced", videoRows.length);
    inc(counters, "action_metric_rows_synced", actionRows.length);
  }
}

async function syncBreakdownsForAccount(
  owner_id: string,
  account_id: string,
  userAccessToken: string,
  counters: SyncCounters,
  since: string,
  until: string,
  breakdowns: string[]
) {
  const accountAct = act(account_id);
  const levels = ["campaign", "adset", "ad"];

  for (const level of levels) {
    for (const breakdownType of breakdowns) {
      await deleteMetricRange("meta_ad_breakdown_metrics_daily", {
        owner_id,
        account_id,
        level,
        since,
        until,
        breakdown_type: breakdownType,
      });

      const rows = await graphGetAllSafe(
        `breakdown_${level}_${breakdownType}_${account_id}`,
        `${accountAct}/insights`,
        userAccessToken,
        {
          level,
          fields: BREAKDOWN_INSIGHT_FIELDS,
          breakdowns: breakdownType,
          time_range: JSON.stringify({ since, until }),
          time_increment: 1,
          limit: 500,
        },
        30
      );

      const mapped = rows.map((r) => mapBreakdownInsight(owner_id, level, account_id, breakdownType, r));
      await supabaseInsert("meta_ad_breakdown_metrics_daily", mapped);
      inc(counters, "breakdown_metric_rows_synced", mapped.length);
    }
  }
}




// =========================================================
// ROUTES - BASIC
// =========================================================

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "vyrexads-merged-marketing-server",
    graph_version: META_GRAPH_VERSION,
    time: new Date().toISOString(),
  });
});

// =========================================================
// ROUTES - META OAUTH
// =========================================================

app.get("/auth/facebook/start", async (req, res) => {
  try {
    if (!META_APP_ID) throw new Error("Missing META_APP_ID / FACEBOOK_APP_ID");

    const owner_id = String(req.query.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const return_to = String(req.query.return_to || FRONTEND_RETURN_URL);
    const state = signState({ owner_id, return_to, ts: Date.now(), provider: "facebook" });

    const authUrl = new URL(`https://www.facebook.com/${META_GRAPH_VERSION}/dialog/oauth`);
    authUrl.searchParams.set("client_id", META_APP_ID);
    authUrl.searchParams.set("redirect_uri", META_OAUTH_REDIRECT_URI);
    authUrl.searchParams.set("state", state);
    authUrl.searchParams.set("scope", META_OAUTH_SCOPES);
    authUrl.searchParams.set("response_type", "code");

    return res.redirect(authUrl.toString());
  } catch (e: any) {
    return res.status(500).json({ error: "Facebook OAuth start failed", details: e?.message || String(e) });
  }
});

app.get("/auth/facebook/callback", async (req, res) => {
  let return_to = FRONTEND_RETURN_URL;

  try {
    if (!META_APP_ID || !META_APP_SECRET) {
      throw new Error("Missing META_APP_ID / META_APP_SECRET");
    }

    const code = String(req.query.code || "");
    const stateRaw = String(req.query.state || "");
    const state = verifyState(stateRaw);

    if (!code) throw new Error("Missing OAuth code");
    if (!state?.owner_id) throw new Error("Invalid OAuth state");

    const owner_id = String(state.owner_id);
    return_to = String(state.return_to || FRONTEND_RETURN_URL);

    const shortToken = await graphGet("oauth/access_token", "", {
      client_id: META_APP_ID,
      client_secret: META_APP_SECRET,
      redirect_uri: META_OAUTH_REDIRECT_URI,
      code,
    });

    let activeToken = shortToken;

    const longToken = await graphGetSafe("long_lived_exchange", "oauth/access_token", "", {
      grant_type: "fb_exchange_token",
      client_id: META_APP_ID,
      client_secret: META_APP_SECRET,
      fb_exchange_token: shortToken.access_token,
    });

    if (longToken?.access_token) activeToken = longToken;

    const accessToken = activeToken.access_token;
    const me = await graphGetSafe("me", "me", accessToken, { fields: "id,name,email" });

    const expiresAt = activeToken.expires_in
      ? new Date(Date.now() + Number(activeToken.expires_in) * 1000).toISOString()
      : null;

    await upsertProviderTokenVault({
      owner_id,
      provider: "facebook",
      token: {
        provider: "facebook",
        access_token: accessToken,
        token_type: activeToken.token_type || shortToken.token_type || "bearer",
        expires_in: activeToken.expires_in ?? shortToken.expires_in ?? null,
        expires_at: expiresAt,
        scopes: META_OAUTH_SCOPES.split(",").map((s) => s.trim()).filter(Boolean),
        user: me,
        short_lived: shortToken,
        long_lived: longToken,
        stored_at: new Date().toISOString(),
      },
    });

    const u = new URL(return_to);
    u.searchParams.set("facebook", "connected");
    if (me?.id) u.searchParams.set("facebook_user_id", String(me.id));
    return res.redirect(u.toString());
  } catch (e: any) {
    const u = new URL(return_to);
    u.searchParams.set("facebook", "error");
    u.searchParams.set("message", e?.message || String(e));
    return res.redirect(u.toString());
  }
});

// =========================================================
// ROUTES - DEBUG
// =========================================================

app.post(["/api/meta/debug/me", "/api/facebook/debug/me"], requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const token = await getMetaUserAccessToken(owner_id);
    const me = await graphGet("me", token, { fields: "id,name,email" });

    return res.json({ ok: true, provider: "facebook", me });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post(["/api/meta/debug/ad-accounts", "/api/facebook/debug/ad-accounts"], requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const token = await getMetaUserAccessToken(owner_id);
    const accounts = await graphGetAll("me/adaccounts", token, { fields: AD_ACCOUNT_FIELDS, limit: 100 });

    return res.json({ ok: true, count: accounts.length, accounts });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// =========================================================
// ROUTES - SYNC SEPARATED
// =========================================================

app.post(["/api/meta/sync/pages", "/api/facebook/sync-pages"], requireAuth, async (req, res) => {
  try {
    const body = req.body as SyncRequestBody;
    const owner_id = String(body.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const counters: SyncCounters = {};
    const token = await getMetaUserAccessToken(owner_id);
    const pages = await syncPages(owner_id, token, counters, body.page_ids);

    return res.json({ ok: true, pages_found: pages.length, ...counters });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post(["/api/meta/sync/organic", "/api/facebook/sync-organic-all", "/api/facebook/sync-page-posts"], requireAuth, async (req, res) => {
  try {
    const body = req.body as SyncRequestBody;
    const owner_id = String(body.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const counters: SyncCounters = {};
    const token = await getMetaUserAccessToken(owner_id);
    const pages = await syncPages(owner_id, token, counters, body.page_ids);
    await syncOrganicForPages(owner_id, pages, counters);

    return res.json({ ok: true, pages_found: pages.length, ...counters });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post(["/api/meta/sync/ad-accounts", "/api/facebook/sync-ad-accounts"], requireAuth, async (req, res) => {
  try {
    const body = req.body as SyncRequestBody;
    const owner_id = String(body.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const counters: SyncCounters = {};
    const token = await getMetaUserAccessToken(owner_id);
    const accountIds = await syncAdAccounts(owner_id, token, counters, body.account_ids?.map(normalizeAccountId));

    return res.json({ ok: true, account_ids: accountIds, ...counters });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post(["/api/meta/sync/ads-metadata", "/api/facebook/sync-ad-campaigns"], requireAuth, async (req, res) => {
  try {
    const body = req.body as SyncRequestBody;
    const owner_id = String(body.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const counters: SyncCounters = {};
    const token = await getMetaUserAccessToken(owner_id);
    const accountIds = await syncAdAccounts(owner_id, token, counters, body.account_ids?.map(normalizeAccountId));

    for (const account_id of accountIds) {
      await syncAdsMetadataForAccount(owner_id, account_id, token, counters);
      await syncAudiencesForAccount(owner_id, account_id, token, counters);
      await syncPixelsAndConversionsForAccount(owner_id, account_id, token, counters);
    }

    return res.json({ ok: true, account_ids: accountIds, ...counters });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post(["/api/meta/sync/ad-metrics", "/api/facebook/sync-ad-campaign-metrics"], requireAuth, async (req, res) => {
  try {
    const body = req.body as SyncRequestBody;
    const owner_id = String(body.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const since = parseDateOrFallback(body.since, daysAgoISODate(30));
    const until = parseDateOrFallback(body.until, todayISODate());

    const counters: SyncCounters = {};
    const token = await getMetaUserAccessToken(owner_id);
    const accountIds = await syncAdAccounts(owner_id, token, counters, body.account_ids?.map(normalizeAccountId));

    for (const account_id of accountIds) {
      await syncInsightsForAccount(owner_id, account_id, token, counters, since, until);
    }

    return res.json({ ok: true, since, until, account_ids: accountIds, ...counters });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/meta/sync/breakdowns", requireAuth, async (req, res) => {
  try {
    const body = req.body as SyncRequestBody;
    const owner_id = String(body.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const since = parseDateOrFallback(body.since, daysAgoISODate(30));
    const until = parseDateOrFallback(body.until, todayISODate());
    const breakdowns = body.breakdowns?.length ? body.breakdowns : DEFAULT_BREAKDOWNS;

    const counters: SyncCounters = {};
    const token = await getMetaUserAccessToken(owner_id);
    const accountIds = await syncAdAccounts(owner_id, token, counters, body.account_ids?.map(normalizeAccountId));

    for (const account_id of accountIds) {
      await syncBreakdownsForAccount(owner_id, account_id, token, counters, since, until, breakdowns);
    }

    return res.json({ ok: true, since, until, breakdowns, account_ids: accountIds, ...counters });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/meta/sync/leads", requireAuth, async (req, res) => {
  try {
    const body = req.body as SyncRequestBody;
    const owner_id = String(body.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const counters: SyncCounters = {};
    const token = await getMetaUserAccessToken(owner_id);
    const pages = await syncPages(owner_id, token, counters, body.page_ids);
    await syncLeadFormsAndLeads(owner_id, pages, counters);

    return res.json({ ok: true, pages_found: pages.length, ...counters });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/meta/sync/catalogs", requireAuth, async (req, res) => {
  try {
    const body = req.body as SyncRequestBody;
    const owner_id = String(body.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const counters: SyncCounters = {};
    const token = await getMetaUserAccessToken(owner_id);
    await syncBusinessesAndCatalogs(owner_id, token, counters, true);

    return res.json({ ok: true, ...counters });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// =========================================================
// ROUTE - FULL SYNC
// =========================================================

app.post(["/api/meta/sync/all", "/api/facebook/sync-ads-all"], requireAuth, async (req, res) => {
  try {
    const body = req.body as SyncRequestBody;
    const owner_id = String(body.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });

    const since = parseDateOrFallback(body.since, daysAgoISODate(30));
    const until = parseDateOrFallback(body.until, todayISODate());
    const includeMetrics = body.include_metrics !== false;
    const includeBreakdowns = body.include_breakdowns === true;
    const includeOrganic = body.include_organic !== false;
    const includeLeads = body.include_leads === true;
    const includeCatalogs = body.include_catalogs === true;
    const breakdowns = body.breakdowns?.length ? body.breakdowns : DEFAULT_BREAKDOWNS;

    const counters: SyncCounters = {};
    const token = await getMetaUserAccessToken(owner_id);

    const pages = await syncPages(owner_id, token, counters, body.page_ids);
    await syncBusinessesAndCatalogs(owner_id, token, counters, includeCatalogs);

    if (includeOrganic) {
      await syncOrganicForPages(owner_id, pages, counters);
    }

    if (includeLeads) {
      await syncLeadFormsAndLeads(owner_id, pages, counters);
    }

    const accountIds = await syncAdAccounts(owner_id, token, counters, body.account_ids?.map(normalizeAccountId));

    for (const account_id of accountIds) {
      await syncAdsMetadataForAccount(owner_id, account_id, token, counters);
      await syncAudiencesForAccount(owner_id, account_id, token, counters);
      await syncPixelsAndConversionsForAccount(owner_id, account_id, token, counters);

      if (includeMetrics) {
        await syncInsightsForAccount(owner_id, account_id, token, counters, since, until);
      }

      if (includeBreakdowns) {
        await syncBreakdownsForAccount(owner_id, account_id, token, counters, since, until, breakdowns);
      }
    }

    return res.json({
      ok: true,
      owner_id,
      since,
      until,
      account_ids: accountIds,
      pages_found: pages.length,
      include_metrics: includeMetrics,
      include_breakdowns: includeBreakdowns,
      include_organic: includeOrganic,
      include_leads: includeLeads,
      include_catalogs: includeCatalogs,
      ...counters,
    });
  } catch (e: any) {
    console.error("[meta][sync/all] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});






// =========================================================
// TIKTOK DISPLAY API SYNC - DB2
// Remplit :
// - tiktok_profiles
// - tiktok_profile_metric_snapshots
// - tiktok_videos
// - tiktok_video_metric_snapshots
// - tiktok_sync_state
// - tiktok_sync_runs
// =========================================================

const TIKTOK_CLIENT_KEY =
  process.env.TIKTOK_CLIENT_KEY ||
  process.env.TIKTOK_CLIENT_ID ||
  "";

const TIKTOK_CLIENT_SECRET =
  process.env.TIKTOK_CLIENT_SECRET ||
  "";

const TIKTOK_OAUTH_REDIRECT_URI =
  process.env.TIKTOK_OAUTH_REDIRECT_URI ||
  "https://vyrexads-backend.onrender.com/auth/tiktok/callback";

const TIKTOK_OAUTH_SCOPES =
  process.env.TIKTOK_OAUTH_SCOPES ||
  ["user.info.basic", "user.info.profile", "user.info.stats", "video.list"].join(",");


const TIKTOK_API_BASE = "https://open.tiktokapis.com";

const TIKTOK_USER_FIELDS_SAFE = [
  "open_id",
  "union_id",
  "avatar_url",
  "avatar_url_100",
  "avatar_large_url",
  "display_name",
  "bio_description",
  "profile_deep_link",
  "is_verified",
  "username",
  "follower_count",
  "following_count",
  "likes_count",
  "video_count",
].join(",");

// On tente profile_web_link, mais si TikTok refuse ce champ,
// on retry automatiquement avec la liste safe.
const TIKTOK_USER_FIELDS_FULL = [
  "open_id",
  "union_id",
  "avatar_url",
  "avatar_url_100",
  "avatar_large_url",
  "display_name",
  "bio_description",
  "profile_deep_link",
  "profile_web_link",
  "is_verified",
  "username",
  "follower_count",
  "following_count",
  "likes_count",
  "video_count",
].join(",");

const TIKTOK_VIDEO_FIELDS = [
  "id",
  "create_time",
  "cover_image_url",
  "share_url",
  "video_description",
  "duration",
  "height",
  "width",
  "title",
  "embed_html",
  "embed_link",
  "like_count",
  "comment_count",
  "share_count",
  "view_count",
].join(",");

type TikTokStoredToken = {
  owner_id?: string;
  provider?: string;
  open_id?: string | null;
  union_id?: string | null;
  scope?: string[] | string;
  scopes?: string[];
  access_token?: string;
  refresh_token?: string | null;
  token_type?: string | null;
  access_token_expires_at?: string | null;
  expires_at?: string | null;
  refresh_token_expires_at?: string | null;
  raw_token?: any;
  stored_at?: string;
  updated_at?: string;
};

type TikTokSyncOptions = {
  cursor?: number | null;
  max_count?: number;
  max_pages?: number;
};

function normalizeTikTokToken(raw: any, owner_id: string): TikTokStoredToken {
  let parsed = raw;

  if (typeof parsed === "string") {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      throw new Error("TikTok token stocké invalide : JSON non parsable");
    }
  }

  if (parsed?.decrypted_secret) {
    parsed = parsed.decrypted_secret;
  }

  if (!parsed || typeof parsed !== "object") {
    throw new Error("TikTok token stocké invalide");
  }

  const accessToken =
    parsed.access_token ||
    parsed.raw_token?.access_token ||
    parsed.short_lived?.access_token ||
    parsed.long_lived?.access_token;

  if (!accessToken || typeof accessToken !== "string") {
    throw new Error("TikTok access_token introuvable dans provider_tokens");
  }

  return {
    ...parsed,
    owner_id,
    provider: "tiktok",
    access_token: accessToken,
    refresh_token: parsed.refresh_token ?? parsed.raw_token?.refresh_token ?? null,
    access_token_expires_at:
      parsed.access_token_expires_at ??
      parsed.expires_at ??
      null,
    refresh_token_expires_at:
      parsed.refresh_token_expires_at ??
      null,
  };
}

function parseTikTokScopes(value: any): string[] {
  if (Array.isArray(value)) {
    return value.map(String).map((s) => s.trim()).filter(Boolean);
  }

  if (typeof value === "string") {
    return value
      .split(/[,\s]+/)
      .map((s) => s.trim())
      .filter(Boolean);
  }

  return [];
}

function isTikTokTokenExpiringSoon(value: any, minutes = 10) {
  if (!value || typeof value !== "string") return false;

  const expiresAt = new Date(value).getTime();
  if (!Number.isFinite(expiresAt)) return false;

  return expiresAt - Date.now() <= minutes * 60 * 1000;
}

function extractTikTokOAuthPayload(raw: any) {
  return raw?.data && typeof raw.data === "object" ? raw.data : raw;
}

function tiktokErrorMessage(json: any) {
  const err = json?.error;
  if (!err) return null;

  const code = err?.code;
  const message = err?.message;

  if (!code || code === "ok") return null;

  return `${code}${message ? `: ${message}` : ""}`;
}

function toTikTokIsoFromCreateTime(value: any) {
  const n = toNumber(value, null);
  if (n === null) return null;
  return new Date(n * 1000).toISOString();
}

function safeRate(delta: number | null, previous: number | null) {
  if (delta === null || previous === null || previous <= 0) return null;
  return delta / previous;
}

async function getTikTokStoredToken(owner_id: string) {
  const token = await getProviderToken(owner_id, "tiktok");
  return normalizeTikTokToken(token, owner_id);
}

async function refreshTikTokTokenIfNeeded(owner_id: string, token: TikTokStoredToken) {
  if (!isTikTokTokenExpiringSoon(token.access_token_expires_at || token.expires_at, 10)) {
    return token;
  }

  if (!token.refresh_token) {
    throw new Error("TikTok refresh_token manquant");
  }

  if (!TIKTOK_CLIENT_KEY || !TIKTOK_CLIENT_SECRET) {
    throw new Error("Missing TIKTOK_CLIENT_KEY or TIKTOK_CLIENT_SECRET");
  }

  const res = await fetch(`${TIKTOK_API_BASE}/v2/oauth/token/`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: new URLSearchParams({
      client_key: TIKTOK_CLIENT_KEY,
      client_secret: TIKTOK_CLIENT_SECRET,
      grant_type: "refresh_token",
      refresh_token: token.refresh_token,
    }),
  });

  const text = await res.text();
  let json: any = null;

  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  const payload = extractTikTokOAuthPayload(json);

  if (!res.ok || json?.error || payload?.error) {
    throw new Error(`TikTok refresh failed: ${res.status} ${text || "unknown error"}`);
  }

  const nextAccessToken = String(payload?.access_token || "");
  if (!nextAccessToken) {
    throw new Error("TikTok refresh failed: access_token manquant");
  }

  const nextRefreshToken = payload?.refresh_token
    ? String(payload.refresh_token)
    : token.refresh_token ?? null;

  const accessTokenExpiresAt = payload?.expires_in
    ? new Date(Date.now() + Number(payload.expires_in) * 1000).toISOString()
    : token.access_token_expires_at ?? token.expires_at ?? null;

  const refreshTokenExpiresAt = payload?.refresh_expires_in
    ? new Date(Date.now() + Number(payload.refresh_expires_in) * 1000).toISOString()
    : token.refresh_token_expires_at ?? null;

  const updatedToken: TikTokStoredToken = {
    ...token,
    owner_id,
    provider: "tiktok",
    access_token: nextAccessToken,
    refresh_token: nextRefreshToken,
    token_type: payload?.token_type || token.token_type || "Bearer",
    scope: payload?.scope ?? token.scope ?? token.scopes ?? [],
    scopes: parseTikTokScopes(payload?.scope ?? token.scope ?? token.scopes),
    access_token_expires_at: accessTokenExpiresAt,
    expires_at: accessTokenExpiresAt,
    refresh_token_expires_at: refreshTokenExpiresAt,
    raw_token: {
      ...(token.raw_token ?? {}),
      refresh_response: json ?? {},
    },
    updated_at: new Date().toISOString(),
  };

  await upsertProviderTokenVault({
    owner_id,
    provider: "tiktok",
    token: updatedToken as Json,
  });

  return updatedToken;
}

async function getFreshTikTokToken(owner_id: string) {
  const token = await getTikTokStoredToken(owner_id);
  return await refreshTikTokTokenIfNeeded(owner_id, token);
}

async function tiktokApiRequest(params: {
  method: "GET" | "POST";
  path: string;
  access_token: string;
  fields: string;
  body?: any;
}) {
  const url = new URL(`${TIKTOK_API_BASE}${params.path}`);
  url.searchParams.set("fields", params.fields);

  const res = await fetch(url.toString(), {
    method: params.method,
    headers: {
      Authorization: `Bearer ${params.access_token}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: params.method === "POST"
      ? JSON.stringify(params.body ?? {})
      : undefined,
  });

  const text = await res.text();
  let json: any = null;

  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  const apiError = tiktokErrorMessage(json);

  if (!res.ok || apiError) {
    throw new Error(`TikTok API ${params.path} failed: ${res.status} ${apiError || text}`);
  }

  return json;
}

async function fetchTikTokUserInfo(access_token: string) {
  try {
    const json = await tiktokApiRequest({
      method: "GET",
      path: "/v2/user/info/",
      access_token,
      fields: TIKTOK_USER_FIELDS_FULL,
    });

    return json?.data?.user ?? {};
  } catch (e: any) {
    const msg = e?.message || String(e);

    if (!msg.includes("profile_web_link")) {
      throw e;
    }

    const json = await tiktokApiRequest({
      method: "GET",
      path: "/v2/user/info/",
      access_token,
      fields: TIKTOK_USER_FIELDS_SAFE,
    });

    return json?.data?.user ?? {};
  }
}

async function fetchTikTokVideoList(access_token: string, options: TikTokSyncOptions) {
  const maxCount = Math.min(Math.max(Number(options.max_count || 20), 1), 20);

  const body: any = {
    max_count: maxCount,
  };

  if (options.cursor !== undefined && options.cursor !== null) {
    body.cursor = options.cursor;
  }

  const json = await tiktokApiRequest({
    method: "POST",
    path: "/v2/video/list/",
    access_token,
    fields: TIKTOK_VIDEO_FIELDS,
    body,
  });

  return {
    videos: Array.isArray(json?.data?.videos) ? json.data.videos : [],
    cursor: json?.data?.cursor ?? null,
    has_more: Boolean(json?.data?.has_more),
    raw: json,
  };
}

async function fetchTikTokVideoQuery(access_token: string, video_ids: string[]) {
  const cleanIds = [...new Set(video_ids.map(String).map((s) => s.trim()).filter(Boolean))];

  if (cleanIds.length === 0) {
    return {
      videos: [],
      raw: {},
    };
  }

  if (cleanIds.length > 20) {
    throw new Error("TikTok video/query accepte maximum 20 video_ids par requête");
  }

  const json = await tiktokApiRequest({
    method: "POST",
    path: "/v2/video/query/",
    access_token,
    fields: TIKTOK_VIDEO_FIELDS,
    body: {
      filters: {
        video_ids: cleanIds,
      },
    },
  });

  return {
    videos: Array.isArray(json?.data?.videos) ? json.data.videos : [],
    raw: json,
  };
}

async function selectTikTokProfile(owner_id: string, open_id: string) {
  const q = new URLSearchParams();
  q.set("select", "id,owner_id,open_id,display_name,username,last_synced_at");
  q.set("owner_id", `eq.${owner_id}`);
  q.set("open_id", `eq.${open_id}`);
  q.set("limit", "1");

  const rows = await supabaseSelect<any>("tiktok_profiles", q);
  return rows[0] || null;
}

async function selectLatestTikTokProfileSnapshot(owner_id: string, open_id: string) {
  const q = new URLSearchParams();
  q.set("select", "follower_count,following_count,likes_count,video_count,snapshot_at");
  q.set("owner_id", `eq.${owner_id}`);
  q.set("tiktok_open_id", `eq.${open_id}`);
  q.set("order", "snapshot_at.desc");
  q.set("limit", "1");

  const rows = await supabaseSelect<any>("tiktok_profile_metric_snapshots", q);
  return rows[0] || null;
}

async function selectLatestTikTokVideoSnapshot(owner_id: string, video_id: string) {
  const q = new URLSearchParams();
  q.set("select", "view_count,like_count,comment_count,share_count,engagement_total,snapshot_at");
  q.set("owner_id", `eq.${owner_id}`);
  q.set("video_id", `eq.${video_id}`);
  q.set("order", "snapshot_at.desc");
  q.set("limit", "1");

  const rows = await supabaseSelect<any>("tiktok_video_metric_snapshots", q);
  return rows[0] || null;
}

async function selectTikTokVideoIdMap(owner_id: string, video_ids: string[]) {
  const cleanIds = [...new Set(video_ids.map(String).filter(Boolean))];

  if (cleanIds.length === 0) return new Map<string, string>();

  const q = new URLSearchParams();
  q.set("select", "id,video_id");
  q.set("owner_id", `eq.${owner_id}`);
  q.set(
    "video_id",
    `in.(${cleanIds.map((id) => `"${id.replace(/"/g, "")}"`).join(",")})`
  );

  const rows = await supabaseSelect<any>("tiktok_videos", q);

  return new Map<string, string>(
    rows
      .filter((r) => r?.video_id && r?.id)
      .map((r) => [String(r.video_id), String(r.id)])
  );
}

function mapTikTokProfileRow(owner_id: string, user: any) {
  return {
    owner_id,
    provider: "tiktok",
    open_id: String(user.open_id || ""),
    union_id: user.union_id ?? null,
    username: user.username ?? null,
    display_name: user.display_name ?? null,

    avatar_url: user.avatar_url ?? null,
    avatar_url_100: user.avatar_url_100 ?? null,
    avatar_large_url: user.avatar_large_url ?? null,

    bio_description: user.bio_description ?? null,
    profile_deep_link: user.profile_deep_link ?? null,
    profile_web_link: user.profile_web_link ?? null,
    is_verified: typeof user.is_verified === "boolean" ? user.is_verified : null,

    follower_count: toBigIntNumber(user.follower_count, 0),
    following_count: toBigIntNumber(user.following_count, 0),
    likes_count: toBigIntNumber(user.likes_count, 0),
    video_count: toBigIntNumber(user.video_count, 0),

    last_synced_at: new Date().toISOString(),
    raw: user,
  };
}

function mapTikTokVideoRow(params: {
  owner_id: string;
  profile_id: string | null;
  open_id: string;
  video: any;
  sync_source: "video_list" | "video_query";
}) {
  const v = params.video;
  const now = new Date().toISOString();

  return {
    owner_id: params.owner_id,
    profile_id: params.profile_id,
    provider: "tiktok",
    tiktok_open_id: params.open_id,

    video_id: String(v.id || ""),

    create_time: toBigIntNumber(v.create_time, 0) || null,
    published_at: toTikTokIsoFromCreateTime(v.create_time),

    title: v.title ?? null,
    video_description: v.video_description ?? null,
    duration: toNumber(v.duration, null),
    width: toNumber(v.width, null),
    height: toNumber(v.height, null),

    cover_image_url: v.cover_image_url ?? null,
    cover_image_fetched_at: v.cover_image_url ? now : null,
    cover_image_expires_at: v.cover_image_url
      ? new Date(Date.now() + 6 * 60 * 60 * 1000).toISOString()
      : null,

    share_url: v.share_url ?? null,
    embed_link: v.embed_link ?? null,
    embed_html: v.embed_html ?? null,

    view_count: toBigIntNumber(v.view_count, 0),
    like_count: toBigIntNumber(v.like_count, 0),
    comment_count: toBigIntNumber(v.comment_count, 0),
    share_count: toBigIntNumber(v.share_count, 0),

    sync_source: params.sync_source,
    last_synced_at: now,
    raw: v,
  };
}

async function insertTikTokProfileSnapshot(params: {
  owner_id: string;
  profile_id: string | null;
  open_id: string;
  user: any;
  raw?: any;
}) {
  const previous = await selectLatestTikTokProfileSnapshot(params.owner_id, params.open_id);

  const followerCount = toBigIntNumber(params.user.follower_count, 0);
  const followingCount = toBigIntNumber(params.user.following_count, 0);
  const likesCount = toBigIntNumber(params.user.likes_count, 0);
  const videoCount = toBigIntNumber(params.user.video_count, 0);

  const followerDelta = previous ? followerCount - toBigIntNumber(previous.follower_count, 0) : null;
  const followingDelta = previous ? followingCount - toBigIntNumber(previous.following_count, 0) : null;
  const likesDelta = previous ? likesCount - toBigIntNumber(previous.likes_count, 0) : null;
  const videoDelta = previous ? videoCount - toBigIntNumber(previous.video_count, 0) : null;

  await supabaseInsert("tiktok_profile_metric_snapshots", [
    {
      owner_id: params.owner_id,
      profile_id: params.profile_id,
      provider: "tiktok",
      tiktok_open_id: params.open_id,

      follower_count: followerCount,
      following_count: followingCount,
      likes_count: likesCount,
      video_count: videoCount,

      follower_delta: followerDelta,
      following_delta: followingDelta,
      likes_delta: likesDelta,
      video_delta: videoDelta,

      follower_growth_rate: safeRate(
        followerDelta,
        previous ? toBigIntNumber(previous.follower_count, 0) : null
      ),
      likes_growth_rate: safeRate(
        likesDelta,
        previous ? toBigIntNumber(previous.likes_count, 0) : null
      ),

      raw: params.raw ?? params.user ?? {},
    },
  ]);
}

async function insertTikTokVideoSnapshots(params: {
  owner_id: string;
  open_id: string;
  videos: any[];
  videoIdMap: Map<string, string>;
  raw?: any;
}) {
  const rows: any[] = [];

  for (const video of params.videos) {
    const videoId = String(video.id || "");
    if (!videoId) continue;

    const previous = await selectLatestTikTokVideoSnapshot(params.owner_id, videoId);

    const viewCount = toBigIntNumber(video.view_count, 0);
    const likeCount = toBigIntNumber(video.like_count, 0);
    const commentCount = toBigIntNumber(video.comment_count, 0);
    const shareCount = toBigIntNumber(video.share_count, 0);
    const engagementTotal = likeCount + commentCount + shareCount;

    const viewsDelta = previous ? viewCount - toBigIntNumber(previous.view_count, 0) : null;
    const likesDelta = previous ? likeCount - toBigIntNumber(previous.like_count, 0) : null;
    const commentsDelta = previous ? commentCount - toBigIntNumber(previous.comment_count, 0) : null;
    const sharesDelta = previous ? shareCount - toBigIntNumber(previous.share_count, 0) : null;
    const engagementDelta = previous
      ? engagementTotal - toBigIntNumber(previous.engagement_total, 0)
      : null;

    rows.push({
      owner_id: params.owner_id,
      video_table_id: params.videoIdMap.get(videoId) ?? null,
      provider: "tiktok",
      tiktok_open_id: params.open_id,
      video_id: videoId,

      view_count: viewCount,
      like_count: likeCount,
      comment_count: commentCount,
      share_count: shareCount,

      views_delta: viewsDelta,
      likes_delta: likesDelta,
      comments_delta: commentsDelta,
      shares_delta: sharesDelta,
      engagement_delta: engagementDelta,

      raw: video,
    });
  }

  return await supabaseInsert("tiktok_video_metric_snapshots", rows);
}

async function upsertTikTokSyncState(params: {
  owner_id: string;
  profile_id?: string | null;
  open_id: string;
  last_profile_sync_at?: string | null;
  last_video_list_sync_at?: string | null;
  last_video_query_sync_at?: string | null;
  last_full_sync_at?: string | null;
  last_cursor?: number | null;
  has_more?: boolean;
  last_error?: any;
  raw?: any;
}) {
  await supabaseUpsert(
    "tiktok_sync_state",
    [
      {
        owner_id: params.owner_id,
        profile_id: params.profile_id ?? null,
        provider: "tiktok",
        tiktok_open_id: params.open_id,

        last_profile_sync_at: params.last_profile_sync_at ?? undefined,
        last_video_list_sync_at: params.last_video_list_sync_at ?? undefined,
        last_video_query_sync_at: params.last_video_query_sync_at ?? undefined,
        last_full_sync_at: params.last_full_sync_at ?? undefined,

        last_cursor: params.last_cursor ?? null,
        has_more: params.has_more ?? false,

        last_error: params.last_error ?? {},
        raw: params.raw ?? {},
      },
    ],
    "owner_id,provider"
  );
}

async function insertTikTokSyncRun(params: {
  owner_id: string;
  profile_id?: string | null;
  open_id?: string | null;
  sync_type: "profile" | "video_list" | "video_query" | "full" | "cover_refresh";
  status: "success" | "partial_success" | "error";
  started_at: string;
  cursor_before?: number | null;
  cursor_after?: number | null;
  has_more?: boolean | null;
  videos_found?: number;
  videos_upserted?: number;
  profile_upserted?: boolean;
  video_snapshots_created?: number;
  profile_snapshots_created?: number;
  error?: any;
  raw?: any;
}) {
  await supabaseInsert("tiktok_sync_runs", [
    {
      owner_id: params.owner_id,
      profile_id: params.profile_id ?? null,
      provider: "tiktok",
      tiktok_open_id: params.open_id ?? null,

      sync_type: params.sync_type,
      status: params.status,
      started_at: params.started_at,
      finished_at: new Date().toISOString(),

      cursor_before: params.cursor_before ?? null,
      cursor_after: params.cursor_after ?? null,
      has_more: params.has_more ?? null,

      videos_found: params.videos_found ?? 0,
      videos_upserted: params.videos_upserted ?? 0,
      profile_upserted: params.profile_upserted ?? false,
      video_snapshots_created: params.video_snapshots_created ?? 0,
      profile_snapshots_created: params.profile_snapshots_created ?? 0,

      error: params.error ?? {},
      raw: params.raw ?? {},
    },
  ]);
}

async function syncTikTokProfileToDb(owner_id: string) {
  const startedAt = new Date().toISOString();

  try {
    const token = await getFreshTikTokToken(owner_id);
    const user = await fetchTikTokUserInfo(String(token.access_token));

    if (!user?.open_id) {
      throw new Error("TikTok user/info n'a pas renvoyé open_id");
    }

    const profileRow = mapTikTokProfileRow(owner_id, user);

    await supabaseUpsert(
      "tiktok_profiles",
      [profileRow],
      "owner_id,provider"
    );

    const profile = await selectTikTokProfile(owner_id, String(user.open_id));

    await insertTikTokProfileSnapshot({
      owner_id,
      profile_id: profile?.id ?? null,
      open_id: String(user.open_id),
      user,
      raw: user,
    });

    const now = new Date().toISOString();

    await upsertTikTokSyncState({
      owner_id,
      profile_id: profile?.id ?? null,
      open_id: String(user.open_id),
      last_profile_sync_at: now,
      last_error: {},
      raw: {
        source: "user_info",
      },
    });

    await insertTikTokSyncRun({
      owner_id,
      profile_id: profile?.id ?? null,
      open_id: String(user.open_id),
      sync_type: "profile",
      status: "success",
      started_at: startedAt,
      profile_upserted: true,
      profile_snapshots_created: 1,
      raw: {
        user,
      },
    });

    return {
      profile,
      user,
    };
  } catch (e: any) {
    await insertTikTokSyncRun({
      owner_id,
      sync_type: "profile",
      status: "error",
      started_at: startedAt,
      error: {
        message: e?.message || String(e),
      },
    });

    throw e;
  }
}

async function syncTikTokVideosToDb(owner_id: string, options: TikTokSyncOptions = {}) {
  const startedAt = new Date().toISOString();

  let cursor = options.cursor ?? null;
  const maxPages = Math.min(Math.max(Number(options.max_pages || 5), 1), 50);
  const maxCount = Math.min(Math.max(Number(options.max_count || 20), 1), 20);

  let hasMore = false;
  let cursorAfter: number | null = null;
  let totalVideosFound = 0;
  let totalVideosUpserted = 0;
  let totalSnapshotsCreated = 0;

  try {
    const token = await getFreshTikTokToken(owner_id);

    const profileSync = await syncTikTokProfileToDb(owner_id);
    const openId = String(profileSync.user.open_id);
    const profileId = profileSync.profile?.id ?? null;

    for (let page = 0; page < maxPages; page++) {
      const result = await fetchTikTokVideoList(String(token.access_token), {
        cursor,
        max_count: maxCount,
      });

      const videos = result.videos;
      hasMore = result.has_more;
      cursorAfter = result.cursor;

      totalVideosFound += videos.length;

      const videoRows = videos
        .filter((v: any) => v?.id)
        .map((video: any) =>
          mapTikTokVideoRow({
            owner_id,
            profile_id: profileId,
            open_id: openId,
            video,
            sync_source: "video_list",
          })
        );

      const upserted = await supabaseUpsert(
        "tiktok_videos",
        videoRows,
        "owner_id,video_id"
      );

      totalVideosUpserted += upserted;

      const videoIds = videos.map((v: any) => String(v.id || "")).filter(Boolean);
      const videoIdMap = await selectTikTokVideoIdMap(owner_id, videoIds);

      totalSnapshotsCreated += await insertTikTokVideoSnapshots({
        owner_id,
        open_id: openId,
        videos,
        videoIdMap,
        raw: result.raw,
      });

      cursor = result.cursor;

      if (!result.has_more || !cursor) {
        break;
      }
    }

    const now = new Date().toISOString();

    await upsertTikTokSyncState({
      owner_id,
      profile_id: profileId,
      open_id: openId,
      last_video_list_sync_at: now,
      last_full_sync_at: now,
      last_cursor: cursorAfter,
      has_more: hasMore,
      last_error: {},
      raw: {
        max_pages: maxPages,
        max_count: maxCount,
      },
    });

    await insertTikTokSyncRun({
      owner_id,
      profile_id: profileId,
      open_id: openId,
      sync_type: "video_list",
      status: "success",
      started_at: startedAt,
      cursor_before: options.cursor ?? null,
      cursor_after: cursorAfter,
      has_more: hasMore,
      videos_found: totalVideosFound,
      videos_upserted: totalVideosUpserted,
      video_snapshots_created: totalSnapshotsCreated,
      raw: {
        max_pages: maxPages,
        max_count: maxCount,
      },
    });

    return {
      open_id: openId,
      profile_id: profileId,
      videos_found: totalVideosFound,
      videos_upserted: totalVideosUpserted,
      video_snapshots_created: totalSnapshotsCreated,
      cursor: cursorAfter,
      has_more: hasMore,
    };
  } catch (e: any) {
    await insertTikTokSyncRun({
      owner_id,
      sync_type: "video_list",
      status: "error",
      started_at: startedAt,
      error: {
        message: e?.message || String(e),
      },
    });

    throw e;
  }
}

async function syncTikTokVideoQueryToDb(owner_id: string, video_ids: string[]) {
  const startedAt = new Date().toISOString();

  try {
    const token = await getFreshTikTokToken(owner_id);

    const profileSync = await syncTikTokProfileToDb(owner_id);
    const openId = String(profileSync.user.open_id);
    const profileId = profileSync.profile?.id ?? null;

    const result = await fetchTikTokVideoQuery(String(token.access_token), video_ids);
    const videos = result.videos;

    const videoRows = videos
      .filter((v: any) => v?.id)
      .map((video: any) =>
        mapTikTokVideoRow({
          owner_id,
          profile_id: profileId,
          open_id: openId,
          video,
          sync_source: "video_query",
        })
      );

    const upserted = await supabaseUpsert(
      "tiktok_videos",
      videoRows,
      "owner_id,video_id"
    );

    const videoIds = videos.map((v: any) => String(v.id || "")).filter(Boolean);
    const videoIdMap = await selectTikTokVideoIdMap(owner_id, videoIds);

    const snapshotsCreated = await insertTikTokVideoSnapshots({
      owner_id,
      open_id: openId,
      videos,
      videoIdMap,
      raw: result.raw,
    });

    const now = new Date().toISOString();

    await upsertTikTokSyncState({
      owner_id,
      profile_id: profileId,
      open_id: openId,
      last_video_query_sync_at: now,
      last_error: {},
      raw: {
        video_ids,
      },
    });

    await insertTikTokSyncRun({
      owner_id,
      profile_id: profileId,
      open_id: openId,
      sync_type: "video_query",
      status: "success",
      started_at: startedAt,
      videos_found: videos.length,
      videos_upserted: upserted,
      video_snapshots_created: snapshotsCreated,
      raw: {
        requested_video_ids: video_ids,
      },
    });

    return {
      open_id: openId,
      profile_id: profileId,
      requested_video_ids: video_ids.length,
      videos_found: videos.length,
      videos_upserted: upserted,
      video_snapshots_created: snapshotsCreated,
    };
  } catch (e: any) {
    await insertTikTokSyncRun({
      owner_id,
      sync_type: "video_query",
      status: "error",
      started_at: startedAt,
      error: {
        message: e?.message || String(e),
      },
    });

    throw e;
  }
}

// =========================================================
// ROUTES - TIKTOK DISPLAY API
// =========================================================


// =========================================================
// TIKTOK OAUTH - conservé/adapté depuis le serveur actif
// Stockage token : provider_tokens + Vault
// Remplissage DB : sync profile + videos via Display API
// =========================================================

app.get("/auth/tiktok/start", async (req, res) => {
  try {
    const owner_id = String(req.query.owner_id || "").trim();
    const return_to = String(req.query.return_to || FRONTEND_RETURN_URL);

    if (!owner_id) return res.status(400).send("Missing owner_id");
    if (!TIKTOK_CLIENT_KEY || !TIKTOK_CLIENT_SECRET || !TIKTOK_OAUTH_REDIRECT_URI) {
      return res.status(500).send("Missing TikTok OAuth env vars");
    }

    const code_verifier = generatePkceVerifier();
    const code_challenge = generatePkceChallenge(code_verifier);

    const state = signState({
      owner_id,
      return_to,
      provider: "tiktok",
      code_verifier,
      ts: Date.now(),
    });

    const authUrl = new URL("https://www.tiktok.com/v2/auth/authorize/");
    authUrl.searchParams.set("client_key", TIKTOK_CLIENT_KEY);
    authUrl.searchParams.set("response_type", "code");
    authUrl.searchParams.set("scope", TIKTOK_OAUTH_SCOPES);
    authUrl.searchParams.set("redirect_uri", TIKTOK_OAUTH_REDIRECT_URI);
    authUrl.searchParams.set("state", state);
    authUrl.searchParams.set("code_challenge", code_challenge);
    authUrl.searchParams.set("code_challenge_method", "S256");
    authUrl.searchParams.set("disable_auto_auth", "1");

    return res.redirect(authUrl.toString());
  } catch (e: any) {
    console.error("[tiktok][oauth/start] error:", e);
    return res.status(500).send("TikTok OAuth start error");
  }
});

app.get("/auth/tiktok/callback", async (req, res) => {
  let return_to = FRONTEND_RETURN_URL;

  try {
    const code = String(req.query.code || "");
    const stateRaw = String(req.query.state || "");
    const error = String(req.query.error || "");

    const state = verifyState(stateRaw);
    return_to = String(state?.return_to || FRONTEND_RETURN_URL);

    if (error) {
      const u = new URL(return_to);
      u.searchParams.set("tiktok", "error");
      u.searchParams.set("reason", error);
      return res.redirect(u.toString());
    }

    if (!code) throw new Error("Missing OAuth code");
    if (!state?.owner_id) throw new Error("Invalid OAuth state");

    const owner_id = String(state.owner_id);
    const code_verifier = String(state.code_verifier || "");
    if (!code_verifier) throw new Error("Missing TikTok code_verifier");

    const tokenRes = await fetch(`${TIKTOK_API_BASE}/v2/oauth/token/`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_key: TIKTOK_CLIENT_KEY,
        client_secret: TIKTOK_CLIENT_SECRET,
        code,
        grant_type: "authorization_code",
        redirect_uri: TIKTOK_OAUTH_REDIRECT_URI,
        code_verifier,
      }),
    });

    const tokenText = await tokenRes.text();
    let tokenJson: any = null;
    try {
      tokenJson = tokenText ? JSON.parse(tokenText) : null;
    } catch {
      tokenJson = null;
    }

    const tokenPayload = extractTikTokOAuthPayload(tokenJson);
    if (!tokenRes.ok || tokenPayload?.error || !tokenPayload?.access_token) {
      console.error("[tiktok][oauth/callback] token exchange failed:", tokenRes.status, tokenText);
      throw new Error("TikTok token exchange failed");
    }

    const accessToken = String(tokenPayload.access_token);
    const user = await fetchTikTokUserInfo(accessToken);

    const expiresAt = tokenPayload?.expires_in
      ? new Date(Date.now() + Number(tokenPayload.expires_in) * 1000).toISOString()
      : null;

    const refreshExpiresAt = tokenPayload?.refresh_expires_in
      ? new Date(Date.now() + Number(tokenPayload.refresh_expires_in) * 1000).toISOString()
      : null;

    await upsertProviderTokenVault({
      owner_id,
      provider: "tiktok",
      token: {
        provider: "tiktok",
        access_token: accessToken,
        refresh_token: tokenPayload.refresh_token || null,
        token_type: tokenPayload.token_type || "bearer",
        scope: parseTikTokScopes(tokenPayload.scope || TIKTOK_OAUTH_SCOPES),
        scopes: parseTikTokScopes(tokenPayload.scope || TIKTOK_OAUTH_SCOPES),
        open_id: tokenPayload.open_id || user?.open_id || null,
        union_id: tokenPayload.union_id || user?.union_id || null,
        expires_in: tokenPayload.expires_in ?? null,
        expires_at: expiresAt,
        access_token_expires_at: expiresAt,
        refresh_expires_in: tokenPayload.refresh_expires_in ?? null,
        refresh_token_expires_at: refreshExpiresAt,
        raw_token: tokenJson ?? {},
        raw_user: user ?? {},
        stored_at: new Date().toISOString(),
      },
    });

    let profileResult: any = null;
    let videosResult: any = null;

    try {
      profileResult = await syncTikTokProfileToDb(owner_id);
      videosResult = await syncTikTokVideosToDb(owner_id, { max_pages: 5 });
    } catch (syncError: any) {
      console.error("[tiktok][oauth/callback] post-oauth sync failed:", syncError);
    }

    const u = new URL(return_to);
    u.searchParams.set("tiktok", "connected");
    if (profileResult?.profile?.id) u.searchParams.set("tiktok_profile_id", String(profileResult.profile.id));
    if (videosResult?.videos_upserted !== undefined) {
      u.searchParams.set("tiktok_videos_synced", String(videosResult.videos_upserted));
    }
    return res.redirect(u.toString());
  } catch (e: any) {
    console.error("[tiktok][oauth/callback] error:", e);
    const u = new URL(return_to);
    u.searchParams.set("tiktok", "error");
    u.searchParams.set("message", e?.message || String(e));
    return res.redirect(u.toString());
  }
});

app.post("/api/tiktok/debug/me", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const token = await getFreshTikTokToken(owner_id);
    const user = await fetchTikTokUserInfo(String(token.access_token));

    return res.json({
      ok: true,
      provider: "tiktok",
      owner_id,
      user,
    });
  } catch (e: any) {
    console.error("[tiktok][debug/me] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/tiktok/sync/profile", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const result = await syncTikTokProfileToDb(owner_id);

    return res.json({
      ok: true,
      provider: "tiktok",
      owner_id,
      profile_id: result.profile?.id ?? null,
      open_id: result.user?.open_id ?? null,
      username: result.user?.username ?? null,
      display_name: result.user?.display_name ?? null,
      profile_synced: true,
      profile_snapshot_created: true,
    });
  } catch (e: any) {
    console.error("[tiktok][sync/profile] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/tiktok/sync/videos", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const result = await syncTikTokVideosToDb(owner_id, {
      cursor:
        req.body?.cursor !== undefined && req.body?.cursor !== null
          ? Number(req.body.cursor)
          : null,
      max_count: Number(req.body?.max_count || 20),
      max_pages: Number(req.body?.max_pages || 5),
    });

    return res.json({
      ok: true,
      provider: "tiktok",
      owner_id,
      ...result,
    });
  } catch (e: any) {
    console.error("[tiktok][sync/videos] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/tiktok/sync/video-query", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const video_ids = Array.isArray(req.body?.video_ids)
      ? req.body.video_ids.map(String).map((s: string) => s.trim()).filter(Boolean)
      : [];

    if (video_ids.length === 0) {
      return res.status(400).json({
        ok: false,
        error: "Missing video_ids",
      });
    }

    if (video_ids.length > 20) {
      return res.status(400).json({
        ok: false,
        error: "video_ids maximum = 20 par requête TikTok video/query",
      });
    }

    const result = await syncTikTokVideoQueryToDb(owner_id, video_ids);

    return res.json({
      ok: true,
      provider: "tiktok",
      owner_id,
      ...result,
    });
  } catch (e: any) {
    console.error("[tiktok][sync/video-query] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/tiktok/sync-all", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "").trim();
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const profile = await syncTikTokProfileToDb(owner_id);

    const videos = await syncTikTokVideosToDb(owner_id, {
      cursor:
        req.body?.cursor !== undefined && req.body?.cursor !== null
          ? Number(req.body.cursor)
          : null,
      max_count: Number(req.body?.max_count || 20),
      max_pages: Number(req.body?.max_pages || 5),
    });

    return res.json({
      ...videos,
      ok: true,
      provider: "tiktok",
      owner_id,
      profile_id: profile.profile?.id ?? videos.profile_id ?? null,
      open_id: profile.user?.open_id ?? videos.open_id ?? null,
      profile_synced: true,
      profile_snapshot_created: true,
    });
  } catch (e: any) {
    console.error("[tiktok][sync-all] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});



// Compatibilité anciens endpoints TikTok du serveur actif.
app.get("/api/tiktok/status", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.query.owner_id || "");
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const q = new URLSearchParams();
    q.set("owner_id", `eq.${owner_id}`);
    q.set("provider", "eq.tiktok");
    q.set("select", "*");
    q.set("limit", "1");

    const profiles = await supabaseSelect("tiktok_profiles", q);
    return res.json({
      ok: true,
      connected: profiles.length > 0,
      profile: profiles[0] || null,
    });
  } catch (e: any) {
    console.error("[tiktok][status] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/tiktok/sync-by-owner", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const profile = await syncTikTokProfileToDb(owner_id);
    const videos = await syncTikTokVideosToDb(owner_id, {
      cursor: req.body?.cursor ?? null,
      max_count: req.body?.max_count ?? 20,
      max_pages: req.body?.max_pages ?? 5,
    });

    return res.json({ ok: true, owner_id, profile, videos });
  } catch (e: any) {
    console.error("[tiktok][sync-by-owner] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});


// =========================================================
// GOOGLE ADS SYNC - SERVER PART
// À coller dans server.ts AVANT le bloc :
// // =========================================================
// // START
// // =========================================================
//
// Pré-requis ENV :
// GOOGLE_ADS_DEVELOPER_TOKEN=...
// GOOGLE_ADS_CLIENT_ID=...
// GOOGLE_ADS_CLIENT_SECRET=...
// GOOGLE_ADS_API_VERSION=v24
// GOOGLE_ADS_LOGIN_CUSTOMER_ID=optionnel_sans_tirets
// =========================================================

type GoogleAdsSyncLevel =
  | "campaign"
  | "ad_group"
  | "ad"
  | "keyword"
  | "search_term"
  | "landing_page"
  | "asset"
  | "asset_group"
  | "shopping";

type GoogleAdsSyncBody = {
  owner_id?: string;
  source_owner_id?: string;
  target_owner_id?: string;
  customer_id?: string;
  customer_ids?: string[];
  login_customer_id?: string;
  date_from?: string;
  date_to?: string;
  level?: GoogleAdsSyncLevel;
};

const GOOGLE_ADS_API_VERSION = process.env.GOOGLE_ADS_API_VERSION || "v24";
const GOOGLE_ADS_DEVELOPER_TOKEN = process.env.GOOGLE_ADS_DEVELOPER_TOKEN || "";
const GOOGLE_ADS_CLIENT_ID = process.env.GOOGLE_ADS_CLIENT_ID || "";
const GOOGLE_ADS_CLIENT_SECRET = process.env.GOOGLE_ADS_CLIENT_SECRET || "";
const GOOGLE_ADS_OAUTH_REDIRECT_URI =
  process.env.GOOGLE_ADS_OAUTH_REDIRECT_URI ||
  process.env.GOOGLE_OAUTH_REDIRECT_URI ||
  "https://vyrexads-backend.onrender.com/auth/google-ads/callback";
const GOOGLE_ADS_DEFAULT_LOGIN_CUSTOMER_ID = cleanGoogleAdsCustomerId(
  process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID || ""
);

if (!GOOGLE_ADS_DEVELOPER_TOKEN) console.warn("[env] Missing GOOGLE_ADS_DEVELOPER_TOKEN");
if (!GOOGLE_ADS_CLIENT_ID) console.warn("[env] Missing GOOGLE_ADS_CLIENT_ID");
if (!GOOGLE_ADS_CLIENT_SECRET) console.warn("[env] Missing GOOGLE_ADS_CLIENT_SECRET");

function cleanGoogleAdsCustomerId(value: any) {
  return String(value || "").replace(/[^0-9]/g, "");
}

function microsToAmount(value: any) {
  const n = toNumber(value, null);
  if (n === null) return null;
  return n / 1_000_000;
}

function googleAdsArray(value: any) {
  return Array.isArray(value) ? value : [];
}

function firstDefined<T = any>(...values: T[]) {
  return values.find((v) => v !== undefined && v !== null && v !== "") ?? null;
}

function getGoogleAdsIdFromResource(resourceName: any) {
  const s = String(resourceName || "");
  const parts = s.split("/");
  return parts[parts.length - 1] || null;
}

function googleAdsErrorMessage(payload: any) {
  const details = asArray(payload?.error?.details);
  const googleAdsFailure = details.find((d) => d?.['@type']?.includes("GoogleAdsFailure"));
  const errors = asArray(googleAdsFailure?.errors)
    .map((e) => {
      const code = e?.errorCode ? JSON.stringify(e.errorCode) : "UNKNOWN";
      const msg = e?.message || "";
      const fieldPath = asArray(e?.location?.fieldPathElements)
        .map((x) => x?.fieldName)
        .filter(Boolean)
        .join(".");
      return fieldPath ? `${code}: ${msg} at ${fieldPath}` : `${code}: ${msg}`;
    })
    .filter(Boolean);

  if (errors.length) return errors.join(" | ");
  return payload?.error?.message || JSON.stringify(payload);
}

function assertGoogleAdsEnv() {
  if (!GOOGLE_ADS_DEVELOPER_TOKEN) throw new Error("Missing GOOGLE_ADS_DEVELOPER_TOKEN");
  if (!GOOGLE_ADS_CLIENT_ID) throw new Error("Missing GOOGLE_ADS_CLIENT_ID");
  if (!GOOGLE_ADS_CLIENT_SECRET) throw new Error("Missing GOOGLE_ADS_CLIENT_SECRET");
}

function normalizeGoogleAdsToken(token: any): Json {
  let parsed = token;
  if (typeof parsed === "string") {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      throw new Error("Stored Google Ads token is not valid JSON");
    }
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Stored Google Ads token is not a JSON object");
  }
  return parsed;
}

function isGoogleAccessTokenExpired(token: Json) {
  const expiresAt = token?.expires_at || token?.expiry_date || token?.expiresAt;
  if (!expiresAt) return false;

  const ms = typeof expiresAt === "number" ? expiresAt : Date.parse(String(expiresAt));
  if (!Number.isFinite(ms)) return false;

  // refresh 2 minutes before expiration
  return ms <= Date.now() + 120_000;
}

async function refreshGoogleAdsToken(owner_id: string, token: Json) {
  assertGoogleAdsEnv();

  const refreshToken = token?.refresh_token || token?.raw_token?.refresh_token;
  if (!refreshToken) {
    throw new Error("Google Ads refresh_token not found in stored token");
  }

  const body = new URLSearchParams();
  body.set("client_id", GOOGLE_ADS_CLIENT_ID);
  body.set("client_secret", GOOGLE_ADS_CLIENT_SECRET);
  body.set("refresh_token", refreshToken);
  body.set("grant_type", "refresh_token");

  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });

  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(`Google OAuth refresh failed: ${res.status} ${JSON.stringify(json)}`);
  }

  const expiresIn = toNumber(json?.expires_in, 3600) || 3600;
  const nextToken = jsonbStripNullsLocal({
    ...token,
    access_token: json.access_token,
    refresh_token: refreshToken,
    token_type: json.token_type || token?.token_type || "Bearer",
    scope: json.scope || token?.scope || "https://www.googleapis.com/auth/adwords",
    expires_in: expiresIn,
    expires_at: new Date(Date.now() + expiresIn * 1000).toISOString(),
    refreshed_at: new Date().toISOString(),
  });

  await upsertProviderTokenVault({
    owner_id,
    provider: "google_ads",
    token: nextToken,
  });

  return nextToken;
}

function jsonbStripNullsLocal<T extends Json>(obj: T): T {
  const out: Json = {};
  for (const [k, v] of Object.entries(obj || {})) {
    if (v !== null && v !== undefined) out[k] = v;
  }
  return out as T;
}

async function getGoogleAdsAccessToken(owner_id: string) {
  let token = normalizeGoogleAdsToken(await getProviderToken(owner_id, "google_ads"));

  if (!token?.access_token || isGoogleAccessTokenExpired(token)) {
    token = await refreshGoogleAdsToken(owner_id, token);
  }

  if (!token?.access_token) {
    throw new Error("Google Ads access_token not found after refresh");
  }

  return {
    accessToken: String(token.access_token),
    token,
  };
}

function googleAdsHeaders(accessToken: string, loginCustomerId?: string) {
  const headers: Record<string, string> = {
    Authorization: `Bearer ${accessToken}`,
    "developer-token": GOOGLE_ADS_DEVELOPER_TOKEN,
    "Content-Type": "application/json",
  };

  const login = cleanGoogleAdsCustomerId(loginCustomerId || GOOGLE_ADS_DEFAULT_LOGIN_CUSTOMER_ID);
  if (login) headers["login-customer-id"] = login;

  return headers;
}

async function googleAdsListAccessibleCustomers(owner_id: string) {
  assertGoogleAdsEnv();

  const { accessToken } = await getGoogleAdsAccessToken(owner_id);
  const url = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers:listAccessibleCustomers`;

  const res = await fetch(url, {
    method: "GET",
    headers: googleAdsHeaders(accessToken),
  });

  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(`Google Ads listAccessibleCustomers failed: ${res.status} ${googleAdsErrorMessage(json)}`);
  }

  return googleAdsArray(json?.resourceNames)
    .map((rn) => cleanGoogleAdsCustomerId(getGoogleAdsIdFromResource(rn)))
    .filter(Boolean);
}

async function googleAdsSearchStream(params: {
  owner_id: string;
  customer_id: string;
  query: string;
  login_customer_id?: string;
}) {
  assertGoogleAdsEnv();

  const { accessToken } = await getGoogleAdsAccessToken(params.owner_id);
  const customerId = cleanGoogleAdsCustomerId(params.customer_id);
  const url = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${customerId}/googleAds:searchStream`;

  const res = await fetch(url, {
    method: "POST",
    headers: googleAdsHeaders(accessToken, params.login_customer_id),
    body: JSON.stringify({ query: params.query }),
  });

  const text = await res.text();
  let json: any = null;

  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    throw new Error(`Google Ads SearchStream returned non-JSON: ${text.slice(0, 500)}`);
  }

  if (!res.ok) {
    throw new Error(`Google Ads SearchStream failed: ${res.status} ${googleAdsErrorMessage(json)}`);
  }

  const batches = Array.isArray(json) ? json : [json];
  return batches.flatMap((batch) => googleAdsArray(batch?.results));
}

async function tryGoogleAdsSearchStream(params: {
  owner_id: string;
  customer_id: string;
  query: string;
  login_customer_id?: string;
  label: string;
}) {
  try {
    return await googleAdsSearchStream(params);
  } catch (e: any) {
    console.warn(`[google-ads][${params.label}] skipped:`, e?.message || String(e));
    return [];
  }
}

function metricRowBase(r: any) {
  const metrics = asObject(r?.metrics);

  return {
    impressions: toBigIntNumber(metrics.impressions, 0),
    clicks: toBigIntNumber(metrics.clicks, 0),
    ctr: toNumber(metrics.ctr, null),

    cost: microsToAmount(metrics.costMicros),
    average_cpc: microsToAmount(metrics.averageCpc),
    average_cpm: microsToAmount(metrics.averageCpm),
    average_cpv: microsToAmount(metrics.trueviewAverageCpv),

    interactions: toBigIntNumber(metrics.interactions, 0),
    interaction_rate: toNumber(metrics.interactionRate, null),
    engagements: toBigIntNumber(metrics.engagements, 0),
    engagement_rate: toNumber(metrics.engagementRate, null),

    conversions: toNumber(metrics.conversions, null),
    conversion_rate: toNumber(metrics.conversionsFromInteractionsRate, null),
    cost_per_conversion: microsToAmount(metrics.costPerConversion),
    conversion_value: toNumber(metrics.conversionsValue, null),

    all_conversions: toNumber(metrics.allConversions, null),
    all_conversions_value: toNumber(metrics.allConversionsValue, null),
    cross_device_conversions: toNumber(metrics.crossDeviceConversions, null),
    view_through_conversions: toNumber(metrics.viewThroughConversions, null),

    video_views: toBigIntNumber(metrics.videoTrueviewViews, 0),
    video_view_rate: toNumber(metrics.videoTrueviewViewRate, null),
    average_watch_time_millis: toBigIntNumber(metrics.averageVideoWatchTimeDurationMillis, 0),
    watch_time_millis: toBigIntNumber(metrics.videoWatchTimeDurationMillis, 0),
    video_quartile_25_rate: toNumber(metrics.videoQuartileP25Rate, null),
    video_quartile_50_rate: toNumber(metrics.videoQuartileP50Rate, null),
    video_quartile_75_rate: toNumber(metrics.videoQuartileP75Rate, null),
    video_quartile_100_rate: toNumber(metrics.videoQuartileP100Rate, null),
  };
}

function googleSegments(r: any) {
  const segments = asObject(r?.segments);
  return {
    date: segments.date || null,
    segments_device: segments.device || null,
    segments_ad_network_type: segments.adNetworkType || null,
    segments_click_type: segments.clickType || null,
    segments_day_of_week: segments.dayOfWeek || null,
    segments_hour: toNumber(segments.hour, null),
    segments_conversion_action: getGoogleAdsIdFromResource(segments.conversionAction),
    segments_conversion_category: segments.conversionActionCategory || null,
  };
}

function googleCampaignTypeFlags(campaign: any) {
  const channel = campaign?.advertisingChannelType || null;
  return {
    campaign_type: channel,
    campaign_sub_type: campaign?.advertisingChannelSubType || null,
    campaign_status: campaign?.status || null,
    campaign_serving_status: campaign?.servingStatus || null,
  };
}

function googleAdCreativeFields(ad: any) {
  return {
    final_urls: googleAdsArray(ad?.finalUrls),
    display_url: ad?.displayUrl || null,
    responsive_search_headlines: googleAdsArray(ad?.responsiveSearchAd?.headlines),
    responsive_search_descriptions: googleAdsArray(ad?.responsiveSearchAd?.descriptions),
    youtube_video_id: firstDefined(
      ad?.videoAd?.video?.youtubeVideoId,
      ad?.videoResponsiveAd?.videos?.[0]?.youtubeVideoId
    ),
    youtube_video_title: firstDefined(
      ad?.videoAd?.video?.youtubeVideoTitle,
      ad?.videoResponsiveAd?.videos?.[0]?.youtubeVideoTitle
    ),
    creative_content_raw: ad || {},
  };
}

async function syncGoogleAdsAccountRows(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const customerId = cleanGoogleAdsCustomerId(params.customer_id);

  const customerRows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: customerId,
    login_customer_id: params.login_customer_id,
    label: `customer:${customerId}`,
    query: `
      SELECT
        customer.id,
        customer.resource_name,
        customer.descriptive_name,
        customer.currency_code,
        customer.time_zone,
        customer.status,
        customer.manager,
        customer.test_account,
        customer.auto_tagging_enabled,
        customer.optimization_score
      FROM customer
    `,
  });

  const accountRows = customerRows.map((r) => {
    const c = asObject(r?.customer);
    const id = cleanGoogleAdsCustomerId(c.id || customerId);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: id,
      resource_name: c.resourceName || null,
      descriptive_name: c.descriptiveName || null,
      currency_code: c.currencyCode || null,
      time_zone: c.timeZone || null,
      status: c.status || null,
      is_manager: c.manager ?? null,
      login_customer_id: cleanGoogleAdsCustomerId(params.login_customer_id || GOOGLE_ADS_DEFAULT_LOGIN_CUSTOMER_ID) || null,
      test_account: c.testAccount ?? null,
      auto_tagging_enabled: c.autoTaggingEnabled ?? null,
      optimization_score: toNumber(c.optimizationScore, null),
      raw: r,
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  });

  await supabaseUpsert("google_ads_accounts", accountRows, "owner_id,customer_id");

  const childRows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: customerId,
    login_customer_id: params.login_customer_id || customerId,
    label: `customer_client:${customerId}`,
    query: `
      SELECT
        customer_client.client_customer,
        customer_client.id,
        customer_client.descriptive_name,
        customer_client.currency_code,
        customer_client.time_zone,
        customer_client.status,
        customer_client.manager,
        customer_client.level
      FROM customer_client
      WHERE customer_client.level <= 1
    `,
  });

  const linkRows = childRows.map((r) => {
    const cc = asObject(r?.customerClient);
    const clientId = cleanGoogleAdsCustomerId(cc.id || getGoogleAdsIdFromResource(cc.clientCustomer));
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      manager_customer_id: customerId,
      client_customer_id: clientId,
      client_resource_name: cc.clientCustomer || null,
      client_name: cc.descriptiveName || null,
      currency_code: cc.currencyCode || null,
      time_zone: cc.timeZone || null,
      status: cc.status || null,
      is_manager: cc.manager ?? null,
      level: toNumber(cc.level, null),
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.client_customer_id);

  const childAccountRows = linkRows.map((r) => ({
    owner_id: r.owner_id,
    provider: "google_ads",
    customer_id: r.client_customer_id,
    descriptive_name: r.client_name,
    currency_code: r.currency_code,
    time_zone: r.time_zone,
    status: r.status,
    is_manager: r.is_manager,
    parent_customer_id: r.manager_customer_id,
    level: r.level,
    login_customer_id: customerId,
    raw: r.raw_json,
    raw_json: r.raw_json,
    last_synced_at: new Date().toISOString(),
  }));

  await supabaseUpsert("google_ads_customer_links", linkRows, "owner_id,manager_customer_id,client_customer_id");
  await supabaseUpsert("google_ads_accounts", childAccountRows, "owner_id,customer_id");

  return {
    accounts_synced: accountRows.length + childAccountRows.length,
    customer_links_synced: linkRows.length,
    child_customer_ids: childAccountRows.map((r) => r.customer_id),
  };
}

async function syncGoogleAdsBudgets(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `budgets:${params.customer_id}`,
    query: `
      SELECT
        campaign_budget.id,
        campaign_budget.resource_name,
        campaign_budget.name,
        campaign_budget.status,
        campaign_budget.delivery_method,
        campaign_budget.amount_micros,
        campaign_budget.total_amount_micros,
        campaign_budget.explicitly_shared,
        campaign_budget.reference_count,
        campaign_budget.has_recommended_budget,
        campaign_budget.recommended_budget_amount_micros
      FROM campaign_budget
    `,
  });

  const mapped = rows.map((r) => {
    const b = asObject(r?.campaignBudget);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      budget_id: String(b.id || getGoogleAdsIdFromResource(b.resourceName) || ""),
      resource_name: b.resourceName || null,
      name: b.name || null,
      status: b.status || null,
      delivery_method: b.deliveryMethod || null,
      amount_micros: toBigIntNumber(b.amountMicros, 0),
      amount: microsToAmount(b.amountMicros),
      total_amount_micros: toBigIntNumber(b.totalAmountMicros, 0),
      total_amount: microsToAmount(b.totalAmountMicros),
      explicitly_shared: b.explicitlyShared ?? null,
      reference_count: toNumber(b.referenceCount, null),
      has_recommended_budget: b.hasRecommendedBudget ?? null,
      recommended_budget_amount_micros: toBigIntNumber(b.recommendedBudgetAmountMicros, 0),
      recommended_budget_amount: microsToAmount(b.recommendedBudgetAmountMicros),
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.budget_id);

  return supabaseUpsert("google_ads_campaign_budgets", mapped, "owner_id,customer_id,budget_id");
}

async function syncGoogleAdsBiddingStrategies(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `bidding_strategies:${params.customer_id}`,
    query: `
      SELECT
        bidding_strategy.id,
        bidding_strategy.resource_name,
        bidding_strategy.name,
        bidding_strategy.type,
        bidding_strategy.status,
        bidding_strategy.target_cpa.target_cpa_micros,
        bidding_strategy.target_roas.target_roas,
        bidding_strategy.maximize_conversions.target_cpa_micros,
        bidding_strategy.maximize_conversion_value.target_roas
      FROM bidding_strategy
    `,
  });

  const mapped = rows.map((r) => {
    const b = asObject(r?.biddingStrategy);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      bidding_strategy_id: String(b.id || getGoogleAdsIdFromResource(b.resourceName) || ""),
      resource_name: b.resourceName || null,
      name: b.name || null,
      type: b.type || null,
      status: b.status || null,
      target_cpa_micros: toBigIntNumber(b?.targetCpa?.targetCpaMicros, 0),
      target_cpa: microsToAmount(b?.targetCpa?.targetCpaMicros),
      target_roas: toNumber(b?.targetRoas?.targetRoas, null),
      maximize_conversions_target_cpa_micros: toBigIntNumber(b?.maximizeConversions?.targetCpaMicros, 0),
      maximize_conversion_value_target_roas: toNumber(b?.maximizeConversionValue?.targetRoas, null),
      is_portfolio: true,
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.bidding_strategy_id);

  return supabaseUpsert("google_ads_bidding_strategies", mapped, "owner_id,customer_id,bidding_strategy_id");
}

async function syncGoogleAdsCampaigns(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await googleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.resource_name,
        campaign.name,
        campaign.status,
        campaign.serving_status,
        campaign.primary_status,
        campaign.primary_status_reasons,
        campaign.advertising_channel_type,
        campaign.advertising_channel_sub_type,
        campaign.campaign_budget,
        campaign.bidding_strategy,
        campaign.bidding_strategy_type,
        campaign.target_cpa.target_cpa_micros,
        campaign.target_roas.target_roas,
        campaign.maximize_conversions.target_cpa_micros,
        campaign.maximize_conversion_value.target_roas,
        campaign.final_url_suffix,
        campaign.tracking_url_template,
        campaign.network_settings.target_google_search,
        campaign.network_settings.target_search_network,
        campaign.network_settings.target_content_network,
        campaign.network_settings.target_partner_search_network,
        campaign.geo_target_type_setting.positive_geo_target_type,
        campaign.geo_target_type_setting.negative_geo_target_type,
        campaign.optimization_goal_setting.optimization_goal_types
      FROM campaign
      WHERE campaign.status != 'REMOVED'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      campaign_id: String(c.id || getGoogleAdsIdFromResource(c.resourceName) || ""),
      resource_name: c.resourceName || null,
      name: c.name || null,
      status: c.status || null,
      serving_status: c.servingStatus || null,
      primary_status: c.primaryStatus || null,
      primary_status_reasons: googleAdsArray(c.primaryStatusReasons),
      advertising_channel_type: c.advertisingChannelType || null,
      advertising_channel_sub_type: c.advertisingChannelSubType || null,
      start_date: c.startDate || null,
      end_date: c.endDate || null,
      campaign_budget_id: getGoogleAdsIdFromResource(c.campaignBudget),
      campaign_budget_resource_name: c.campaignBudget || null,
      bidding_strategy_id: getGoogleAdsIdFromResource(c.biddingStrategy),
      bidding_strategy_resource_name: c.biddingStrategy || null,
      bidding_strategy_type: c.biddingStrategyType || null,
      target_cpa_micros: toBigIntNumber(c?.targetCpa?.targetCpaMicros, 0),
      target_cpa: microsToAmount(c?.targetCpa?.targetCpaMicros),
      target_roas: toNumber(c?.targetRoas?.targetRoas, null),
      maximize_conversions_target_cpa_micros: toBigIntNumber(c?.maximizeConversions?.targetCpaMicros, 0),
      maximize_conversion_value_target_roas: toNumber(c?.maximizeConversionValue?.targetRoas, null),
      final_url_suffix: c.finalUrlSuffix || null,
      tracking_url_template: c.trackingUrlTemplate || null,
      network_settings: c.networkSettings || {},
      geo_target_type_setting: c.geoTargetTypeSetting || {},
      optimization_goal_setting: c.optimizationGoalSetting || {},
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.campaign_id);

  return supabaseUpsert("google_ads_campaigns", mapped, "owner_id,customer_id,campaign_id");
}

async function syncGoogleAdsAdGroups(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await googleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.resource_name,
        ad_group.name,
        ad_group.status,
        ad_group.type,
        ad_group.cpc_bid_micros,
        ad_group.cpm_bid_micros,
        ad_group.cpv_bid_micros,
        ad_group.target_cpa_micros,
        ad_group.target_roas,
        ad_group.effective_target_cpa_micros,
        ad_group.effective_target_roas,
        ad_group.tracking_url_template,
        ad_group.final_url_suffix
      FROM ad_group
      WHERE ad_group.status != 'REMOVED'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const a = asObject(r?.adGroup);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      campaign_id: String(c.id || ""),
      ad_group_id: String(a.id || getGoogleAdsIdFromResource(a.resourceName) || ""),
      resource_name: a.resourceName || null,
      campaign_name: c.name || null,
      name: a.name || null,
      status: null,
      type: a.type || null,
      cpc_bid_micros: toBigIntNumber(a.cpcBidMicros, 0),
      cpc_bid: microsToAmount(a.cpcBidMicros),
      cpm_bid_micros: toBigIntNumber(a.cpmBidMicros, 0),
      cpm_bid: microsToAmount(a.cpmBidMicros),
      cpv_bid_micros: toBigIntNumber(a.cpvBidMicros, 0),
      cpv_bid: microsToAmount(a.cpvBidMicros),
      target_cpa_micros: toBigIntNumber(a.targetCpaMicros, 0),
      target_cpa: microsToAmount(a.targetCpaMicros),
      target_roas: toNumber(a.targetRoas, null),
      effective_target_cpa_micros: toBigIntNumber(a.effectiveTargetCpaMicros, 0),
      effective_target_cpa: microsToAmount(a.effectiveTargetCpaMicros),
      effective_target_roas: toNumber(a.effectiveTargetRoas, null),
      tracking_url_template: a.trackingUrlTemplate || null,
      final_url_suffix: a.finalUrlSuffix || null,
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.ad_group_id);

  return supabaseUpsert("google_ads_ad_groups", mapped, "owner_id,customer_id,ad_group_id");
}

async function syncGoogleAdsAds(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await googleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        ad_group_ad.resource_name,
        ad_group_ad.status,
        ad_group_ad.policy_summary.approval_status,
        ad_group_ad.policy_summary.review_status,
        ad_group_ad.policy_summary.policy_topic_entries,
        ad_group_ad.ad.id,
        ad_group_ad.ad.name,
        ad_group_ad.ad.type,
        ad_group_ad.ad.final_urls,
        ad_group_ad.ad.final_mobile_urls,
        ad_group_ad.ad.display_url,
        ad_group_ad.ad.tracking_url_template,
        ad_group_ad.ad.final_url_suffix,
        ad_group_ad.ad.responsive_search_ad.headlines,
        ad_group_ad.ad.responsive_search_ad.descriptions
      FROM ad_group_ad
      WHERE ad_group_ad.status != 'REMOVED'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.adGroup);
    const aga = asObject(r?.adGroupAd);
    const ad = asObject(aga?.ad);

    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      campaign_id: c.id ? String(c.id) : null,
      ad_group_id: ag.id ? String(ag.id) : null,
      ad_id: String(ad.id || ""),
      resource_name: aga.resourceName || null,
      campaign_name: c.name || null,
      ad_group_name: ag.name || null,
      ad_name: ad.name || null,
      ad_type: ad.type || null,
      ad_status: aga.status || null,
      policy_approval_status: aga?.policySummary?.approvalStatus || null,
      policy_review_status: aga?.policySummary?.reviewStatus || null,
      policy_summary: aga?.policySummary || {},
      final_urls: googleAdsArray(ad.finalUrls),
      final_mobile_urls: googleAdsArray(ad.finalMobileUrls),
      display_url: ad.displayUrl || null,
      tracking_url_template: ad.trackingUrlTemplate || null,
      final_url_suffix: ad.finalUrlSuffix || null,
      responsive_search_headlines: googleAdsArray(ad?.responsiveSearchAd?.headlines),
      responsive_search_descriptions: googleAdsArray(ad?.responsiveSearchAd?.descriptions),
      creative_content_raw: ad,
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.ad_id);

  return supabaseUpsert("google_ads_ads", mapped, "owner_id,customer_id,ad_id");
}

async function syncGoogleAdsCampaignMetrics(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
}) {
  const rows = await googleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        segments.date,
        campaign.id,
        campaign.name,
        campaign.advertising_channel_type,
        campaign.advertising_channel_sub_type,
        campaign.status,
        campaign.serving_status,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.trueview_average_cpv,
        metrics.interactions,
        metrics.interaction_rate,
        metrics.engagements,
        metrics.engagement_rate,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value,
        metrics.all_conversions,
        metrics.all_conversions_value,
        metrics.cross_device_conversions,
        metrics.view_through_conversions,
        metrics.video_trueview_views,
        metrics.video_trueview_view_rate,
        metrics.average_video_watch_time_duration_millis,
        metrics.video_watch_time_duration_millis,
        metrics.video_quartile_p25_rate,
        metrics.video_quartile_p50_rate,
        metrics.video_quartile_p75_rate,
        metrics.video_quartile_p100_rate,
        metrics.search_impression_share,
        metrics.search_rank_lost_impression_share,
        metrics.search_budget_lost_impression_share,
        metrics.top_impression_percentage,
        metrics.absolute_top_impression_percentage
      FROM campaign
      WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const m = asObject(r?.metrics);
    const seg = googleSegments(r);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      campaign_id: String(c.id || ""),
      campaign_name: c.name || null,
      date: seg.date,
      ...googleCampaignTypeFlags(c),
      ...metricRowBase(r),
      search_impression_share: toNumber(m.searchImpressionShare, null),
      search_rank_lost_impression_share: toNumber(m.searchRankLostImpressionShare, null),
      search_budget_lost_impression_share: toNumber(m.searchBudgetLostImpressionShare, null),
      top_impression_percentage: toNumber(m.topImpressionPercentage, null),
      absolute_top_impression_percentage: toNumber(m.absoluteTopImpressionPercentage, null),
      raw: r,
    };
  }).filter((r) => r.campaign_id && r.date);

  return supabaseUpsert(
    "ads_metrics_campaigns",
    mapped,
    "owner_id,provider,customer_id,campaign_id,date"
  );
}

async function syncGoogleAdsAdGroupMetrics(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
}) {
  const rows = await googleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        segments.date,
        campaign.id,
        campaign.name,
        campaign.advertising_channel_type,
        campaign.advertising_channel_sub_type,
        campaign.status,
        campaign.serving_status,
        ad_group.id,
        ad_group.name,
        ad_group.status,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.trueview_average_cpv,
        metrics.interactions,
        metrics.interaction_rate,
        metrics.engagements,
        metrics.engagement_rate,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value,
        metrics.all_conversions,
        metrics.all_conversions_value,
        metrics.cross_device_conversions,
        metrics.view_through_conversions,
        metrics.video_trueview_views,
        metrics.video_trueview_view_rate,
        metrics.average_video_watch_time_duration_millis,
        metrics.video_watch_time_duration_millis,
        metrics.video_quartile_p25_rate,
        metrics.video_quartile_p50_rate,
        metrics.video_quartile_p75_rate,
        metrics.video_quartile_p100_rate
      FROM ad_group
      WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
        AND ad_group.status != 'REMOVED'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.adGroup);
    const seg = googleSegments(r);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      campaign_id: c.id ? String(c.id) : null,
      campaign_name: c.name || null,
      ad_group_id: String(ag.id || ""),
      ad_group_name: ag.name || null,
      ad_group_status: ag.status || null,
      date: seg.date,
      ...googleCampaignTypeFlags(c),
      ...metricRowBase(r),
      raw: r,
    };
  }).filter((r) => r.ad_group_id && r.date);

  return supabaseUpsert(
    "ads_metrics_ad_groups",
    mapped,
    "owner_id,provider,customer_id,ad_group_id,date"
  );
}

async function syncGoogleAdsAdMetrics(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
}) {
  const rows = await googleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        segments.date,
        campaign.id,
        campaign.name,
        campaign.advertising_channel_type,
        campaign.advertising_channel_sub_type,
        campaign.status,
        campaign.serving_status,
        ad_group.id,
        ad_group.name,
        ad_group_ad.status,
        ad_group_ad.ad.id,
        ad_group_ad.ad.name,
        ad_group_ad.ad.type,
        ad_group_ad.ad.final_urls,
        ad_group_ad.ad.display_url,
        ad_group_ad.ad.responsive_search_ad.headlines,
        ad_group_ad.ad.responsive_search_ad.descriptions,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.trueview_average_cpv,
        metrics.interactions,
        metrics.interaction_rate,
        metrics.engagements,
        metrics.engagement_rate,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value,
        metrics.all_conversions,
        metrics.all_conversions_value,
        metrics.cross_device_conversions,
        metrics.view_through_conversions,
        metrics.video_trueview_views,
        metrics.video_trueview_view_rate,
        metrics.average_video_watch_time_duration_millis,
        metrics.video_watch_time_duration_millis,
        metrics.video_quartile_p25_rate,
        metrics.video_quartile_p50_rate,
        metrics.video_quartile_p75_rate,
        metrics.video_quartile_p100_rate
      FROM ad_group_ad
      WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
        AND ad_group_ad.status != 'REMOVED'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.adGroup);
    const aga = asObject(r?.adGroupAd);
    const ad = asObject(aga?.ad);
    const seg = googleSegments(r);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      campaign_id: c.id ? String(c.id) : null,
      campaign_name: c.name || null,
      ad_group_id: ag.id ? String(ag.id) : null,
      ad_group_name: ag.name || null,
      ad_id: String(ad.id || ""),
      ad_name: ad.name || null,
      ad_type: ad.type || null,
      ad_status: aga.status || null,
      date: seg.date,
      ...googleCampaignTypeFlags(c),
      ...metricRowBase(r),
      ...googleAdCreativeFields(ad),
      raw: r,
    };
  }).filter((r) => r.ad_id && r.date);

  return supabaseUpsert(
    "ads_metrics_ads",
    mapped,
    "owner_id,provider,customer_id,ad_id,date"
  );
}

async function syncGoogleAdsKeywordMetrics(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
}) {
  const rows = await googleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        segments.date,
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        ad_group_criterion.criterion_id,
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        ad_group_criterion.status,
        ad_group_criterion.negative,
        ad_group_criterion.quality_info.quality_score,
        ad_group_criterion.quality_info.creative_quality_score,
        ad_group_criterion.quality_info.post_click_quality_score,
        ad_group_criterion.quality_info.search_predicted_ctr,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value
      FROM keyword_view
      WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
        AND ad_group_criterion.status != 'REMOVED'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.adGroup);
    const criterion = asObject(r?.adGroupCriterion);
    const keyword = asObject(criterion?.keyword);
    const qualityInfo = asObject(criterion?.qualityInfo);
    const metrics = asObject(r?.metrics);
    const seg = googleSegments(r);

    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),

      campaign_id: c.id ? String(c.id) : null,
      campaign_name: c.name || null,

      ad_group_id: ag.id ? String(ag.id) : "",
      ad_group_name: ag.name || null,

      criterion_id: String(criterion.criterionId || ""),
      keyword_text: keyword.text || null,
      keyword_match_type: keyword.matchType || null,

      date: seg.date,

      impressions: toBigIntNumber(metrics.impressions, 0),
      clicks: toBigIntNumber(metrics.clicks, 0),
      ctr: toNumber(metrics.ctr, null),
      cost: microsToAmount(metrics.costMicros),
      average_cpc: microsToAmount(metrics.averageCpc),
      average_cpm: microsToAmount(metrics.averageCpm),

      conversions: toNumber(metrics.conversions, null),
      conversion_rate: toNumber(metrics.conversionsFromInteractionsRate, null),
      cost_per_conversion: microsToAmount(metrics.costPerConversion),
      conversion_value: toNumber(metrics.conversionsValue, null),

      quality_score: toNumber(qualityInfo.qualityScore, null),
      ad_relevance: qualityInfo.creativeQualityScore || null,
      landing_page_experience: qualityInfo.postClickQualityScore || null,
      expected_click_through_rate: qualityInfo.searchPredictedCtr || null,

      raw: r,
    };
  }).filter((r) => r.ad_group_id && r.criterion_id && r.date);

  return supabaseUpsert(
    "ads_metrics_keywords",
    mapped,
    "owner_id,provider,customer_id,ad_group_id,criterion_id,date"
  );
}


function googleAdsStableUuid(...parts: any[]) {
  const input = parts.map((p) => String(p ?? "")).join("|");
  const h = crypto.createHash("sha256").update(input).digest("hex");
  const variant = ((parseInt(h.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${variant}${h.slice(18, 20)}-${h.slice(20, 32)}`;
}

function addStableGoogleAdsId(table: string, row: Json, ...parts: any[]): Json {
  return {
    id: googleAdsStableUuid(table, ...parts),
    ...row,
  };
}

async function googleAdsUpsertById(table: string, rows: Json[]) {
  const deduped = Array.from(
    new Map(
      rows
        .filter((row: any) => row?.id)
        .map((row: any) => [String(row.id), row])
    ).values()
  );

  return supabaseUpsert(table, deduped as Json[], "id");
}

async function optionalGoogleAdsSync(label: string, fn: () => Promise<number>) {
  try {
    return await fn();
  } catch (e: any) {
    console.warn(`[google-ads][${label}] optional sync skipped:`, e?.message || String(e));
    return 0;
  }
}

function googleAdsChangedFields(value: any) {
  if (Array.isArray(value)) return value.map(String);
  if (Array.isArray(value?.paths)) return value.paths.map(String);
  return [];
}

function googleAdsCriterionFields(scope: "CAMPAIGN" | "AD_GROUP", r: any) {
  const prefix = scope === "CAMPAIGN" ? asObject(r?.campaignCriterion) : asObject(r?.adGroupCriterion);
  const c = asObject(r?.campaign);
  const ag = asObject(r?.adGroup);
  const keyword = asObject(prefix?.keyword);
  const location = asObject(prefix?.location);
  const language = asObject(prefix?.language);
  const ageRange = asObject(prefix?.ageRange);
  const gender = asObject(prefix?.gender);
  const parentalStatus = asObject(prefix?.parentalStatus);
  const incomeRange = asObject(prefix?.incomeRange);
  const placement = asObject(prefix?.placement);
  const topic = asObject(prefix?.topic);
  const userList = asObject(prefix?.userList);
  const audience = asObject(prefix?.audience);
  const adSchedule = asObject(prefix?.adSchedule);
  const listingGroup = asObject(prefix?.listingGroup);
  const webpage = asObject(prefix?.webpage);

  return addStableGoogleAdsId(
    "google_ads_criteria",
    {
      owner_id: "",
      provider: "google_ads",
      customer_id: "",
      scope,
      campaign_id: c.id ? String(c.id) : null,
      ad_group_id: ag.id ? String(ag.id) : null,
      criterion_id: String(prefix.criterionId || ""),
      type: prefix.type || null,
      status: prefix.status || null,
      negative: Boolean(prefix.negative),
      keyword_text: keyword.text || null,
      keyword_match_type: keyword.matchType || null,
      location_geo_target_constant: location.geoTargetConstant || null,
      language_constant: language.languageConstant || null,
      age_range_type: ageRange.type || null,
      gender_type: gender.type || null,
      parental_status_type: parentalStatus.type || null,
      income_range_type: incomeRange.type || null,
      placement_url: placement.url || null,
      topic_constant: topic.topicConstant || null,
      topic_path: [],
      user_list: userList.userList || null,
      audience: audience.audience || null,
      ad_schedule: adSchedule,
      listing_group: listingGroup,
      webpage,
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    },
    scope,
    c.id,
    ag.id,
    prefix.criterionId
  );
}

async function syncGoogleAdsKeywords(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `keywords:${params.customer_id}`,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        ad_group_criterion.criterion_id,
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        ad_group_criterion.status,
        ad_group_criterion.negative,
        ad_group_criterion.quality_info.quality_score,
        ad_group_criterion.quality_info.creative_quality_score,
        ad_group_criterion.quality_info.post_click_quality_score,
        ad_group_criterion.quality_info.search_predicted_ctr
      FROM ad_group_criterion
      WHERE ad_group_criterion.type = 'KEYWORD'
        AND ad_group_criterion.status != 'REMOVED'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.adGroup);
    const criterion = asObject(r?.adGroupCriterion);
    const keyword = asObject(criterion?.keyword);
    const qualityInfo = asObject(criterion?.qualityInfo);

    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      campaign_id: c.id ? String(c.id) : null,
      ad_group_id: ag.id ? String(ag.id) : "",
      criterion_id: String(criterion.criterionId || ""),
      campaign_name: c.name || null,
      ad_group_name: ag.name || null,
      keyword_text: keyword.text || "",
      match_type: keyword.matchType || null,
      status: criterion.status || null,
      negative: Boolean(criterion.negative),
      quality_score: toNumber(qualityInfo.qualityScore, null),
      ad_relevance: qualityInfo.creativeQualityScore || null,
      landing_page_experience: qualityInfo.postClickQualityScore || null,
      expected_click_through_rate: qualityInfo.searchPredictedCtr || null,
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.ad_group_id && r.criterion_id && r.keyword_text);

  return supabaseUpsert("google_ads_keywords", mapped, "owner_id,customer_id,ad_group_id,criterion_id");
}

async function syncGoogleAdsCriteria(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const campaignRows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `campaign_criteria:${params.customer_id}`,
    query: `
      SELECT
        campaign.id,
        campaign_criterion.criterion_id,
        campaign_criterion.type,
        campaign_criterion.status,
        campaign_criterion.negative,
        campaign_criterion.keyword.text,
        campaign_criterion.keyword.match_type,
        campaign_criterion.location.geo_target_constant,
        campaign_criterion.language.language_constant,
        campaign_criterion.ad_schedule.day_of_week,
        campaign_criterion.ad_schedule.start_hour,
        campaign_criterion.ad_schedule.start_minute,
        campaign_criterion.ad_schedule.end_hour,
        campaign_criterion.ad_schedule.end_minute
      FROM campaign_criterion
      WHERE campaign_criterion.status != 'REMOVED'
    `,
  });

  const adGroupRows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `ad_group_criteria:${params.customer_id}`,
    query: `
      SELECT
        campaign.id,
        ad_group.id,
        ad_group_criterion.criterion_id,
        ad_group_criterion.type,
        ad_group_criterion.status,
        ad_group_criterion.negative,
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        ad_group_criterion.location.geo_target_constant,
        ad_group_criterion.age_range.type,
        ad_group_criterion.gender.type,
        ad_group_criterion.parental_status.type,
        ad_group_criterion.income_range.type,
        ad_group_criterion.placement.url,
        ad_group_criterion.topic.topic_constant,
        ad_group_criterion.user_list.user_list,
        ad_group_criterion.audience.audience
      FROM ad_group_criterion
      WHERE ad_group_criterion.status != 'REMOVED'
    `,
  });

  const customerId = cleanGoogleAdsCustomerId(params.customer_id);
  const mapped: Json[] = [
    ...campaignRows.map((r) => googleAdsCriterionFields("CAMPAIGN", r)),
    ...adGroupRows.map((r) => googleAdsCriterionFields("AD_GROUP", r)),
  ].map((row: Json) => ({
    ...row,
    id: googleAdsStableUuid("google_ads_criteria", customerId, row.scope, row.campaign_id, row.ad_group_id, row.criterion_id),
    owner_id: params.owner_id,
    customer_id: customerId,
  })).filter((r: Json) => r.criterion_id);

  return googleAdsUpsertById("google_ads_criteria", mapped);
}

async function syncGoogleAdsConversionActions(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `conversion_actions:${params.customer_id}`,
    query: `
      SELECT
        conversion_action.id,
        conversion_action.resource_name,
        conversion_action.name,
        conversion_action.type,
        conversion_action.category,
        conversion_action.status,
        conversion_action.origin,
        conversion_action.primary_for_goal,
        conversion_action.include_in_conversions_metric,
        conversion_action.counting_type,
        conversion_action.attribution_model_settings.attribution_model,
        conversion_action.click_through_lookback_window_days,
        conversion_action.view_through_lookback_window_days,
        conversion_action.value_settings.default_value,
        conversion_action.value_settings.default_currency_code,
        conversion_action.value_settings.always_use_default_value
      FROM conversion_action
    `,
  });

  const mapped = rows.map((r) => {
    const a = asObject(r?.conversionAction);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      conversion_action_id: String(a.id || getGoogleAdsIdFromResource(a.resourceName) || ""),
      resource_name: a.resourceName || null,
      name: a.name || null,
      type: a.type || null,
      category: a.category || null,
      status: a.status || null,
      origin: a.origin || null,
      primary_for_goal: a.primaryForGoal ?? null,
      include_in_conversions_metric: a.includeInConversionsMetric ?? null,
      counting_type: a.countingType || null,
      attribution_model: a?.attributionModelSettings?.attributionModel || null,
      click_through_lookback_window_days: toNumber(a.clickThroughLookbackWindowDays, null),
      view_through_lookback_window_days: toNumber(a.viewThroughLookbackWindowDays, null),
      value_settings: a.valueSettings || {},
      tag_snippets: googleAdsArray(a.tagSnippets),
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.conversion_action_id);

  return supabaseUpsert(
    "google_ads_conversion_actions",
    mapped,
    "owner_id,customer_id,conversion_action_id"
  );
}

async function syncGoogleAdsAssets(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `assets:${params.customer_id}`,
    query: `
      SELECT
        asset.id,
        asset.resource_name,
        asset.name,
        asset.type,
        asset.text_asset.text,
        asset.image_asset.full_size.url,
        asset.image_asset.full_size.width_pixels,
        asset.image_asset.full_size.height_pixels,
        asset.youtube_video_asset.youtube_video_id,
        asset.youtube_video_asset.youtube_video_title,
        asset.call_asset.phone_number,
        asset.sitelink_asset.link_text,
        asset.sitelink_asset.description1,
        asset.sitelink_asset.description2,
        asset.final_urls
      FROM asset
    `,
  });

  const mapped = rows.map((r) => {
    const a = asObject(r?.asset);
    const image = asObject(a?.imageAsset);
    const fullSize = asObject(image?.fullSize);
    const yt = asObject(a?.youtubeVideoAsset);
    const call = asObject(a?.callAsset);
    const sitelink = asObject(a?.sitelinkAsset);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      asset_id: String(a.id || getGoogleAdsIdFromResource(a.resourceName) || ""),
      resource_name: a.resourceName || null,
      name: a.name || null,
      type: a.type || null,
      status: a.status || null,
      text_asset_text: a?.textAsset?.text || null,
      image_full_size_url: fullSize.url || null,
      image_file_size: toBigIntNumber(image.fileSize, 0),
      image_mime_type: image.mimeType || null,
      image_width: toNumber(fullSize.widthPixels, null),
      image_height: toNumber(fullSize.heightPixels, null),
      youtube_video_id: yt.youtubeVideoId || null,
      youtube_video_title: yt.youtubeVideoTitle || null,
      call_asset_phone_number: call.phoneNumber || null,
      sitelink_title: sitelink.linkText || null,
      sitelink_description_1: sitelink.description1 || null,
      sitelink_description_2: sitelink.description2 || null,
      sitelink_final_urls: googleAdsArray(a.finalUrls),
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.asset_id);

  return supabaseUpsert("google_ads_assets", mapped, "owner_id,customer_id,asset_id");
}

function googleAdsAssetLinkRow(params: {
  owner_id: string;
  customer_id: string;
  link_scope: "CUSTOMER" | "CAMPAIGN" | "AD_GROUP" | "AD" | "ASSET_GROUP";
  row: any;
}) {
  const r = params.row;
  const c = asObject(r?.campaign);
  const ag = asObject(r?.adGroup);
  const aga = asObject(r?.adGroupAd);
  const ad = asObject(aga?.ad);
  const assetGroup = asObject(r?.assetGroup);
  const asset = asObject(r?.asset);
  const link = asObject(
    r?.customerAsset ||
    r?.campaignAsset ||
    r?.adGroupAsset ||
    r?.adGroupAdAssetView ||
    r?.assetGroupAsset
  );

  const assetId = String(asset.id || getGoogleAdsIdFromResource(link.asset) || "");
  const campaignId = c.id ? String(c.id) : null;
  const adGroupId = ag.id ? String(ag.id) : null;
  const adId = ad.id ? String(ad.id) : null;
  const assetGroupId = assetGroup.id ? String(assetGroup.id) : null;
  const fieldType = link.fieldType || null;

  return addStableGoogleAdsId(
    "google_ads_asset_links",
    {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      link_scope: params.link_scope,
      campaign_id: campaignId,
      ad_group_id: adGroupId,
      ad_id: adId,
      asset_group_id: assetGroupId,
      asset_id: assetId || null,
      asset_resource_name: asset.resourceName || link.asset || null,
      field_type: fieldType,
      status: link.status || null,
      primary_status: link.primaryStatus || null,
      primary_status_reasons: googleAdsArray(link.primaryStatusReasons),
      performance_label: link.performanceLabel || null,
      source: link.source || null,
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    },
    params.owner_id,
    params.customer_id,
    params.link_scope,
    campaignId,
    adGroupId,
    adId,
    assetGroupId,
    assetId,
    fieldType
  );
}

async function syncGoogleAdsAssetLinks(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const customerRows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `customer_assets:${params.customer_id}`,
    query: `
      SELECT
        customer_asset.asset,
        customer_asset.field_type,
        customer_asset.status,
        asset.id,
        asset.resource_name,
        asset.type
      FROM customer_asset
      WHERE customer_asset.status != 'REMOVED'
    `,
  });

  const campaignRows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `campaign_assets:${params.customer_id}`,
    query: `
      SELECT
        campaign.id,
        campaign_asset.asset,
        campaign_asset.field_type,
        campaign_asset.status,
        campaign_asset.primary_status,
        campaign_asset.primary_status_reasons,
        asset.id,
        asset.resource_name,
        asset.type
      FROM campaign_asset
      WHERE campaign_asset.status != 'REMOVED'
    `,
  });

  const adGroupRows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `ad_group_assets:${params.customer_id}`,
    query: `
      SELECT
        campaign.id,
        ad_group.id,
        ad_group_asset.asset,
        ad_group_asset.field_type,
        ad_group_asset.status,
        ad_group_asset.primary_status,
        ad_group_asset.primary_status_reasons,
        asset.id,
        asset.resource_name,
        asset.type
      FROM ad_group_asset
      WHERE ad_group_asset.status != 'REMOVED'
    `,
  });

  const mapped = [
    ...customerRows.map((row) => googleAdsAssetLinkRow({ ...params, link_scope: "CUSTOMER", row })),
    ...campaignRows.map((row) => googleAdsAssetLinkRow({ ...params, link_scope: "CAMPAIGN", row })),
    ...adGroupRows.map((row) => googleAdsAssetLinkRow({ ...params, link_scope: "AD_GROUP", row })),
  ].filter((r) => r.asset_id || r.asset_resource_name);

  return googleAdsUpsertById("google_ads_asset_links", mapped);
}

async function syncGoogleAdsAssetGroups(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `asset_groups:${params.customer_id}`,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        asset_group.id,
        asset_group.resource_name,
        asset_group.name,
        asset_group.status,
        asset_group.primary_status,
        asset_group.primary_status_reasons,
        asset_group.ad_strength,
        asset_group.final_urls,
        asset_group.final_mobile_urls,
        asset_group.path1,
        asset_group.path2
      FROM asset_group
      WHERE asset_group.status != 'REMOVED'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.assetGroup);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      campaign_id: c.id ? String(c.id) : "",
      asset_group_id: String(ag.id || getGoogleAdsIdFromResource(ag.resourceName) || ""),
      resource_name: ag.resourceName || null,
      campaign_name: c.name || null,
      name: ag.name || null,
      status: ag.status || null,
      primary_status: ag.primaryStatus || null,
      primary_status_reasons: googleAdsArray(ag.primaryStatusReasons),
      ad_strength: ag.adStrength || null,
      final_urls: googleAdsArray(ag.finalUrls),
      final_mobile_urls: googleAdsArray(ag.finalMobileUrls),
      path1: ag.path1 || null,
      path2: ag.path2 || null,
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.campaign_id && r.asset_group_id);

  return supabaseUpsert("google_ads_asset_groups", mapped, "owner_id,customer_id,asset_group_id");
}

async function syncGoogleAdsAssetGroupAssets(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `asset_group_assets:${params.customer_id}`,
    query: `
      SELECT
        campaign.id,
        asset_group.id,
        asset_group_asset.asset,
        asset_group_asset.field_type,
        asset_group_asset.status,
        asset_group_asset.source,
        asset.id,
        asset.resource_name,
        asset.type
      FROM asset_group_asset
      WHERE asset_group_asset.status != 'REMOVED'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.assetGroup);
    const aga = asObject(r?.assetGroupAsset);
    const asset = asObject(r?.asset);
    const assetId = String(asset.id || getGoogleAdsIdFromResource(aga.asset) || "");
    return addStableGoogleAdsId(
      "google_ads_asset_group_assets",
      {
        owner_id: params.owner_id,
        provider: "google_ads",
        customer_id: cleanGoogleAdsCustomerId(params.customer_id),
        campaign_id: c.id ? String(c.id) : null,
        asset_group_id: String(ag.id || getGoogleAdsIdFromResource(aga.assetGroup) || ""),
        asset_id: assetId || null,
        asset_resource_name: asset.resourceName || aga.asset || null,
        field_type: aga.fieldType || null,
        status: aga.status || null,
        primary_status: null,
        primary_status_reasons: [],
        performance_label: null,
        source: aga.source || null,
        raw_json: r,
        last_synced_at: new Date().toISOString(),
      },
      params.owner_id,
      params.customer_id,
      ag.id,
      assetId,
      aga.fieldType
    );
  }).filter((r) => r.asset_group_id && (r.asset_id || r.asset_resource_name));

  return googleAdsUpsertById("google_ads_asset_group_assets", mapped);
}

async function syncGoogleAdsAssetGroupSignals(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `asset_group_signals:${params.customer_id}`,
    query: `
      SELECT
        campaign.id,
        asset_group.id,
        asset_group_signal.resource_name,
        asset_group_signal.asset_group,
        asset_group_signal.audience.audience,
        asset_group_signal.search_theme.text
      FROM asset_group_signal
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.assetGroup);
    const s = asObject(r?.assetGroupSignal);
    const signalId = getGoogleAdsIdFromResource(s.resourceName) || s.resourceName || "";
    return addStableGoogleAdsId(
      "google_ads_asset_group_signals",
      {
        owner_id: params.owner_id,
        provider: "google_ads",
        customer_id: cleanGoogleAdsCustomerId(params.customer_id),
        campaign_id: c.id ? String(c.id) : null,
        asset_group_id: String(ag.id || getGoogleAdsIdFromResource(s.assetGroup) || ""),
        signal_id: signalId,
        signal_type: s.audience ? "AUDIENCE" : s.searchTheme ? "SEARCH_THEME" : null,
        audience_resource_name: s?.audience?.audience || null,
        search_theme: s?.searchTheme?.text || null,
        user_interest: null,
        user_list: null,
        custom_segment: null,
        raw_json: r,
        last_synced_at: new Date().toISOString(),
      },
      params.owner_id,
      params.customer_id,
      ag.id,
      signalId,
      s?.searchTheme?.text
    );
  }).filter((r) => r.asset_group_id && (r.signal_id || r.audience_resource_name || r.search_theme));

  return googleAdsUpsertById("google_ads_asset_group_signals", mapped);
}

async function syncGoogleAdsAssetGroupTopCombinations(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `asset_group_top_combinations:${params.customer_id}`,
    query: `
      SELECT
        campaign.id,
        asset_group.id,
        asset_group_top_combination_view.asset_group_top_combinations
      FROM asset_group_top_combination_view
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.assetGroup);
    const v = asObject(r?.assetGroupTopCombinationView);
    const combinations = googleAdsArray(v.assetGroupTopCombinations);

    return addStableGoogleAdsId(
      "google_ads_asset_group_top_combinations",
      {
        owner_id: params.owner_id,
        provider: "google_ads",
        customer_id: cleanGoogleAdsCustomerId(params.customer_id),
        campaign_id: c.id ? String(c.id) : null,
        asset_group_id: ag.id ? String(ag.id) : null,
        served_assets: combinations,
        raw_json: r,
        last_synced_at: new Date().toISOString(),
      },
      params.owner_id,
      params.customer_id,
      c.id,
      ag.id,
      JSON.stringify(combinations)
    );
  }).filter((r) => r.asset_group_id || googleAdsArray(r.served_assets).length);

  return googleAdsUpsertById("google_ads_asset_group_top_combinations", mapped);
}
async function syncGoogleAdsRecommendations(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `recommendations:${params.customer_id}`,
    query: `
      SELECT
        recommendation.resource_name,
        recommendation.type,
        recommendation.campaign,
        recommendation.ad_group,
        recommendation.dismissed
      FROM recommendation
    `,
  });

  const mapped = rows.map((r) => {
    const rec = asObject(r?.recommendation);
    const resourceName = rec.resourceName || "";
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      recommendation_id: getGoogleAdsIdFromResource(resourceName) || resourceName,
      resource_name: resourceName || null,
      type: rec.type || null,
      campaign_id: getGoogleAdsIdFromResource(rec.campaign),
      ad_group_id: getGoogleAdsIdFromResource(rec.adGroup),
      dismissed: Boolean(rec.dismissed),
      optimization_score_uplift: null,
      impact_base_metrics: rec?.impact?.baseMetrics || {},
      impact_potential_metrics: rec?.impact?.potentialMetrics || {},
      recommendation_payload: rec,
      raw_json: r,
      last_synced_at: new Date().toISOString(),
    };
  }).filter((r) => r.recommendation_id);

  return supabaseUpsert("google_ads_recommendations", mapped, "owner_id,customer_id,recommendation_id");
}

async function syncGoogleAdsSearchTermMetrics(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
}) {
  let rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `search_terms:${params.customer_id}`,
    query: `
      SELECT
        segments.date,
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        search_term_view.search_term,
        search_term_view.status,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value
      FROM search_term_view
      WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
    `,
  });

  if (!rows.length) {
    rows = await tryGoogleAdsSearchStream({
      owner_id: params.owner_id,
      customer_id: params.customer_id,
      login_customer_id: params.login_customer_id,
      label: `search_terms_fallback:${params.customer_id}`,
      query: `
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          search_term_view.search_term,
          search_term_view.status,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.cost_micros,
          metrics.average_cpc,
          metrics.average_cpm,
          metrics.conversions,
          metrics.conversions_from_interactions_rate,
          metrics.cost_per_conversion,
          metrics.conversions_value
        FROM search_term_view
        WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
      `,
    });
  }

  const now = new Date().toISOString();
  const customerId = cleanGoogleAdsCustomerId(params.customer_id);

  const baseRows = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.adGroup);
    const view = asObject(r?.searchTermView);
    const criterion = asObject(r?.adGroupCriterion);
    const keyword = asObject(criterion?.keyword);
    const metrics = asObject(r?.metrics);
    const seg = googleSegments(r);
    const searchTerm = view.searchTerm || "";

    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: customerId,
      campaign_id: c.id ? String(c.id) : null,
      campaign_name: c.name || null,
      ad_group_id: ag.id ? String(ag.id) : null,
      ad_group_name: ag.name || null,
      search_term: searchTerm,
      keyword_text: keyword.text || null,
      keyword_match_type: keyword.matchType || null,
      status: view.status || null,
      date: seg.date,
      impressions: toBigIntNumber(metrics.impressions, 0),
      clicks: toBigIntNumber(metrics.clicks, 0),
      ctr: toNumber(metrics.ctr, null),
      cost: microsToAmount(metrics.costMicros),
      average_cpc: microsToAmount(metrics.averageCpc),
      average_cpm: microsToAmount(metrics.averageCpm),
      conversions: toNumber(metrics.conversions, null),
      conversion_rate: toNumber(metrics.conversionsFromInteractionsRate, null),
      cost_per_conversion: microsToAmount(metrics.costPerConversion),
      conversion_value: toNumber(metrics.conversionsValue, null),
      raw: r,
      raw_json: r,
      first_seen_at: now,
      last_seen_at: now,
      last_synced_at: now,
    };
  }).filter((r) => r.search_term && r.date);

  const structureRows = baseRows.map((r) => addStableGoogleAdsId(
    "google_ads_search_terms",
    {
      owner_id: r.owner_id,
      provider: r.provider,
      customer_id: r.customer_id,
      campaign_id: r.campaign_id,
      ad_group_id: r.ad_group_id,
      campaign_name: r.campaign_name,
      ad_group_name: r.ad_group_name,
      search_term: r.search_term,
      keyword_text: r.keyword_text,
      keyword_match_type: r.keyword_match_type,
      status: r.status,
      raw_json: r.raw_json,
      first_seen_at: r.first_seen_at,
      last_seen_at: r.last_seen_at,
      last_synced_at: r.last_synced_at,
    },
    r.owner_id,
    r.customer_id,
    r.campaign_id,
    r.ad_group_id,
    r.search_term
  ));

  const metricRows = baseRows.map((r) => addStableGoogleAdsId(
    "ads_metrics_search_terms",
    {
      owner_id: r.owner_id,
      provider: r.provider,
      customer_id: r.customer_id,
      campaign_id: r.campaign_id,
      campaign_name: r.campaign_name,
      ad_group_id: r.ad_group_id,
      ad_group_name: r.ad_group_name,
      search_term: r.search_term,
      keyword_text: r.keyword_text,
      keyword_match_type: r.keyword_match_type,
      date: r.date,
      impressions: r.impressions,
      clicks: r.clicks,
      ctr: r.ctr,
      cost: r.cost,
      average_cpc: r.average_cpc,
      average_cpm: r.average_cpm,
      conversions: r.conversions,
      conversion_rate: r.conversion_rate,
      cost_per_conversion: r.cost_per_conversion,
      conversion_value: r.conversion_value,
      raw: r.raw,
    },
    r.owner_id,
    r.customer_id,
    r.campaign_id,
    r.ad_group_id,
    r.search_term,
    r.date
  ));

  const a = await googleAdsUpsertById("google_ads_search_terms", structureRows);
  const b = await googleAdsUpsertById("ads_metrics_search_terms", metricRows);
  return a + b;
}

async function syncGoogleAdsLandingPageMetrics(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `landing_pages:${params.customer_id}`,
    query: `
      SELECT
        segments.date,
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        landing_page_view.unexpanded_final_url,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value
      FROM landing_page_view
      WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
    `,
  });

  const now = new Date().toISOString();
  const customerId = cleanGoogleAdsCustomerId(params.customer_id);
  const baseRows = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.adGroup);
    const lp = asObject(r?.landingPageView);
    const metrics = asObject(r?.metrics);
    const seg = googleSegments(r);
    const finalUrl = lp.unexpandedFinalUrl || "";

    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: customerId,
      campaign_id: c.id ? String(c.id) : null,
      campaign_name: c.name || null,
      ad_group_id: ag.id ? String(ag.id) : null,
      ad_group_name: ag.name || null,
      final_url: finalUrl,
      expanded_final_url: null,
      date: seg.date,
      impressions: toBigIntNumber(metrics.impressions, 0),
      clicks: toBigIntNumber(metrics.clicks, 0),
      ctr: toNumber(metrics.ctr, null),
      cost: microsToAmount(metrics.costMicros),
      conversions: toNumber(metrics.conversions, null),
      conversion_rate: toNumber(metrics.conversionsFromInteractionsRate, null),
      cost_per_conversion: microsToAmount(metrics.costPerConversion),
      conversion_value: toNumber(metrics.conversionsValue, null),
      raw: r,
      raw_json: r,
      first_seen_at: now,
      last_seen_at: now,
      last_synced_at: now,
    };
  }).filter((r) => r.final_url && r.date);

  const structureRows = baseRows.map((r) => addStableGoogleAdsId(
    "google_ads_landing_pages",
    {
      owner_id: r.owner_id,
      provider: r.provider,
      customer_id: r.customer_id,
      campaign_id: r.campaign_id,
      ad_group_id: r.ad_group_id,
      final_url: r.final_url,
      expanded_final_url: r.expanded_final_url,
      raw_json: r.raw_json,
      first_seen_at: r.first_seen_at,
      last_seen_at: r.last_seen_at,
      last_synced_at: r.last_synced_at,
    },
    r.owner_id,
    r.customer_id,
    r.campaign_id,
    r.ad_group_id,
    r.final_url
  ));

  const metricRows = baseRows.map((r) => addStableGoogleAdsId(
    "ads_metrics_landing_pages",
    {
      owner_id: r.owner_id,
      provider: r.provider,
      customer_id: r.customer_id,
      campaign_id: r.campaign_id,
      campaign_name: r.campaign_name,
      ad_group_id: r.ad_group_id,
      ad_group_name: r.ad_group_name,
      final_url: r.final_url,
      expanded_final_url: r.expanded_final_url,
      date: r.date,
      impressions: r.impressions,
      clicks: r.clicks,
      ctr: r.ctr,
      cost: r.cost,
      conversions: r.conversions,
      conversion_rate: r.conversion_rate,
      cost_per_conversion: r.cost_per_conversion,
      conversion_value: r.conversion_value,
      raw: r.raw,
    },
    r.owner_id,
    r.customer_id,
    r.campaign_id,
    r.ad_group_id,
    r.final_url,
    r.date
  ));

  const a = await googleAdsUpsertById("google_ads_landing_pages", structureRows);
  const b = await googleAdsUpsertById("ads_metrics_landing_pages", metricRows);
  return a + b;
}

async function syncGoogleAdsAssetMetrics(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
}) {
  const campaignAssetRows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `campaign_asset_metrics:${params.customer_id}`,
    query: `
      SELECT
        segments.date,
        campaign.id,
        campaign.name,
        campaign_asset.asset,
        campaign_asset.field_type,
        campaign_asset.status,
        asset.id,
        asset.resource_name,
        asset.type,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value
      FROM campaign_asset
      WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
        AND campaign_asset.status != 'REMOVED'
    `,
  });

  const adGroupAssetRows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `ad_group_asset_metrics:${params.customer_id}`,
    query: `
      SELECT
        segments.date,
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        ad_group_asset.asset,
        ad_group_asset.field_type,
        ad_group_asset.status,
        asset.id,
        asset.resource_name,
        asset.type,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value
      FROM ad_group_asset
      WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
        AND ad_group_asset.status != 'REMOVED'
    `,
  });

  const rows = [
    ...campaignAssetRows.map((r) => ({ r, scope: "CAMPAIGN" })),
    ...adGroupAssetRows.map((r) => ({ r, scope: "AD_GROUP" })),
  ];

  const mapped = rows.map(({ r, scope }) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.adGroup);
    const asset = asObject(r?.asset);
    const link = scope === "CAMPAIGN" ? asObject(r?.campaignAsset) : asObject(r?.adGroupAsset);
    const metrics = asObject(r?.metrics);
    const seg = googleSegments(r);
    const assetId = String(asset.id || getGoogleAdsIdFromResource(link.asset) || "");
    return addStableGoogleAdsId(
      "ads_metrics_assets",
      {
        owner_id: params.owner_id,
        provider: "google_ads",
        customer_id: cleanGoogleAdsCustomerId(params.customer_id),
        campaign_id: c.id ? String(c.id) : null,
        campaign_name: c.name || null,
        ad_group_id: ag.id ? String(ag.id) : null,
        ad_group_name: ag.name || null,
        ad_id: null,
        asset_group_id: null,
        asset_id: assetId || null,
        asset_resource_name: asset.resourceName || link.asset || null,
        asset_type: asset.type || null,
        field_type: link.fieldType || null,
        performance_label: link.performanceLabel || null,
        source: null,
        date: seg.date,
        impressions: toBigIntNumber(metrics.impressions, 0),
        clicks: toBigIntNumber(metrics.clicks, 0),
        ctr: toNumber(metrics.ctr, null),
        cost: microsToAmount(metrics.costMicros),
        average_cpc: microsToAmount(metrics.averageCpc),
        average_cpm: microsToAmount(metrics.averageCpm),
        conversions: toNumber(metrics.conversions, null),
        conversion_rate: toNumber(metrics.conversionsFromInteractionsRate, null),
        cost_per_conversion: microsToAmount(metrics.costPerConversion),
        conversion_value: toNumber(metrics.conversionsValue, null),
        raw: r,
      },
      params.owner_id,
      params.customer_id,
      scope,
      c.id,
      ag.id,
      assetId,
      link.fieldType,
      seg.date
    );
  }).filter((r) => r.date && (r.asset_id || r.asset_resource_name));

  return googleAdsUpsertById("ads_metrics_assets", mapped);
}

async function syncGoogleAdsAssetGroupMetrics(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `asset_group_metrics:${params.customer_id}`,
    query: `
      SELECT
        segments.date,
        segments.device,
        segments.ad_network_type,
        segments.day_of_week,
        campaign.id,
        campaign.name,
        asset_group.id,
        asset_group.name,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value,
        metrics.all_conversions,
        metrics.all_conversions_value
      FROM asset_group
      WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
    `,
  });

  const mapped = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.assetGroup);
    const seg = googleSegments(r);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      campaign_id: c.id ? String(c.id) : null,
      campaign_name: c.name || null,
      asset_group_id: String(ag.id || ""),
      asset_group_name: ag.name || null,
      date: seg.date,
      segments_device: seg.segments_device,
      segments_ad_network_type: seg.segments_ad_network_type,
      segments_day_of_week: seg.segments_day_of_week,
      ...metricRowBase(r),
      raw: r,
    };
  }).filter((r) => r.asset_group_id && r.date);

  return supabaseUpsert("ads_metrics_asset_groups", mapped, "owner_id,provider,customer_id,asset_group_id,date");
}

async function syncGoogleAdsShoppingProductMetrics(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
}) {
  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `shopping_products:${params.customer_id}`,
    query: `
      SELECT
        segments.date,
        campaign.id,
        campaign.name,
        ad_group.id,
        segments.product_merchant_id,
        segments.product_item_id,
        segments.product_title,
        segments.product_brand,
        segments.product_category_level1,
        segments.product_type_l1,
        segments.product_channel,
        segments.product_condition,
        segments.product_language,
        segments.product_country,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value,
        metrics.all_conversions,
        metrics.all_conversions_value
      FROM shopping_performance_view
      WHERE segments.date BETWEEN '${params.date_from}' AND '${params.date_to}'
    `,
  });

  const now = new Date().toISOString();
  const customerId = cleanGoogleAdsCustomerId(params.customer_id);
  const baseRows = rows.map((r) => {
    const c = asObject(r?.campaign);
    const ag = asObject(r?.adGroup);
    const s = asObject(r?.segments);
    const m = asObject(r?.metrics);
    const productItemId = s.productItemId || "";
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: customerId,
      campaign_id: c.id ? String(c.id) : null,
      campaign_name: c.name || null,
      ad_group_id: ag.id ? String(ag.id) : null,
      asset_group_id: null,
      merchant_center_id: s.productMerchantId ? String(s.productMerchantId) : null,
      product_item_id: productItemId,
      product_title: s.productTitle || null,
      product_brand: s.productBrand || null,
      product_category_l1: s.productCategoryLevel1 || null,
      product_type_l1: s.productTypeL1 || null,
      product_channel: s.productChannel || null,
      product_condition: s.productCondition || null,
      product_language: s.productLanguage || null,
      product_country: s.productCountry || null,
      date: s.date || null,
      impressions: toBigIntNumber(m.impressions, 0),
      clicks: toBigIntNumber(m.clicks, 0),
      ctr: toNumber(m.ctr, null),
      cost: microsToAmount(m.costMicros),
      conversions: toNumber(m.conversions, null),
      conversion_rate: toNumber(m.conversionsFromInteractionsRate, null),
      cost_per_conversion: microsToAmount(m.costPerConversion),
      conversion_value: toNumber(m.conversionsValue, null),
      all_conversions: toNumber(m.allConversions, null),
      all_conversions_value: toNumber(m.allConversionsValue, null),
      raw: r,
      raw_json: r,
      first_seen_at: now,
      last_seen_at: now,
      last_synced_at: now,
    };
  }).filter((r) => r.product_item_id && r.date);

  const structureRows = baseRows.map((r) => addStableGoogleAdsId(
    "google_ads_shopping_products",
    {
      owner_id: r.owner_id,
      provider: r.provider,
      customer_id: r.customer_id,
      campaign_id: r.campaign_id,
      ad_group_id: r.ad_group_id,
      asset_group_id: r.asset_group_id,
      merchant_center_id: r.merchant_center_id,
      product_item_id: r.product_item_id,
      product_title: r.product_title,
      product_brand: r.product_brand,
      product_category_l1: r.product_category_l1,
      product_type_l1: r.product_type_l1,
      product_channel: r.product_channel,
      product_condition: r.product_condition,
      product_language: r.product_language,
      product_country: r.product_country,
      raw_json: r.raw_json,
      first_seen_at: r.first_seen_at,
      last_seen_at: r.last_seen_at,
      last_synced_at: r.last_synced_at,
    },
    r.owner_id,
    r.customer_id,
    r.product_item_id,
    r.campaign_id,
    r.ad_group_id,
    r.asset_group_id
  ));

  const metricRows = baseRows.map((r) => addStableGoogleAdsId(
    "ads_metrics_shopping_products",
    {
      owner_id: r.owner_id,
      provider: r.provider,
      customer_id: r.customer_id,
      campaign_id: r.campaign_id,
      campaign_name: r.campaign_name,
      ad_group_id: r.ad_group_id,
      asset_group_id: r.asset_group_id,
      merchant_center_id: r.merchant_center_id,
      product_item_id: r.product_item_id,
      product_title: r.product_title,
      product_brand: r.product_brand,
      product_category_l1: r.product_category_l1,
      product_type_l1: r.product_type_l1,
      date: r.date,
      impressions: r.impressions,
      clicks: r.clicks,
      ctr: r.ctr,
      cost: r.cost,
      conversions: r.conversions,
      conversion_rate: r.conversion_rate,
      cost_per_conversion: r.cost_per_conversion,
      conversion_value: r.conversion_value,
      all_conversions: r.all_conversions,
      all_conversions_value: r.all_conversions_value,
      raw: r.raw,
    },
    r.owner_id,
    r.customer_id,
    r.product_item_id,
    r.campaign_id,
    r.ad_group_id,
    r.asset_group_id,
    r.date
  ));

  const a = await googleAdsUpsertById("google_ads_shopping_products", structureRows);
  const b = await googleAdsUpsertById("ads_metrics_shopping_products", metricRows);
  return a + b;
}

async function syncGoogleAdsChangeEvents(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
}) {
  const minChangeEventDate = new Date();
  minChangeEventDate.setDate(minChangeEventDate.getDate() - 29);

  const minChangeEventDateStr = minChangeEventDate.toISOString().slice(0, 10);
  const todayStr = new Date().toISOString().slice(0, 10);

  const safeDateFrom =
    params.date_from && params.date_from > minChangeEventDateStr
      ? params.date_from
      : minChangeEventDateStr;

  const safeDateTo =
    params.date_to && params.date_to < todayStr
      ? params.date_to
      : todayStr;

  const rows = await tryGoogleAdsSearchStream({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    label: `change_events:${params.customer_id}`,
    query: `
      SELECT
        change_event.resource_name,
        change_event.change_date_time,
        change_event.user_email,
        change_event.client_type,
        change_event.change_resource_type,
        change_event.change_resource_name,
        change_event.resource_change_operation,
        change_event.changed_fields,
        change_event.old_resource,
        change_event.new_resource,
        change_event.campaign,
        change_event.ad_group
      FROM change_event
      WHERE change_event.change_date_time >= '${safeDateFrom} 00:00:00'
        AND change_event.change_date_time <= '${safeDateTo} 23:59:59'
      ORDER BY change_event.change_date_time DESC
      LIMIT 10000
    `,
  });

  const mapped = rows.map((r) => {
    const e = asObject(r?.changeEvent);
    return {
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: cleanGoogleAdsCustomerId(params.customer_id),
      change_event_resource_name: e.resourceName || "",
      change_date_time: e.changeDateTime || null,
      user_email: e.userEmail || null,
      client_type: e.clientType || null,
      resource_type: e.changeResourceType || null,
      resource_name: e.changeResourceName || null,
      campaign_id: getGoogleAdsIdFromResource(e.campaign),
      ad_group_id: getGoogleAdsIdFromResource(e.adGroup),
      ad_id: null,
      changed_fields: googleAdsChangedFields(e.changedFields),
      old_resource: e.oldResource || {},
      new_resource: e.newResource || {},
      raw_json: r,
    };
  }).filter((r) => r.change_event_resource_name);

  return supabaseUpsert("google_ads_change_events", mapped, "owner_id,customer_id,change_event_resource_name");
}

async function syncGoogleAdsStructureForCustomer(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const counters: SyncCounters = {};

  const budgets = await syncGoogleAdsBudgets(params);
  inc(counters, "budgets_synced", budgets);

  const bidding = await syncGoogleAdsBiddingStrategies(params);
  inc(counters, "bidding_strategies_synced", bidding);

  const campaigns = await syncGoogleAdsCampaigns(params);
  inc(counters, "campaigns_synced", campaigns);

  const adGroups = await syncGoogleAdsAdGroups(params);
  inc(counters, "ad_groups_synced", adGroups);

  const ads = await syncGoogleAdsAds(params);
  inc(counters, "ads_synced", ads);

  inc(counters, "keywords_synced", await optionalGoogleAdsSync("keywords", () => syncGoogleAdsKeywords(params)));
  inc(counters, "criteria_synced", await optionalGoogleAdsSync("criteria", () => syncGoogleAdsCriteria(params)));
  inc(counters, "conversion_actions_synced", await optionalGoogleAdsSync("conversion_actions", () => syncGoogleAdsConversionActions(params)));
  inc(counters, "assets_synced", await optionalGoogleAdsSync("assets", () => syncGoogleAdsAssets(params)));
  inc(counters, "asset_links_synced", await optionalGoogleAdsSync("asset_links", () => syncGoogleAdsAssetLinks(params)));
  inc(counters, "asset_groups_synced", await optionalGoogleAdsSync("asset_groups", () => syncGoogleAdsAssetGroups(params)));
  inc(counters, "asset_group_assets_synced", await optionalGoogleAdsSync("asset_group_assets", () => syncGoogleAdsAssetGroupAssets(params)));
  inc(counters, "asset_group_signals_synced", await optionalGoogleAdsSync("asset_group_signals", () => syncGoogleAdsAssetGroupSignals(params)));
  inc(counters, "asset_group_top_combinations_synced", await optionalGoogleAdsSync("asset_group_top_combinations", () => syncGoogleAdsAssetGroupTopCombinations(params)));
  inc(counters, "recommendations_synced", await optionalGoogleAdsSync("recommendations", () => syncGoogleAdsRecommendations(params)));

  return counters;
}

async function syncGoogleAdsMetricsForCustomer(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  date_from: string;
  date_to: string;
  level?: GoogleAdsSyncLevel;
}) {
  const counters: SyncCounters = {};
  const level = params.level || "campaign";

  if (level === "campaign") {
    const n = await syncGoogleAdsCampaignMetrics(params);
    inc(counters, "campaign_metrics_synced", n);
  }

  if (level === "ad_group") {
    const n = await syncGoogleAdsAdGroupMetrics(params);
    inc(counters, "ad_group_metrics_synced", n);
  }

  if (level === "ad") {
    const n = await syncGoogleAdsAdMetrics(params);
    inc(counters, "ad_metrics_synced", n);
  }

  if (level === "keyword") {
    const n = await syncGoogleAdsKeywordMetrics(params);
    inc(counters, "keyword_metrics_synced", n);
  }

  if (level === "search_term") {
    const n = await optionalGoogleAdsSync("search_term_metrics", () => syncGoogleAdsSearchTermMetrics(params));
    inc(counters, "search_term_metrics_synced", n);
  }

  if (level === "landing_page") {
    const n = await optionalGoogleAdsSync("landing_page_metrics", () => syncGoogleAdsLandingPageMetrics(params));
    inc(counters, "landing_page_metrics_synced", n);
  }

  if (level === "asset") {
    const n = await optionalGoogleAdsSync("asset_metrics", () => syncGoogleAdsAssetMetrics(params));
    inc(counters, "asset_metrics_synced", n);
  }

  if (level === "asset_group") {
    const n = await optionalGoogleAdsSync("asset_group_metrics", () => syncGoogleAdsAssetGroupMetrics(params));
    inc(counters, "asset_group_metrics_synced", n);
  }

  if (level === "shopping") {
    const n = await optionalGoogleAdsSync("shopping_product_metrics", () => syncGoogleAdsShoppingProductMetrics(params));
    inc(counters, "shopping_product_metrics_synced", n);
  }

  return counters;
}

async function resolveGoogleAdsCustomerIds(body: GoogleAdsSyncBody, owner_id: string) {
  const requested = [
    ...(body.customer_ids || []),
    ...(body.customer_id ? [body.customer_id] : []),
  ].map(cleanGoogleAdsCustomerId).filter(Boolean);

  if (requested.length) return Array.from(new Set(requested));

  const q = new URLSearchParams();
  q.set("owner_id", `eq.${owner_id}`);
  q.set("provider", "eq.google_ads");
  q.set("select", "customer_id");
  q.set("limit", "1000");

  const existing = await supabaseSelect<{ customer_id: string }>("google_ads_accounts", q);
  const existingIds = existing.map((r) => cleanGoogleAdsCustomerId(r.customer_id)).filter(Boolean);
  if (existingIds.length) return Array.from(new Set(existingIds));

  return await googleAdsListAccessibleCustomers(owner_id);
}

// =========================================================
// GOOGLE ADS ROUTES
// =========================================================


// =========================================================
// GOOGLE ADS OAUTH - conservé/adapté depuis le serveur actif
// Stockage token : provider_tokens + Vault
// =========================================================

app.get("/auth/google-ads/start", async (req, res) => {
  try {
    const owner_id = String(req.query.owner_id || "").trim();
    const return_to = String(req.query.return_to || FRONTEND_RETURN_URL);

    if (!owner_id) return res.status(400).send("Missing owner_id");

    if (!GOOGLE_ADS_CLIENT_ID || !GOOGLE_ADS_CLIENT_SECRET || !GOOGLE_ADS_OAUTH_REDIRECT_URI) {
      return res.status(500).send("Missing Google Ads OAuth env vars");
    }

    const state = signState({
      owner_id,
      return_to,
      provider: "google_ads",
      ts: Date.now(),
    });

    const authUrl = new URL("https://accounts.google.com/o/oauth2/v2/auth");
    authUrl.searchParams.set("client_id", GOOGLE_ADS_CLIENT_ID);
    authUrl.searchParams.set("redirect_uri", GOOGLE_ADS_OAUTH_REDIRECT_URI);
    authUrl.searchParams.set("response_type", "code");
    authUrl.searchParams.set("access_type", "offline");
    authUrl.searchParams.set("prompt", "consent");
    authUrl.searchParams.set("scope", "https://www.googleapis.com/auth/adwords");
    authUrl.searchParams.set("state", state);

    return res.redirect(authUrl.toString());
  } catch (e: any) {
    console.error("[google-ads][oauth/start] error:", e);
    return res.status(500).send("Google Ads OAuth start error");
  }
});

app.get("/auth/google-ads/callback", async (req, res) => {
  let return_to = FRONTEND_RETURN_URL;

  try {
    const code = String(req.query.code || "");
    const stateRaw = String(req.query.state || "");
    const error = String(req.query.error || "");

    const state = stateRaw ? verifyState(stateRaw) : null;
    return_to = String(state?.return_to || FRONTEND_RETURN_URL);

    if (error) {
      const u = new URL(return_to);
      u.searchParams.set("google_ads", "error");
      u.searchParams.set("reason", error);
      return res.redirect(u.toString());
    }

    if (!code) throw new Error("Missing OAuth code");
    if (!state?.owner_id) throw new Error("Invalid OAuth state");

    const owner_id = String(state.owner_id);

    const tokenRes = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: GOOGLE_ADS_CLIENT_ID,
        client_secret: GOOGLE_ADS_CLIENT_SECRET,
        code,
        grant_type: "authorization_code",
        redirect_uri: GOOGLE_ADS_OAUTH_REDIRECT_URI,
      }),
    });

    const tokenText = await tokenRes.text();
    let tokenJson: any = null;
    try {
      tokenJson = tokenText ? JSON.parse(tokenText) : null;
    } catch {
      tokenJson = null;
    }

    if (!tokenRes.ok || !tokenJson?.access_token) {
      console.error("[google-ads][oauth/callback] token exchange failed:", tokenRes.status, tokenText);
      throw new Error("Google Ads token exchange failed");
    }

    const expiresAt = tokenJson?.expires_in
      ? new Date(Date.now() + Number(tokenJson.expires_in) * 1000).toISOString()
      : null;

    await upsertProviderTokenVault({
      owner_id,
      provider: "google_ads",
      token: {
        provider: "google_ads",
        ...tokenJson,
        expires_at: expiresAt,
        stored_at: new Date().toISOString(),
      },
    });

    const u = new URL(return_to);
    u.searchParams.set("google_ads", "connected");
    return res.redirect(u.toString());
  } catch (e: any) {
    console.error("[google-ads][oauth/callback] error:", e);
    const u = new URL(return_to);
    u.searchParams.set("google_ads", "error");
    u.searchParams.set("message", e?.message || String(e));
    return res.redirect(u.toString());
  }
});



app.post("/api/google-ads/debug/token", requireAuth, async (req, res) => {
  try {
    const { owner_id } = req.body;
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const token = normalizeGoogleAdsToken(await getProviderToken(owner_id, "google_ads"));
    const refreshed = !token?.access_token || isGoogleAccessTokenExpired(token)
      ? await refreshGoogleAdsToken(owner_id, token)
      : token;

    return res.json({
      ok: true,
      owner_id,
      provider: "google_ads",
      has_access_token: Boolean(refreshed?.access_token),
      has_refresh_token: Boolean(refreshed?.refresh_token),
      token_type: refreshed?.token_type || null,
      scope: refreshed?.scope || null,
      expires_at: refreshed?.expires_at || null,
      access_token_preview: refreshed?.access_token ? `${String(refreshed.access_token).slice(0, 20)}...` : null,
      refresh_token_preview: refreshed?.refresh_token ? `${String(refreshed.refresh_token).slice(0, 20)}...` : null,
    });
  } catch (e: any) {
    console.error("[google-ads][debug/token] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.get("/api/google-ads/customers", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.query.owner_id || "");
    const login_customer_id = cleanGoogleAdsCustomerId(req.query.login_customer_id || GOOGLE_ADS_DEFAULT_LOGIN_CUSTOMER_ID);

    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const accessible = await googleAdsListAccessibleCustomers(owner_id);
    const counters: SyncCounters = {};

    for (const customer_id of accessible) {
      const result = await syncGoogleAdsAccountRows({
        owner_id,
        customer_id,
        login_customer_id: login_customer_id || customer_id,
      });
      inc(counters, "accounts_synced", result.accounts_synced);
      inc(counters, "customer_links_synced", result.customer_links_synced);
    }

    const q = new URLSearchParams();
    q.set("owner_id", `eq.${owner_id}`);
    q.set("provider", "eq.google_ads");
    q.set("select", "*");
    q.set("order", "is_manager.desc,descriptive_name.asc");
    q.set("limit", "1000");

    const accounts = await supabaseSelect("google_ads_accounts", q);

    return res.json({
      ok: true,
      owner_id,
      accessible_customers: accessible,
      ...counters,
      accounts,
    });
  } catch (e: any) {
    console.error("[google-ads][customers] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/google-ads/sync-structure", requireAuth, async (req, res) => {
  try {
    const body = req.body as GoogleAdsSyncBody;
    const owner_id = body.owner_id;
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const login_customer_id = cleanGoogleAdsCustomerId(body.login_customer_id || GOOGLE_ADS_DEFAULT_LOGIN_CUSTOMER_ID);
    const customerIds = await resolveGoogleAdsCustomerIds(body, owner_id);
    const counters: SyncCounters = {};
    const errors: any[] = [];

    for (const customer_id of customerIds) {
      try {
        const c = await syncGoogleAdsStructureForCustomer({
          owner_id,
          customer_id,
          login_customer_id: login_customer_id || customer_id,
        });
        Object.entries(c).forEach(([k, v]) => inc(counters, k, v));
      } catch (e: any) {
        errors.push({ customer_id, error: e?.message || String(e) });
      }
    }

    return res.json({
      ok: errors.length === 0,
      owner_id,
      customer_ids: customerIds,
      ...counters,
      failed_customers: errors.length,
      customer_errors: errors,
    });
  } catch (e: any) {
    console.error("[google-ads][sync-structure] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/google-ads/sync-metrics", requireAuth, async (req, res) => {
  try {
    const body = req.body as GoogleAdsSyncBody;
    const owner_id = body.owner_id;
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const date_from = parseDateOrFallback(body.date_from, daysAgoISODate(30));
    const date_to = parseDateOrFallback(body.date_to, todayISODate());
    const login_customer_id = cleanGoogleAdsCustomerId(body.login_customer_id || GOOGLE_ADS_DEFAULT_LOGIN_CUSTOMER_ID);
    const level = body.level || "campaign";

    if (!["campaign", "ad_group", "ad", "keyword", "search_term", "landing_page", "asset", "asset_group", "shopping"].includes(level)) {
      return res.status(400).json({ ok: false, error: "level must be campaign, ad_group, ad, keyword, search_term, landing_page, asset, asset_group, or shopping" });
    }

    const customerIds = await resolveGoogleAdsCustomerIds(body, owner_id);
    const counters: SyncCounters = {};
    const errors: any[] = [];

    for (const customer_id of customerIds) {
      try {
        const c = await syncGoogleAdsMetricsForCustomer({
          owner_id,
          customer_id,
          login_customer_id: login_customer_id || customer_id,
          date_from,
          date_to,
          level,
        });
        Object.entries(c).forEach(([k, v]) => inc(counters, k, v));
      } catch (e: any) {
        errors.push({ customer_id, error: e?.message || String(e) });
      }
    }

    return res.json({
      ok: errors.length === 0,
      owner_id,
      customer_ids: customerIds,
      date_from,
      date_to,
      level,
      ...counters,
      failed_customers: errors.length,
      customer_errors: errors,
    });
  } catch (e: any) {
    console.error("[google-ads][sync-metrics] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/api/google-ads/sync-all", requireAuth, async (req, res) => {
  try {
    const body = req.body as GoogleAdsSyncBody;
    const owner_id = body.owner_id;
    if (!owner_id) return res.status(400).json({ ok: false, error: "Missing owner_id" });

    const date_from = parseDateOrFallback(body.date_from, daysAgoISODate(30));
    const date_to = parseDateOrFallback(body.date_to, todayISODate());
    const login_customer_id = cleanGoogleAdsCustomerId(body.login_customer_id || GOOGLE_ADS_DEFAULT_LOGIN_CUSTOMER_ID);

    const customerIds = await resolveGoogleAdsCustomerIds(body, owner_id);
    const counters: SyncCounters = {};
    const errors: any[] = [];

    for (const customer_id of customerIds) {
      try {
        const structure = await syncGoogleAdsStructureForCustomer({
          owner_id,
          customer_id,
          login_customer_id: login_customer_id || customer_id,
        });
        Object.entries(structure).forEach(([k, v]) => inc(counters, k, v));

        for (const level of ["campaign", "ad_group", "ad", "keyword", "search_term", "landing_page", "asset", "asset_group", "shopping"] as GoogleAdsSyncLevel[]) {
          const metrics = await syncGoogleAdsMetricsForCustomer({
            owner_id,
            customer_id,
            login_customer_id: login_customer_id || customer_id,
            date_from,
            date_to,
            level,
          });
          Object.entries(metrics).forEach(([k, v]) => inc(counters, k, v));
        }

        inc(
          counters,
          "change_events_synced",
          await optionalGoogleAdsSync("change_events", () => syncGoogleAdsChangeEvents({
            owner_id,
            customer_id,
            login_customer_id: login_customer_id || customer_id,
            date_from,
            date_to,
          }))
        );
      } catch (e: any) {
        errors.push({ customer_id, error: e?.message || String(e) });
      }
    }

    return res.json({
      ok: errors.length === 0,
      owner_id,
      customer_ids: customerIds,
      date_from,
      date_to,
      ...counters,
      failed_customers: errors.length,
      customer_errors: errors,
    });
  } catch (e: any) {
    console.error("[google-ads][sync-all] error:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// =========================================================
// START
// =========================================================

app.listen(PORT, () => {
  console.log(`[vyrexads-merged-server] listening on :${PORT}`);
  console.log(`[vyrexads-merged-server] graph version: ${META_GRAPH_VERSION}`);
});
 