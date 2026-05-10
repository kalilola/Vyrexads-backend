import "dotenv/config";
import express from "express";
import helmet from "helmet";
import cors from "cors";
import morgan from "morgan";
import rateLimit from "express-rate-limit";
import crypto from "crypto";

const app = express();

const PORT = Number(process.env.PORT || 3001);
const N8N_BASE_URL = process.env.N8N_BASE_URL || "";
const API_AUTH_TOKEN = process.env.API_AUTH_TOKEN || "";
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// ⚠️ Mets ces variables dans backend/.env
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
// ✅ NEW:  produit (CompanyPage)
const N8N_COMPANY_PRODUCT_WEBHOOK_PATH =
  process.env.N8N_COMPANY_PRODUCT_WEBHOOK_PATH || "";
// ✅ NEW: description post (caption)
const N8N_DESCRIPTION_POST_WEBHOOK_PATH =
  process.env.N8N_DESCRIPTION_POST_WEBHOOK_PATH || "";
// ✅ NEW : analyse concurrentielle
const N8N_COMPETITOR_ANALYSIS_WEBHOOK_PATH =
  process.env.N8N_COMPETITOR_ANALYSIS_WEBHOOK_PATH || "";



// ==============================
// TikTok PKCE helpers
// ==============================
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


if (!N8N_BASE_URL) {
  console.error("Missing N8N_BASE_URL in backend/.env");
  process.exit(1);
}
if (!API_AUTH_TOKEN) {
  console.error("Missing API_AUTH_TOKEN in backend/.env");
  process.exit(1);
}

app.use(helmet());
app.use(express.json({ limit: "2mb" }));
app.use(
  cors({
    origin: (origin, cb) => {
      if (!origin) return cb(null, true); // server-to-server
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
    windowMs: 6000_000,
    max: 6000,
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

function unwrapPayload(body: any) {
  // Le frontend envoie un wrapper { url, target_url, path, payload }
  // mais on veut relayer UNIQUEMENT payload à n8n.
  if (body && typeof body === "object" && "payload" in body) return body.payload;
  return body;
}

async function relayToN8N(
  webhookPath: string,
  payload: unknown,
  timeoutMs = 20_000
) {
  if (!webhookPath) {
    return {
      status: 500,
      contentType: "application/json",
      body: JSON.stringify({ error: "Missing webhookPath in backend/.env" }),
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

// ✅ Endpoints RELAY (ceux que ton frontend doit appeler)
app.post("/relay/content-example", requireAuth, async (req, res) => {
  try {
    const payload = unwrapPayload(req.body);
    // ✅ FIX: ce workflow peut prendre longtemps → 60s
    const out = await relayToN8N(
      N8N_CONTENT_EXAMPLE_WEBHOOK_PATH,
      payload,
      6000_000
    );
    return sendRelayedResponse(out, res);
  } catch (e: any) {
    const msg = e?.name === "AbortError" ? "n8n timeout" : String(e);
    return res.status(502).json({ error: "Bad Gateway", details: msg });
  }
});

app.post("/relay/template-regenerate", requireAuth, async (req, res) => {
  try {
    const payload = unwrapPayload(req.body);
    // ✅ Recommandé: 60s aussi si ton template est long
    const out = await relayToN8N(
      N8N_TEMPLATE_REGEN_WEBHOOK_PATH,
      payload,
      6000_000
    );
    return sendRelayedResponse(out, res);
  } catch (e: any) {
    const msg = e?.name === "AbortError" ? "n8n timeout" : String(e);
    return res.status(502).json({ error: "Bad Gateway", details: msg });
  }
});

// ✅ NOUVEAU : même logique que les autres pour la régénération de contenu
app.post("/relay/content-regenerate", requireAuth, async (req, res) => {
  try {
    const payload = unwrapPayload(req.body);
    // 60s car ce workflow peut être plus long
    const out = await relayToN8N(
      N8N_CONTENT_REGENERATE_WEBHOOK_PATH,
      payload,
      6000_000
    );
    return sendRelayedResponse(out, res);
  } catch (e: any) {
    const msg = e?.name === "AbortError" ? "n8n timeout" : String(e);
    return res.status(502).json({ error: "Bad Gateway", details: msg });
  }
});

// ✅ NOUVEAU : régénération IMAGE
app.post("/relay/content-regenerate-image", requireAuth, async (req, res) => {
  try {
    const payload = unwrapPayload(req.body);
    const out = await relayToN8N(N8N_CONTENT_IMAGE_WEBHOOK_PATH, payload, 6000_000);
    return sendRelayedResponse(out, res);
  } catch (e: any) {
    const msg = e?.name === "AbortError" ? "n8n timeout" : String(e);
    return res.status(502).json({ error: "Bad Gateway", details: msg });
  }
});

// ✅ NOUVEAU : régénération CARROUSEL
app.post("/relay/content-regenerate-carrousel", requireAuth, async (req, res) => {
  try {
    const payload = unwrapPayload(req.body);
    const out = await relayToN8N(
      N8N_CONTENT_CARROUSEL_WEBHOOK_PATH,
      payload,
      60_000
    );
    return sendRelayedResponse(out, res);
  } catch (e: any) {
    const msg = e?.name === "AbortError" ? "n8n timeout" : String(e);
    return res.status(502).json({ error: "Bad Gateway", details: msg });
  }
});


// ✅ NOUVEAU : génération / régénération du PROMPT (texte proposé)
app.post("/relay/content-prompt", requireAuth, async (req, res) => {
  try {
    const payload = unwrapPayload(req.body);
    const out = await relayToN8N(N8N_CONTENT_PROMPT_WEBHOOK_PATH, payload, 6000_000);
    return sendRelayedResponse(out, res);
  } catch (e: any) {
    const msg = e?.name === "AbortError" ? "n8n timeout" : String(e);
    return res.status(502).json({ error: "Bad Gateway", details: msg });
  }
});


// ✅ NEW : génération / régénération de la DESCRIPTION DU POST (caption)
app.post("/relay/description-post", requireAuth, async (req, res) => {
  try {
    const payload = unwrapPayload(req.body);
    const out = await relayToN8N(
      N8N_DESCRIPTION_POST_WEBHOOK_PATH,
      payload,
      6000_000
    );
    return sendRelayedResponse(out, res);
  } catch (e: any) {
    const msg = e?.name === "AbortError" ? "n8n timeout" : String(e);
    return res.status(502).json({ error: "Bad Gateway", details: msg });
  }
});



// ✅ NEW :  produit (démo / contexte)
app.post("/relay/company-product", requireAuth, async (req, res) => {
  try {
    const payload = unwrapPayload(req.body);
    const out = await relayToN8N(
      N8N_COMPANY_PRODUCT_WEBHOOK_PATH,
      payload,
      6000_000
    );
    return sendRelayedResponse(out, res);
  } catch (e: any) {
    const msg = e?.name === "AbortError" ? "n8n timeout" : String(e);
    return res.status(502).json({ error: "Bad Gateway", details: msg });
  }
});


// ✅ NEW : analyse concurrentielle
app.post("/relay/competitor-analysis", requireAuth, async (req, res) => {
  try {
    const payload = unwrapPayload(req.body);
    const out = await relayToN8N(
      N8N_COMPETITOR_ANALYSIS_WEBHOOK_PATH,
      payload,
      6000_000
    );
    return sendRelayedResponse(out, res);
  } catch (e: any) {
    const msg = e?.name === "AbortError" ? "n8n timeout" : String(e);
    return res.status(502).json({ error: "Bad Gateway", details: msg });
  }
});



// ==============================
// Google Ads OAuth2 (LOCAL)
// ==============================

const GOOGLE_OAUTH_CLIENT_ID = process.env.GOOGLE_OAUTH_CLIENT_ID || "";
const GOOGLE_OAUTH_CLIENT_SECRET = process.env.GOOGLE_OAUTH_CLIENT_SECRET || "";
const GOOGLE_OAUTH_REDIRECT_URI =
  process.env.GOOGLE_OAUTH_REDIRECT_URI ||
  "https://vyrexads-backend.onrender.com/auth/google-ads/callback";
  

// ==============================
// TikTok OAuth
// ==============================
const TIKTOK_CLIENT_KEY = process.env.TIKTOK_CLIENT_KEY || "";
const TIKTOK_CLIENT_SECRET = process.env.TIKTOK_CLIENT_SECRET || "";
const TIKTOK_OAUTH_REDIRECT_URI =
  process.env.TIKTOK_OAUTH_REDIRECT_URI ||
  "";
  

// ==============================
// Facebook OAuth
// ==============================
const FACEBOOK_APP_ID = process.env.FACEBOOK_APP_ID || "";
const FACEBOOK_APP_SECRET = process.env.FACEBOOK_APP_SECRET || "";
const FACEBOOK_OAUTH_REDIRECT_URI =
  process.env.FACEBOOK_OAUTH_REDIRECT_URI ||
  "https://vyrexads-backend.onrender.com/auth/facebook/callback";

const FACEBOOK_OAUTH_SCOPES =
  process.env.FACEBOOK_OAUTH_SCOPES ||
  "pages_show_list,pages_read_engagement,read_insights,ads_read,business_management";

const FRONTEND_RETURN_URL =
  process.env.FRONTEND_RETURN_URL || "http://localhost:8080/analytics";





const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

// IMPORTANT: mets une vraie valeur en prod
const OAUTH_STATE_SECRET = process.env.OAUTH_STATE_SECRET || "dev_state_secret_change_me";

function signState(payload: Record<string, unknown>) {
  const json = JSON.stringify(payload);
  const sig = crypto.createHmac("sha256", OAUTH_STATE_SECRET).update(json).digest("hex");
  return Buffer.from(`${json}.${sig}`).toString("base64url");
}

function verifyState(state: string) {
  const raw = Buffer.from(state, "base64url").toString("utf8");
  const idx = raw.lastIndexOf(".");
  if (idx === -1) return null;
  const json = raw.slice(0, idx);
  const sig = raw.slice(idx + 1);
  const expected = crypto.createHmac("sha256", OAUTH_STATE_SECRET).update(json).digest("hex");
  if (sig !== expected) return null;
  try {
    return JSON.parse(json) as Record<string, unknown>;
  } catch {
    return null;
  }
}

async function supabaseUpsertProviderTokenVault(params: {
  owner_id: string;
  provider: string; // "google_ads"
  token: any;       // JSON object
}) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend env");
  }

  const url = `${SUPABASE_URL}/rest/v1/rpc/upsert_provider_token_admin_json`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      p_owner_id: params.owner_id,
      p_provider: params.provider,
      p_token: params.token, // jsonb côté SQL
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase RPC upsert_provider_token_admin_json failed: ${res.status} ${text}`);
  }

  // RPC returns void -> PostgREST renvoie souvent "" ou "null"
  return;
}


type TikTokTokenRow = {
  id: string;
  profile_id: string;
  owner_id: string;
  provider: string;
  access_token: string;
  refresh_token: string | null;
  access_token_expires_at: string | null;
  refresh_token_expires_at: string | null;
  raw_token?: any;
};

type TikTokUserInfoResponse = {
  data?: {
    user?: {
      open_id?: string;
      union_id?: string;
      display_name?: string;
      avatar_url?: string;
      avatar_url_100?: string;
      avatar_large_url?: string;
      bio_description?: string;
      profile_deep_link?: string;
      profile_web_link?: string;
      is_verified?: boolean;
      follower_count?: number;
      following_count?: number;
      likes_count?: number;
      video_count?: number;
    };
  };
  error?: {
    code?: string;
    message?: string;
    log_id?: string;
  };
};

type TikTokVideo = {
  id: string;
  title?: string;
  video_description?: string;
  duration?: number;
  cover_image_url?: string;
  share_url?: string;
  embed_link?: string;
  embed_html?: string;
  create_time?: number;
  like_count?: number;
  comment_count?: number;
  share_count?: number;
  view_count?: number;
  width?: number;
  height?: number;
};

type TikTokVideoListResponse = {
  data?: {
    videos?: TikTokVideo[];
    cursor?: number;
    has_more?: boolean;
  };
  error?: {
    code?: string;
    message?: string;
    log_id?: string;
  };
};



function fbActionsArrayToObject(arr: any): Record<string, number> {
  if (!Array.isArray(arr)) return {};

  const out: Record<string, number> = {};

  for (const item of arr) {
    const key = String(item?.action_type || "");
    const value = Number(item?.value ?? 0) || 0;
    if (!key) continue;
    out[key] = (out[key] || 0) + value;
  }

  return out;
}

function mergeNumberMaps(
  target: Record<string, number>,
  source: Record<string, number>
) {
  for (const [key, value] of Object.entries(source || {})) {
    target[key] = (target[key] || 0) + (Number(value) || 0);
  }
}

function adminHeaders(extra?: Record<string, string>) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend env");
  }

  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    "Content-Type": "application/json",
    ...extra,
  };
}

async function supabaseAdminInsert(path: string, rows: any[]) {
  const url = `${SUPABASE_URL}/rest/v1/${path}`;
  const res = await fetch(url, {
    method: "POST",
    headers: adminHeaders({
      Prefer: "return=minimal",
    }),
    body: JSON.stringify(rows),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase admin insert failed: ${res.status} ${text}`);
  }
}

async function supabaseAdminUpsertRows(path: string, rows: any[]) {
  const url = `${SUPABASE_URL}/rest/v1/${path}`;
  const res = await fetch(url, {
    method: "POST",
    headers: adminHeaders({
      Prefer: "resolution=merge-duplicates,return=representation",
    }),
    body: JSON.stringify(rows),
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Supabase admin upsert failed: ${res.status} ${text}`);
  }

  try {
    return text ? JSON.parse(text) : [];
  } catch {
    return [];
  }
}

async function supabaseAdminPatch(path: string, patch: any) {
  const url = `${SUPABASE_URL}/rest/v1/${path}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: adminHeaders({
      Prefer: "return=representation",
    }),
    body: JSON.stringify(patch),
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Supabase admin patch failed: ${res.status} ${text}`);
  }

  try {
    return text ? JSON.parse(text) : [];
  } catch {
    return [];
  }
}

async function supabaseAdminSelectSingleRow<T>(path: string): Promise<T | null> {
  const url = `${SUPABASE_URL}/rest/v1/${path}`;
  const res = await fetch(url, {
    method: "GET",
    headers: adminHeaders(),
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Supabase admin select failed: ${res.status} ${text}`);
  }

  let json: any = [];
  try {
    json = text ? JSON.parse(text) : [];
  } catch {
    json = [];
  }

  return Array.isArray(json) ? (json[0] || null) : (json || null);
}


async function supabaseAdminSelectRows<T>(path: string): Promise<T[]> {
  const url = `${SUPABASE_URL}/rest/v1/${path}`;
  const res = await fetch(url, {
    method: "GET",
    headers: adminHeaders(),
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Supabase admin select rows failed: ${res.status} ${text}`);
  }

  try {
    const json = text ? JSON.parse(text) : [];
    return Array.isArray(json) ? json : [];
  } catch {
    return [];
  }
}

async function supabaseAdminDelete(path: string) {
  const url = `${SUPABASE_URL}/rest/v1/${path}`;
  const res = await fetch(url, {
    method: "DELETE",
    headers: adminHeaders({
      Prefer: "return=minimal",
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase admin delete failed: ${res.status} ${text}`);
  }
}



async function supabaseAdminRpc<T = any>(fnName: string, payload: Record<string, any>) {
  const url = `${SUPABASE_URL}/rest/v1/rpc/${fnName}`;
  const res = await fetch(url, {
    method: "POST",
    headers: adminHeaders(),
    body: JSON.stringify(payload),
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Supabase admin rpc ${fnName} failed: ${res.status} ${text}`);
  }

  try {
    return text ? JSON.parse(text) : null as T;
  } catch {
    return null as T;
  }
}





function getFacebookPageProviderKey(page_id: string) {
  return `facebook_page:${page_id}`;
}

async function upsertFacebookPageAccessToken(params: {
  owner_id: string;
  page_id: string;
  page_name?: string | null;
  page_access_token: string;
}) {
  await supabaseUpsertProviderTokenVault({
    owner_id: params.owner_id,
    provider: getFacebookPageProviderKey(params.page_id),
    token: {
      provider: "facebook_page",
      page_id: params.page_id,
      page_name: params.page_name ?? null,
      access_token: params.page_access_token,
      stored_at: new Date().toISOString(),
    },
  });
}

async function getFacebookPageToken(owner_id: string, page_id: string) {
  const data = await supabaseAdminRpc<any>("get_provider_token", {
    p_owner_id: owner_id,
    p_provider: getFacebookPageProviderKey(page_id),
  });

  const token = Array.isArray(data)
    ? (data[0]?.decrypted_secret ?? data[0] ?? null)
    : (data?.decrypted_secret ?? data ?? null);

  if (!token) {
    throw new Error(`No facebook page token found for page_id=${page_id}`);
  }

  return token;
}



async function getTikTokProfileByOwnerId(owner_id: string) {
  return await supabaseAdminSelectSingleRow<{
    id: string;
    owner_id: string;
    display_name: string | null;
    open_id: string;
    last_synced_at: string | null;
  }>(
    `tiktok_profiles?select=id,owner_id,display_name,open_id,last_synced_at&owner_id=eq.${encodeURIComponent(
      owner_id
    )}&order=updated_at.desc&limit=1`
  );
}


async function getAllTikTokProfiles() {
  return await supabaseAdminSelectRows<{
    id: string;
    owner_id: string;
    display_name: string | null;
    open_id: string;
    last_synced_at: string | null;
  }>(
    `tiktok_profiles?select=id,owner_id,display_name,open_id,last_synced_at&order=updated_at.desc`
  );
}

async function markMissingTikTokVideosInactive(params: {
  profile_id: string;
  currentTikTokVideoIds: string[];
}) {
  const existingRows = await supabaseAdminSelectRows<{
    id: string;
    tiktok_video_id: string;
  }>(
    `tiktok_videos?select=id,tiktok_video_id&profile_id=eq.${encodeURIComponent(params.profile_id)}`
  );

  const keep = new Set(params.currentTikTokVideoIds.map(String));
  const toDelete = existingRows.filter((row) => !keep.has(String(row.tiktok_video_id)));

  for (const row of toDelete) {
    await supabaseAdminDelete(`tiktok_videos?id=eq.${encodeURIComponent(row.id)}`);
  }
}




function toIsoFromUnixSeconds(value?: number | null): string | null {
  if (!value) return null;
  return new Date(value * 1000).toISOString();
}

function isExpiringSoon(iso: string | null | undefined, minutes = 10) {
  if (!iso) return true;
  const exp = Date.parse(iso);
  if (!Number.isFinite(exp)) return true;
  return exp - Date.now() <= minutes * 60 * 1000;
}

async function refreshTikTokTokenIfNeeded(tokenRow: TikTokTokenRow): Promise<TikTokTokenRow> {
  if (!isExpiringSoon(tokenRow.access_token_expires_at, 10)) {
    return tokenRow;
  }

  if (!tokenRow.refresh_token) {
    throw new Error("TikTok refresh_token manquant.");
  }

  const tokenRes = await fetch("https://open.tiktokapis.com/v2/oauth/token/", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: new URLSearchParams({
      client_key: TIKTOK_CLIENT_KEY,
      client_secret: TIKTOK_CLIENT_SECRET,
      grant_type: "refresh_token",
      refresh_token: tokenRow.refresh_token,
    }),
  });

  const tokenText = await tokenRes.text();
  let tokenJson: any = null;
  try {
    tokenJson = tokenText ? JSON.parse(tokenText) : null;
  } catch {
    tokenJson = null;
  }

  if (!tokenRes.ok || tokenJson?.error) {
    throw new Error(`TikTok refresh token failed: ${tokenRes.status} ${tokenText || "unknown error"}`);
  }

  const nextAccessToken = String(tokenJson?.access_token || "");
  const nextRefreshToken = String(tokenJson?.refresh_token || tokenRow.refresh_token || "");

  const access_token_expires_at = tokenJson?.expires_in
    ? new Date(Date.now() + Number(tokenJson.expires_in) * 1000).toISOString()
    : null;

  const refresh_token_expires_at = tokenJson?.refresh_expires_in
    ? new Date(Date.now() + Number(tokenJson.refresh_expires_in) * 1000).toISOString()
    : tokenRow.refresh_token_expires_at;

  await supabaseAdminRpc("update_tiktok_connection_token", {
    p_id: tokenRow.id,
    p_access_token: nextAccessToken,
    p_refresh_token: nextRefreshToken,
    p_access_token_expires_at: access_token_expires_at,
    p_refresh_token_expires_at: refresh_token_expires_at,
    p_raw_token: tokenJson ?? {},
  });

  return {
    ...tokenRow,
    access_token: nextAccessToken,
    refresh_token: nextRefreshToken,
    access_token_expires_at,
    refresh_token_expires_at,
    raw_token: tokenJson ?? {},
  };
}

async function fetchTikTokUserInfo(accessToken: string): Promise<TikTokUserInfoResponse> {
  const userFields = [
    "open_id",
    "union_id",
    "display_name",
    "avatar_url",
    "avatar_url_100",
    "avatar_large_url",
    "bio_description",
    "profile_deep_link",
    "profile_web_link",
    "is_verified",
    "follower_count",
    "following_count",
    "likes_count",
    "video_count",
  ].join(",");

  const userRes = await fetch(
    `https://open.tiktokapis.com/v2/user/info/?fields=${encodeURIComponent(userFields)}`,
    {
      method: "GET",
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    }
  );

  const userText = await userRes.text();
  let userJson: any = null;
  try {
    userJson = userText ? JSON.parse(userText) : null;
  } catch {
    userJson = null;
  }

  if (!userRes.ok || (userJson?.error && userJson.error.code !== "ok")) {
    throw new Error(`TikTok user/info failed: ${userRes.status} ${userText}`);
  }

  return userJson as TikTokUserInfoResponse;
}


async function fetchTikTokVideos(accessToken: string): Promise<TikTokVideo[]> {
  const videoFields =
    "id,title,video_description,duration,cover_image_url,embed_html,embed_link,like_count,comment_count,share_count,view_count,create_time";

  let allVideos: TikTokVideo[] = [];
  let cursor: number | undefined = undefined;
  let hasMore: boolean = true;

  while (hasMore) {
    const videosRes: Response = await fetch(
      `https://open.tiktokapis.com/v2/video/list/?fields=${encodeURIComponent(videoFields)}`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          max_count: 20,
          ...(typeof cursor === "number" ? { cursor } : {}),
        }),
      }
    );

    const videosText: string = await videosRes.text();

    let videosJson: TikTokVideoListResponse | null = null;
    try {
      videosJson = videosText ? (JSON.parse(videosText) as TikTokVideoListResponse) : null;
    } catch {
      videosJson = null;
    }

    if (!videosRes.ok || (videosJson?.error && videosJson.error.code !== "ok")) {
      throw new Error(`TikTok video/list failed: ${videosRes.status} ${videosText}`);
    }

    const chunk: TikTokVideo[] = Array.isArray(videosJson?.data?.videos)
      ? videosJson.data!.videos!
      : [];

    allVideos = allVideos.concat(chunk);
    hasMore = Boolean(videosJson?.data?.has_more);
    cursor =
      typeof videosJson?.data?.cursor === "number"
        ? videosJson.data.cursor
        : undefined;
  }

  return allVideos;
}

async function upsertTikTokProfileAndTokens(params: {
  owner_id: string;
  open_id: string;
  union_id: string | null;
  scope: string[];
  access_token: string;
  refresh_token: string | null;
  access_token_expires_at: string | null;
  refresh_token_expires_at: string | null;
  raw_token: any;
  userInfo: TikTokUserInfoResponse;
}) {
  const user = params.userInfo?.data?.user || {};

  const profileRows = await supabaseAdminUpsertRows(
    "tiktok_profiles?on_conflict=owner_id,open_id",
    [
      {
        owner_id: params.owner_id,
        provider: "tiktok",
        open_id: params.open_id,
        union_id: params.union_id,
        scope: params.scope,
        display_name: user?.display_name ?? null,
        avatar_url: user?.avatar_url ?? null,
        avatar_url_100: user?.avatar_url_100 ?? null,
        avatar_large_url: user?.avatar_large_url ?? null,
        profile_deep_link: user?.profile_deep_link ?? null,
        profile_web_link: user?.profile_web_link ?? null,
        bio_description: user?.bio_description ?? null,
        is_verified: user?.is_verified ?? null,
        follower_count: user?.follower_count ?? null,
        following_count: user?.following_count ?? null,
        likes_count: user?.likes_count ?? null,
        video_count: user?.video_count ?? null,
        raw_user: params.userInfo ?? {},
        last_synced_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      },
    ]
  );

  const profile = Array.isArray(profileRows) ? profileRows[0] : null;
  if (!profile?.id) {
    throw new Error("Impossible de récupérer l'id du profil TikTok après upsert.");
  }

  await supabaseAdminRpc("upsert_tiktok_connection_token", {
    p_in_profile_id: profile.id,
    p_in_owner_id: params.owner_id,
    p_in_provider: "tiktok",
    p_in_access_token: params.access_token,
    p_in_refresh_token: params.refresh_token,
    p_in_access_token_expires_at: params.access_token_expires_at,
    p_in_refresh_token_expires_at: params.refresh_token_expires_at,
    p_in_raw_token: params.raw_token ?? {},
  });

  await supabaseAdminInsert("tiktok_profile_snapshots", [
    {
      profile_id: profile.id,
      owner_id: params.owner_id,
      provider: "tiktok",
      snapshot_at: new Date().toISOString(),
      follower_count: user?.follower_count ?? null,
      following_count: user?.following_count ?? null,
      likes_count: user?.likes_count ?? null,
      video_count: user?.video_count ?? null,
      raw_user: params.userInfo ?? {},
    },
  ]);

  return profile;
}

async function upsertTikTokVideosAndSnapshots(params: {
  owner_id: string;
  profile_id: string;
  videos: TikTokVideo[];
}) {
  const syncedAt = new Date().toISOString();

  for (const video of params.videos) {
    const videoRows = await supabaseAdminUpsertRows(
      "tiktok_videos?on_conflict=profile_id,tiktok_video_id",
      [
        {
          profile_id: params.profile_id,
          owner_id: params.owner_id,
          provider: "tiktok",
          tiktok_video_id: video.id,
          title: video.title ?? null,
          video_description: video.video_description ?? null,
          create_time: toIsoFromUnixSeconds(video.create_time),
          duration_seconds: video.duration ?? null,
          width: video.width ?? null,
          height: video.height ?? null,
          cover_image_url: video.cover_image_url ?? null,
          share_url: video.share_url ?? null,
          embed_link: video.embed_link ?? null,
          embed_html: video.embed_html ?? null,
          like_count: video.like_count ?? null,
          comment_count: video.comment_count ?? null,
          share_count: video.share_count ?? null,
          view_count: video.view_count ?? null,
          raw_video: video,
          last_synced_at: syncedAt,
          updated_at: syncedAt,
        },
      ]
    );

    const savedVideo = Array.isArray(videoRows) ? videoRows[0] : null;
    if (!savedVideo?.id) {
      throw new Error(`Impossible de récupérer l'id de la vidéo TikTok ${video.id}`);
    }

    await supabaseAdminInsert("tiktok_video_snapshots", [
      {
        video_id: savedVideo.id,
        profile_id: params.profile_id,
        owner_id: params.owner_id,
        provider: "tiktok",
        snapshot_at: syncedAt,
        like_count: video.like_count ?? null,
        comment_count: video.comment_count ?? null,
        share_count: video.share_count ?? null,
        view_count: video.view_count ?? null,
        raw_video: video,
      },
    ]);
  }

  await markMissingTikTokVideosInactive({
    profile_id: params.profile_id,
    currentTikTokVideoIds: params.videos.map((v) => String(v.id)),
  });
}

async function getTikTokTokenByProfileId(profile_id: string): Promise<TikTokTokenRow | null> {
  const data = await supabaseAdminRpc<TikTokTokenRow[] | TikTokTokenRow | null>(
    "get_tiktok_token_by_profile_id",
    { p_profile_id: profile_id }
  );

  if (Array.isArray(data)) return data[0] || null;
  return data || null;
}

async function syncTikTokDataForProfile(profile_id: string) {
  const tokenRow = await getTikTokTokenByProfileId(profile_id);
  if (!tokenRow) {
    throw new Error(`Aucun token TikTok trouvé pour profile_id=${profile_id}`);
  }

  const freshToken = await refreshTikTokTokenIfNeeded(tokenRow);
  const userInfo = await fetchTikTokUserInfo(freshToken.access_token);
  const videos = await fetchTikTokVideos(freshToken.access_token);

  const user = userInfo?.data?.user || {};
  const finalOpenId = user?.open_id || null;
  const finalUnionId = user?.union_id || null;

  if (!finalOpenId) {
    throw new Error("open_id manquant dans la réponse TikTok.");
  }

  const rawScope = freshToken.raw_token?.scope;
  const scope: string[] =
    typeof rawScope === "string"
      ? rawScope.split(",").map((s: string) => s.trim()).filter(Boolean)
      : Array.isArray(rawScope)
      ? rawScope
      : [];

  const profile = await upsertTikTokProfileAndTokens({
    owner_id: freshToken.owner_id,
    open_id: finalOpenId,
    union_id: finalUnionId,
    scope,
    access_token: freshToken.access_token,
    refresh_token: freshToken.refresh_token,
    access_token_expires_at: freshToken.access_token_expires_at,
    refresh_token_expires_at: freshToken.refresh_token_expires_at,
    raw_token: freshToken.raw_token ?? {},
    userInfo,
  });

  await upsertTikTokVideosAndSnapshots({
    owner_id: freshToken.owner_id,
    profile_id: profile.id,
    videos,
  });

  return {
    ok: true,
    profile_id: profile.id,
    videos_count: videos.length,
  };
}

// ==============================
// TikTok OAuth2 + Sync + Storage
// ==============================

/**
 * START: redirect user to TikTok consent screen
 * Usage:
 *   http://localhost:3001/auth/tiktok/start?owner_id=xxx&return_to=http://localhost:8080/analytics
 */
app.get("/auth/tiktok/start", async (req, res) => {
  try {
    const owner_id = String(req.query.owner_id || "");
    const return_to = String(req.query.return_to || "http://localhost:8080");

    if (!owner_id) return res.status(400).send("Missing owner_id");
    if (!TIKTOK_CLIENT_KEY || !TIKTOK_CLIENT_SECRET) {
      return res.status(500).send("Missing TikTok OAuth env vars");
    }

    const code_verifier = generatePkceVerifier();
    const code_challenge = generatePkceChallenge(code_verifier);

    const state = signState({
      owner_id,
      return_to,
      t: Date.now(),
      provider: "tiktok",
      code_verifier,
    });

    const authUrl = new URL("https://www.tiktok.com/v2/auth/authorize/");
    authUrl.searchParams.set("client_key", TIKTOK_CLIENT_KEY);
    authUrl.searchParams.set("response_type", "code");
    authUrl.searchParams.set(
      "scope",
      ["user.info.basic", "user.info.profile", "user.info.stats", "video.list"].join(",")
    );
    authUrl.searchParams.set("redirect_uri", TIKTOK_OAUTH_REDIRECT_URI);
    authUrl.searchParams.set("state", state);
    authUrl.searchParams.set("code_challenge", code_challenge);
    authUrl.searchParams.set("code_challenge_method", "S256");
    authUrl.searchParams.set("disable_auto_auth", "1");
    return res.redirect(authUrl.toString());
    
  } catch (e: any) {
    console.error("[tiktok][start] error:", e);
    return res.status(500).send("TikTok OAuth start error");
  }
});

/**
 * CALLBACK: TikTok redirects here with ?code=...&state=...
 */
app.get("/auth/tiktok/callback", async (req, res) => {
  try {
    const code = String(req.query.code || "");
    const state = String(req.query.state || "");

    if (!code) return res.status(400).send("Missing code");

    const parsed = verifyState(state);
    if (!parsed) return res.status(400).send("Invalid state");

    const code_verifier = String(parsed.code_verifier || "");
    const owner_id = String(parsed.owner_id || "");
    const return_to = String(parsed.return_to || "http://localhost:8080");

    if (!code_verifier) return res.status(400).send("Missing code_verifier");
    if (!owner_id) return res.status(400).send("Missing owner_id");

    const tokenRes = await fetch("https://open.tiktokapis.com/v2/oauth/token/", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
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

    if (!tokenRes.ok || (tokenJson?.error && tokenJson.error.code !== "ok")) {
      console.error("[tiktok][callback] token exchange failed:", tokenRes.status, tokenText);
      const u = new URL(return_to);
      u.searchParams.set("tiktok", "error");
      return res.redirect(u.toString());
    }

    const access_token = String(tokenJson?.access_token || "");
    const refresh_token = tokenJson?.refresh_token ? String(tokenJson.refresh_token) : null;
    const open_id_from_token = tokenJson?.open_id ? String(tokenJson.open_id) : null;
    const union_id_from_token = tokenJson?.union_id ? String(tokenJson.union_id) : null;

    const access_token_expires_at = tokenJson?.expires_in
      ? new Date(Date.now() + Number(tokenJson.expires_in) * 1000).toISOString()
      : null;

    const refresh_token_expires_at = tokenJson?.refresh_expires_in
      ? new Date(Date.now() + Number(tokenJson.refresh_expires_in) * 1000).toISOString()
      : null;

    const scope: string[] =
      typeof tokenJson?.scope === "string"
        ? tokenJson.scope.split(",").map((s: string) => s.trim()).filter(Boolean)
        : [];

    if (!access_token) {
      const u = new URL(return_to);
      u.searchParams.set("tiktok", "error");
      return res.redirect(u.toString());
    }

    const userInfo = await fetchTikTokUserInfo(access_token);
    const user = userInfo?.data?.user || {};
    const videos = await fetchTikTokVideos(access_token);

    const finalOpenId = user?.open_id || open_id_from_token;
    const finalUnionId = user?.union_id || union_id_from_token;

    if (!finalOpenId) {
      console.error("[tiktok][callback] missing open_id");
      const u = new URL(return_to);
      u.searchParams.set("tiktok", "error");
      return res.redirect(u.toString());
    }

    const profile = await upsertTikTokProfileAndTokens({
      owner_id,
      open_id: finalOpenId,
      union_id: finalUnionId,
      scope,
      access_token,
      refresh_token,
      access_token_expires_at,
      refresh_token_expires_at,
      raw_token: tokenJson ?? {},
      userInfo,
    });

    await upsertTikTokVideosAndSnapshots({
      owner_id,
      profile_id: profile.id,
      videos,
    });

    const u = new URL(return_to);
    u.searchParams.set("tiktok", "connected");
    u.searchParams.set("tiktok_profile_id", String(profile.id));
    return res.redirect(u.toString());
  } catch (e: any) {
    console.error("[tiktok][callback] error:", e);
    const return_to = "http://localhost:8080";
    const u = new URL(return_to);
    u.searchParams.set("tiktok", "error");
    return res.redirect(u.toString());
  }
});

/**
 * Manual sync route
 * POST /api/tiktok/sync
 * body: { profile_id: "..." }
 */
app.post("/api/tiktok/sync", requireAuth, async (req, res) => {
  try {
    const profile_id = String(req.body?.profile_id || "");
    if (!profile_id) {
      return res.status(400).json({ error: "Missing profile_id" });
    }

    const result = await syncTikTokDataForProfile(profile_id);
    return res.json(result);
  } catch (e: any) {
    console.error("[tiktok][sync] error:", e);
    return res.status(500).json({
      error: e?.message || "TikTok sync error",
    });
  }
});

/**
 * GET /api/tiktok/status?owner_id=...
 * Retourne le profil TikTok connecté pour un owner
 */
app.get("/api/tiktok/status", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.query.owner_id || "");
    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    const profile = await getTikTokProfileByOwnerId(owner_id);

    return res.json({
      connected: Boolean(profile?.id),
      profile: profile ?? null,
    });
  } catch (e: any) {
    console.error("[tiktok][status] error:", e);
    return res.status(500).json({
      error: e?.message || "TikTok status error",
    });
  }
});

/**
 * POST /api/tiktok/sync-by-owner
 * body: { owner_id: "..." }
 */
app.post("/api/tiktok/sync-by-owner", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    const profile = await getTikTokProfileByOwnerId(owner_id);
    if (!profile?.id) {
      return res.status(404).json({ error: "No TikTok profile found for this owner_id" });
    }

    const result = await syncTikTokDataForProfile(profile.id);
    return res.json(result);
  } catch (e: any) {
    console.error("[tiktok][sync-by-owner] error:", e);
    return res.status(500).json({
      error: e?.message || "TikTok sync-by-owner error",
    });
  }
});

/**
 * POST /api/tiktok/sync-all
 * Lance un sync global pour tous les profils TikTok
 */
app.post("/api/tiktok/sync-all", requireAuth, async (_req, res) => {
  try {
    const profiles = await getAllTikTokProfiles();
    const results: any[] = [];

    for (const profile of profiles) {
      try {
        const result = await syncTikTokDataForProfile(profile.id);
        results.push({
          profile_id: profile.id,
          ok: true,
          result,
        });
      } catch (e: any) {
        results.push({
          profile_id: profile.id,
          ok: false,
          error: e?.message || "Unknown error",
        });
      }
    }

    return res.json({
      ok: true,
      total: profiles.length,
      results,
    });
  } catch (e: any) {
    console.error("[tiktok][sync-all] error:", e);
    return res.status(500).json({
      error: e?.message || "TikTok sync-all error",
    });
  }
});



// ==============================
// Google Ads API (v23) - Metrics + Storage
// ==============================
const GOOGLE_ADS_DEVELOPER_TOKEN = process.env.GOOGLE_ADS_DEVELOPER_TOKEN || "";
const GOOGLE_ADS_LOGIN_CUSTOMER_ID = process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID || ""; // optional MCC
const GOOGLE_ADS_API_VERSION = "v23";

type ProviderTokenRow = {
  provider: string;
  owner_id: string;
  token: any;
};

type GoogleAdsStoredToken = {
  access_token?: string;
  refresh_token?: string;
  expires_at?: string | null;
  [key: string]: any;
};

type GoogleAdsCustomerRow = {
  owner_id: string;
  customer_id: string;
  resource_name: string;
  descriptive_name: string | null;
  currency_code: string | null;
  time_zone: string | null;
  is_manager: boolean | null;
  parent_customer_id?: string | null;
  level?: number | null;
  status?: string | null;
};

async function supabaseAdminUpsert(path: string, rows: any[]) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend env");
  }

  const url = `${SUPABASE_URL}/rest/v1/${path}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal",
    },
    body: JSON.stringify(rows),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase admin upsert failed: ${res.status} ${text}`);
  }
}

function normalizeGoogleAdsToken(raw: any): GoogleAdsStoredToken {
  let parsed = raw;

  if (typeof parsed === "string") {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      throw new Error("Google Ads token stocké invalide: JSON non parsable");
    }
  }

  if (!parsed || typeof parsed !== "object") {
    throw new Error("Google Ads token stocké invalide");
  }

  if (!parsed.access_token || typeof parsed.access_token !== "string") {
    throw new Error("Google Ads access_token introuvable dans le token stocké");
  }

  return parsed as GoogleAdsStoredToken;
}

async function getGoogleAdsToken(owner_id: string) {
  const data = await supabaseAdminRpc<any>("get_provider_token", {
    p_owner_id: owner_id,
    p_provider: "google_ads",
  });

  const token = Array.isArray(data)
    ? (data[0]?.decrypted_secret ?? data[0] ?? null)
    : (data?.decrypted_secret ?? data ?? null);

  if (!token) {
    throw new Error("No google_ads token found for this owner_id");
  }

  return normalizeGoogleAdsToken(token);
}

function googleAdsMoneyToDecimal(value: any): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n / 1_000_000;
}

function googleAdsNumber(value: any): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function googleAdsString(value: any): string | null {
  if (value === null || value === undefined || value === "") return null;
  return String(value);
}

function googleAdsBool(value: any): boolean | null {
  if (typeof value === "boolean") return value;
  return null;
}

function getGoogleAdsMetricValue(metrics: any, camelKey: string, snakeKey?: string) {
  if (!metrics || typeof metrics !== "object") return null;
  if (metrics[camelKey] !== undefined) return metrics[camelKey];
  if (snakeKey && metrics[snakeKey] !== undefined) return metrics[snakeKey];
  return null;
}

async function refreshGoogleAccessTokenIfNeeded(owner_id: string, token: GoogleAdsStoredToken) {
  if (!isExpired(token)) return token;
  if (!token?.refresh_token) throw new Error("Token expired and no refresh_token available");

  const tokenRes = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: GOOGLE_OAUTH_CLIENT_ID,
      client_secret: GOOGLE_OAUTH_CLIENT_SECRET,
      grant_type: "refresh_token",
      refresh_token: token.refresh_token,
    }),
  });

  const tokenText = await tokenRes.text();
  let tokenJson: any = null;

  try {
    tokenJson = tokenText ? JSON.parse(tokenText) : null;
  } catch {
    tokenJson = null;
  }

  if (!tokenRes.ok) {
    throw new Error(`Google refresh_token exchange failed: ${tokenRes.status} ${tokenText}`);
  }

  const expires_at = tokenJson?.expires_in
    ? new Date(Date.now() + Number(tokenJson.expires_in) * 1000).toISOString()
    : null;

  const newToken: GoogleAdsStoredToken = {
    ...token,
    ...tokenJson,
    refresh_token: token.refresh_token,
    expires_at,
    stored_at: new Date().toISOString(),
  };

  await supabaseUpsertProviderTokenVault({
    owner_id,
    provider: "google_ads",
    token: newToken,
  });

  return newToken;
}

async function googleAdsFetch(
  access_token: string,
  url: string,
  opts?: { loginCustomerId?: string; method?: string; body?: any }
) {
  if (!GOOGLE_ADS_DEVELOPER_TOKEN) {
    throw new Error("Missing GOOGLE_ADS_DEVELOPER_TOKEN in backend env");
  }

  const headers: Record<string, string> = {
    Authorization: `Bearer ${access_token}`,
    "developer-token": GOOGLE_ADS_DEVELOPER_TOKEN,
    "Content-Type": "application/json",
  };

  const loginId = opts?.loginCustomerId || GOOGLE_ADS_LOGIN_CUSTOMER_ID;
  if (loginId) headers["login-customer-id"] = loginId;

  const res = await fetch(url, {
    method: opts?.method || "GET",
    headers,
    body: opts?.body ? JSON.stringify(opts.body) : undefined,
  });

  const text = await res.text();
  let json: any = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  if (!res.ok) {
    throw new Error(`Google Ads API error ${res.status}: ${text}`);
  }

  return json;
}

function collectSearchStreamResults(stream: any): any[] {
  const chunks = Array.isArray(stream) ? stream : [];
  const rows: any[] = [];

  for (const chunk of chunks) {
    const results = Array.isArray(chunk?.results) ? chunk.results : [];
    for (const row of results) {
      rows.push(row);
    }
  }

  return rows;
}

async function googleAdsSearchStream(params: {
  access_token: string;
  customer_id: string;
  query: string;
  login_customer_id?: string;
}) {
  const url = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${params.customer_id}/googleAds:searchStream`;

  const stream = await googleAdsFetch(params.access_token, url, {
    loginCustomerId: params.login_customer_id || undefined,
    method: "POST",
    body: { query: params.query },
  });

  return collectSearchStreamResults(stream);
}

async function googleAdsSearch(params: {
  access_token: string;
  customer_id: string;
  query: string;
  login_customer_id?: string;
}) {
  const url = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${params.customer_id}/googleAds:search`;

  const out = await googleAdsFetch(params.access_token, url, {
    loginCustomerId: params.login_customer_id || undefined,
    method: "POST",
    body: { query: params.query },
  });

  return Array.isArray(out?.results) ? out.results : [];
}




// ==============================
// Google Ads reporting helpers
// ==============================

// Valide une date GAQL au format YYYY-MM-DD.
function safeGaqlDate(value: string) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`Invalid date format: ${value}`);
  }
  return value;
}

// Classe une campagne Google Ads par type/statut.
function classifyGoogleAdsCampaign(campaign: any) {
  const channelType =
    campaign?.advertisingChannelType ||
    campaign?.advertising_channel_type ||
    null;

  const channelSubType =
    campaign?.advertisingChannelSubType ||
    campaign?.advertising_channel_sub_type ||
    null;

  const status = campaign?.status || null;
  const servingStatus =
    campaign?.servingStatus ||
    campaign?.serving_status ||
    null;

  return {
    channelType,
    channelSubType,
    status,
    servingStatus,
    isVideoCampaign: channelType === "VIDEO",
    isPerformanceMax: channelType === "PERFORMANCE_MAX",
  };
}

// Convertit les métriques Google Ads vers le format DB.
function mapCommonGoogleAdsMetrics(metrics: any) {
  return {
    impressions: googleAdsNumber(getGoogleAdsMetricValue(metrics, "impressions")) || 0,
    clicks: googleAdsNumber(getGoogleAdsMetricValue(metrics, "clicks")) || 0,
    ctr: googleAdsNumber(getGoogleAdsMetricValue(metrics, "ctr")),
    cost: googleAdsMoneyToDecimal(getGoogleAdsMetricValue(metrics, "costMicros", "cost_micros")),
    average_cpc: googleAdsMoneyToDecimal(getGoogleAdsMetricValue(metrics, "averageCpc", "average_cpc")),
    average_cpm: googleAdsMoneyToDecimal(getGoogleAdsMetricValue(metrics, "averageCpm", "average_cpm")),
    conversions: googleAdsNumber(getGoogleAdsMetricValue(metrics, "conversions")),
    conversion_rate: googleAdsNumber(
      getGoogleAdsMetricValue(metrics, "conversionsFromInteractionsRate", "conversions_from_interactions_rate")
    ),
    cost_per_conversion: googleAdsMoneyToDecimal(
      getGoogleAdsMetricValue(metrics, "costPerConversion", "cost_per_conversion")
    ),
    conversion_value: googleAdsNumber(getGoogleAdsMetricValue(metrics, "conversionsValue", "conversions_value")),
    video_views: googleAdsNumber(getGoogleAdsMetricValue(metrics, "videoTrueviewViews", "video_trueview_views")),
    video_view_rate: googleAdsNumber(getGoogleAdsMetricValue(metrics, "videoTrueviewViewRate", "video_trueview_view_rate")),
    average_watch_time_millis: googleAdsNumber(
      getGoogleAdsMetricValue(metrics, "averageVideoWatchTimeDurationMillis", "average_video_watch_time_duration_millis")
    ),
    watch_time_millis: googleAdsNumber(
      getGoogleAdsMetricValue(metrics, "videoWatchTimeDurationMillis", "video_watch_time_duration_millis")
    ),
    video_quartile_25_rate: googleAdsNumber(getGoogleAdsMetricValue(metrics, "videoQuartileP25Rate", "video_quartile_p25_rate")),
    video_quartile_50_rate: googleAdsNumber(getGoogleAdsMetricValue(metrics, "videoQuartileP50Rate", "video_quartile_p50_rate")),
    video_quartile_75_rate: googleAdsNumber(getGoogleAdsMetricValue(metrics, "videoQuartileP75Rate", "video_quartile_p75_rate")),
    video_quartile_100_rate: googleAdsNumber(getGoogleAdsMetricValue(metrics, "videoQuartileP100Rate", "video_quartile_p100_rate")),
  };
}

// Construit les champs communs liés à la campagne.
function baseGoogleAdsMeta(params: {
  owner_id: string;
  customer_id: string;
  campaign: any;
}) {
  const meta = classifyGoogleAdsCampaign(params.campaign);

  return {
    owner_id: params.owner_id,
    provider: "google_ads",
    customer_id: String(params.customer_id),

    campaign_id: googleAdsString(params.campaign?.id),
    campaign_name: googleAdsString(params.campaign?.name),

    campaign_type: meta.channelType,
    campaign_sub_type: meta.channelSubType,
    campaign_status: meta.status,
    campaign_serving_status: meta.servingStatus,
    is_video_campaign: meta.isVideoCampaign,
    is_performance_max: meta.isPerformanceMax,
  };
}

// Récupère un access token Google Ads frais.
async function getFreshGoogleAdsAccessToken(owner_id: string) {
  let token = await getGoogleAdsToken(owner_id);
  token = await refreshGoogleAccessTokenIfNeeded(owner_id, token);
  return String(token.access_token);
}

// ==============================
// Google Ads structure listing
// ==============================

// Liste les campagnes du compte.
async function listGoogleAdsCampaigns(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const access_token = await getFreshGoogleAdsAccessToken(params.owner_id);

  const rows = await googleAdsSearchStream({
    access_token,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.serving_status,
        campaign.advertising_channel_type,
        campaign.advertising_channel_sub_type
      FROM campaign
      WHERE campaign.status != 'REMOVED'
      ORDER BY campaign.name
    `.trim(),
  });

  return rows.map((r: any) => {
    const c = r?.campaign || {};
    const meta = classifyGoogleAdsCampaign(c);

    return {
      id: googleAdsString(c.id),
      name: googleAdsString(c.name),
      status: meta.status,
      serving_status: meta.servingStatus,
      type: meta.channelType,
      sub_type: meta.channelSubType,
      is_video_campaign: meta.isVideoCampaign,
      is_performance_max: meta.isPerformanceMax,
      raw: c,
    };
  });
}

// Liste les ad groups du compte.
async function listGoogleAdsAdGroups(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const access_token = await getFreshGoogleAdsAccessToken(params.owner_id);

  const rows = await googleAdsSearchStream({
    access_token,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.serving_status,
        campaign.advertising_channel_type,
        campaign.advertising_channel_sub_type,
        ad_group.id,
        ad_group.name,
        ad_group.status
      FROM ad_group
      WHERE campaign.status != 'REMOVED'
        AND ad_group.status != 'REMOVED'
      ORDER BY campaign.name, ad_group.name
    `.trim(),
  });

  return rows.map((r: any) => {
    const campaign = r?.campaign || {};
    const adGroup = r?.adGroup || r?.ad_group || {};
    const meta = classifyGoogleAdsCampaign(campaign);

    return {
      campaign_id: googleAdsString(campaign.id),
      campaign_name: googleAdsString(campaign.name),
      campaign_type: meta.channelType,
      campaign_sub_type: meta.channelSubType,
      ad_group_id: googleAdsString(adGroup.id),
      ad_group_name: googleAdsString(adGroup.name),
      ad_group_status: googleAdsString(adGroup.status),
      raw: r,
    };
  });
}

// Liste les ads du compte.
async function listGoogleAdsAds(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const access_token = await getFreshGoogleAdsAccessToken(params.owner_id);

  const rows = await googleAdsSearchStream({
    access_token,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.serving_status,
        campaign.advertising_channel_type,
        campaign.advertising_channel_sub_type,
        ad_group.id,
        ad_group.name,
        ad_group.status,
        ad_group_ad.status,
        ad_group_ad.ad.id,
        ad_group_ad.ad.name,
        ad_group_ad.ad.type
      FROM ad_group_ad
      WHERE campaign.status != 'REMOVED'
        AND ad_group.status != 'REMOVED'
        AND ad_group_ad.status != 'REMOVED'
      ORDER BY campaign.name, ad_group.name
    `.trim(),
  });

  return rows.map((r: any) => {
    const campaign = r?.campaign || {};
    const adGroup = r?.adGroup || r?.ad_group || {};
    const adGroupAd = r?.adGroupAd || r?.ad_group_ad || {};
    const ad = adGroupAd?.ad || {};
    const meta = classifyGoogleAdsCampaign(campaign);

    return {
      campaign_id: googleAdsString(campaign.id),
      campaign_name: googleAdsString(campaign.name),
      campaign_type: meta.channelType,
      campaign_sub_type: meta.channelSubType,
      ad_group_id: googleAdsString(adGroup.id),
      ad_group_name: googleAdsString(adGroup.name),
      ad_id: googleAdsString(ad.id),
      ad_name: googleAdsString(ad.name),
      ad_type: googleAdsString(ad.type),
      ad_status: googleAdsString(adGroupAd.status),
      raw: r,
    };
  });
}

// ==============================
// Google Ads metrics row builders
// ==============================

// Prépare les lignes de métriques campaign pour Supabase.
function buildCampaignMetricRows(params: {
  owner_id: string;
  customer_id: string;
  sourceRows: any[];
}) {
  return params.sourceRows.map((r: any) => {
    const campaign = r?.campaign || {};
    const segments = r?.segments || {};
    const metrics = r?.metrics || {};

    return {
      ...baseGoogleAdsMeta({
        owner_id: params.owner_id,
        customer_id: params.customer_id,
        campaign,
      }),
      date: googleAdsString(segments.date),
      ...mapCommonGoogleAdsMetrics(metrics),
      raw: r,
      updated_at: new Date().toISOString(),
    };
  }).filter((row: any) => row.campaign_id && row.date);
}

// Prépare les lignes de métriques ad group pour Supabase.
function buildAdGroupMetricRows(params: {
  owner_id: string;
  customer_id: string;
  sourceRows: any[];
}) {
  return params.sourceRows.map((r: any) => {
    const campaign = r?.campaign || {};
    const adGroup = r?.adGroup || r?.ad_group || {};
    const segments = r?.segments || {};
    const metrics = r?.metrics || {};

    return {
      ...baseGoogleAdsMeta({
        owner_id: params.owner_id,
        customer_id: params.customer_id,
        campaign,
      }),
      ad_group_id: googleAdsString(adGroup.id),
      ad_group_name: googleAdsString(adGroup.name),
      ad_group_status: googleAdsString(adGroup.status),
      date: googleAdsString(segments.date),
      ...mapCommonGoogleAdsMetrics(metrics),
      raw: r,
      updated_at: new Date().toISOString(),
    };
  }).filter((row: any) => row.campaign_id && row.ad_group_id && row.date);
}



async function upsertGoogleAdsStructureSnapshot(params: {
  owner_id: string;
  customer_id: string;
  date: string;
  login_customer_id?: string;
}) {
  const date = safeGaqlDate(params.date);

  const campaigns = await listGoogleAdsCampaigns(params);
  const adGroups = await listGoogleAdsAdGroups(params);
  const ads = await listGoogleAdsAds(params);

  const campaignRows = campaigns
    .filter((c: any) => c.id)
    .map((c: any) => ({
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: params.customer_id,
      campaign_id: c.id,
      campaign_name: c.name,
      date,
      impressions: 0,
      clicks: 0,
      campaign_type: c.type,
      campaign_sub_type: c.sub_type,
      campaign_status: c.status,
      campaign_serving_status: c.serving_status,
      is_video_campaign: c.is_video_campaign,
      is_performance_max: c.is_performance_max,
      raw: c.raw || {},
      updated_at: new Date().toISOString(),
    }));

  const adGroupRows = adGroups
    .filter((g: any) => g.campaign_id && g.ad_group_id)
    .map((g: any) => ({
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: params.customer_id,
      campaign_id: g.campaign_id,
      campaign_name: g.campaign_name,
      ad_group_id: g.ad_group_id,
      ad_group_name: g.ad_group_name,
      date,
      impressions: 0,
      clicks: 0,
      campaign_type: g.campaign_type,
      campaign_sub_type: g.campaign_sub_type,
      ad_group_status: g.ad_group_status,
      raw: g.raw || {},
      updated_at: new Date().toISOString(),
    }));

  const adRows = ads
    .filter((a: any) => a.campaign_id && a.ad_group_id && a.ad_id)
    .map((a: any) => ({
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: params.customer_id,
      campaign_id: a.campaign_id,
      campaign_name: a.campaign_name,
      ad_group_id: a.ad_group_id,
      ad_group_name: a.ad_group_name,
      ad_id: a.ad_id,
      ad_name: a.ad_name,
      ad_type: a.ad_type,
      ad_status: a.ad_status,
      date,
      impressions: 0,
      clicks: 0,
      campaign_type: a.campaign_type,
      campaign_sub_type: a.campaign_sub_type,
      raw: a.raw || {},
      updated_at: new Date().toISOString(),
    }));

  if (campaignRows.length > 0) {
    await supabaseAdminUpsert(
      "ads_metrics_campaigns?on_conflict=owner_id,provider,customer_id,campaign_id,date",
      campaignRows
    );
  }

  if (adGroupRows.length > 0) {
    await supabaseAdminUpsert(
      "ads_metrics_ad_groups?on_conflict=owner_id,provider,customer_id,ad_group_id,date",
      adGroupRows
    );
  }

  if (adRows.length > 0) {
    await supabaseAdminUpsert(
      "ads_metrics_ads?on_conflict=owner_id,provider,customer_id,ad_id,date",
      adRows
    );
  }

  return {
    campaigns: campaignRows.length,
    ad_groups: adGroupRows.length,
    ads: adRows.length,
  };
}



// Prépare les lignes de métriques ad pour Supabase.
function pickGoogleAdsTextAssetArray(value: any) {
  const items = Array.isArray(value) ? value : [];
  return items.map((x: any) => ({
    text: x?.text ?? null,
    pinned_field: x?.pinnedField ?? x?.pinned_field ?? null,
    asset_performance_label:
      x?.assetPerformanceLabel ?? x?.asset_performance_label ?? null,
  }));
}

function buildAdCreativeFields(ad: any) {
  const responsiveSearchAd =
    ad?.responsiveSearchAd || ad?.responsive_search_ad || {};
  const videoAd = ad?.videoAd || ad?.video_ad || {};
  const inStream = videoAd?.inStream || videoAd?.in_stream || {};
  const bumper = videoAd?.bumper || {};
  const outStream = videoAd?.outStream || videoAd?.out_stream || {};

  return {
    final_urls: ad?.finalUrls || ad?.final_urls || [],
    display_url: googleAdsString(ad?.displayUrl || ad?.display_url),

    responsive_search_headlines: pickGoogleAdsTextAssetArray(
      responsiveSearchAd?.headlines
    ),
    responsive_search_descriptions: pickGoogleAdsTextAssetArray(
      responsiveSearchAd?.descriptions
    ),

    video_asset_resource_name:
      googleAdsString(videoAd?.video?.asset) ||
      googleAdsString(videoAd?.videoAsset) ||
      googleAdsString(videoAd?.video_asset),

    video_ad_headline:
      googleAdsString(inStream?.actionHeadline || inStream?.action_headline) ||
      googleAdsString(bumper?.actionHeadline || bumper?.action_headline) ||
      googleAdsString(outStream?.headline),

    video_ad_description: null,

    call_to_action:
      googleAdsString(inStream?.actionButtonLabel || inStream?.action_button_label) ||
      googleAdsString(bumper?.actionButtonLabel || bumper?.action_button_label),

    creative_content_raw: {
      final_urls: ad?.finalUrls || ad?.final_urls || [],
      responsive_search_ad: responsiveSearchAd,
      video_ad: videoAd,
    },
  };
}

function buildAdMetricRows(params: {
  owner_id: string;
  customer_id: string;
  sourceRows: any[];
  videoAssetsByResourceName?: Map<string, any>;
}) {
  return params.sourceRows.map((r: any) => {
    const campaign = r?.campaign || {};
    const adGroup = r?.adGroup || r?.ad_group || {};
    const adGroupAd = r?.adGroupAd || r?.ad_group_ad || {};
    const ad = adGroupAd?.ad || {};
    const segments = r?.segments || {};
    const metrics = r?.metrics || {};

    const creative = buildAdCreativeFields(ad);
    const videoAsset = creative.video_asset_resource_name
      ? params.videoAssetsByResourceName?.get(creative.video_asset_resource_name)
      : null;

    return {
      ...baseGoogleAdsMeta({
        owner_id: params.owner_id,
        customer_id: params.customer_id,
        campaign,
      }),
      ad_group_id: googleAdsString(adGroup.id),
      ad_group_name: googleAdsString(adGroup.name),
      ad_id: googleAdsString(ad.id),
      ad_name: googleAdsString(ad.name),
      ad_type: googleAdsString(ad.type),
      ad_status: googleAdsString(adGroupAd.status),
      date: googleAdsString(segments.date),
      ...mapCommonGoogleAdsMetrics(metrics),

      ...creative,
      youtube_video_id:
        googleAdsString(videoAsset?.youtubeVideoAsset?.youtubeVideoId) ||
        googleAdsString(videoAsset?.youtube_video_asset?.youtube_video_id),
      youtube_video_title:
        googleAdsString(videoAsset?.youtubeVideoAsset?.youtubeVideoTitle) ||
        googleAdsString(videoAsset?.youtube_video_asset?.youtube_video_title),

      raw: r,
      updated_at: new Date().toISOString(),
    };
  }).filter((row: any) => row.campaign_id && row.ad_group_id && row.ad_id && row.date);
}

// ==============================
// Google Ads metrics sync
// ==============================

// Synchronise les métriques au niveau campaign.
async function syncGoogleAdsCampaignMetrics(params: {
  owner_id: string;
  customer_id: string;
  date_from: string;
  date_to: string;
  login_customer_id?: string;
}) {
  const dateFrom = safeGaqlDate(params.date_from);
  const dateTo = safeGaqlDate(params.date_to);
  const access_token = await getFreshGoogleAdsAccessToken(params.owner_id);

  const sourceRows = await googleAdsSearchStream({
    access_token,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.serving_status,
        campaign.advertising_channel_type,
        campaign.advertising_channel_sub_type,
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value,
        metrics.video_trueview_views,
        metrics.video_trueview_view_rate,
        metrics.average_video_watch_time_duration_millis,
        metrics.video_watch_time_duration_millis,
        metrics.video_quartile_p25_rate,
        metrics.video_quartile_p50_rate,
        metrics.video_quartile_p75_rate,
        metrics.video_quartile_p100_rate
      FROM campaign
      WHERE segments.date BETWEEN '${dateFrom}' AND '${dateTo}'
        AND campaign.status != 'REMOVED'
    `.trim(),
  });

  const rows = buildCampaignMetricRows({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    sourceRows,
  });

  if (rows.length > 0) {
    await supabaseAdminUpsert(
      "ads_metrics_campaigns?on_conflict=owner_id,provider,customer_id,campaign_id,date",
      rows
    );
  }

  return { ok: true, level: "campaign", total_rows: rows.length };
}

// Synchronise les métriques au niveau ad group.
async function syncGoogleAdsAdGroupMetrics(params: {
  owner_id: string;
  customer_id: string;
  date_from: string;
  date_to: string;
  login_customer_id?: string;
}) {
  const dateFrom = safeGaqlDate(params.date_from);
  const dateTo = safeGaqlDate(params.date_to);
  const access_token = await getFreshGoogleAdsAccessToken(params.owner_id);

  const sourceRows = await googleAdsSearchStream({
    access_token,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.serving_status,
        campaign.advertising_channel_type,
        campaign.advertising_channel_sub_type,
        ad_group.id,
        ad_group.name,
        ad_group.status,
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value,
        metrics.video_trueview_views,
        metrics.video_trueview_view_rate,
        metrics.average_video_watch_time_duration_millis,
        metrics.video_watch_time_duration_millis,
        metrics.video_quartile_p25_rate,
        metrics.video_quartile_p50_rate,
        metrics.video_quartile_p75_rate,
        metrics.video_quartile_p100_rate
      FROM ad_group
      WHERE segments.date BETWEEN '${dateFrom}' AND '${dateTo}'
        AND campaign.status != 'REMOVED'
        AND ad_group.status != 'REMOVED'
    `.trim(),
  });

  const rows = buildAdGroupMetricRows({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    sourceRows,
  });

  if (rows.length > 0) {
    await supabaseAdminUpsert(
      "ads_metrics_ad_groups?on_conflict=owner_id,provider,customer_id,ad_group_id,date",
      rows
    );
  }

  return { ok: true, level: "ad_group", total_rows: rows.length };
}




//Ajoute la synchro ciblage ad group
async function syncGoogleAdsAdGroupTargeting(params: {
  owner_id: string;
  customer_id: string;
  date_to: string;
  login_customer_id?: string;
}) {
  const date = safeGaqlDate(params.date_to);
  const access_token = await getFreshGoogleAdsAccessToken(params.owner_id);

  const sourceRows = await googleAdsSearchStream({
    access_token,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        ad_group_criterion.criterion_id,
        ad_group_criterion.type,
        ad_group_criterion.status,
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        ad_group_criterion.placement.url,
        ad_group_criterion.topic.topic_constant,
        ad_group_criterion.topic.path,
        ad_group_criterion.user_list.user_list
      FROM ad_group_criterion
      WHERE campaign.status != 'REMOVED'
        AND ad_group.status != 'REMOVED'
        AND ad_group_criterion.status != 'REMOVED'
    `.trim(),
  });

  const grouped = new Map<string, any>();

  for (const r of sourceRows) {
    const campaign = r?.campaign || {};
    const adGroup = r?.adGroup || r?.ad_group || {};
    const criterion = r?.adGroupCriterion || r?.ad_group_criterion || {};
    const adGroupId = googleAdsString(adGroup.id);

    if (!adGroupId) continue;

    if (!grouped.has(adGroupId)) {
      grouped.set(adGroupId, {
        owner_id: params.owner_id,
        provider: "google_ads",
        customer_id: params.customer_id,
        campaign_id: googleAdsString(campaign.id),
        campaign_name: googleAdsString(campaign.name),
        ad_group_id: adGroupId,
        ad_group_name: googleAdsString(adGroup.name),
        date,
        impressions: 0,
        clicks: 0,
        keywords: [],
        placements: [],
        topics: [],
        audiences: [],
        targeting_raw: [],
        updated_at: new Date().toISOString(),
      });
    }

    const row = grouped.get(adGroupId);
    const type = googleAdsString(criterion.type);
    const item = {
      criterion_id: googleAdsString(criterion.criterionId || criterion.criterion_id),
      type,
      status: googleAdsString(criterion.status),
      raw: criterion,
    };

    if (type === "KEYWORD") {
      row.keywords.push({
        ...item,
        text: googleAdsString(criterion?.keyword?.text),
        match_type: googleAdsString(
          criterion?.keyword?.matchType || criterion?.keyword?.match_type
        ),
      });
    } else if (type === "PLACEMENT") {
      row.placements.push({
        ...item,
        url: googleAdsString(criterion?.placement?.url),
      });
    } else if (type === "TOPIC") {
      row.topics.push({
        ...item,
        topic_constant: googleAdsString(
          criterion?.topic?.topicConstant || criterion?.topic?.topic_constant
        ),
        path: criterion?.topic?.path || [],
      });
    } else if (type === "USER_LIST") {
      row.audiences.push({
        ...item,
        user_list: googleAdsString(
          criterion?.userList?.userList || criterion?.user_list?.user_list
        ),
      });
    }

    row.targeting_raw.push(criterion);
  }

  const rows = Array.from(grouped.values());

  if (rows.length > 0) {
    await supabaseAdminUpsert(
      "ads_metrics_ad_groups?on_conflict=owner_id,provider,customer_id,ad_group_id,date",
      rows
    );
  }

  return { ok: true, level: "ad_group_targeting", total_rows: rows.length };
}




// Synchronise les métriques au niveau ad.
async function syncGoogleAdsAdMetrics(params: {
  owner_id: string;
  customer_id: string;
  date_from: string;
  date_to: string;
  login_customer_id?: string;
}) {
  const dateFrom = safeGaqlDate(params.date_from);
  const dateTo = safeGaqlDate(params.date_to);
  const access_token = await getFreshGoogleAdsAccessToken(params.owner_id);

  const sourceRows = await googleAdsSearchStream({
    access_token,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.serving_status,
        campaign.advertising_channel_type,
        campaign.advertising_channel_sub_type,
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
        ad_group_ad.ad.video_ad.video.asset,
        ad_group_ad.ad.video_ad.in_stream.action_headline,
        ad_group_ad.ad.video_ad.in_stream.action_button_label,
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.cost_micros,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.cost_per_conversion,
        metrics.conversions_value,
        metrics.video_trueview_views,
        metrics.video_trueview_view_rate,
        metrics.average_video_watch_time_duration_millis,
        metrics.video_watch_time_duration_millis,
        metrics.video_quartile_p25_rate,
        metrics.video_quartile_p50_rate,
        metrics.video_quartile_p75_rate,
        metrics.video_quartile_p100_rate
      FROM ad_group_ad
      WHERE segments.date BETWEEN '${dateFrom}' AND '${dateTo}'
        AND campaign.status != 'REMOVED'
        AND ad_group_ad.status != 'REMOVED'
    `.trim(),
  });



    const videoAssetResourceNames = Array.from(
    new Set(
      sourceRows
        .map((r: any) => {
          const adGroupAd = r?.adGroupAd || r?.ad_group_ad || {};
          const ad = adGroupAd?.ad || {};
          const creative = buildAdCreativeFields(ad);
          return creative.video_asset_resource_name;
        })
        .filter(Boolean)
    )
  );

  const videoAssetsByResourceName = new Map<string, any>();

  for (const resourceName of videoAssetResourceNames) {
    const assetRows = await googleAdsSearchStream({
      access_token,
      customer_id: params.customer_id,
      login_customer_id: params.login_customer_id,
      query: `
        SELECT
          asset.resource_name,
          asset.id,
          asset.name,
          asset.type,
          asset.youtube_video_asset.youtube_video_id,
          asset.youtube_video_asset.youtube_video_title
        FROM asset
        WHERE asset.resource_name = '${resourceName}'
      `.trim(),
    });

    for (const assetRow of assetRows) {
      const asset = assetRow?.asset || {};
      const key = googleAdsString(asset?.resourceName || asset?.resource_name);
      if (key) videoAssetsByResourceName.set(key, asset);
    }
  }


  const rows = buildAdMetricRows({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    sourceRows,
    videoAssetsByResourceName,
  });

  if (rows.length > 0) {
    await supabaseAdminUpsert(
      "ads_metrics_ads?on_conflict=owner_id,provider,customer_id,ad_id,date",
      rows
    );
  }

  return { ok: true, level: "ad", total_rows: rows.length };
}
// differencier les different type de campagnes :



async function safeGoogleAdsSearchStream(args: {
  access_token: string;
  customer_id: string;
  login_customer_id?: string;
  query: string;
}) {
  try {
    return await googleAdsSearchStream(args);
  } catch (err: any) {
    console.warn("[google-ads][creative-content] skipped query:", err?.message || err);
    return [];
  }
}

function textAssetsToJson(items: any) {
  return Array.isArray(items)
    ? items.map((x: any) => ({
        text: googleAdsString(x?.text),
        pinned_field: googleAdsString(x?.pinnedField || x?.pinned_field),
        asset_performance_label: googleAdsString(
          x?.assetPerformanceLabel || x?.asset_performance_label
        ),
      }))
    : [];
}

function extractAssetResourceNames(items: any): string[] {
  if (!Array.isArray(items)) return [];

  return items.reduce<string[]>((acc, x: any) => {
    const value = googleAdsString(
      x?.asset || x?.assetResourceName || x?.asset_resource_name
    );

    if (value) acc.push(value);
    return acc;
  }, []);
}

async function syncGoogleAdsAdCreativeContent(params: {
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
}) {
  const access_token = await getFreshGoogleAdsAccessToken(params.owner_id);
  const now = new Date().toISOString();

  const creativeRows: any[] = [];

  // 1) Search Ads
  const responsiveSearchRows = await safeGoogleAdsSearchStream({
    access_token,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        ad_group_ad.ad.id,
        ad_group_ad.ad.type,
        ad_group_ad.ad.final_urls,
        ad_group_ad.ad.display_url,
        ad_group_ad.ad.responsive_search_ad.headlines,
        ad_group_ad.ad.responsive_search_ad.descriptions
      FROM ad_group_ad
      WHERE ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
        AND campaign.status != 'REMOVED'
        AND ad_group_ad.status != 'REMOVED'
    `.trim(),
  });

  for (const r of responsiveSearchRows) {
    const campaign = r?.campaign || {};
    const adGroup = r?.adGroup || r?.ad_group || {};
    const ad = (r?.adGroupAd || r?.ad_group_ad || {})?.ad || {};
    const rsa = ad?.responsiveSearchAd || ad?.responsive_search_ad || {};

    creativeRows.push({
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: params.customer_id,
      campaign_id: googleAdsString(campaign.id),
      campaign_name: googleAdsString(campaign.name),
      ad_group_id: googleAdsString(adGroup.id),
      ad_group_name: googleAdsString(adGroup.name),
      ad_id: googleAdsString(ad.id),
      ad_type: googleAdsString(ad.type),
      responsive_search_headlines: textAssetsToJson(rsa.headlines),
      responsive_search_descriptions: textAssetsToJson(rsa.descriptions),
      final_urls: ad?.finalUrls || ad?.final_urls || [],
      display_url: googleAdsString(ad?.displayUrl || ad?.display_url),
      creative_content_raw: r,
      updated_at: now,
    });
  }

  // 2) Demand Gen Video Responsive Ads
  const demandGenVideoRows = await safeGoogleAdsSearchStream({
    access_token,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        ad_group_ad.ad.id,
        ad_group_ad.ad.type,
        ad_group_ad.ad.final_urls,
        ad_group_ad.ad.demand_gen_video_responsive_ad.business_name,
        ad_group_ad.ad.demand_gen_video_responsive_ad.call_to_actions,
        ad_group_ad.ad.demand_gen_video_responsive_ad.descriptions,
        ad_group_ad.ad.demand_gen_video_responsive_ad.headlines,
        ad_group_ad.ad.demand_gen_video_responsive_ad.long_headlines,
        ad_group_ad.ad.demand_gen_video_responsive_ad.videos
      FROM ad_group_ad
      WHERE ad_group_ad.ad.type = DEMAND_GEN_VIDEO_RESPONSIVE_AD
        AND campaign.status != 'REMOVED'
        AND ad_group_ad.status != 'REMOVED'
    `.trim(),
  });

  for (const r of demandGenVideoRows) {
    const campaign = r?.campaign || {};
    const adGroup = r?.adGroup || r?.ad_group || {};
    const ad = (r?.adGroupAd || r?.ad_group_ad || {})?.ad || {};
    const dg =
      ad?.demandGenVideoResponsiveAd ||
      ad?.demand_gen_video_responsive_ad ||
      {};

    const videoAssets = extractAssetResourceNames(dg.videos);

    creativeRows.push({
      owner_id: params.owner_id,
      provider: "google_ads",
      customer_id: params.customer_id,
      campaign_id: googleAdsString(campaign.id),
      campaign_name: googleAdsString(campaign.name),
      ad_group_id: googleAdsString(adGroup.id),
      ad_group_name: googleAdsString(adGroup.name),
      ad_id: googleAdsString(ad.id),
      ad_type: googleAdsString(ad.type),
      final_urls: ad?.finalUrls || ad?.final_urls || [],
      video_asset_resource_name: videoAssets[0] || null,
      video_ad_headline:
        googleAdsString(dg?.longHeadlines?.[0]?.text || dg?.long_headlines?.[0]?.text) ||
        googleAdsString(dg?.headlines?.[0]?.text),
      video_ad_description: googleAdsString(dg?.descriptions?.[0]?.text),
      call_to_action: googleAdsString(
        dg?.callToActions?.[0]?.text ||
        dg?.call_to_actions?.[0]?.text
      ),
      creative_content_raw: r,
      updated_at: now,
    });
  }

  if (creativeRows.length > 0) {
    await supabaseAdminUpsert(
      "ads_metrics_ads?on_conflict=owner_id,provider,customer_id,ad_id,date",
      creativeRows.map((row) => ({
        ...row,
        date: new Date().toISOString().slice(0, 10),
        impressions: 0,
        clicks: 0,
      }))
    );
  }

  return { ok: true, level: "ad_creative_content", total_rows: creativeRows.length };
}



// Synchronise campaign + ad group + ad.
async function syncGoogleAdsMetrics(params: {
  owner_id: string;
  customer_id: string;
  date_from: string;
  date_to: string;
  login_customer_id?: string;
}) {
  const structure = await upsertGoogleAdsStructureSnapshot({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    date: params.date_to,
    login_customer_id: params.login_customer_id,
  });

  const campaign = await syncGoogleAdsCampaignMetrics(params);
  const ad_group = await syncGoogleAdsAdGroupMetrics(params);
  const ad_group_targeting = await syncGoogleAdsAdGroupTargeting({
    owner_id: params.owner_id,
    customer_id: params.customer_id,
    date_to: params.date_to,
    login_customer_id: params.login_customer_id,
  });
  
  const ad = await syncGoogleAdsAdMetrics(params);  
  const ad_creative_content = await syncGoogleAdsAdCreativeContent({
  owner_id: params.owner_id,
  customer_id: params.customer_id,
  login_customer_id: params.login_customer_id,
  });

  return {
    ok: true,
    structure,
    campaign,
    ad_group,
    ad_group_targeting,
    ad,
    ad_creative_content,
  };
}

// Lit les paramètres communs Google Ads depuis req.
function readGoogleAdsBaseParams(req: express.Request) {
  return {
    owner_id: String(req.body?.owner_id || req.query?.owner_id || ""),
    customer_id: String(req.body?.customer_id || req.query?.customer_id || ""),
    login_customer_id: String(req.body?.login_customer_id || req.query?.login_customer_id || "") || undefined,
  };
}



type GoogleAdsDiscoveredAccount = GoogleAdsCustomerRow & {
  parent_customer_id: string | null;
  level: number;
};

async function getGoogleAdsCustomerInfo(params: {
  access_token: string;
  owner_id: string;
  customer_id: string;
  login_customer_id?: string;
  parent_customer_id?: string | null;
  level?: number;
}): Promise<GoogleAdsDiscoveredAccount> {
  const rows = await googleAdsSearch({
    access_token: params.access_token,
    customer_id: params.customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        customer.id,
        customer.descriptive_name,
        customer.currency_code,
        customer.time_zone,
        customer.manager
      FROM customer
      LIMIT 1
    `.trim(),
  });

  const c = rows[0]?.customer || {};

  return {
    owner_id: params.owner_id,
    customer_id: String(c.id || params.customer_id),
    resource_name: `customers/${String(c.id || params.customer_id)}`,
    descriptive_name: c.descriptiveName ?? c.descriptive_name ?? null,
    currency_code: c.currencyCode ?? c.currency_code ?? null,
    time_zone: c.timeZone ?? c.time_zone ?? null,
    is_manager: googleAdsBool(c.manager),
    parent_customer_id: params.parent_customer_id ?? null,
    level: params.level ?? 0,
  };
}

async function listGoogleAdsCustomerClients(params: {
  access_token: string;
  owner_id: string;
  manager_customer_id: string;
  login_customer_id?: string;
  level: number;
}) {
  const rows = await googleAdsSearchStream({
    access_token: params.access_token,
    customer_id: params.manager_customer_id,
    login_customer_id: params.login_customer_id,
    query: `
      SELECT
        customer_client.client_customer,
        customer_client.id,
        customer_client.descriptive_name,
        customer_client.currency_code,
        customer_client.time_zone,
        customer_client.manager,
        customer_client.level,
        customer_client.status
      FROM customer_client
      WHERE customer_client.status = 'ENABLED'
    `.trim(),
  });

  return rows
    .map((r: any) => {
      const cc = r?.customerClient || r?.customer_client || {};
      const childId = String(cc.id || "").replace(/\D/g, "");

      if (!childId || childId === params.manager_customer_id) return null;

      return {
        owner_id: params.owner_id,
        customer_id: childId,
        resource_name: cc.clientCustomer || cc.client_customer || `customers/${childId}`,
        descriptive_name: cc.descriptiveName ?? cc.descriptive_name ?? null,
        currency_code: cc.currencyCode ?? cc.currency_code ?? null,
        time_zone: cc.timeZone ?? cc.time_zone ?? null,
        is_manager: googleAdsBool(cc.manager),
        parent_customer_id: params.manager_customer_id,
        level: params.level,
      } satisfies GoogleAdsDiscoveredAccount;
    })
    .filter(Boolean) as GoogleAdsDiscoveredAccount[];
}


//sub mcc

async function discoverGoogleAdsAccountsRecursive(params: {
  access_token: string;
  owner_id: string;
  root_customer_ids: string[];
  login_customer_id?: string;
}) {
  const accountsById = new Map<string, GoogleAdsDiscoveredAccount>();
  const skipped_accounts: Array<{ customer_id: string; error: string }> = [];
  const visitedManagers = new Set<string>();

  async function visit(customer_id: string, parent_customer_id: string | null, level: number) {
    if (level > 10) return;

    try {
      const info = await getGoogleAdsCustomerInfo({
        access_token: params.access_token,
        owner_id: params.owner_id,
        customer_id,
        login_customer_id: params.login_customer_id || undefined,
        parent_customer_id,
        level,
      });

      accountsById.set(info.customer_id, info);

      if (!info.is_manager || visitedManagers.has(info.customer_id)) return;
      visitedManagers.add(info.customer_id);

      const children = await listGoogleAdsCustomerClients({
        access_token: params.access_token,
        owner_id: params.owner_id,
        manager_customer_id: info.customer_id,
        login_customer_id: params.login_customer_id || info.customer_id,
        level: level + 1,
      });

      for (const child of children) {
        accountsById.set(child.customer_id, child);

        if (child.is_manager) {
          await visit(child.customer_id, child.parent_customer_id, child.level);
        }
      }
    } catch (e: any) {
      skipped_accounts.push({
        customer_id,
        error: e?.message || "Unknown error",
      });
    }
  }

  for (const rootId of params.root_customer_ids) {
    await visit(rootId, null, 0);
  }

  return {
    accounts: Array.from(accountsById.values()),
    skipped_accounts,
  };
}

// ==============================
// Google Ads API routes
// ==============================

// Route: liste les comptes Google Ads accessibles.
// Route: liste les comptes Google Ads accessibles + upsert google_ads_accounts.
app.get("/api/google-ads/customers", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.query.owner_id || "");
    const login_customer_id = String(req.query.login_customer_id || "");

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    let token = await getGoogleAdsToken(owner_id);
    token = await refreshGoogleAccessTokenIfNeeded(owner_id, token);

    const listUrl = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers:listAccessibleCustomers`;

    const listJson = await googleAdsFetch(String(token.access_token), listUrl, {
      loginCustomerId: login_customer_id || undefined,
      method: "GET",
    });

    const rootCustomerIds = (Array.isArray(listJson?.resourceNames) ? listJson.resourceNames : [])
      .map((r: string) => String(r).split("/")[1])
      .filter(Boolean);

    const accountsById = new Map<string, GoogleAdsCustomerRow>();
    const skipped_accounts: Array<{ customer_id: string; error: string }> = [];
    const visitedManagers = new Set<string>();

    async function listChildren(managerCustomerId: string, level: number) {
      if (visitedManagers.has(managerCustomerId)) return;
      visitedManagers.add(managerCustomerId);

      try {
        const rows = await googleAdsSearchStream({
          access_token: String(token.access_token),
          customer_id: managerCustomerId,
          login_customer_id: login_customer_id || managerCustomerId,
          query: `
            SELECT
              customer_client.client_customer,
              customer_client.id,
              customer_client.descriptive_name,
              customer_client.currency_code,
              customer_client.time_zone,
              customer_client.manager,
              customer_client.level
            FROM customer_client
          `.trim(),
        });

        for (const r of rows) {
          const cc = r?.customerClient || r?.customer_client || {};
          const childId = String(cc.id || "").replace(/\D/g, "");

          if (!childId || childId === managerCustomerId) continue;

          const isManager = googleAdsBool(cc.manager);

          accountsById.set(childId, {
            owner_id,
            customer_id: childId,
            resource_name: cc.clientCustomer || cc.client_customer || `customers/${childId}`,
            descriptive_name: cc.descriptiveName ?? cc.descriptive_name ?? null,
            currency_code: cc.currencyCode ?? cc.currency_code ?? null,
            time_zone: cc.timeZone ?? cc.time_zone ?? null,
            is_manager: isManager,
            parent_customer_id: managerCustomerId,
            level,
            status: null,
          });

          if (isManager) {
            await listChildren(childId, level + 1);
          }
        }
      } catch (e: any) {
        skipped_accounts.push({
          customer_id: managerCustomerId,
          error: e?.message || "Unknown error",
        });
      }
    }

    for (const rootId of rootCustomerIds) {
      try {
        const rootInfo = await getGoogleAdsCustomerInfo({
          access_token: String(token.access_token),
          owner_id,
          customer_id: rootId,
          login_customer_id: login_customer_id || undefined,
          parent_customer_id: null,
          level: 0,
        });

        accountsById.set(rootId, {
          ...rootInfo,
          status: null,
        });
      } catch {
        accountsById.set(rootId, {
          owner_id,
          customer_id: rootId,
          resource_name: `customers/${rootId}`,
          descriptive_name: `Compte ${rootId}`,
          currency_code: null,
          time_zone: null,
          is_manager: null,
          parent_customer_id: null,
          level: 0,
          status: null,
        });
      }

      await listChildren(rootId, 1);
    }

    const accounts = Array.from(accountsById.values());

    if (accounts.length > 0) {
      await supabaseAdminUpsert(
        "google_ads_accounts?on_conflict=owner_id,customer_id",
        accounts
      );
    }

    return res.json({
      ok: true,
      count: accounts.length,
      accounts,
      skipped_accounts,
    });
  } catch (e: any) {
    console.error("[google-ads][customers] error:", e);

    return res.status(500).json({
      error: e?.message || "Google Ads customers error",
    });
  }
});

// Route: liste les campagnes.
app.get("/api/google-ads/campaigns", requireAuth, async (req, res) => {
  try {
    const params = readGoogleAdsBaseParams(req);
    if (!params.owner_id) return res.status(400).json({ error: "Missing owner_id" });
    if (!params.customer_id) return res.status(400).json({ error: "Missing customer_id" });

    const campaigns = await listGoogleAdsCampaigns(params);

    return res.json({
      ok: true,
      total: campaigns.length,
      campaigns,
    });
  } catch (e: any) {
    console.error("[google-ads][campaigns] error:", e);
    return res.status(500).json({ error: e?.message || "Google Ads campaigns error" });
  }
});

// Route: liste les ad groups.
app.get("/api/google-ads/ad-groups", requireAuth, async (req, res) => {
  try {
    const params = readGoogleAdsBaseParams(req);
    if (!params.owner_id) return res.status(400).json({ error: "Missing owner_id" });
    if (!params.customer_id) return res.status(400).json({ error: "Missing customer_id" });

    const ad_groups = await listGoogleAdsAdGroups(params);

    return res.json({
      ok: true,
      total: ad_groups.length,
      ad_groups,
    });
  } catch (e: any) {
    console.error("[google-ads][ad-groups] error:", e);
    return res.status(500).json({ error: e?.message || "Google Ads ad groups error" });
  }
});

// Route: liste les ads.
app.get("/api/google-ads/ads", requireAuth, async (req, res) => {
  try {
    const params = readGoogleAdsBaseParams(req);
    if (!params.owner_id) return res.status(400).json({ error: "Missing owner_id" });
    if (!params.customer_id) return res.status(400).json({ error: "Missing customer_id" });

    const ads = await listGoogleAdsAds(params);

    return res.json({
      ok: true,
      total: ads.length,
      ads,
    });
  } catch (e: any) {
    console.error("[google-ads][ads] error:", e);
    return res.status(500).json({ error: e?.message || "Google Ads ads error" });
  }
});

// Route: synchronise les métriques.
app.post("/api/google-ads/sync-metrics", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    const customer_id = String(req.body?.customer_id || "");
    const date_from = String(req.body?.date_from || "");
    const date_to = String(req.body?.date_to || "");
    const login_customer_id = String(req.body?.login_customer_id || "") || undefined;

    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });
    if (!customer_id) return res.status(400).json({ error: "Missing customer_id" });
    if (!date_from) return res.status(400).json({ error: "Missing date_from" });
    if (!date_to) return res.status(400).json({ error: "Missing date_to" });

    const result = await syncGoogleAdsMetrics({
      owner_id,
      customer_id,
      date_from,
      date_to,
      login_customer_id,
    });

    return res.json(result);
  } catch (e: any) {
    console.error("[google-ads][sync-metrics] error:", e);
    return res.status(500).json({ error: e?.message || "Google Ads sync metrics error" });
  }
});

// Route: liste la structure + synchronise les métriques.
app.post("/api/google-ads/sync-all", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    const customer_id = String(req.body?.customer_id || "");
    const date_from = String(req.body?.date_from || "");
    const date_to = String(req.body?.date_to || "");
    const login_customer_id = String(req.body?.login_customer_id || "") || undefined;

    if (!owner_id) return res.status(400).json({ error: "Missing owner_id" });
    if (!customer_id) return res.status(400).json({ error: "Missing customer_id" });
    if (!date_from) return res.status(400).json({ error: "Missing date_from" });
    if (!date_to) return res.status(400).json({ error: "Missing date_to" });

    const campaigns = await listGoogleAdsCampaigns({ owner_id, customer_id, login_customer_id });
    const ad_groups = await listGoogleAdsAdGroups({ owner_id, customer_id, login_customer_id });
    const ads = await listGoogleAdsAds({ owner_id, customer_id, login_customer_id });

    const metrics = await syncGoogleAdsMetrics({
      owner_id,
      customer_id,
      date_from,
      date_to,
      login_customer_id,
    });

    return res.json({
      ok: true,
      structure: {
        campaigns: campaigns.length,
        ad_groups: ad_groups.length,
        ads: ads.length,
      },
      metrics,
    });
  } catch (e: any) {
    console.error("[google-ads][sync-all] error:", e);
    return res.status(500).json({ error: e?.message || "Google Ads sync all error" });
  }
});






// ==============================
// Facebook token helpers
// ==============================
async function getFacebookToken(owner_id: string) {
  const data = await supabaseAdminRpc<any>("get_provider_token", {
    p_owner_id: owner_id,
    p_provider: "facebook",
  });

  const token = Array.isArray(data)
    ? (data[0]?.decrypted_secret ?? data[0] ?? null)
    : (data?.decrypted_secret ?? data ?? null);

  if (!token) {
    throw new Error("No facebook token found for this owner_id");
  }

  return token;
}


function getFacebookAccessTokenFromToken(token: any) {
  let parsed = token;

  if (typeof parsed === "string") {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      throw new Error("Facebook token stocké invalide: JSON non parsable");
    }
  }

  const accessToken =
    parsed?.access_token ||
    parsed?.raw_token?.access_token ||
    parsed?.long_lived?.access_token ||
    parsed?.short_lived?.access_token ||
    "";

  if (!accessToken || typeof accessToken !== "string") {
    throw new Error("Facebook access_token introuvable dans le token stocké");
  }

  return accessToken;
}

async function facebookGraphGetWithAccessToken(
  path: string,
  access_token: string,
  query?: Record<string, string | number | boolean | null | undefined>
) {
  const url = new URL(`https://graph.facebook.com/v23.0/${path.replace(/^\/+/, "")}`);

  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null) continue;
      url.searchParams.set(key, String(value));
    }
  }

  url.searchParams.set("access_token", access_token);

  const res = await fetch(url.toString(), {
    method: "GET",
    headers: {
      Accept: "application/json",
    },
  });

  const text = await res.text();
  let json: any = null;

  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  if (!res.ok || json?.error) {
    throw new Error(`Facebook Graph error: ${res.status} ${text}`);
  }

  return json;
}


function getFacebookPageAccessTokenFromStoredToken(token: any) {
  let parsed = token;

  if (typeof parsed === "string") {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      throw new Error("Facebook page token stocké invalide: JSON non parsable");
    }
  }

  const accessToken = parsed?.access_token || "";

  if (!accessToken || typeof accessToken !== "string") {
    throw new Error("Facebook page access_token introuvable dans le token stocké");
  }

  return accessToken;
}




function isExpired(token: any) {
  const exp = token?.expires_at ? Date.parse(token.expires_at) : NaN;
  if (!Number.isFinite(exp)) return false; // if unknown, assume ok
  return Date.now() > exp - 60_000; // refresh 1 min early
}

// ==============================
// Facebook OAuth2
// ==============================

async function facebookGraphGet(url: URL) {
  const r = await fetch(url.toString(), {
    method: "GET",
    headers: { Accept: "application/json" },
  });

  const text = await r.text();
  let json: any = null;

  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  if (!r.ok || json?.error) {
    throw new Error(`Facebook Graph error: ${r.status} ${text}`);
  }

  return json;
}

/**
 * START:
 * GET /auth/facebook/start?owner_id=xxx&return_to=https://ton-front.com/analytics
 */
app.get("/auth/facebook/start", async (req, res) => {
  try {
    const owner_id = String(req.query.owner_id || "");
    const return_to = String(req.query.return_to || FRONTEND_RETURN_URL);

    if (!owner_id) {
      return res.status(400).send("Missing owner_id");
    }

    if (!FACEBOOK_APP_ID || !FACEBOOK_APP_SECRET || !FACEBOOK_OAUTH_REDIRECT_URI) {
      return res.status(500).send("Missing Facebook OAuth env vars");
    }

    const state = signState({
      owner_id,
      return_to,
      provider: "facebook",
      t: Date.now(),
    });

    const authUrl = new URL("https://www.facebook.com/v23.0/dialog/oauth");
    authUrl.searchParams.set("client_id", FACEBOOK_APP_ID);
    authUrl.searchParams.set("redirect_uri", FACEBOOK_OAUTH_REDIRECT_URI);
    authUrl.searchParams.set("state", state);
    authUrl.searchParams.set("response_type", "code");
    authUrl.searchParams.set("scope", FACEBOOK_OAUTH_SCOPES);

    return res.redirect(authUrl.toString());
  } catch (e: any) {
    console.error("[facebook][start] error:", e);
    return res.status(500).send("Facebook OAuth start error");
  }
});

/**
 * CALLBACK:
 * Facebook redirects here with ?code=...&state=...
 *
 * On fait :
 * 1) code -> short-lived user token
 * 2) short-lived -> long-lived user token
 * 3) /me pour stocker l'identité Meta de l'utilisateur
 * 4) stockage sécurisé en DB
 */
app.get("/auth/facebook/callback", async (req, res) => {
  try {
    const code = String(req.query.code || "");
    const state = String(req.query.state || "");
    const error = String(req.query.error || "");

    if (error) {
      const parsed = state ? verifyState(state) : null;
      const return_to = String(parsed?.return_to || FRONTEND_RETURN_URL);
      const u = new URL(return_to);
      u.searchParams.set("facebook", "error");
      u.searchParams.set("reason", error);
      return res.redirect(u.toString());
    }

    if (!code) {
      return res.status(400).send("Missing code");
    }

    const parsed = verifyState(state);
    if (!parsed) {
      return res.status(400).send("Invalid state");
    }

    const owner_id = String(parsed.owner_id || "");
    const return_to = String(parsed.return_to || FRONTEND_RETURN_URL);

    if (!owner_id) {
      return res.status(400).send("Missing owner_id");
    }

    // 1) code -> short-lived token
    const shortTokenUrl = new URL("https://graph.facebook.com/v23.0/oauth/access_token");
    shortTokenUrl.searchParams.set("client_id", FACEBOOK_APP_ID);
    shortTokenUrl.searchParams.set("client_secret", FACEBOOK_APP_SECRET);
    shortTokenUrl.searchParams.set("redirect_uri", FACEBOOK_OAUTH_REDIRECT_URI);
    shortTokenUrl.searchParams.set("code", code);

    const shortTokenJson = await facebookGraphGet(shortTokenUrl);

    const short_access_token = String(shortTokenJson?.access_token || "");
    const short_token_type = String(shortTokenJson?.token_type || "");
    const short_expires_in = Number(shortTokenJson?.expires_in || 0);

    if (!short_access_token) {
      const u = new URL(return_to);
      u.searchParams.set("facebook", "error");
      u.searchParams.set("reason", "missing_short_token");
      return res.redirect(u.toString());
    }

    // 2) short-lived -> long-lived token
    const longTokenUrl = new URL("https://graph.facebook.com/v23.0/oauth/access_token");
    longTokenUrl.searchParams.set("grant_type", "fb_exchange_token");
    longTokenUrl.searchParams.set("client_id", FACEBOOK_APP_ID);
    longTokenUrl.searchParams.set("client_secret", FACEBOOK_APP_SECRET);
    longTokenUrl.searchParams.set("fb_exchange_token", short_access_token);

    let active_access_token = short_access_token;
    let active_token_type = short_token_type;
    let active_expires_in = short_expires_in;
    let raw_long_token: any = null;

    try {
      const longTokenJson = await facebookGraphGet(longTokenUrl);

      if (longTokenJson?.access_token) {
        active_access_token = String(longTokenJson.access_token);
        active_token_type = String(longTokenJson.token_type || short_token_type || "");
        active_expires_in = Number(longTokenJson.expires_in || 0);
        raw_long_token = longTokenJson;
      }
    } catch (e) {
      console.warn("[facebook][callback] long-lived exchange failed, fallback short-lived token:", e);
    }

    const expires_at =
      active_expires_in > 0
        ? new Date(Date.now() + active_expires_in * 1000).toISOString()
        : null;

    // 3) user info
    let meJson: any = null;
    try {
      const meUrl = new URL("https://graph.facebook.com/v23.0/me");
      meUrl.searchParams.set("fields", "id,name");
      meUrl.searchParams.set("access_token", active_access_token);
      meJson = await facebookGraphGet(meUrl);
    } catch (e) {
      console.warn("[facebook][callback] /me failed:", e);
    }

    // 4) stockage sécurisé
    await supabaseUpsertProviderTokenVault({
      owner_id,
      provider: "facebook",
      token: {
        provider: "facebook",
        access_token: active_access_token,
        token_type: active_token_type,
        expires_in: active_expires_in,
        expires_at,
        scopes: FACEBOOK_OAUTH_SCOPES.split(",").map((s) => s.trim()).filter(Boolean),
        user: meJson ?? null,
        short_lived: {
          access_token: short_access_token,
          token_type: short_token_type,
          expires_in: short_expires_in,
          raw: shortTokenJson ?? {},
        },
        long_lived: raw_long_token ?? null,
        stored_at: new Date().toISOString(),
      },
    });

    const u = new URL(return_to);
    u.searchParams.set("facebook", "connected");
    if (meJson?.id) u.searchParams.set("facebook_user_id", String(meJson.id));
    return res.redirect(u.toString());
  } catch (e: any) {
    console.error("[facebook][callback] error:", e);
    const u = new URL(FRONTEND_RETURN_URL);
    u.searchParams.set("facebook", "error");
    u.searchParams.set("reason", e?.message || "unknown");
    return res.redirect(u.toString());
  }
});


/**
 * POST /api/facebook/debug/me
 * body: { owner_id: "..." }
 *
 * Test minimal :
 * 1) lit le token Facebook stocké en base
 * 2) extrait access_token
 * 3) appelle /me
 */
app.post("/api/facebook/debug/me", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    const token = await getFacebookToken(owner_id);
    const access_token = getFacebookAccessTokenFromToken(token);

    const me = await facebookGraphGetWithAccessToken("me", access_token, {
      fields: "id,name",
    });

    return res.json({
      ok: true,
      provider: "facebook",
      owner_id,
      facebook_user: me,
      expires_at: token?.expires_at ?? null,
      scopes: Array.isArray(token?.scopes) ? token.scopes : [],
    });
  } catch (e: any) {
    console.error("[facebook][debug/me] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook debug me error",
    });
  }
});


/**
 * POST /api/facebook/debug/pages
 * body: { owner_id: "..." }
 *
 * Test minimal :
 * 1) lit le token Facebook stocké en base
 * 2) extrait access_token
 * 3) appelle /me/accounts
 */
app.post("/api/facebook/debug/pages", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    const token = await getFacebookToken(owner_id);
    const access_token = getFacebookAccessTokenFromToken(token);

    const pagesJson = await facebookGraphGetWithAccessToken("me/accounts", access_token, {
      fields: "id,name,category,access_token,tasks",
      limit: 100,
    });

    const pages = Array.isArray(pagesJson?.data)
      ? pagesJson.data.map((p: any) => ({
          page_id: String(p?.id || ""),
          name: p?.name ?? null,
          category: p?.category ?? null,
          tasks: Array.isArray(p?.tasks) ? p.tasks : [],
          has_page_access_token: Boolean(p?.access_token),
          page_access_token: p?.access_token || null,
        }))
      : [];

    return res.json({
      ok: true,
      provider: "facebook",
      owner_id,
      count: pages.length,
      pages,
    });
  } catch (e: any) {
    console.error("[facebook][debug/pages] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook debug pages error",
    });
  }
});


/**
 * POST /api/facebook/sync-pages
 * body: { owner_id: "..." }
 *
 * 1) récupère les pages via /me/accounts
 * 2) upsert les métadonnées dans meta_pages
 * 3) stocke un page access token chiffré par page dans provider_tokens
 */
app.post("/api/facebook/sync-pages", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    const token = await getFacebookToken(owner_id);
    const access_token = getFacebookAccessTokenFromToken(token);

    const pagesJson = await facebookGraphGetWithAccessToken("me/accounts", access_token, {
      fields: "id,name,category,access_token,tasks",
      limit: 100,
    });

    const pagesRaw = Array.isArray(pagesJson?.data) ? pagesJson.data : [];

    const metaPageRows = pagesRaw
      .filter((p: any) => p?.id)
      .map((p: any) => ({
        owner_id,
        provider: "facebook",
        page_id: String(p.id),
        name: String(p?.name || ""),
        category: p?.category ?? null,
        tasks: Array.isArray(p?.tasks) ? p.tasks : [],
        raw: p,
      }));

    if (metaPageRows.length > 0) {
      await supabaseAdminUpsert(
        "meta_pages?on_conflict=owner_id,provider,page_id",
        metaPageRows
      );
    }

    let stored_page_tokens = 0;

    for (const p of pagesRaw) {
      const page_id = String(p?.id || "");
      const page_name = p?.name ?? null;
      const page_access_token = String(p?.access_token || "");

      if (!page_id || !page_access_token) continue;

      await upsertFacebookPageAccessToken({
        owner_id,
        page_id,
        page_name,
        page_access_token,
      });

      stored_page_tokens += 1;
    }

    return res.json({
      ok: true,
      owner_id,
      fetched_pages: pagesRaw.length,
      stored_meta_pages: metaPageRows.length,
      stored_page_tokens,
      page_ids: metaPageRows.map((p: { page_id: string }) => p.page_id),
    });
  } catch (e: any) {
    console.error("[facebook][sync-pages] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook sync pages error",
    });
  }
});






/**
 * POST /api/facebook/sync-page-posts
 * body: { owner_id: "...", page_id: "...", limit?: 100 }
 *
 * 1) lit le page access token stocké
 * 2) appelle /{page_id}/posts
 * 3) upsert dans meta_page_posts
 */
app.post("/api/facebook/sync-page-posts", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    const page_id = String(req.body?.page_id || "");
    const limit = Number(req.body?.limit || 100);

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    if (!page_id) {
      return res.status(400).json({ error: "Missing page_id" });
    }

    const pageToken = await getFacebookPageToken(owner_id, page_id);
    const page_access_token = getFacebookPageAccessTokenFromStoredToken(pageToken);

    const postsJson = await facebookGraphGetWithAccessToken(`${page_id}/posts`, page_access_token, {
      fields: "id,message,created_time,permalink_url,permalink",
      limit,
    });

    const postsRaw = Array.isArray(postsJson?.data) ? postsJson.data : [];

    const rows = postsRaw
      .filter((p: any) => p?.id)
      .map((p: any) => ({
        owner_id,
        provider: "facebook",
        page_id,
        post_id: String(p.id),
        message: p?.message ?? null,
        created_time: p?.created_time ?? null,
        permalink: p?.permalink ?? null,
        permalink_url: p?.permalink_url ?? null,
        raw: p,
      }));

    if (rows.length > 0) {
      await supabaseAdminUpsert(
        "meta_page_posts?on_conflict=owner_id,provider,post_id",
        rows
      );
    }

    return res.json({
      ok: true,
      owner_id,
      page_id,
      fetched_posts: postsRaw.length,
      stored_posts: rows.length,
      post_ids: rows.map((p: { post_id: string }) => p.post_id),
    });
  } catch (e: any) {
    console.error("[facebook][sync-page-posts] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook sync page posts error",
    });
  }
});




/**
 * POST /api/facebook/sync-page-post-metrics
 * body: { owner_id: "...", page_id: "...", post_id: "..." }
 */
app.post("/api/facebook/sync-page-post-metrics", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    const page_id = String(req.body?.page_id || "");
    const post_id = String(req.body?.post_id || "");

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }
    if (!page_id) {
      return res.status(400).json({ error: "Missing page_id" });
    }
    if (!post_id) {
      return res.status(400).json({ error: "Missing post_id" });
    }

    const pageToken = await getFacebookPageToken(owner_id, page_id);
    const page_access_token = getFacebookPageAccessTokenFromStoredToken(pageToken);

    const [insightsJson, socialJson] = await Promise.all([
      facebookGraphGetWithAccessToken(`${post_id}/insights`, page_access_token, {
        metric: "post_impressions_unique",
      }),
      facebookGraphGetWithAccessToken(post_id, page_access_token, {
        fields:
          "created_time,permalink_url,shares,comments.summary(true).limit(0),likes.summary(true).limit(0),reactions.summary(true).limit(0),message",
      }),
    ]);

    const impressionsMetric = Array.isArray(insightsJson?.data)
      ? insightsJson.data.find((x: any) => x?.name === "post_impressions_unique")
      : null;

    const impressions_unique = Number(impressionsMetric?.values?.[0]?.value ?? 0) || 0;
    const shares = Number(socialJson?.shares?.count ?? 0) || 0;
    const comments_count = Number(socialJson?.comments?.summary?.total_count ?? 0) || 0;
    const likes_count = Number(socialJson?.likes?.summary?.total_count ?? 0) || 0;
    const reactions_count = Number(socialJson?.reactions?.summary?.total_count ?? 0) || 0;

    const row = {
      owner_id,
      provider: "facebook",
      page_id,
      post_id,
      message: typeof socialJson?.message === "string" ? socialJson.message : null,
      created_time: socialJson?.created_time ?? null,
      permalink_url: socialJson?.permalink_url ?? null,
      impressions_unique,
      shares,
      comments_count,
      likes_count,
      reactions_count,
      metrics_fetched_at: new Date().toISOString(),
      metrics_raw: {
        impressions: insightsJson,
        social: socialJson,
      },
      raw: socialJson,
    };

    await supabaseAdminUpsert(
      "meta_page_posts?on_conflict=owner_id,provider,post_id",
      [row]
    );

    return res.json({
      ok: true,
      owner_id,
      page_id,
      post_id,
      impressions_unique,
      shares,
      comments_count,
      likes_count,
      reactions_count,
    });
  } catch (e: any) {
    console.error("[facebook][sync-page-post-metrics] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook sync page post metrics error",
    });
  }
});



/**
 * POST /api/facebook/sync-page-post-metrics-batch
 * body: { owner_id: "...", page_id: "...", limit?: 100 }
 */
app.post("/api/facebook/sync-page-post-metrics-batch", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    const page_id = String(req.body?.page_id || "");
    const limit = Number(req.body?.limit || 100);

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }
    if (!page_id) {
      return res.status(400).json({ error: "Missing page_id" });
    }

    const existingPosts = await supabaseAdminSelectRows<{
      post_id: string;
    }>(
      `meta_page_posts?select=post_id&owner_id=eq.${encodeURIComponent(
        owner_id
      )}&provider=eq.facebook&page_id=eq.${encodeURIComponent(
        page_id
      )}&order=created_time.desc.nullslast&limit=${limit}`
    );

    const postIds = existingPosts
      .map((row) => String(row.post_id || ""))
      .filter(Boolean);

    const pageToken = await getFacebookPageToken(owner_id, page_id);
    const page_access_token = getFacebookPageAccessTokenFromStoredToken(pageToken);

    let synced_posts = 0;
    let failed_posts = 0;
    const errors: Array<{ post_id: string; error: string }> = [];

    for (const post_id of postIds) {
      try {
        const [insightsJson, socialJson] = await Promise.all([
          facebookGraphGetWithAccessToken(`${post_id}/insights`, page_access_token, {
            metric: "post_impressions_unique",
          }),
          facebookGraphGetWithAccessToken(post_id, page_access_token, {
            fields:
              "created_time,permalink_url,shares,comments.summary(true).limit(0),likes.summary(true).limit(0),reactions.summary(true).limit(0),message",
          }),
        ]);

        const impressionsMetric = Array.isArray(insightsJson?.data)
          ? insightsJson.data.find((x: any) => x?.name === "post_impressions_unique")
          : null;

        const impressions_unique = Number(impressionsMetric?.values?.[0]?.value ?? 0) || 0;
        const shares = Number(socialJson?.shares?.count ?? 0) || 0;
        const comments_count = Number(socialJson?.comments?.summary?.total_count ?? 0) || 0;
        const likes_count = Number(socialJson?.likes?.summary?.total_count ?? 0) || 0;
        const reactions_count = Number(socialJson?.reactions?.summary?.total_count ?? 0) || 0;

        const row = {
          owner_id,
          provider: "facebook",
          page_id,
          post_id,
          message: typeof socialJson?.message === "string" ? socialJson.message : null,
          created_time: socialJson?.created_time ?? null,
          permalink_url: socialJson?.permalink_url ?? null,
          impressions_unique,
          shares,
          comments_count,
          likes_count,
          reactions_count,
          metrics_fetched_at: new Date().toISOString(),
          metrics_raw: {
            impressions: insightsJson,
            social: socialJson,
          },
          raw: socialJson,
        };

        await supabaseAdminUpsert(
          "meta_page_posts?on_conflict=owner_id,provider,post_id",
          [row]
        );

        synced_posts += 1;
      } catch (e: any) {
        failed_posts += 1;
        errors.push({
          post_id,
          error: e?.message || "Unknown error",
        });
      }
    }

    return res.json({
      ok: true,
      owner_id,
      page_id,
      total_posts: postIds.length,
      synced_posts,
      failed_posts,
      errors,
    });
  } catch (e: any) {
    console.error("[facebook][sync-page-post-metrics-batch] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook sync page post metrics batch error",
    });
  }
});




/**
 * POST /api/facebook/sync-organic-all
 * body: { owner_id: "...", page_limit?: 100, post_limit?: 100 }
 *
 * Pipeline complet organique :
 * 1) récupère les pages via /me/accounts avec le user token
 * 2) upsert meta_pages
 * 3) stocke chaque page access token chiffré
 * 4) pour chaque page, récupère les posts
 * 5) upsert meta_page_posts
 * 6) pour chaque post, récupère les metrics
 * 7) met à jour meta_page_posts
 */
app.post("/api/facebook/sync-organic-all", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    const page_limit = Number(req.body?.page_limit || 100);
    const post_limit = Number(req.body?.post_limit || 100);

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    // 1) User token Facebook
    const token = await getFacebookToken(owner_id);
    const access_token = getFacebookAccessTokenFromToken(token);

    // 2) Pages
    const pagesJson = await facebookGraphGetWithAccessToken("me/accounts", access_token, {
      fields: "id,name,category,access_token,tasks",
      limit: page_limit,
    });

    const pagesRaw = Array.isArray(pagesJson?.data) ? pagesJson.data : [];

    const metaPageRows = pagesRaw
      .filter((p: any) => p?.id)
      .map((p: any) => ({
        owner_id,
        provider: "facebook",
        page_id: String(p.id),
        name: String(p?.name || ""),
        category: p?.category ?? null,
        tasks: Array.isArray(p?.tasks) ? p.tasks : [],
        raw: p,
      }));

    if (metaPageRows.length > 0) {
      await supabaseAdminUpsert(
        "meta_pages?on_conflict=owner_id,provider,page_id",
        metaPageRows
      );
    }

    let stored_page_tokens = 0;
    let pages_synced = 0;
    let posts_synced = 0;
    let post_metrics_synced = 0;
    let failed_pages = 0;
    const page_errors: Array<{ page_id: string; error: string }> = [];

    for (const p of pagesRaw) {
      const page_id = String(p?.id || "");
      const page_name = p?.name ?? null;
      const page_access_token = String(p?.access_token || "");

      if (!page_id) continue;

      try {
        // 3) Store encrypted page token
        if (page_access_token) {
          await upsertFacebookPageAccessToken({
            owner_id,
            page_id,
            page_name,
            page_access_token,
          });
          stored_page_tokens += 1;
        }

        // 4) Fetch posts for the page
        const postsJson = await facebookGraphGetWithAccessToken(`${page_id}/posts`, page_access_token, {
          fields: "id,message,created_time,permalink_url,permalink",
          limit: post_limit,
        });

        const postsRaw = Array.isArray(postsJson?.data) ? postsJson.data : [];

        const postRows = postsRaw
          .filter((post: any) => post?.id)
          .map((post: any) => ({
            owner_id,
            provider: "facebook",
            page_id,
            post_id: String(post.id),
            message: post?.message ?? null,
            created_time: post?.created_time ?? null,
            permalink: post?.permalink ?? null,
            permalink_url: post?.permalink_url ?? null,
            raw: post,
          }));

        if (postRows.length > 0) {
          await supabaseAdminUpsert(
            "meta_page_posts?on_conflict=owner_id,provider,post_id",
            postRows
          );
        }

        posts_synced += postRows.length;

        // 5) Fetch metrics for each post
        for (const post of postsRaw) {
          const post_id = String(post?.id || "");
          if (!post_id) continue;

          try {
            const [insightsJson, socialJson] = await Promise.all([
              facebookGraphGetWithAccessToken(`${post_id}/insights`, page_access_token, {
                metric: "post_impressions_unique",
              }),
              facebookGraphGetWithAccessToken(post_id, page_access_token, {
                fields:
                  "created_time,permalink_url,shares,comments.summary(true).limit(0),likes.summary(true).limit(0),reactions.summary(true).limit(0),message",
              }),
            ]);

            const impressionsMetric = Array.isArray(insightsJson?.data)
              ? insightsJson.data.find((x: any) => x?.name === "post_impressions_unique")
              : null;

            const impressions_unique = Number(impressionsMetric?.values?.[0]?.value ?? 0) || 0;
            const shares = Number(socialJson?.shares?.count ?? 0) || 0;
            const comments_count = Number(socialJson?.comments?.summary?.total_count ?? 0) || 0;
            const likes_count = Number(socialJson?.likes?.summary?.total_count ?? 0) || 0;
            const reactions_count = Number(socialJson?.reactions?.summary?.total_count ?? 0) || 0;

            await supabaseAdminUpsert(
              "meta_page_posts?on_conflict=owner_id,provider,post_id",
              [
                {
                  owner_id,
                  provider: "facebook",
                  page_id,
                  post_id,
                  message: typeof socialJson?.message === "string" ? socialJson.message : null,
                  created_time: socialJson?.created_time ?? null,
                  permalink_url: socialJson?.permalink_url ?? null,
                  impressions_unique,
                  shares,
                  comments_count,
                  likes_count,
                  reactions_count,
                  metrics_fetched_at: new Date().toISOString(),
                  metrics_raw: {
                    impressions: insightsJson,
                    social: socialJson,
                  },
                  raw: socialJson,
                },
              ]
            );

            post_metrics_synced += 1;
          } catch {
            // on laisse passer les erreurs post-level
          }
        }

        pages_synced += 1;
      } catch (e: any) {
        failed_pages += 1;
        page_errors.push({
          page_id,
          error: e?.message || "Unknown error",
        });
      }
    }

    return res.json({
      ok: true,
      owner_id,
      pages_found: pagesRaw.length,
      pages_synced,
      stored_meta_pages: metaPageRows.length,
      stored_page_tokens,
      posts_synced,
      post_metrics_synced,
      failed_pages,
      page_errors,
    });
  } catch (e: any) {
    console.error("[facebook][sync-organic-all] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook sync organic all error",
    });
  }
});




/**
 * POST /api/facebook/sync-ad-accounts
 * body: { owner_id: "...", limit?: 100 }
 *
 * 1) lit le user token Facebook
 * 2) appelle /me/adaccounts
 * 3) upsert les comptes pub dans meta_ad_accounts
 */
app.post("/api/facebook/sync-ad-accounts", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    const limit = Number(req.body?.limit || 100);

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    const token = await getFacebookToken(owner_id);
    const access_token = getFacebookAccessTokenFromToken(token);

    const adAccountsJson = await facebookGraphGetWithAccessToken("me/adaccounts", access_token, {
      fields: "id,account_id,name,currency,timezone_id,timezone_name",
      limit,
    });

    const accountsRaw = Array.isArray(adAccountsJson?.data) ? adAccountsJson.data : [];

    const rows = accountsRaw
      .filter((a: any) => a?.account_id)
      .map((a: any) => ({
        owner_id,
        provider: "facebook",
        account_id: String(a.account_id),
        name: String(a?.name || ""),
        currency: a?.currency ?? null,
        timezone_id: a?.timezone_id != null ? String(a.timezone_id) : null,
        timezone_name: a?.timezone_name ?? null,
        fetched_at: new Date().toISOString(),
        raw: a,
      }));

    if (rows.length > 0) {
      await supabaseAdminUpsert(
        "meta_ad_accounts?on_conflict=owner_id,provider,account_id",
        rows
      );
    }

    return res.json({
      ok: true,
      owner_id,
      ad_accounts_found: accountsRaw.length,
      stored_ad_accounts: rows.length,
      account_ids: rows.map((r: { account_id: string }) => r.account_id),
    });
  } catch (e: any) {
    console.error("[facebook][sync-ad-accounts] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook sync ad accounts error",
    });
  }
});


 


/**
 * POST /api/facebook/sync-ad-campaigns
 * body: { owner_id: "...", limit?: 100 }
 *
 * 1) lit le user token Facebook
 * 2) lit les comptes pub déjà stockés dans meta_ad_accounts
 * 3) appelle /act_<account_id>/campaigns pour chaque compte
 * 4) upsert les campagnes dans meta_ad_campaigns
 */
app.post("/api/facebook/sync-ad-campaigns", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    const limit = Number(req.body?.limit || 100);

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    const token = await getFacebookToken(owner_id);
    const access_token = getFacebookAccessTokenFromToken(token);

    const adAccounts = await supabaseAdminSelectRows<{
      account_id: string;
      name?: string | null;
    }>(
      `meta_ad_accounts?select=account_id,name&owner_id=eq.${encodeURIComponent(
        owner_id
      )}&provider=eq.facebook&limit=${limit}`
    );

    let ad_accounts_found = adAccounts.length;
    let campaigns_found = 0;
    let stored_campaigns = 0;
    let failed_accounts = 0;
    const account_errors: Array<{ account_id: string; error: string }> = [];

    for (const acc of adAccounts) {
      const account_id = String(acc?.account_id || "");
      if (!account_id) continue;

      const ad_account_id_act = account_id.startsWith("act_")
        ? account_id
        : `act_${account_id}`;

      try {
        const campaignsJson = await facebookGraphGetWithAccessToken(
          `${ad_account_id_act}/campaigns`,
          access_token,
          {
            fields:
              "id,account_id,name,objective,status,effective_status,created_time,updated_time,start_time,stop_time",
            limit,
          }
        );

        const campaignsRaw = Array.isArray(campaignsJson?.data) ? campaignsJson.data : [];
        campaigns_found += campaignsRaw.length;

        const rows = campaignsRaw
          .filter((c: any) => c?.id)
          .map((c: any) => ({
            owner_id,
            provider: "facebook",
            campaign_id: String(c.id),
            account_id: String(c?.account_id || account_id),
            ad_account_id_act: `act_${String(c?.account_id || account_id)}`,
            name: String(c?.name || ""),
            objective: c?.objective ?? null,
            status: c?.status ?? null,
            effective_status: c?.effective_status ?? null,
            created_time: c?.created_time ?? null,
            updated_time: c?.updated_time ?? null,
            start_time: c?.start_time ?? null,
            stop_time: c?.stop_time ?? null,
            fetched_at: new Date().toISOString(),
            raw: c,
          }));

        if (rows.length > 0) {
          await supabaseAdminUpsert(
            "meta_ad_campaigns?on_conflict=owner_id,provider,campaign_id",
            rows
          );
          stored_campaigns += rows.length;
        }
      } catch (e: any) {
        failed_accounts += 1;
        account_errors.push({
          account_id,
          error: e?.message || "Unknown error",
        });
      }
    }

    return res.json({
      ok: true,
      owner_id,
      ad_accounts_found,
      campaigns_found,
      stored_campaigns,
      failed_accounts,
      account_errors,
    });
  } catch (e: any) {
    console.error("[facebook][sync-ad-campaigns] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook sync ad campaigns error",
    });
  }
});


/**
 * POST /api/facebook/sync-ad-campaign-metrics
 * body: { owner_id: "...", limit?: 100 }
 *
 * 1) lit le user token Facebook
 * 2) lit les comptes pub déjà stockés dans meta_ad_accounts
 * 3) récupère les ads/creatives de chaque compte
 * 4) récupère les insights niveau ad de chaque compte
 * 5) agrège par campaign_id
 * 6) upsert les métriques dans meta_ad_campaigns
 */
app.post("/api/facebook/sync-ad-campaign-metrics", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    const limit = Number(req.body?.limit || 100);

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    const token = await getFacebookToken(owner_id);
    const access_token = getFacebookAccessTokenFromToken(token);

    const adAccounts = await supabaseAdminSelectRows<{
      account_id: string;
    }>(
      `meta_ad_accounts?select=account_id&owner_id=eq.${encodeURIComponent(
        owner_id
      )}&provider=eq.facebook&limit=${limit}`
    );

    let ad_accounts_found = adAccounts.length;
    let campaigns_updated = 0;
    let failed_accounts = 0;
    const account_errors: Array<{ account_id: string; error: string }> = [];

    for (const acc of adAccounts) {
      const account_id = String(acc?.account_id || "");
      if (!account_id) continue;

      const ad_account_id_act = account_id.startsWith("act_")
        ? account_id
        : `act_${account_id}`;

      try {
        const [adsJson, insightsJson] = await Promise.all([
          facebookGraphGetWithAccessToken(`${ad_account_id_act}/ads`, access_token, {
            fields:
              "id,name,creative{id,object_story_id,object_story_spec,asset_feed_spec,image_hash,video_id,instagram_permalink_url},effective_object_story_id",
            limit,
          }),
          facebookGraphGetWithAccessToken(`${ad_account_id_act}/insights`, access_token, {
            fields:
              "account_id,account_name,campaign_id,ad_id,ad_name,impressions,clicks,spend,reach,cpm,cpc,ctr,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_p100_watched_actions,video_play_actions,actions",
            level: "ad",
            date_preset: "maximum",
            action_breakdowns: "action_type",
            limit,
          }),
        ]);

        const adsRaw = Array.isArray(adsJson?.data) ? adsJson.data : [];
        const insightsRaw = Array.isArray(insightsJson?.data) ? insightsJson.data : [];

        const creativeByAdId = new Map<
          string,
          {
            creative_id: string | null;
            object_story_id: string | null;
            raw_creative: any;
          }
        >();

        for (const ad of adsRaw) {
          const ad_id = String(ad?.id || "");
          if (!ad_id) continue;

          creativeByAdId.set(ad_id, {
            creative_id: ad?.creative?.id ? String(ad.creative.id) : null,
            object_story_id:
              ad?.creative?.object_story_id
                ? String(ad.creative.object_story_id)
                : ad?.effective_object_story_id
                ? String(ad.effective_object_story_id)
                : null,
            raw_creative: ad?.creative ?? null,
          });
        }

        const campaignMap = new Map<
          string,
          {
            owner_id: string;
            provider: string;
            campaign_id: string;
            account_id: string;
            ad_account_id_act: string;
            impressions: number;
            clicks: number;
            reach: number;
            spend: number;
            cpm: number | null;
            cpc: number | null;
            ctr: number | null;
            creative_ids: Set<string>;
            object_story_ids: Set<string>;
            actions: Record<string, number>;
            video_play_actions: Record<string, number>;
            video_p25_watched_actions: Record<string, number>;
            video_p50_watched_actions: Record<string, number>;
            video_p75_watched_actions: Record<string, number>;
            video_p95_watched_actions: Record<string, number>;
            video_p100_watched_actions: Record<string, number>;
            date_start: string | null;
            date_stop: string | null;
            metrics_raw: any[];
            raw_creative: any | null;
          }
        >();

        for (const row of insightsRaw) {
          const campaign_id = String(row?.campaign_id || "");
          const ad_id = String(row?.ad_id || "");

          if (!campaign_id) continue;

          if (!campaignMap.has(campaign_id)) {
            campaignMap.set(campaign_id, {
              owner_id,
              provider: "facebook",
              campaign_id,
              account_id: String(row?.account_id || account_id),
              ad_account_id_act: `act_${String(row?.account_id || account_id)}`,
              impressions: 0,
              clicks: 0,
              reach: 0,
              spend: 0,
              cpm: null,
              cpc: null,
              ctr: null,
              creative_ids: new Set<string>(),
              object_story_ids: new Set<string>(),
              actions: {},
              video_play_actions: {},
              video_p25_watched_actions: {},
              video_p50_watched_actions: {},
              video_p75_watched_actions: {},
              video_p95_watched_actions: {},
              video_p100_watched_actions: {},
              date_start: row?.date_start ?? null,
              date_stop: row?.date_stop ?? null,
              metrics_raw: [],
              raw_creative: null,
            });
          }

          const camp = campaignMap.get(campaign_id)!;
          const creative = creativeByAdId.get(ad_id);

          camp.impressions += Number(row?.impressions ?? 0) || 0;
          camp.clicks += Number(row?.clicks ?? 0) || 0;
          camp.reach += Number(row?.reach ?? 0) || 0;
          camp.spend += Number(row?.spend ?? 0) || 0;

          if (row?.cpm != null) camp.cpm = Number(row.cpm) || 0;
          if (row?.cpc != null) camp.cpc = Number(row.cpc) || 0;
          if (row?.ctr != null) camp.ctr = Number(row.ctr) || 0;

          if (creative?.creative_id) camp.creative_ids.add(creative.creative_id);
          if (creative?.object_story_id) camp.object_story_ids.add(creative.object_story_id);
          if (creative?.raw_creative && !camp.raw_creative) camp.raw_creative = creative.raw_creative;

          mergeNumberMaps(camp.actions, fbActionsArrayToObject(row?.actions));
          mergeNumberMaps(
            camp.video_play_actions,
            fbActionsArrayToObject(row?.video_play_actions)
          );
          mergeNumberMaps(
            camp.video_p25_watched_actions,
            fbActionsArrayToObject(row?.video_p25_watched_actions)
          );
          mergeNumberMaps(
            camp.video_p50_watched_actions,
            fbActionsArrayToObject(row?.video_p50_watched_actions)
          );
          mergeNumberMaps(
            camp.video_p75_watched_actions,
            fbActionsArrayToObject(row?.video_p75_watched_actions)
          );
          mergeNumberMaps(
            camp.video_p95_watched_actions,
            fbActionsArrayToObject(row?.video_p95_watched_actions)
          );
          mergeNumberMaps(
            camp.video_p100_watched_actions,
            fbActionsArrayToObject(row?.video_p100_watched_actions)
          );

          camp.metrics_raw.push({
            ad_id,
            insight: row,
            creative: creative ?? null,
          });
        }

        const upsertRows = Array.from(campaignMap.values()).map((camp) => ({
          owner_id: camp.owner_id,
          provider: camp.provider,
          campaign_id: camp.campaign_id,
          account_id: camp.account_id,
          ad_account_id_act: camp.ad_account_id_act,
          impressions: camp.impressions,
          clicks: camp.clicks,
          reach: camp.reach,
          spend: Math.round(camp.spend * 100) / 100,
          cpm: camp.cpm,
          cpc: camp.cpc,
          ctr: camp.ctr,
          creative_ids: Array.from(camp.creative_ids),
          object_story_ids: Array.from(camp.object_story_ids),
          date_start: camp.date_start,
          date_stop: camp.date_stop,
          metrics_raw: camp.metrics_raw,
          actions: camp.actions,
          video_play_actions: camp.video_play_actions,
          video_p25_watched_actions: camp.video_p25_watched_actions,
          video_p50_watched_actions: camp.video_p50_watched_actions,
          video_p75_watched_actions: camp.video_p75_watched_actions,
          video_p95_watched_actions: camp.video_p95_watched_actions,
          video_p100_watched_actions: camp.video_p100_watched_actions,
          raw_creative: camp.raw_creative,
          fetched_at: new Date().toISOString(),
        }));

        if (upsertRows.length > 0) {
          await supabaseAdminUpsert(
            "meta_ad_campaigns?on_conflict=owner_id,provider,campaign_id",
            upsertRows
          );
          campaigns_updated += upsertRows.length;
        }
      } catch (e: any) {
        failed_accounts += 1;
        account_errors.push({
          account_id,
          error: e?.message || "Unknown error",
        });
      }
    }

    return res.json({
      ok: true,
      owner_id,
      ad_accounts_found,
      campaigns_updated,
      failed_accounts,
      account_errors,
    });
  } catch (e: any) {
    console.error("[facebook][sync-ad-campaign-metrics] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook sync ad campaign metrics error",
    });
  }
});




/**
 * POST /api/facebook/sync-ads-all
 * body: { owner_id: "...", limit?: 100 }
 *
 * Pipeline complet ads :
 * 1) récupère les comptes pub via /me/adaccounts
 * 2) upsert meta_ad_accounts
 * 3) récupère les campagnes de chaque compte
 * 4) upsert meta_ad_campaigns
 * 5) récupère les ads + insights de chaque compte
 * 6) agrège les métriques par campaign_id
 * 7) met à jour meta_ad_campaigns
 */
app.post("/api/facebook/sync-ads-all", requireAuth, async (req, res) => {
  try {
    const owner_id = String(req.body?.owner_id || "");
    const limit = Number(req.body?.limit || 100);

    if (!owner_id) {
      return res.status(400).json({ error: "Missing owner_id" });
    }

    const token = await getFacebookToken(owner_id);
    const access_token = getFacebookAccessTokenFromToken(token);

    // 1) Ad accounts
    const adAccountsJson = await facebookGraphGetWithAccessToken("me/adaccounts", access_token, {
      fields: "id,account_id,name,currency,timezone_id,timezone_name",
      limit,
    });

    const accountsRaw = Array.isArray(adAccountsJson?.data) ? adAccountsJson.data : [];

    const accountRows = accountsRaw
      .filter((a: any) => a?.account_id)
      .map((a: any) => ({
        owner_id,
        provider: "facebook",
        account_id: String(a.account_id),
        name: String(a?.name || ""),
        currency: a?.currency ?? null,
        timezone_id: a?.timezone_id != null ? String(a.timezone_id) : null,
        timezone_name: a?.timezone_name ?? null,
        fetched_at: new Date().toISOString(),
        raw: a,
      }));

    if (accountRows.length > 0) {
      await supabaseAdminUpsert(
        "meta_ad_accounts?on_conflict=owner_id,provider,account_id",
        accountRows
      );
    }

    let campaigns_synced = 0;
    let campaign_metrics_synced = 0;
    let failed_accounts = 0;
    const account_errors: Array<{ account_id: string; error: string }> = [];

    // 2) campaigns + metrics per account
    for (const acc of accountsRaw) {
      const account_id = String(acc?.account_id || "");
      if (!account_id) continue;

      const ad_account_id_act = account_id.startsWith("act_")
        ? account_id
        : `act_${account_id}`;

      try {
        // campaigns
        const campaignsJson = await facebookGraphGetWithAccessToken(
          `${ad_account_id_act}/campaigns`,
          access_token,
          {
            fields:
              "id,account_id,name,objective,status,effective_status,created_time,updated_time,start_time,stop_time",
            limit,
          }
        );

        const campaignsRaw = Array.isArray(campaignsJson?.data) ? campaignsJson.data : [];

        const campaignRows = campaignsRaw
          .filter((c: any) => c?.id)
          .map((c: any) => ({
            owner_id,
            provider: "facebook",
            campaign_id: String(c.id),
            account_id: String(c?.account_id || account_id),
            ad_account_id_act: `act_${String(c?.account_id || account_id)}`,
            name: String(c?.name || ""),
            objective: c?.objective ?? null,
            status: c?.status ?? null,
            effective_status: c?.effective_status ?? null,
            created_time: c?.created_time ?? null,
            updated_time: c?.updated_time ?? null,
            start_time: c?.start_time ?? null,
            stop_time: c?.stop_time ?? null,
            fetched_at: new Date().toISOString(),
            raw: c,
          }));

        if (campaignRows.length > 0) {
          await supabaseAdminUpsert(
            "meta_ad_campaigns?on_conflict=owner_id,provider,campaign_id",
            campaignRows
          );
          campaigns_synced += campaignRows.length;
        }

        // creatives + insights
        const [adsJson, insightsJson] = await Promise.all([
          facebookGraphGetWithAccessToken(`${ad_account_id_act}/ads`, access_token, {
            fields:
              "id,name,creative{id,object_story_id,object_story_spec,asset_feed_spec,image_hash,video_id,instagram_permalink_url},effective_object_story_id",
            limit,
          }),
          facebookGraphGetWithAccessToken(`${ad_account_id_act}/insights`, access_token, {
            fields:
              "account_id,account_name,campaign_id,ad_id,ad_name,impressions,clicks,spend,reach,cpm,cpc,ctr,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_p100_watched_actions,video_play_actions,actions,date_start,date_stop",
            level: "ad",
            date_preset: "maximum",
            action_breakdowns: "action_type",
            limit,
          }),
        ]);

        const adsRaw = Array.isArray(adsJson?.data) ? adsJson.data : [];
        const insightsRaw = Array.isArray(insightsJson?.data) ? insightsJson.data : [];

        const creativeByAdId = new Map<
          string,
          {
            creative_id: string | null;
            object_story_id: string | null;
            raw_creative: any;
          }
        >();

        for (const ad of adsRaw) {
          const ad_id = String(ad?.id || "");
          if (!ad_id) continue;

          creativeByAdId.set(ad_id, {
            creative_id: ad?.creative?.id ? String(ad.creative.id) : null,
            object_story_id:
              ad?.creative?.object_story_id
                ? String(ad.creative.object_story_id)
                : ad?.effective_object_story_id
                ? String(ad.effective_object_story_id)
                : null,
            raw_creative: ad?.creative ?? null,
          });
        }

        const campaignMap = new Map<
          string,
          {
            owner_id: string;
            provider: string;
            campaign_id: string;
            account_id: string;
            ad_account_id_act: string;
            impressions: number;
            clicks: number;
            reach: number;
            spend: number;
            cpm: number | null;
            cpc: number | null;
            ctr: number | null;
            creative_ids: Set<string>;
            object_story_ids: Set<string>;
            actions: Record<string, number>;
            video_play_actions: Record<string, number>;
            video_p25_watched_actions: Record<string, number>;
            video_p50_watched_actions: Record<string, number>;
            video_p75_watched_actions: Record<string, number>;
            video_p95_watched_actions: Record<string, number>;
            video_p100_watched_actions: Record<string, number>;
            date_start: string | null;
            date_stop: string | null;
            metrics_raw: any[];
            raw_creative: any | null;
          }
        >();

        for (const row of insightsRaw) {
          const campaign_id = String(row?.campaign_id || "");
          const ad_id = String(row?.ad_id || "");
          if (!campaign_id) continue;

          if (!campaignMap.has(campaign_id)) {
            campaignMap.set(campaign_id, {
              owner_id,
              provider: "facebook",
              campaign_id,
              account_id: String(row?.account_id || account_id),
              ad_account_id_act: `act_${String(row?.account_id || account_id)}`,
              impressions: 0,
              clicks: 0,
              reach: 0,
              spend: 0,
              cpm: null,
              cpc: null,
              ctr: null,
              creative_ids: new Set<string>(),
              object_story_ids: new Set<string>(),
              actions: {},
              video_play_actions: {},
              video_p25_watched_actions: {},
              video_p50_watched_actions: {},
              video_p75_watched_actions: {},
              video_p95_watched_actions: {},
              video_p100_watched_actions: {},
              date_start: row?.date_start ?? null,
              date_stop: row?.date_stop ?? null,
              metrics_raw: [],
              raw_creative: null,
            });
          }

          const camp = campaignMap.get(campaign_id)!;
          const creative = creativeByAdId.get(ad_id);

          camp.impressions += Number(row?.impressions ?? 0) || 0;
          camp.clicks += Number(row?.clicks ?? 0) || 0;
          camp.reach += Number(row?.reach ?? 0) || 0;
          camp.spend += Number(row?.spend ?? 0) || 0;

          if (row?.cpm != null) camp.cpm = Number(row.cpm) || 0;
          if (row?.cpc != null) camp.cpc = Number(row.cpc) || 0;
          if (row?.ctr != null) camp.ctr = Number(row.ctr) || 0;

          if (creative?.creative_id) camp.creative_ids.add(creative.creative_id);
          if (creative?.object_story_id) camp.object_story_ids.add(creative.object_story_id);
          if (creative?.raw_creative && !camp.raw_creative) camp.raw_creative = creative.raw_creative;

          mergeNumberMaps(camp.actions, fbActionsArrayToObject(row?.actions));
          mergeNumberMaps(camp.video_play_actions, fbActionsArrayToObject(row?.video_play_actions));
          mergeNumberMaps(camp.video_p25_watched_actions, fbActionsArrayToObject(row?.video_p25_watched_actions));
          mergeNumberMaps(camp.video_p50_watched_actions, fbActionsArrayToObject(row?.video_p50_watched_actions));
          mergeNumberMaps(camp.video_p75_watched_actions, fbActionsArrayToObject(row?.video_p75_watched_actions));
          mergeNumberMaps(camp.video_p95_watched_actions, fbActionsArrayToObject(row?.video_p95_watched_actions));
          mergeNumberMaps(camp.video_p100_watched_actions, fbActionsArrayToObject(row?.video_p100_watched_actions));

          camp.metrics_raw.push({
            ad_id,
            insight: row,
            creative: creative ?? null,
          });
        }

        const metricRows = Array.from(campaignMap.values()).map((camp) => ({
          owner_id: camp.owner_id,
          provider: camp.provider,
          campaign_id: camp.campaign_id,
          account_id: camp.account_id,
          ad_account_id_act: camp.ad_account_id_act,
          impressions: camp.impressions,
          clicks: camp.clicks,
          reach: camp.reach,
          spend: Math.round(camp.spend * 100) / 100,
          cpm: camp.cpm,
          cpc: camp.cpc,
          ctr: camp.ctr,
          creative_ids: Array.from(camp.creative_ids),
          object_story_ids: Array.from(camp.object_story_ids),
          date_start: camp.date_start,
          date_stop: camp.date_stop,
          metrics_raw: camp.metrics_raw,
          actions: camp.actions,
          video_play_actions: camp.video_play_actions,
          video_p25_watched_actions: camp.video_p25_watched_actions,
          video_p50_watched_actions: camp.video_p50_watched_actions,
          video_p75_watched_actions: camp.video_p75_watched_actions,
          video_p95_watched_actions: camp.video_p95_watched_actions,
          video_p100_watched_actions: camp.video_p100_watched_actions,
          raw_creative: camp.raw_creative,
          fetched_at: new Date().toISOString(),
        }));

        if (metricRows.length > 0) {
          await supabaseAdminUpsert(
            "meta_ad_campaigns?on_conflict=owner_id,provider,campaign_id",
            metricRows
          );
          campaign_metrics_synced += metricRows.length;
        }
      } catch (e: any) {
        failed_accounts += 1;
        account_errors.push({
          account_id,
          error: e?.message || "Unknown error",
        });
      }
    }

    return res.json({
      ok: true,
      owner_id,
      ad_accounts_found: accountsRaw.length,
      ad_accounts_synced: accountRows.length,
      campaigns_synced,
      campaign_metrics_synced,
      failed_accounts,
      account_errors,
    });
  } catch (e: any) {
    console.error("[facebook][sync-ads-all] error:", e);
    return res.status(500).json({
      error: e?.message || "Facebook sync ads all error",
    });
  }
});


/**
 * START:
 * GET /auth/google-ads/start?owner_id=xxx&return_to=https://ton-front.com/analytics
 */
app.get("/auth/google-ads/start", async (req, res) => {
  try {
    const owner_id = String(req.query.owner_id || "");
    const return_to = String(req.query.return_to || FRONTEND_RETURN_URL);

    if (!owner_id) {
      return res.status(400).send("Missing owner_id");
    }

    if (!GOOGLE_OAUTH_CLIENT_ID || !GOOGLE_OAUTH_CLIENT_SECRET || !GOOGLE_OAUTH_REDIRECT_URI) {
      return res.status(500).send("Missing Google OAuth env vars");
    }

    const state = signState({
      owner_id,
      return_to,
      provider: "google_ads",
      t: Date.now(),
    });

    const authUrl = new URL("https://accounts.google.com/o/oauth2/v2/auth");
    authUrl.searchParams.set("client_id", GOOGLE_OAUTH_CLIENT_ID);
    authUrl.searchParams.set("redirect_uri", GOOGLE_OAUTH_REDIRECT_URI);
    authUrl.searchParams.set("response_type", "code");
    authUrl.searchParams.set("access_type", "offline");
    authUrl.searchParams.set("prompt", "consent");
    authUrl.searchParams.set("scope", "https://www.googleapis.com/auth/adwords");
    authUrl.searchParams.set("state", state);

    return res.redirect(authUrl.toString());
  } catch (e: any) {
    console.error("[google-ads][start] error:", e);
    return res.status(500).send("Google Ads OAuth start error");
  }
});

/**
 * CALLBACK:
 * GET /auth/google-ads/callback?code=...&state=...
 */
app.get("/auth/google-ads/callback", async (req, res) => {
  try {
    const code = String(req.query.code || "");
    const state = String(req.query.state || "");
    const error = String(req.query.error || "");

    const parsed = state ? verifyState(state) : null;
    const return_to = String(parsed?.return_to || FRONTEND_RETURN_URL);

    if (error) {
      const u = new URL(return_to);
      u.searchParams.set("google_ads", "error");
      u.searchParams.set("reason", error);
      return res.redirect(u.toString());
    }

    if (!code) {
      return res.status(400).send("Missing code");
    }

    if (!parsed) {
      return res.status(400).send("Invalid state");
    }

    const owner_id = String(parsed.owner_id || "");
    if (!owner_id) {
      return res.status(400).send("Missing owner_id");
    }

    const tokenRes = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: GOOGLE_OAUTH_CLIENT_ID,
        client_secret: GOOGLE_OAUTH_CLIENT_SECRET,
        code,
        grant_type: "authorization_code",
        redirect_uri: GOOGLE_OAUTH_REDIRECT_URI,
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
      const u = new URL(return_to);
      u.searchParams.set("google_ads", "error");
      u.searchParams.set("reason", "token_exchange_failed");
      return res.redirect(u.toString());
    }

    const expires_at = tokenJson?.expires_in
      ? new Date(Date.now() + Number(tokenJson.expires_in) * 1000).toISOString()
      : null;

    await supabaseUpsertProviderTokenVault({
      owner_id,
      provider: "google_ads",
      token: {
        ...tokenJson,
        expires_at,
        stored_at: new Date().toISOString(),
      },
    });

    const u = new URL(return_to);
    u.searchParams.set("google_ads", "connected");
    return res.redirect(u.toString());
  } catch (e: any) {
    console.error("[google-ads][callback] error:", e);
    const u = new URL(FRONTEND_RETURN_URL);
    u.searchParams.set("google_ads", "error");
    u.searchParams.set("reason", e?.message || "unknown");
    return res.redirect(u.toString());
  }
});





app.get("/health", (_, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log(`Relay API listening on http://localhost:${PORT}`);
});


