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
  "http://localhost:3001/auth/google-ads/callback";

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
const GOOGLE_ADS_API_VERSION = "v23"; // latest as of 2026-01-28

type ProviderTokenRow = {
  provider: string;
  owner_id: string;
  token: any; // jsonb
};

async function supabaseAdminSelectSingle<T>(path: string) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend env");
  }
  const url = `${SUPABASE_URL}/rest/v1/${path}`;
  const res = await fetch(url, {
    method: "GET",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
    },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase admin select failed: ${res.status} ${text}`);
  }
  const data = await res.json();
  return (Array.isArray(data) ? data[0] : data) as T | null;
}

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

async function getGoogleAdsToken(owner_id: string) {
  const row = await supabaseAdminSelectSingle<ProviderTokenRow>(
    `provider_tokens?select=provider,owner_id,token&owner_id=eq.${owner_id}&provider=eq.google_ads`
  );
  if (!row?.token) throw new Error("No google_ads token found for this owner_id");
  return row.token;
}


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


function isExpired(token: any) {
  const exp = token?.expires_at ? Date.parse(token.expires_at) : NaN;
  if (!Number.isFinite(exp)) return false; // if unknown, assume ok
  return Date.now() > exp - 60_000; // refresh 1 min early
}

async function refreshGoogleAccessTokenIfNeeded(owner_id: string, token: any) {
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

  if (!tokenRes.ok) {
    const text = await tokenRes.text();
    throw new Error(`Google refresh_token exchange failed: ${tokenRes.status} ${text}`);
  }

  const tokenJson = await tokenRes.json();
  const expires_at = tokenJson.expires_in
    ? new Date(Date.now() + Number(tokenJson.expires_in) * 1000).toISOString()
    : null;

  const newToken = {
    ...token,
    ...tokenJson,
    // Google ne renvoie pas toujours refresh_token au refresh
    refresh_token: token.refresh_token,
    expires_at,
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
    // keep text
  }

  if (!res.ok) {
    throw new Error(`Google Ads API error ${res.status}: ${text}`);
  }
  return json;
}

// GET: list accessible customers + upsert google_ads_accounts
app.get("/api/google-ads/customers", async (req, res) => {
  try {
    const owner_id = String(req.query.owner_id || "");
    const login_customer_id = String(req.query.login_customer_id || "");

    if (!owner_id) return res.status(400).send("Missing owner_id");

    let token = await getGoogleAdsToken(owner_id);
    token = await refreshGoogleAccessTokenIfNeeded(owner_id, token);

    // List accessible customers (no customerId needed) :contentReference[oaicite:4]{index=4}
    const listUrl = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers:listAccessibleCustomers`;
    const listJson = await googleAdsFetch(token.access_token, listUrl, {
      loginCustomerId: login_customer_id || undefined,
      method: "GET",
    });

    const resourceNames: string[] = listJson?.resourceNames || [];
    const customerIds = resourceNames
      .map((r) => (typeof r === "string" ? r.split("/")[1] : null))
      .filter(Boolean) as string[];

    // Enrich each customer with basic info using GAQL on that customer
    const accounts: any[] = [];
    for (const cid of customerIds) {
      const searchUrl = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${cid}/googleAds:search`;
      const q = `
        SELECT
          customer.id,
          customer.descriptive_name,
          customer.currency_code,
          customer.time_zone,
          customer.manager
        FROM customer
        LIMIT 1
      `.trim();

      const out = await googleAdsFetch(token.access_token, searchUrl, {
        loginCustomerId: login_customer_id || undefined,
        method: "POST",
        body: { query: q },
      });

      const row = out?.results?.[0] || {};
      const c = row?.customer || {};

      accounts.push({
        owner_id,
        customer_id: String(c.id || cid),
        resource_name: `customers/${String(c.id || cid)}`,
        descriptive_name: c.descriptiveName || null,
        currency_code: c.currencyCode || null,
        time_zone: c.timeZone || null,
        is_manager: typeof c.manager === "boolean" ? c.manager : null,
      });
    }

    // upsert DB
    if (accounts.length) {
      await supabaseAdminUpsert("google_ads_accounts?on_conflict=owner_id,customer_id", accounts);
    }

    return res.json({ accounts });
  } catch (e: any) {
    console.error("[google-ads][customers] error:", e);
    return res.status(500).json({ error: e?.message || "unknown error" });
  }
});

// POST: sync daily metrics and upsert google_ads_metrics_daily
app.post("/api/google-ads/sync-daily", async (req, res) => {
  try {
    const {
      owner_id,
      customer_id,
      date_from,
      date_to,
      level = "campaign", // "customer" | "campaign"
      login_customer_id,
    } = req.body || {};

    if (!owner_id) return res.status(400).send("Missing owner_id");
    if (!customer_id) return res.status(400).send("Missing customer_id");
    if (!date_from || !date_to) return res.status(400).send("Missing date_from/date_to");

    let token = await getGoogleAdsToken(String(owner_id));
    token = await refreshGoogleAccessTokenIfNeeded(String(owner_id), token);

    const from = String(date_from);
    const to = String(date_to);
    const lvl = String(level);

    const fromResource = lvl === "customer" ? "customer" : "campaign";
    const selectEntity =
      lvl === "customer"
        ? `customer.id`
        : `campaign.id, campaign.name, campaign.status`;

    // GAQL example pattern :contentReference[oaicite:5]{index=5}
    const q = `
      SELECT
        ${selectEntity},
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.average_cpc,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversion_value
      FROM ${fromResource}
      WHERE segments.date BETWEEN '${from}' AND '${to}'
    `.trim();

    const searchUrl = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${customer_id}/googleAds:searchStream`;
    const stream = await googleAdsFetch(token.access_token, searchUrl, {
      loginCustomerId: login_customer_id || undefined,
      method: "POST",
      body: { query: q },
    });

    // searchStream returns array of chunks
    const rows: any[] = [];
    const chunks = Array.isArray(stream) ? stream : [];
    for (const ch of chunks) {
      const results = ch?.results || [];
      for (const r of results) {
        const metrics = r?.metrics || {};
        const seg = r?.segments || {};

        if (lvl === "customer") {
          const c = r?.customer || {};
          rows.push({
            owner_id,
            customer_id: String(c.id || customer_id),
            level: "customer",
            entity_id: String(c.id || customer_id),
            entity_name: null,
            date: seg.date,
            impressions: metrics.impressions ?? null,
            clicks: metrics.clicks ?? null,
            cost_micros: metrics.costMicros ?? metrics.cost_micros ?? null,
            conversions: metrics.conversions ?? null,
            conversion_value: metrics.conversionValue ?? null,
            ctr: metrics.ctr ?? null,
            average_cpc_micros: metrics.averageCpc ?? null, // average_cpc is money type -> micros in REST
            raw: r,
          });
        } else {
          const c = r?.campaign || {};
          rows.push({
            owner_id,
            customer_id: String(customer_id),
            level: "campaign",
            entity_id: String(c.id),
            entity_name: c.name || null,
            date: seg.date,
            impressions: metrics.impressions ?? null,
            clicks: metrics.clicks ?? null,
            cost_micros: metrics.costMicros ?? metrics.cost_micros ?? null,
            conversions: metrics.conversions ?? null,
            conversion_value: metrics.conversionValue ?? null,
            ctr: metrics.ctr ?? null,
            average_cpc_micros: metrics.averageCpc ?? null,
            raw: r,
          });
        }
      }
    }

    if (rows.length) {
      await supabaseAdminUpsert(
        "google_ads_metrics_daily?on_conflict=owner_id,customer_id,level,entity_id,date",
        rows
      );
    }

    return res.json({ ok: true, inserted_or_merged: rows.length });
  } catch (e: any) {
    console.error("[google-ads][sync-daily] error:", e);
    return res.status(500).json({ error: e?.message || "unknown error" });
  }
});

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

app.get("/health", (_, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log(`Relay API listening on http://localhost:${PORT}`);
});




