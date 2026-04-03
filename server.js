const express = require("express");
const path = require("path");
const puppeteer = require("puppeteer");

const app = express();
const PORT = process.env.PORT || 3000;
const HOST = "0.0.0.0";

const TIKTOK_HOME_URL = "https://www.tiktok.com";
const TIKTOK_LOGIN_URL = "https://www.tiktok.com/login";
const TIKTOK_LOGIN_QR_URL = "https://www.tiktok.com/login/qrcode";
const TIKTOK_LIVE_CREATOR_URL = "https://www.tiktok.com/live/creator?lang=en";
const DEFAULT_TIMEOUT_MS = 45000;
const DESKTOP_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36";
const AUTH_COOKIE_NAMES = new Set([
  "sessionid",
  "sessionid_ss",
  "sid_tt",
  "sid_guard",
  "uid_tt",
  "uid_tt_ss",
  "passport_auth_status",
  "passport_auth_status_ss"
]);

const state = {
  browser: null,
  page: null,
  launchPromise: null,
  busy: false,
  loginPreview: null,
  loginPreviewAt: null,
  remoteSnapshot: null,
  liveStarted: false,
  liveConfig: {
    streamUrl: null,
    streamKey: null,
    source: null,
    capturedAt: null
  }
};

app.use(express.json({ limit: "2mb" }));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function cleanValue(value) {
  return String(value || "")
    .replace(/\\u002F/gi, "/")
    .replace(/\\\//g, "/")
    .replace(/&amp;/gi, "&")
    .trim();
}

function looksLikeStreamUrl(value) {
  const candidate = cleanValue(value);
  return /^rtmps?:\/\//i.test(candidate);
}

function looksLikeStreamKey(value) {
  const candidate = cleanValue(value);

  if (!candidate || candidate.length < 6) {
    return false;
  }

  if (looksLikeStreamUrl(candidate)) {
    return false;
  }

  return /^[A-Za-z0-9_\-+=:/.]+$/.test(candidate);
}

function mergeLiveConfig(target, incoming) {
  let changed = false;

  if (!incoming) {
    return changed;
  }

  if (!target.streamUrl && incoming.streamUrl) {
    target.streamUrl = cleanValue(incoming.streamUrl);
    changed = true;
  }

  if (!target.streamKey && incoming.streamKey) {
    target.streamKey = cleanValue(incoming.streamKey);
    changed = true;
  }

  if (!target.source && incoming.source) {
    target.source = incoming.source;
    changed = true;
  }

  if ((incoming.streamUrl || incoming.streamKey) && !target.capturedAt) {
    target.capturedAt = new Date().toISOString();
    changed = true;
  }

  return changed;
}

function extractFromText(rawText) {
  const text = cleanValue(rawText);
  const result = {};

  if (!text) {
    return result;
  }

  const urlMatch = text.match(/rtmps?:\/\/[^\s"'\\]+/i);
  if (urlMatch) {
    result.streamUrl = cleanValue(urlMatch[0]);
  }

  const keyPatterns = [
    /"stream_key"\s*:\s*"([^"]+)"/i,
    /"streamKey"\s*:\s*"([^"]+)"/i,
    /"push_stream_key"\s*:\s*"([^"]+)"/i,
    /stream key\s*[:\n]\s*([A-Za-z0-9_\-+=:/.]+)/i,
    /clave\s+de\s+stream\s*[:\n]\s*([A-Za-z0-9_\-+=:/.]+)/i
  ];

  for (const pattern of keyPatterns) {
    const match = text.match(pattern);
    if (match && looksLikeStreamKey(match[1])) {
      result.streamKey = cleanValue(match[1]);
      break;
    }
  }

  return result;
}

function walkObjectForStreamData(node, accumulator = {}) {
  if (!node) {
    return accumulator;
  }

  if (typeof node === "string") {
    mergeLiveConfig(accumulator, extractFromText(node));
    return accumulator;
  }

  if (Array.isArray(node)) {
    for (const item of node) {
      walkObjectForStreamData(item, accumulator);
    }
    return accumulator;
  }

  if (typeof node !== "object") {
    return accumulator;
  }

  for (const [key, value] of Object.entries(node)) {
    const normalizedKey = normalizeText(key);

    if (typeof value === "string") {
      const candidate = cleanValue(value);

      if (
        !accumulator.streamUrl &&
        (normalizedKey.includes("stream url") ||
          normalizedKey.includes("stream_url") ||
          normalizedKey.includes("server url") ||
          normalizedKey.includes("push url") ||
          normalizedKey.includes("rtmp") ||
          looksLikeStreamUrl(candidate))
      ) {
        accumulator.streamUrl = candidate;
      }

      if (
        !accumulator.streamKey &&
        (normalizedKey.includes("stream key") ||
          normalizedKey.includes("stream_key") ||
          normalizedKey.includes("streamkey") ||
          normalizedKey.includes("push stream key")) &&
        looksLikeStreamKey(candidate)
      ) {
        accumulator.streamKey = candidate;
      }

      mergeLiveConfig(accumulator, extractFromText(candidate));
      continue;
    }

    walkObjectForStreamData(value, accumulator);
  }

  return accumulator;
}

function tryParseJson(text) {
  try {
    return JSON.parse(text);
  } catch (error) {
    return null;
  }
}

function extractStreamData(payload, sourceUrl) {
  const result = {
    streamUrl: null,
    streamKey: null,
    source: sourceUrl || null
  };

  mergeLiveConfig(result, extractFromText(payload));

  const parsed = tryParseJson(payload);
  if (parsed) {
    mergeLiveConfig(result, walkObjectForStreamData(parsed, {}));
  }

  return result;
}

function resetLiveConfig() {
  state.liveStarted = false;
  state.liveConfig = {
    streamUrl: null,
    streamKey: null,
    source: null,
    capturedAt: null
  };
}

function resetLoginPreview() {
  state.loginPreview = null;
  state.loginPreviewAt = null;
}

function formatBrowserLaunchError(error) {
  const message = String(error && error.message ? error.message : error || "");

  if (/Could not find Chrome|Could not find Chromium|Browser was not found/i.test(message)) {
    const wrappedError = new Error(
      "Puppeteer no encontro Chrome. En Render debes redeployar con el navegador instalado dentro del proyecto. " +
        "Solucion: agrega el script postinstall de Puppeteer, usa una cache local como .cache/puppeteer y vuelve a desplegar."
    );
    wrappedError.statusCode = 500;
    return wrappedError;
  }

  return error;
}

async function ensureBrowser() {
  if (state.browser) {
    return state.browser;
  }

  if (state.launchPromise) {
    return state.launchPromise;
  }

  state.launchPromise = puppeteer
    .launch({
      headless: process.env.PUPPETEER_HEADLESS === "false" ? false : "new",
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      ignoreDefaultArgs: ["--enable-automation"],
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "--lang=en-US,en;q=0.9"
      ]
    })
    .catch((error) => {
      throw formatBrowserLaunchError(error);
    })
    .then(async (browser) => {
      state.browser = browser;

      browser.on("disconnected", () => {
        state.browser = null;
        state.page = null;
        state.launchPromise = null;
      });

      const pages = await browser.pages();
      state.page = pages[0] || (await browser.newPage());
      await preparePage(state.page);

      return browser;
    })
    .finally(() => {
      state.launchPromise = null;
    });

  return state.launchPromise;
}

async function getPage() {
  await ensureBrowser();

  if (state.page && !state.page.isClosed()) {
    await preparePage(state.page);
    return state.page;
  }

  state.page = await state.browser.newPage();
  await preparePage(state.page);

  return state.page;
}

async function preparePage(page) {
  if (page.__tiktokPrepared) {
    return page;
  }

  await page.setViewport({ width: 1366, height: 900, deviceScaleFactor: 1 });
  await page.setUserAgent(DESKTOP_USER_AGENT);
  await page.setExtraHTTPHeaders({
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8"
  });
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", {
      get: () => false
    });
    Object.defineProperty(navigator, "platform", {
      get: () => "Win32"
    });
    Object.defineProperty(navigator, "language", {
      get: () => "en-US"
    });
    Object.defineProperty(navigator, "languages", {
      get: () => ["en-US", "en", "es"]
    });
    Object.defineProperty(navigator, "plugins", {
      get: () => [1, 2, 3, 4, 5]
    });

    window.chrome = window.chrome || { runtime: {} };

    if (navigator.permissions && navigator.permissions.query) {
      const originalQuery = navigator.permissions.query.bind(navigator.permissions);
      navigator.permissions.query = (parameters) =>
        parameters && parameters.name === "notifications"
          ? Promise.resolve({ state: Notification.permission })
          : originalQuery(parameters);
    }
  }).catch(() => null);
  await page.emulateTimezone(process.env.TZ || "America/Mexico_City").catch(() => null);
  page.setDefaultNavigationTimeout(DEFAULT_TIMEOUT_MS);
  page.setDefaultTimeout(DEFAULT_TIMEOUT_MS);
  page.__tiktokPrepared = true;

  return page;
}

async function getSessionStatus(page) {
  const cookies = await page.cookies();
  const cookieNames = new Set(cookies.map((cookie) => cookie.name));
  const currentUrl = page.url();
  const authCookieNames = [...cookieNames].filter((name) => AUTH_COOKIE_NAMES.has(name));

  return {
    currentUrl,
    loggedIn:
      authCookieNames.length > 0 ||
      (/tiktok\.com/i.test(currentUrl) && !/\/login(\/|$)/i.test(currentUrl) && cookieNames.size > 3),
    cookiesFound: [...cookieNames].filter((name) =>
      /(session|sid|uid)/i.test(name)
    ),
    authCookiesFound: authCookieNames
  };
}

async function updateLoginPreview(page) {
  const screenshotBase64 = await page.screenshot({
    type: "png",
    encoding: "base64"
  });

  state.loginPreview = `data:image/png;base64,${screenshotBase64}`;
  state.loginPreviewAt = new Date().toISOString();

  return {
    image: state.loginPreview,
    capturedAt: state.loginPreviewAt
  };
}

function getViewport(page) {
  const viewport = page.viewport() || {};
  return {
    width: Number(viewport.width) || 1366,
    height: Number(viewport.height) || 900
  };
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

async function captureQrImage(page) {
  const currentUrl = page.url();
  if (!/\/login\/qrcode/i.test(currentUrl)) {
    return null;
  }

  const handle = await page.evaluateHandle(() => {
    const isVisible = (element) => {
      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return (
        style &&
        style.visibility !== "hidden" &&
        style.display !== "none" &&
        rect.width > 80 &&
        rect.height > 80
      );
    };

    const candidates = Array.from(document.querySelectorAll("canvas, img, svg"));
    return (
      candidates.find((element) => {
        if (!isVisible(element)) {
          return false;
        }

        const rect = element.getBoundingClientRect();
        const ratio = rect.width / rect.height;
        const centered =
          rect.left > 0 &&
          rect.top > 0 &&
          rect.left + rect.width < window.innerWidth &&
          rect.top + rect.height < window.innerHeight;

        return ratio > 0.8 && ratio < 1.2 && rect.width >= 120 && centered;
      }) || null
    );
  });

  const element = handle.asElement();
  if (!element) {
    await handle.dispose();
    return null;
  }

  const qrBase64 = await element.screenshot({
    type: "png",
    encoding: "base64"
  });
  await handle.dispose();

  return `data:image/png;base64,${qrBase64}`;
}

async function captureRemoteFrame(page) {
  const screenshotBase64 = await page.screenshot({
    type: "png",
    encoding: "base64"
  });
  const session = await getSessionStatus(page);
  const viewport = getViewport(page);
  const qrImage = await captureQrImage(page).catch(() => null);
  const payload = {
    image: `data:image/png;base64,${screenshotBase64}`,
    qrImage,
    capturedAt: new Date().toISOString(),
    currentUrl: session.currentUrl,
    loggedIn: session.loggedIn,
    cookiesFound: session.cookiesFound,
    authCookiesFound: session.authCookiesFound,
    viewportWidth: viewport.width,
    viewportHeight: viewport.height,
    liveStarted: state.liveStarted
  };

  state.loginPreview = payload.image;
  state.loginPreviewAt = payload.capturedAt;
  state.remoteSnapshot = payload;

  return payload;
}

async function respondWithRemoteFrame(res, message, page) {
  const frame = await captureRemoteFrame(page);
  res.json({
    ok: true,
    message,
    ...frame
  });
}

function parseNormalizedCoordinate(rawValue, axisName) {
  const value = Number(rawValue);

  if (!Number.isFinite(value)) {
    const error = new Error(`La coordenada ${axisName} no es valida.`);
    error.statusCode = 400;
    throw error;
  }

  return clamp(value, 0, 1);
}

async function clearTikTokSession(page) {
  const cookies = await page.cookies(
    "https://www.tiktok.com",
    "https://www.tiktok.com/login",
    "https://www.tiktok.com/foryou",
    "https://www.tiktok.com/live/creator?lang=en"
  );

  if (cookies.length) {
    await page.deleteCookie(...cookies);
  }

  await page.goto("https://www.tiktok.com/logout", {
    waitUntil: "domcontentloaded"
  }).catch(() => null);

  await page.goto(TIKTOK_LOGIN_URL, {
    waitUntil: "domcontentloaded"
  }).catch(() => null);

  await page.evaluate(() => {
    try {
      localStorage.clear();
      sessionStorage.clear();
    } catch (error) {
      return null;
    }
    return null;
  }).catch(() => null);
}

async function openQrLoginMode(page) {
  await page.goto(TIKTOK_LOGIN_QR_URL, { waitUntil: "domcontentloaded" }).catch(() => null);
  await sleep(2000);
  await dismissCommonPopups(page);

  if (/\/login\/qrcode/i.test(page.url())) {
    return true;
  }

  const labels = [
    "Use QR code",
    "Usar codigo QR",
    "Usar código QR",
    "QR code"
  ];

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const clicked = await clickByVisibleText(page, labels);
    if (clicked) {
      await sleep(1400);
      return true;
    }

    await sleep(800);
  }

  return false;
}

async function hasVisibleInputs(page) {
  return page.evaluate(() => {
    const isVisible = (element) => {
      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return (
        style &&
        style.visibility !== "hidden" &&
        style.display !== "none" &&
        rect.width > 0 &&
        rect.height > 0
      );
    };

    return Array.from(document.querySelectorAll("input")).some((element) =>
      isVisible(element)
    );
  });
}

async function openCredentialLoginMode(page) {
  await page.goto(TIKTOK_LOGIN_URL, { waitUntil: "domcontentloaded" }).catch(() => null);
  await sleep(2000);
  await dismissCommonPopups(page);

  if (await hasVisibleInputs(page).catch(() => false)) {
    return true;
  }

  const labels = [
    "Use phone / email / username",
    "Use phone / email / username",
    "Usar telefono / correo / usuario",
    "Usar telefono, correo o usuario",
    "Usar correo o usuario",
    "Use phone or email",
    "Use email / username",
    "Use email or username"
  ];

  for (let attempt = 0; attempt < 4; attempt += 1) {
    const clicked = await clickByVisibleText(page, labels);
    await sleep(clicked ? 1800 : 900);

    if (await hasVisibleInputs(page).catch(() => false)) {
      return true;
    }
  }

  return false;
}

async function clickByVisibleText(page, labels) {
  const handle = await page.evaluateHandle((visibleLabels) => {
    const normalize = (value) =>
      String(value || "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();

    const isVisible = (element) => {
      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return (
        style &&
        style.visibility !== "hidden" &&
        style.display !== "none" &&
        rect.width > 0 &&
        rect.height > 0
      );
    };

    const candidates = Array.from(
      document.querySelectorAll("button, a, [role='button'], div, span")
    );

    return (
      candidates.find((element) => {
        const text = normalize(element.innerText || element.textContent);
        return (
          text &&
          visibleLabels.some((label) => text.includes(normalize(label))) &&
          isVisible(element)
        );
      }) || null
    );
  }, labels);

  const element = handle.asElement();
  if (!element) {
    await handle.dispose();
    return false;
  }

  try {
    await element.click({ delay: 50 });
  } catch (error) {
    await page.evaluate((node) => node.click(), element);
  }

  await handle.dispose();
  return true;
}

async function dismissCommonPopups(page) {
  const labels = ["Not now", "Ahora no", "Later", "Skip"];
  for (const label of labels) {
    await clickByVisibleText(page, [label]);
  }
}

async function extractFromDom(page) {
  const payload = await page.evaluate(() => {
    const inputs = Array.from(document.querySelectorAll("input, textarea")).map(
      (element) => ({
        value: element.value || "",
        name: element.name || "",
        placeholder: element.placeholder || "",
        ariaLabel: element.getAttribute("aria-label") || ""
      })
    );

    return {
      text: document.body ? document.body.innerText : "",
      inputs
    };
  });

  const result = extractFromText(payload.text);

  for (const input of payload.inputs) {
    const label = normalizeText(
      [input.name, input.placeholder, input.ariaLabel].join(" ")
    );

    if (!result.streamUrl && looksLikeStreamUrl(input.value)) {
      result.streamUrl = cleanValue(input.value);
    }

    if (
      !result.streamKey &&
      (label.includes("stream key") ||
        label.includes("stream_key") ||
        label.includes("key")) &&
      looksLikeStreamKey(input.value)
    ) {
      result.streamKey = cleanValue(input.value);
    }

    mergeLiveConfig(result, extractFromText(input.value));
  }

  return result;
}

function createResponseCollector(page) {
  const collected = {
    streamUrl: null,
    streamKey: null,
    source: null,
    capturedAt: null
  };

  const responseHandler = async (response) => {
    try {
      const url = response.url();
      const contentType = String(
        response.headers()["content-type"] || ""
      ).toLowerCase();
      const interestingUrl = /(stream|rtmp|webcast|live)/i.test(url);
      const readablePayload =
        interestingUrl || contentType.includes("json") || contentType.includes("text");

      if (!readablePayload) {
        return;
      }

      const text = await response.text().catch(() => "");
      if (!text) {
        return;
      }

      if (!interestingUrl && !/(stream_key|streamurl|stream_url|rtmp|webcast|live)/i.test(text)) {
        return;
      }

      const extracted = extractStreamData(text, url);
      mergeLiveConfig(collected, extracted);
    } catch (error) {
      console.warn("No se pudo procesar una respuesta:", error.message);
    }
  };

  page.on("response", responseHandler);

  return {
    async waitForConfig(timeoutMs = DEFAULT_TIMEOUT_MS) {
      const startedAt = Date.now();

      while (Date.now() - startedAt < timeoutMs) {
        const domData = await extractFromDom(page).catch(() => null);
        mergeLiveConfig(collected, domData);

        if (collected.streamUrl && collected.streamKey) {
          return { ...collected };
        }

        await sleep(1000);
      }

      return collected.streamUrl || collected.streamKey ? { ...collected } : null;
    },
    stop() {
      page.off("response", responseHandler);
    }
  };
}

async function withBusyBrowser(task) {
  if (state.busy) {
    const error = new Error("El navegador Puppeteer ya está ocupado con otra operación.");
    error.statusCode = 409;
    throw error;
  }

  state.busy = true;
  try {
    return await task();
  } finally {
    state.busy = false;
  }
}

function sendError(res, error) {
  const statusCode = error.statusCode || 500;
  res.status(statusCode).json({
    ok: false,
    message: error.message || "Ocurrió un error inesperado."
  });
}

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.get("/remote-browser/open-login", async (req, res) => {
  try {
    const page = await withBusyBrowser(async () => {
      const activePage = await getPage();
      await activePage.goto(TIKTOK_LOGIN_URL, { waitUntil: "domcontentloaded" });
      await sleep(2500);
      await dismissCommonPopups(activePage);
      await openCredentialLoginMode(activePage);
      return activePage;
    });

    await respondWithRemoteFrame(res, "Navegador remoto TikTok abierto.", page);
  } catch (error) {
    sendError(res, error);
  }
});

app.get("/remote-browser/show-qr", async (req, res) => {
  try {
    const page = await withBusyBrowser(async () => {
      const activePage = await getPage();
      await openQrLoginMode(activePage);
      return activePage;
    });

    await respondWithRemoteFrame(res, "QR de TikTok abierto.", page);
  } catch (error) {
    sendError(res, error);
  }
});

app.get("/remote-browser/show-login-form", async (req, res) => {
  try {
    const page = await withBusyBrowser(async () => {
      const activePage = await getPage();
      await openCredentialLoginMode(activePage);
      return activePage;
    });

    await respondWithRemoteFrame(res, "Formulario de acceso TikTok abierto.", page);
  } catch (error) {
    sendError(res, error);
  }
});

app.get("/remote-browser/frame", async (req, res) => {
  try {
    if (state.busy && state.remoteSnapshot) {
      return res.json({
        ok: true,
        message: "Frame remoto en cache mientras el navegador esta ocupado.",
        cached: true,
        ...state.remoteSnapshot
      });
    }

    const page = await withBusyBrowser(async () => getPage());
    await respondWithRemoteFrame(res, "Frame remoto actualizado.", page);
  } catch (error) {
    if (error.statusCode === 409 && state.remoteSnapshot) {
      return res.json({
        ok: true,
        message: "Frame remoto en cache mientras el navegador esta ocupado.",
        cached: true,
        ...state.remoteSnapshot
      });
    }
    sendError(res, error);
  }
});

app.post("/remote-browser/navigation", async (req, res) => {
  try {
    const page = await withBusyBrowser(async () => {
      const activePage = await getPage();
      const action = String(
        (req.body && req.body.action) || req.query.action || ""
      )
        .trim()
        .toLowerCase();

      if (action === "login" || action === "open-login") {
        await activePage.goto(TIKTOK_LOGIN_URL, { waitUntil: "domcontentloaded" });
      } else if (action === "home") {
        await activePage.goto(TIKTOK_HOME_URL, { waitUntil: "domcontentloaded" });
      } else if (action === "live-creator") {
        await activePage.goto(TIKTOK_LIVE_CREATOR_URL, { waitUntil: "domcontentloaded" });
      } else if (action === "show-qr" || action === "qr" || action === "qrcode") {
        await openQrLoginMode(activePage);
      } else if (
        action === "show-login-form" ||
        action === "show-form" ||
        action === "credentials" ||
        action === "email-login"
      ) {
        await openCredentialLoginMode(activePage);
      } else if (action === "reload") {
        await activePage.reload({ waitUntil: "domcontentloaded" }).catch(() => null);
      } else if (action === "back") {
        await activePage.goBack({ waitUntil: "domcontentloaded" }).catch(() => null);
      } else {
        const error = new Error("La accion de navegacion no es valida.");
        error.statusCode = 400;
        throw error;
      }

      await sleep(1500);
      await dismissCommonPopups(activePage);
      return activePage;
    });

    await respondWithRemoteFrame(res, "Navegacion remota completada.", page);
  } catch (error) {
    sendError(res, error);
  }
});

app.post("/remote-browser/click", async (req, res) => {
  try {
    const page = await withBusyBrowser(async () => {
      const activePage = await getPage();
      const viewport = getViewport(activePage);
      const normalizedX = parseNormalizedCoordinate(req.body && req.body.x, "x");
      const normalizedY = parseNormalizedCoordinate(req.body && req.body.y, "y");
      const clickX = Math.round(normalizedX * viewport.width);
      const clickY = Math.round(normalizedY * viewport.height);

      await activePage.mouse.click(clickX, clickY, { delay: 40 });
      await sleep(700);
      return activePage;
    });

    await respondWithRemoteFrame(res, "Click remoto enviado.", page);
  } catch (error) {
    sendError(res, error);
  }
});

app.post("/remote-browser/type", async (req, res) => {
  try {
    const page = await withBusyBrowser(async () => {
      const activePage = await getPage();
      const text = String(req.body && req.body.text ? req.body.text : "");
      const clearFirst = Boolean(req.body && req.body.clearFirst);
      const pressEnter = Boolean(req.body && req.body.pressEnter);

      if (!text && !pressEnter && !clearFirst) {
        const error = new Error("No recibi texto ni accion para escribir.");
        error.statusCode = 400;
        throw error;
      }

      if (clearFirst) {
        await activePage.keyboard.down("Control");
        await activePage.keyboard.press("KeyA");
        await activePage.keyboard.up("Control");
        await activePage.keyboard.press("Backspace");
      }

      if (text) {
        await activePage.keyboard.type(text, { delay: 25 });
      }

      if (pressEnter) {
        await activePage.keyboard.press("Enter");
      }

      await sleep(700);
      return activePage;
    });

    await respondWithRemoteFrame(res, "Texto remoto enviado.", page);
  } catch (error) {
    sendError(res, error);
  }
});

app.post("/remote-browser/key", async (req, res) => {
  try {
    const page = await withBusyBrowser(async () => {
      const activePage = await getPage();
      const key = String(req.body && req.body.key ? req.body.key : "").trim();
      const allowedKeys = new Set([
        "Enter",
        "Tab",
        "Backspace",
        "Escape",
        "ArrowUp",
        "ArrowDown",
        "ArrowLeft",
        "ArrowRight",
        "Space"
      ]);

      if (!allowedKeys.has(key)) {
        const error = new Error("La tecla remota no esta permitida.");
        error.statusCode = 400;
        throw error;
      }

      await activePage.keyboard.press(key);
      await sleep(500);
      return activePage;
    });

    await respondWithRemoteFrame(res, "Tecla remota enviada.", page);
  } catch (error) {
    sendError(res, error);
  }
});

app.post("/remote-browser/scroll", async (req, res) => {
  try {
    const page = await withBusyBrowser(async () => {
      const activePage = await getPage();
      const deltaY = clamp(Number(req.body && req.body.deltaY ? req.body.deltaY : 0), -1600, 1600);

      if (!Number.isFinite(deltaY) || deltaY === 0) {
        const error = new Error("El scroll remoto no es valido.");
        error.statusCode = 400;
        throw error;
      }

      await activePage.mouse.wheel({ deltaY });
      await sleep(700);
      return activePage;
    });

    await respondWithRemoteFrame(res, "Scroll remoto enviado.", page);
  } catch (error) {
    sendError(res, error);
  }
});

app.get("/login", async (req, res) => {
  try {
    const payload = await withBusyBrowser(async () => {
      const activePage = await getPage();
      await activePage.goto(TIKTOK_LOGIN_URL, { waitUntil: "domcontentloaded" });
      await sleep(2500);
      await dismissCommonPopups(activePage);
      const preview = await updateLoginPreview(activePage);
      const session = await getSessionStatus(activePage);

      return {
        page: activePage,
        preview,
        session
      };
    });

    res.json({
      ok: true,
      message:
        "TikTok Login abierto en la sesion Puppeteer global. Si aparece un QR en la vista previa, escanealo con la app de TikTok.",
      headless: true,
      warning:
        "En Render Free no ves el navegador real de Puppeteer. Por eso te devolvemos una vista previa del login para intentar entrar por QR.",
      currentUrl: payload.session.currentUrl,
      loggedIn: payload.session.loggedIn,
      loginPreview: payload.preview.image,
      loginPreviewAt: payload.preview.capturedAt
    });
  } catch (error) {
    sendError(res, error);
  }
});

app.get("/logout", async (req, res) => {
  try {
    const payload = await withBusyBrowser(async () => {
      const page = await getPage();

      await clearTikTokSession(page);
      resetLiveConfig();
      resetLoginPreview();

      const preview = await updateLoginPreview(page);
      const session = await getSessionStatus(page);

      return {
        preview,
        session
      };
    });

    res.json({
      ok: true,
      message: "Sesion TikTok cerrada y datos LIVE limpiados.",
      loggedIn: payload.session.loggedIn,
      currentUrl: payload.session.currentUrl,
      loginPreview: payload.preview.image,
      loginPreviewAt: payload.preview.capturedAt
    });
  } catch (error) {
    sendError(res, error);
  }
});

app.get("/login-status", async (req, res) => {
  try {
    const payload = await withBusyBrowser(async () => {
      const page = await getPage();
      const session = await getSessionStatus(page);

      let preview = null;
      if (!session.loggedIn) {
        preview = await updateLoginPreview(page);
      }

      return {
        session,
        preview
      };
    });

    res.json({
      ok: true,
      loggedIn: payload.session.loggedIn,
      currentUrl: payload.session.currentUrl,
      cookiesFound: payload.session.cookiesFound,
      loginPreview: payload.preview ? payload.preview.image : state.loginPreview,
      loginPreviewAt: payload.preview ? payload.preview.capturedAt : state.loginPreviewAt
    });
  } catch (error) {
    sendError(res, error);
  }
});

app.get("/generate", async (req, res) => {
  let collector = null;

  try {
    const payload = await withBusyBrowser(async () => {
      const page = await getPage();
      const session = await getSessionStatus(page);

      if (!session.loggedIn) {
        const error = new Error(
          "No hay una sesión TikTok activa en Puppeteer. Usa /login y completa el acceso antes de generar la key."
        );
        error.statusCode = 400;
        throw error;
      }

      collector = createResponseCollector(page);

      await page.goto(TIKTOK_LIVE_CREATOR_URL, {
        waitUntil: "domcontentloaded"
      });

      await sleep(5000);
      await dismissCommonPopups(page);

      await clickByVisibleText(page, [
        "Transmitir desde software",
        "Transmitir con software",
        "Stream with software",
        "Use streaming software",
        "Streaming software"
      ]);

      await sleep(3000);

      const captured = await collector.waitForConfig();
      collector.stop();
      collector = null;

      if (!captured || !captured.streamUrl || !captured.streamKey) {
        const fallback = await extractFromDom(page);
        const finalData = {
          streamUrl: captured && captured.streamUrl ? captured.streamUrl : fallback.streamUrl,
          streamKey: captured && captured.streamKey ? captured.streamKey : fallback.streamKey,
          source:
            (captured && captured.source) || "dom-extraction",
          capturedAt:
            (captured && captured.capturedAt) || new Date().toISOString()
        };

        if (!finalData.streamUrl || !finalData.streamKey) {
          const error = new Error(
            "No se pudo detectar la stream key todavía. TikTok puede haber cambiado el flujo, o la sesión aún no mostró la configuración RTMP."
          );
          error.statusCode = 422;
          throw error;
        }

        return finalData;
      }

      return captured;
    });

    state.liveConfig = {
      streamUrl: payload.streamUrl,
      streamKey: payload.streamKey,
      source: payload.source,
      capturedAt: payload.capturedAt || new Date().toISOString()
    };
    state.liveStarted = false;

    res.json({
      ok: true,
      message: "Stream key detectada correctamente.",
      server: state.liveConfig.streamUrl,
      key: state.liveConfig.streamKey,
      source: state.liveConfig.source,
      capturedAt: state.liveConfig.capturedAt,
      liveStarted: state.liveStarted
    });
  } catch (error) {
    if (collector) {
      collector.stop();
    }
    sendError(res, error);
  }
});

app.get("/start-live", async (req, res) => {
  try {
    const result = await withBusyBrowser(async () => {
      const page = await getPage();

      if (!state.liveConfig.streamUrl || !state.liveConfig.streamKey) {
        const error = new Error(
          "Primero debes generar la stream key antes de intentar iniciar el LIVE."
        );
        error.statusCode = 400;
        throw error;
      }

      const clicked = await clickByVisibleText(page, [
        "Go LIVE",
        "Go Live",
        "Iniciar LIVE",
        "Iniciar live",
        "Ir en vivo"
      ]);

      if (!clicked) {
        const error = new Error(
          "No encontré el botón Go LIVE. Asegúrate de que OBS ya esté enviando señal y que la página de TikTok siga abierta."
        );
        error.statusCode = 422;
        throw error;
      }

      await sleep(3000);
      state.liveStarted = true;

      return {
        currentUrl: page.url()
      };
    });

    res.json({
      ok: true,
      message: "Botón Go LIVE presionado automáticamente.",
      currentUrl: result.currentUrl,
      liveStarted: true
    });
  } catch (error) {
    sendError(res, error);
  }
});

app.get("/stop-live", async (req, res) => {
  try {
    const result = await withBusyBrowser(async () => {
      const page = await getPage();
      const currentUrl = page.url();
      const shouldNavigate =
        !/tiktok\.com\/live\/creator/i.test(currentUrl) &&
        !/tiktok\.com\/live/i.test(currentUrl);

      if (shouldNavigate) {
        await page.goto(TIKTOK_LIVE_CREATOR_URL, {
          waitUntil: "domcontentloaded"
        });
        await sleep(4000);
      }

      await dismissCommonPopups(page);

      const clicked = await clickByVisibleText(page, [
        "End LIVE",
        "End Live",
        "Stop LIVE",
        "Stop Live",
        "Detener LIVE",
        "Finalizar LIVE",
        "Finalizar live",
        "End stream",
        "Stop stream"
      ]);

      if (!clicked) {
        const error = new Error(
          "No encontre un boton para detener el LIVE. Verifica que la transmision siga activa en TikTok."
        );
        error.statusCode = 422;
        throw error;
      }

      await sleep(1500);

      await clickByVisibleText(page, [
        "End now",
        "Confirm",
        "Detener",
        "Finalizar",
        "Stop",
        "End LIVE"
      ]);

      await sleep(2000);
      state.liveStarted = false;

      return {
        currentUrl: page.url()
      };
    });

    res.json({
      ok: true,
      message: "Intento de detener el LIVE ejecutado.",
      currentUrl: result.currentUrl,
      liveStarted: false
    });
  } catch (error) {
    sendError(res, error);
  }
});

app.listen(PORT, HOST, () => {
  console.log(`TikTok stream key app listening on http://${HOST}:${PORT}`);
});

async function shutdown() {
  if (state.browser) {
    await state.browser.close().catch(() => null);
  }
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
