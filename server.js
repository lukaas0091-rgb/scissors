const express = require("express");
const path = require("path");
const puppeteer = require("puppeteer");

const app = express();
const PORT = process.env.PORT || 3000;
const HOST = "0.0.0.0";

const TIKTOK_LOGIN_URL = "https://www.tiktok.com/login";
const TIKTOK_LIVE_CREATOR_URL = "https://www.tiktok.com/live/creator?lang=en";
const DEFAULT_TIMEOUT_MS = 45000;

const state = {
  browser: null,
  page: null,
  launchPromise: null,
  busy: false,
  loginPreview: null,
  loginPreviewAt: null,
  liveConfig: {
    streamUrl: null,
    streamKey: null,
    source: null,
    capturedAt: null
  }
};

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
      headless: true,
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage"
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
      await state.page.setViewport({ width: 1366, height: 900 });
      state.page.setDefaultNavigationTimeout(DEFAULT_TIMEOUT_MS);
      state.page.setDefaultTimeout(DEFAULT_TIMEOUT_MS);

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
    return state.page;
  }

  state.page = await state.browser.newPage();
  await state.page.setViewport({ width: 1366, height: 900 });
  state.page.setDefaultNavigationTimeout(DEFAULT_TIMEOUT_MS);
  state.page.setDefaultTimeout(DEFAULT_TIMEOUT_MS);

  return state.page;
}

async function getSessionStatus(page) {
  const cookies = await page.cookies();
  const cookieNames = new Set(cookies.map((cookie) => cookie.name));

  return {
    currentUrl: page.url(),
    loggedIn:
      cookieNames.has("sessionid") ||
      cookieNames.has("sid_tt") ||
      cookieNames.has("uid_tt"),
    cookiesFound: [...cookieNames].filter((name) =>
      /(session|sid|uid)/i.test(name)
    )
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

    res.json({
      ok: true,
      message: "Stream key detectada correctamente.",
      server: state.liveConfig.streamUrl,
      key: state.liveConfig.streamKey,
      source: state.liveConfig.source,
      capturedAt: state.liveConfig.capturedAt
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

      return {
        currentUrl: page.url()
      };
    });

    res.json({
      ok: true,
      message: "Botón Go LIVE presionado automáticamente.",
      currentUrl: result.currentUrl
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
