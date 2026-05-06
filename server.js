const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, "queue.json");
const DEFAULT_DB = { queues: [], counters: { G: 0, T: 0, O: 0, U: 0, K: 0 } };

const services = {
  gadai: { code: "G", label: "Gadai" },
  tebus: { code: "T", label: "Tebus" },
  overlap: { code: "O", label: "Overlap" },
  upah: { code: "U", label: "Bayaran Upah" },
  koperasi: { code: "K", label: "Koperasi" },
};

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
};

function readDb() {
  if (!fs.existsSync(DB_FILE)) {
    writeDb(DEFAULT_DB);
    return { ...DEFAULT_DB, queues: [] };
  }

  try {
    const content = fs.readFileSync(DB_FILE, "utf8").trim();
    const db = content ? JSON.parse(content) : {};
    return {
      queues: Array.isArray(db.queues) ? db.queues : [],
      counters: { ...DEFAULT_DB.counters, ...(db.counters || {}) },
    };
  } catch (error) {
    console.error("queue.json invalid, resetting storage:", error.message);
    writeDb(DEFAULT_DB);
    return { ...DEFAULT_DB, queues: [] };
  }
}

function writeDb(data) {
  fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2));
}

function formatNumber(code, count) {
  return `${code}${String(count).padStart(3, "0")}`;
}

function sendJson(res, statusCode, data) {
  res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(data));
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";

    req.on("data", (chunk) => {
      body += chunk;
    });

    req.on("end", () => {
      if (!body) return resolve({});

      try {
        resolve(JSON.parse(body));
      } catch (error) {
        reject(error);
      }
    });
  });
}

function sendWhatsApp(phone, message) {
  if (!phone) return null;

  const cleanPhone = phone.replace(/[^\d]/g, "");
  if (!cleanPhone) return null;

  const whatsappUrl = `https://wa.me/${cleanPhone}?text=${encodeURIComponent(message)}`;
  console.log(`WhatsApp: ${message} (${whatsappUrl})`);
  return whatsappUrl;
}

function buildWhatsAppMessage(queue) {
  return [
    "Assalamualaikum,",
    "",
    `Giliran anda: ${queue.number}`,
    `Servis: ${queue.service}`,
    `Sila hadir ke Kaunter ${queue.counter} sekarang.`,
    "",
    "Sahabat Ar Rahnu Kuantan",
  ].join("\n");
}

function sortByCreatedAt(a, b) {
  return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
}

function normalizeCounter(counter) {
  const selectedCounter = Number(counter);
  return [1, 2, 3].includes(selectedCounter) ? selectedCounter : null;
}

async function createQueue(req, res) {
  const { service, phone } = await parseBody(req);
  const config = services[service];

  if (!config) {
    return sendJson(res, 400, { error: "Invalid service" });
  }

  const db = readDb();
  db.counters[config.code] += 1;

  const item = {
    id: Date.now().toString(),
    service: config.label,
    serviceKey: service,
    number: formatNumber(config.code, db.counters[config.code]),
    status: "Menunggu",
    counter: null,
    phone: phone || "",
    createdAt: new Date().toISOString(),
  };

  db.queues.push(item);
  writeDb(db);
  sendJson(res, 200, item);
}

function listQueue(res) {
  const queues = readDb().queues.slice().sort(sortByCreatedAt);
  sendJson(res, 200, queues);
}

function callQueue(db, queue, counter) {
  queue.status = "Serving";
  queue.counter = counter;
  queue.calledAt = new Date().toISOString();

  const message = buildWhatsAppMessage(queue);
  queue.notification = sendWhatsApp(queue.phone, message);

  writeDb(db);
  return { queue, message };
}

async function callNext(req, res, service) {
  const { counter } = await parseBody(req);
  const selectedCounter = normalizeCounter(counter);

  if (!selectedCounter) {
    return sendJson(res, 400, { error: "Pilih kaunter 1, 2, atau 3" });
  }

  if (service && !services[service]) {
    return sendJson(res, 400, { error: "Invalid service" });
  }

  const db = readDb();
  const waiting = db.queues
    .filter((item) => {
      const isWaiting = item.status === "Menunggu" || item.status === "Waiting";
      return service ? isWaiting && item.serviceKey === service : isWaiting;
    })
    .sort(sortByCreatedAt);
  const next = waiting[0];

  if (!next) {
    return sendJson(res, 404, { error: "Tiada pelanggan menunggu" });
  }

  sendJson(res, 200, callQueue(db, next, selectedCounter));
}

async function callById(req, res, id) {
  const { counter } = await parseBody(req);
  const selectedCounter = normalizeCounter(counter);

  if (!selectedCounter) {
    return sendJson(res, 400, { error: "Pilih kaunter 1, 2, atau 3" });
  }

  const db = readDb();
  const queue = db.queues.find((item) => item.id === id);

  if (!queue) {
    return sendJson(res, 404, { error: "Queue tidak dijumpai" });
  }

  if (queue.status !== "Menunggu" && queue.status !== "Waiting") {
    return sendJson(res, 400, { error: "Queue ini sudah dipanggil" });
  }

  sendJson(res, 200, callQueue(db, queue, selectedCounter));
}

function resetQueue(res) {
  const emptyDb = { queues: [], counters: { ...DEFAULT_DB.counters } };
  writeDb(emptyDb);
  sendJson(res, 200, { message: "Queue reset berjaya", ...emptyDb });
}

function serveStatic(req, res) {
  const url = new URL(req.url, "http://queue.local");

  if (url.pathname === "/") {
    res.writeHead(302, { Location: "/index.html" });
    return res.end();
  }

  const safePath = decodeURIComponent(url.pathname);
  const filePath = path.join(__dirname, path.normalize(safePath));

  if (!filePath.startsWith(__dirname)) {
    res.writeHead(403);
    return res.end("Forbidden");
  }

  fs.readFile(filePath, (error, content) => {
    if (error) {
      res.writeHead(404);
      return res.end("Not found");
    }

    const contentType = mimeTypes[path.extname(filePath)] || "text/plain; charset=utf-8";
    res.writeHead(200, { "Content-Type": contentType });
    res.end(content);
  });
}

const app = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, "http://queue.local");

    if (req.method === "GET" && url.pathname === "/health") {
      return sendJson(res, 200, { status: "ok" });
    }

    if (req.method === "POST" && url.pathname === "/api/queue") {
      return await createQueue(req, res);
    }

    if (req.method === "GET" && url.pathname === "/api/queue") {
      return listQueue(res);
    }

    if (req.method === "POST" && url.pathname === "/api/next") {
      return await callNext(req, res);
    }

    if (req.method === "POST" && url.pathname.startsWith("/api/next/")) {
      const service = url.pathname.replace("/api/next/", "");
      return await callNext(req, res, service);
    }

    if (req.method === "POST" && url.pathname.startsWith("/api/call/")) {
      const id = url.pathname.replace("/api/call/", "");
      return await callById(req, res, id);
    }

    if (req.method === "POST" && url.pathname === "/api/reset") {
      return resetQueue(res);
    }

    if (req.method === "GET") {
      return serveStatic(req, res);
    }

    sendJson(res, 405, { error: "Method not allowed" });
  } catch (error) {
    sendJson(res, 500, { error: error.message });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log("Server running on port " + PORT);
});
