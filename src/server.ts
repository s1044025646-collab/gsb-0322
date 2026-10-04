import * as http from "node:http";
import { FollowerService, validateInitialLog } from "./follower.js";
import { RaftError } from "./types.js";

function readBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      const text = Buffer.concat(chunks).toString("utf8");
      if (!text) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(text));
      } catch {
        reject(new RaftError("BAD_REQUEST", "请求体不是合法 JSON"));
      }
    });
    req.on("error", reject);
  });
}

export function startServer(service: FollowerService, port: number): Promise<http.Server> {
  const server = http.createServer((req, res) => {
    void handle(req, res).catch((err) => {
      const body =
        err instanceof RaftError
          ? { status: err.code === "NOT_FOUND" ? 404 : 400, payload: { error: { code: err.code, message: err.message } } }
          : { status: 500, payload: { error: { code: "INTERNAL", message: String(err) } } };
      res.writeHead(body.status, { "content-type": "application/json; charset=utf-8" });
      res.end(JSON.stringify(body.payload, null, 2));
    });
  });
  return new Promise((resolve) => {
    server.listen(port, "127.0.0.1", () => resolve(server));
  });

  async function handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const send = (status: number, body: unknown) => {
      res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
      res.end(JSON.stringify(body, null, 2));
    };
    const url = new URL(req.url ?? "/", "http://localhost");
    const parts = url.pathname.split("/").filter(Boolean);
    const method = req.method ?? "GET";

    if (method === "GET" && url.pathname === "/health") return send(200, { ok: true });
    if (method === "GET" && url.pathname === "/experiments")
      return send(200, { experiments: service.listExperiments() });
    if (method === "POST" && url.pathname === "/experiments") {
      const body = (await readBody(req)) as Record<string, unknown>;
      const id = typeof body.id === "string" && body.id ? body.id : `exp-${Date.now()}`;
      const currentTerm = typeof body.currentTerm === "number" ? body.currentTerm : 0;
      const initialLog = body.initialLog === undefined ? [] : validateInitialLog(body.initialLog);
      service.createExperiment(id, currentTerm, initialLog);
      return send(201, service.getState(id));
    }
    if (parts[0] === "experiments" && parts.length >= 2) {
      const id = decodeURIComponent(parts[1]!);
      const sub = parts[2];
      if (method === "GET" && (sub === undefined || sub === "log")) return send(200, service.getState(id));
      if (method === "GET" && sub === "records") return send(200, { records: service.getRecords(id) });
      if (method === "GET" && sub === "export") return send(200, service.exportExperiment(id));
      if (method === "POST" && sub === "append-entries") return send(200, service.apply(id, await readBody(req)));
      if (method === "POST" && sub === "batch") {
        const body = (await readBody(req)) as Record<string, unknown>;
        const list = Array.isArray(body) ? body : (body.requests as unknown[]);
        return send(200, service.applyBatch(id, list));
      }
    }
    return send(404, { error: { code: "NOT_FOUND", message: "未知路由" } });
  }
}
