/** 纯 Node http 实现的 JSON API；不引入第三方框架。 */
import * as http from "node:http";
import { AddressInfo } from "node:net";
import { URL } from "node:url";
import { ValidationError } from "./core";
import { RaftService } from "./service";

interface ErrorBody {
  error: { code: string; message: string };
}

export function createServer(service: RaftService): http.Server {
  const server = http.createServer(async (req, res) => {
    try {
      await handle(service, req, res);
    } catch (err) {
      sendError(res, errorStatus(err), errorBody(err));
    }
  });
  return server;
}

async function handle(
  service: RaftService,
  req: http.IncomingMessage,
  res: http.ServerResponse,
): Promise<void> {
  const url = new URL(req.url ?? "/", "http://localhost");
  const pathParts = url.pathname.split("/").filter(Boolean); // /followers/:id/...
  const method = req.method ?? "GET";

  if (method === "GET" && pathParts[0] === "health") {
    sendJson(res, 200, { status: "ok" });
    return;
  }

  if (method === "POST" && pathParts[0] === "followers" && pathParts.length === 1) {
    const body = await readJson(req);
    const name = String(body?.name ?? "");
    const entries = Array.isArray(body?.entries) ? body.entries : [];
    const id = service.create(name, entries);
    sendJson(res, 201, { id, state: service.state(id) });
    return;
  }

  if (method === "GET" && pathParts[0] === "followers" && pathParts.length === 1) {
    sendJson(res, 200, { followers: service.list() });
    return;
  }

  // /followers/:id/...
  const id = Number(pathParts[1]);
  if (pathParts[0] === "followers" && Number.isInteger(id) && id > 0) {
    if (method === "GET" && pathParts.length === 2) {
      sendJson(res, 200, { state: service.state(id) });
      return;
    }
    if (method === "GET" && pathParts[2] === "records") {
      sendJson(res, 200, { records: service.records(id) });
      return;
    }
    if (method === "GET" && pathParts[2] === "export") {
      sendJson(res, 200, service.export(id));
      return;
    }
    if (method === "POST" && pathParts[2] === "append" && pathParts.length === 3) {
      const body = await readJson(req);
      sendJson(res, 200, { result: service.append(id, body) });
      return;
    }
    if (method === "POST" && pathParts[2] === "batch" && pathParts.length === 3) {
      const body = await readJson(req);
      if (!Array.isArray(body)) {
        throw new ValidationError("batch 请求体必须是 AppendEntries 对象数组");
      }
      sendJson(res, 200, service.appendBatch(id, body));
      return;
    }
  }

  sendError(res, 404, {
    error: { code: "NOT_FOUND", message: `未知路由 ${method} ${url.pathname}` },
  });
}

async function readJson(req: http.IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const raw = Buffer.concat(chunks).toString("utf-8");
  if (raw.trim() === "") return {};
  try {
    return JSON.parse(raw);
  } catch {
    throw new ValidationError("请求体不是合法 JSON");
  }
}

function sendJson(res: http.ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body, null, 2);
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(payload);
}

function sendError(res: http.ServerResponse, status: number, body: ErrorBody): void {
  sendJson(res, status, body);
}

function errorBody(err: unknown): ErrorBody {
  const code =
    (err as { code?: string }).code ??
    (err instanceof ValidationError ? "INVALID_ARGUMENT" : "INTERNAL");
  return { error: { code, message: (err as Error).message } };
}

function errorStatus(err: unknown): number {
  const code = (err as { code?: string }).code ?? "";
  if (err instanceof ValidationError || code === "INVALID_ARGUMENT") return 400;
  if (code === "NOT_FOUND") return 404;
  if (code === "NAME_CONFLICT") return 409;
  return 500;
}

/** 选择空闲端口：port=0 时由操作系统分配。 */
export function listen(server: http.Server, port: number): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => {
      const addr = server.address() as AddressInfo;
      resolve(addr.port);
    });
  });
}
